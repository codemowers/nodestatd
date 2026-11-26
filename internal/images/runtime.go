package images

import (
	"context"
	"fmt"
	"strings"

	"github.com/codemowers/nodestatd/internal/containerd"
	"github.com/codemowers/nodestatd/internal/crio"
	"github.com/codemowers/nodestatd/internal/images/inventory"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// Runtime is the container runtime whose image store the module records.
type Runtime interface {
	// Subscribe signals image and container changes. It sends true when
	// changes may have been missed, which calls for a full reconciliation.
	Subscribe(ctx context.Context) <-chan bool
	// Snapshot returns the image store's contents.
	Snapshot(ctx context.Context) (*Snapshot, error)
}

// Snapshot holds image records in containerd's form, one per reference an
// image is held under, with the content their targets resolve against.
type Snapshot struct {
	Records []images.Image
	Content content.Provider
	// InUse holds the names of records containers reference.
	InUse map[string]bool
}

// Containerd reads images from containerd's image store.
func Containerd(client *containerd.Client) Runtime {
	return containerdRuntime{client}
}

type containerdRuntime struct {
	client *containerd.Client
}

func (r containerdRuntime) Subscribe(ctx context.Context) <-chan bool {
	return r.client.Subscribe(ctx, "^/(images|containers)/")
}

func (r containerdRuntime) Snapshot(ctx context.Context) (*Snapshot, error) {
	records, err := r.client.ImageService().List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list images: %w", err)
	}
	inUse, err := r.client.ContainerImages(ctx)
	if err != nil {
		return nil, err
	}
	return &Snapshot{Records: records, Content: r.client.ContentStore(), InUse: inUse}, nil
}

// CRIO reads images CRI-O reports through the CRI, with content from its
// containers/storage image store.
func CRIO(client *crio.Client) Runtime {
	return crioRuntime{client}
}

type crioRuntime struct {
	client *crio.Client
}

func (r crioRuntime) Subscribe(ctx context.Context) <-chan bool {
	return r.client.Subscribe(ctx)
}

func (r crioRuntime) Snapshot(ctx context.Context) (*Snapshot, error) {
	listed, err := r.client.ListImages(ctx)
	if err != nil {
		return nil, err
	}
	containerImages, err := r.client.ContainerImages(ctx)
	if err != nil {
		return nil, err
	}
	stored, err := r.client.Storage.Images()
	if err != nil {
		return nil, err
	}
	provider := r.client.Storage.Provider(stored)
	records, inUse := crioRecords(listed, stored, provider, containerImages)
	return &Snapshot{Records: records, Content: provider, InUse: inUse}, nil
}

// blobs is the part of crio.Provider records are built against.
type blobs interface {
	Has(digest.Digest) bool
	Size(digest.Digest) int64
}

// crioRecords translates CRI images into containerd-style records: one per
// repo tag targeting the digest the image was pulled by, one per repo digest
// targeting that digest, and one named by the image ID so images without
// references are recorded too. CRI-O lists a repo digest for each manifest
// digest of an image, so references to platform manifests merge into the
// image of their index. Images missing from the store, such as OCI artifacts
// or images removed since listing, are skipped.
func crioRecords(listed []*runtimeapi.Image, stored map[string]*crio.StoredImage, provider blobs, containerImages map[string]bool) ([]images.Image, map[string]bool) {
	var records []images.Image
	inUse := map[string]bool{}
	for _, image := range listed {
		id := strings.TrimPrefix(image.Id, "sha256:")
		storedImage := stored[id]
		if storedImage == nil || !provider.Has(storedImage.Digest) {
			continue
		}
		used := containerImages[image.Id] || containerImages[id] || containerImages["sha256:"+id]
		var labels map[string]string
		if image.Pinned {
			labels = map[string]string{inventory.PinnedLabel: "pinned"}
		}
		add := func(name string, target digest.Digest) {
			records = append(records, images.Image{
				Name:      name,
				Labels:    labels,
				Target:    ocispec.Descriptor{Digest: target, Size: provider.Size(target)},
				CreatedAt: storedImage.Created,
			})
		}
		names := []string{"sha256:" + id}
		add(names[0], storedImage.Digest)
		for _, tag := range image.RepoTags {
			add(tag, storedImage.Digest)
			names = append(names, tag)
		}
		for _, ref := range image.RepoDigests {
			target := storedImage.Digest
			if _, value, ok := strings.Cut(ref, "@"); ok {
				if dgst, err := digest.Parse(value); err == nil && provider.Has(dgst) {
					target = dgst
				}
			}
			add(ref, target)
			names = append(names, ref)
		}
		for _, name := range names {
			used = used || containerImages[name]
		}
		if used {
			for _, name := range names {
				inUse[name] = true
			}
		}
	}
	return records, inUse
}
