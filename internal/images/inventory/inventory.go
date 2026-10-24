// Package inventory reads image metadata from a containerd image store.
package inventory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/platforms"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// Image is an image as pulled onto the node, keyed by the digest it was
// pulled by: an index for multi-platform images, otherwise a platform
// manifest. containerd keeps several records (tag, digest and ID references)
// per pull; they are merged by target digest. Records targeting a platform
// manifest listed in a local index are merged into that index's image.
type Image struct {
	Digest string
	// Index is set when Digest is an image index.
	Index *Index
	// Manifests are the platform manifests the node holds for the image,
	// usually the one matching the node platform.
	Manifests   []Manifest
	RepoTags    []string
	RepoDigests []string
	// PulledAt is the earliest creation time of the image's containerd records.
	PulledAt time.Time
	// InUse reports whether a container record references one of the
	// image's records, as kubelet image garbage collection considers it.
	InUse bool
	// Pinned reports whether one of the image's records carries the CRI pin
	// label, which exempts the image from kubelet image garbage collection.
	Pinned bool
}

const (
	// PinnedLabel is the containerd image label through which the CRI plugin
	// reports images as pinned.
	PinnedLabel = "io.cri-containerd.pinned"
	// OwnPinLabel marks records nodestatd pinned, so it only ever removes its
	// own pins and leaves those of containerd's configuration, k3s image
	// import or manual tooling alone.
	OwnPinLabel = "codemowers.cloud/pinned"
)

type Index struct {
	Digest      string
	MediaType   string
	Annotations map[string]string
	// Platforms lists the index entries that declare a platform other than
	// unknown/unknown, which marks attestation manifests.
	Platforms []IndexEntry
}

type IndexEntry struct {
	Digest       string
	MediaType    string
	OS           string
	Architecture string
	Variant      string
}

// Manifest describes a platform manifest and its image config.
type Manifest struct {
	Digest       string
	MediaType    string
	ConfigDigest string
	OS           string
	Architecture string
	Variant      string
	Created      *time.Time
	Author       string
	Labels       map[string]string
	Annotations  map[string]string
	User         string
	WorkingDir   string
	StopSignal   string
	Entrypoint   []string
	Cmd          []string
	Env          []string
	ExposedPorts []string
	Volumes      []string
	Layers       []Layer
	// Size is the compressed size of the config and layers.
	Size int64
}

// Layer pairs a manifest layer with its uncompressed diff ID when the config
// lists one diff ID per manifest layer.
type Layer struct {
	Digest    string
	DiffID    string
	MediaType string
	Size      int64
}

// Options control platform selection and which config fields are recorded.
type Options struct {
	Platform platforms.MatchComparer
	// RecordEnv records the config environment, which may contain credentials
	// baked into images.
	RecordEnv bool
	// InUse holds the image record names containers reference.
	InUse map[string]bool
}

// Collect groups containerd image records by the digest they were pulled by.
// Records whose content cannot be resolved for the platform are skipped and
// reported in the returned error.
func Collect(ctx context.Context, records []images.Image, provider content.Provider, opts Options) (map[string]*Image, error) {
	result := map[string]*Image{}
	var errs []error
	for _, record := range records {
		key := record.Target.Digest.String()
		image, ok := result[key]
		if !ok {
			var err error
			image, err = describeImage(ctx, provider, record.Target, opts)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", record.Name, err))
				continue
			}
			result[key] = image
		}
		addReference(image, record.Name, record.CreatedAt)
		image.InUse = image.InUse || opts.InUse[record.Name]
		image.Pinned = image.Pinned || record.Labels[PinnedLabel] == "pinned"
	}
	mergeIntoIndexes(result)
	for _, image := range result {
		image.RepoTags = sortedUnique(image.RepoTags)
		image.RepoDigests = sortedUnique(image.RepoDigests)
		sort.Slice(image.Manifests, func(i, j int) bool { return image.Manifests[i].Digest < image.Manifests[j].Digest })
	}
	return result, errors.Join(errs...)
}

// mergeIntoIndexes folds images pulled by a platform manifest digest into the
// image of a local index listing that manifest.
func mergeIntoIndexes(result map[string]*Image) {
	parents := map[string]*Image{}
	for _, image := range result {
		if image.Index == nil {
			continue
		}
		for _, entry := range image.Index.Platforms {
			parents[entry.Digest] = image
		}
	}
	for key, image := range result {
		parent := parents[key]
		if image.Index != nil || parent == nil {
			continue
		}
		for _, manifest := range image.Manifests {
			if !hasManifest(parent, manifest.Digest) {
				parent.Manifests = append(parent.Manifests, manifest)
			}
		}
		parent.RepoTags = append(parent.RepoTags, image.RepoTags...)
		parent.RepoDigests = append(parent.RepoDigests, image.RepoDigests...)
		parent.InUse = parent.InUse || image.InUse
		parent.Pinned = parent.Pinned || image.Pinned
		if !image.PulledAt.IsZero() && (parent.PulledAt.IsZero() || image.PulledAt.Before(parent.PulledAt)) {
			parent.PulledAt = image.PulledAt
		}
		delete(result, key)
	}
}

func hasManifest(image *Image, digest string) bool {
	for _, manifest := range image.Manifests {
		if manifest.Digest == digest {
			return true
		}
	}
	return false
}

// addReference classifies a containerd record name. CRI stores the image ID
// itself as a bare digest name, which is not a repository reference.
func addReference(image *Image, name string, created time.Time) {
	switch {
	case isDigest(name):
	case strings.Contains(name, "@"):
		image.RepoDigests = append(image.RepoDigests, name)
	default:
		image.RepoTags = append(image.RepoTags, name)
	}
	if !created.IsZero() && (image.PulledAt.IsZero() || created.Before(image.PulledAt)) {
		image.PulledAt = created
	}
}

func isDigest(name string) bool {
	_, err := digest.Parse(name)
	return err == nil
}

func describeImage(ctx context.Context, provider content.Provider, target ocispec.Descriptor, opts Options) (*Image, error) {
	target, err := detectMediaType(ctx, provider, target)
	if err != nil {
		return nil, err
	}
	image := &Image{Digest: target.Digest.String()}
	if images.IsIndexType(target.MediaType) {
		var index ocispec.Index
		if err := readJSON(ctx, provider, target, &index); err != nil {
			return nil, err
		}
		image.Index = &Index{Digest: target.Digest.String(), MediaType: target.MediaType, Annotations: index.Annotations}
		for _, entry := range index.Manifests {
			if entry.Platform == nil || (entry.Platform.OS == "unknown" && entry.Platform.Architecture == "unknown") {
				continue
			}
			image.Index.Platforms = append(image.Index.Platforms, IndexEntry{
				Digest:       entry.Digest.String(),
				MediaType:    entry.MediaType,
				OS:           entry.Platform.OS,
				Architecture: entry.Platform.Architecture,
				Variant:      entry.Platform.Variant,
			})
		}
	}
	manifestDesc, manifest, err := resolveManifest(ctx, provider, target, opts.Platform)
	if err != nil {
		return nil, err
	}
	described, err := describeManifest(ctx, provider, manifestDesc, manifest, opts)
	if err != nil {
		return nil, err
	}
	image.Manifests = []Manifest{described}
	return image, nil
}

func resolveManifest(ctx context.Context, provider content.Provider, desc ocispec.Descriptor, platform platforms.MatchComparer) (ocispec.Descriptor, ocispec.Manifest, error) {
	desc, err := detectMediaType(ctx, provider, desc)
	if err != nil {
		return desc, ocispec.Manifest{}, err
	}
	switch {
	case images.IsManifestType(desc.MediaType):
		var manifest ocispec.Manifest
		if err := readJSON(ctx, provider, desc, &manifest); err != nil {
			return desc, manifest, err
		}
		return desc, manifest, nil
	case images.IsIndexType(desc.MediaType):
		var index ocispec.Index
		if err := readJSON(ctx, provider, desc, &index); err != nil {
			return desc, ocispec.Manifest{}, err
		}
		var candidates []ocispec.Descriptor
		for _, child := range index.Manifests {
			if child.Platform == nil {
				// Single-entry indexes may omit the platform.
				if len(index.Manifests) == 1 {
					candidates = append(candidates, child)
				}
				continue
			}
			if platform.Match(*child.Platform) {
				candidates = append(candidates, child)
			}
		}
		if len(candidates) == 0 {
			return desc, ocispec.Manifest{}, fmt.Errorf("index %s has no manifest for the node platform", desc.Digest)
		}
		sort.SliceStable(candidates, func(i, j int) bool {
			if candidates[i].Platform == nil || candidates[j].Platform == nil {
				return false
			}
			return platform.Less(*candidates[i].Platform, *candidates[j].Platform)
		})
		return resolveManifest(ctx, provider, candidates[0], platform)
	default:
		return desc, ocispec.Manifest{}, fmt.Errorf("unsupported media type %q", desc.MediaType)
	}
}

// detectMediaType replaces generic descriptor media types, such as
// application/octet-stream recorded for registries that serve manifests with
// a generic content type, using the blob's mediaType field or structure.
func detectMediaType(ctx context.Context, provider content.Provider, desc ocispec.Descriptor) (ocispec.Descriptor, error) {
	if images.IsIndexType(desc.MediaType) || images.IsManifestType(desc.MediaType) {
		return desc, nil
	}
	var probe struct {
		MediaType string          `json:"mediaType"`
		Manifests json.RawMessage `json:"manifests"`
		Config    json.RawMessage `json:"config"`
	}
	if err := readJSON(ctx, provider, desc, &probe); err != nil {
		return desc, fmt.Errorf("media type %q: %w", desc.MediaType, err)
	}
	switch {
	case images.IsIndexType(probe.MediaType) || images.IsManifestType(probe.MediaType):
		desc.MediaType = probe.MediaType
	case probe.Manifests != nil:
		desc.MediaType = ocispec.MediaTypeImageIndex
	case probe.Config != nil:
		desc.MediaType = ocispec.MediaTypeImageManifest
	default:
		return desc, fmt.Errorf("unsupported media type %q", desc.MediaType)
	}
	return desc, nil
}

func describeManifest(ctx context.Context, provider content.Provider, desc ocispec.Descriptor, manifest ocispec.Manifest, opts Options) (Manifest, error) {
	var config ocispec.Image
	if err := readJSON(ctx, provider, manifest.Config, &config); err != nil {
		return Manifest{}, fmt.Errorf("config: %w", err)
	}
	described := Manifest{
		Digest:       desc.Digest.String(),
		MediaType:    desc.MediaType,
		ConfigDigest: manifest.Config.Digest.String(),
		OS:           config.OS,
		Architecture: config.Architecture,
		Variant:      config.Variant,
		Created:      config.Created,
		Author:       config.Author,
		Labels:       config.Config.Labels,
		Annotations:  manifest.Annotations,
		User:         config.Config.User,
		WorkingDir:   config.Config.WorkingDir,
		StopSignal:   config.Config.StopSignal,
		Entrypoint:   config.Config.Entrypoint,
		Cmd:          config.Config.Cmd,
		ExposedPorts: sortedKeys(config.Config.ExposedPorts),
		Volumes:      sortedKeys(config.Config.Volumes),
		Size:         manifest.Config.Size,
	}
	if opts.RecordEnv {
		described.Env = config.Config.Env
	}
	diffIDs := config.RootFS.DiffIDs
	for i, layer := range manifest.Layers {
		entry := Layer{Digest: layer.Digest.String(), MediaType: layer.MediaType, Size: layer.Size}
		if len(diffIDs) == len(manifest.Layers) {
			entry.DiffID = diffIDs[i].String()
		}
		described.Layers = append(described.Layers, entry)
		described.Size += layer.Size
	}
	return described, nil
}

func readJSON(ctx context.Context, provider content.Provider, desc ocispec.Descriptor, target any) error {
	data, err := content.ReadBlob(ctx, provider, desc)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

func sortedKeys(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedUnique(values []string) []string {
	sort.Strings(values)
	out := values[:0]
	for i, value := range values {
		if i == 0 || value != values[i-1] {
			out = append(out, value)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
