package crio

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// Storage reads the containers/storage image store CRI-O pulls into. It only
// reads the store's JSON and big data files and takes no locks: files are
// replaced atomically, and a sync racing a pull or removal is corrected by
// the next one.
type Storage struct {
	// Root is the graph root, such as /var/lib/containers/storage.
	Root string
	// Driver is the storage driver, such as overlay. Stores of other drivers
	// left in the graph root after a driver change are ignored.
	Driver string
}

// StoredImage is an image record of containers/storage. Manifests pulled
// through an image index are stored as big data keyed manifest-<digest>,
// alongside the index itself, and the image config is keyed by its digest.
type StoredImage struct {
	ID string `json:"id"`
	// Digest is the digest the image was pulled by: the image index for
	// multi-platform images, otherwise the platform manifest.
	Digest         digest.Digest            `json:"digest"`
	Names          []string                 `json:"names"`
	NamesHistory   []string                 `json:"names-history"`
	BigDataNames   []string                 `json:"big-data-names"`
	BigDataSizes   map[string]int64         `json:"big-data-sizes"`
	BigDataDigests map[string]digest.Digest `json:"big-data-digests"`
	Created        time.Time                `json:"created"`
}

// ImagesDir holds images.json and a big data directory per image.
func (s *Storage) ImagesDir() string {
	return filepath.Join(s.Root, s.Driver+"-images")
}

// ContainersDir holds containers.json and volatile-containers.json.
func (s *Storage) ContainersDir() string {
	return filepath.Join(s.Root, s.Driver+"-containers")
}

// Images returns the stored images by ID.
func (s *Storage) Images() (map[string]*StoredImage, error) {
	data, err := os.ReadFile(filepath.Join(s.ImagesDir(), "images.json"))
	if err != nil {
		return nil, fmt.Errorf("image store: %w", err)
	}
	var list []*StoredImage
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("image store: %w", err)
	}
	images := make(map[string]*StoredImage, len(list))
	for _, image := range list {
		if image.Digest == "" {
			image.Digest = image.BigDataDigests["manifest"]
		}
		images[image.ID] = image
	}
	return images, nil
}

// Provider serves the manifests, indexes and configs of images as content
// addressed by digest.
func (s *Storage) Provider(images map[string]*StoredImage) *Provider {
	blobs := map[digest.Digest]blob{}
	for _, image := range images {
		for _, key := range image.BigDataNames {
			dgst := image.BigDataDigests[key]
			if dgst == "" {
				continue
			}
			if _, ok := blobs[dgst]; ok {
				continue
			}
			blobs[dgst] = blob{
				path: filepath.Join(s.ImagesDir(), image.ID, bigDataBaseName(key)),
				size: image.BigDataSizes[key],
			}
		}
	}
	return &Provider{blobs: blobs}
}

type blob struct {
	path string
	size int64
}

// Provider is a content.Provider over containers/storage big data.
type Provider struct {
	blobs map[digest.Digest]blob
}

var _ content.Provider = (*Provider)(nil)

// Has reports whether a blob with the digest is stored.
func (p *Provider) Has(dgst digest.Digest) bool {
	_, ok := p.blobs[dgst]
	return ok
}

// Size returns the size of a stored blob.
func (p *Provider) Size(dgst digest.Digest) int64 {
	return p.blobs[dgst].size
}

func (p *Provider) ReaderAt(_ context.Context, desc ocispec.Descriptor) (content.ReaderAt, error) {
	blob, ok := p.blobs[desc.Digest]
	if !ok {
		return nil, fmt.Errorf("blob %s: %w", desc.Digest, errdefs.ErrNotFound)
	}
	file, err := os.Open(blob.path)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	return &readerAt{File: file, size: info.Size()}, nil
}

type readerAt struct {
	*os.File
	size int64
}

func (r *readerAt) Size() int64 { return r.size }

// bigDataBaseName mirrors containers/storage's file naming for big data
// keys: keys made only of lowercase letters, digits and dots are used as is,
// others are base64 encoded behind a "=" prefix.
func bigDataBaseName(key string) string {
	if strings.IndexFunc(key, func(ch rune) bool {
		return ch != '.' && (ch < '0' || ch > '9') && (ch < 'a' || ch > 'z')
	}) < 0 {
		return key
	}
	return "=" + base64.StdEncoding.EncodeToString([]byte(key))
}
