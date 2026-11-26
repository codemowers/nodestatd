package images

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/codemowers/nodestatd/internal/crio"
	"github.com/codemowers/nodestatd/internal/images/inventory"
	"github.com/containerd/platforms"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// storeImage writes an image as containers/storage holds it after CRI-O
// pulled it through an index: the index and platform manifest as
// manifest-<digest> big data, the platform manifest again as "manifest", and
// the config keyed by its digest. It returns the image ID and the index and
// platform manifest digests.
func storeImage(t *testing.T, storage *crio.Storage, created time.Time) (string, digest.Digest, digest.Digest) {
	t.Helper()
	mustJSON := func(value any) []byte {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	config := mustJSON(ocispec.Image{
		Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"},
		Config:   ocispec.ImageConfig{Labels: map[string]string{"org.opencontainers.image.version": "1.2.3"}},
		RootFS:   ocispec.RootFS{Type: "layers", DiffIDs: []digest.Digest{digest.FromString("diff")}},
	})
	configDigest := digest.FromBytes(config)
	manifest := ocispec.Manifest{
		MediaType: ocispec.MediaTypeImageManifest,
		Config:    ocispec.Descriptor{MediaType: ocispec.MediaTypeImageConfig, Digest: configDigest, Size: int64(len(config))},
		Layers:    []ocispec.Descriptor{{MediaType: ocispec.MediaTypeImageLayerGzip, Digest: digest.FromString("layer"), Size: 100}},
	}
	manifest.SchemaVersion = 2
	manifestData := mustJSON(manifest)
	manifestDigest := digest.FromBytes(manifestData)
	index := ocispec.Index{MediaType: ocispec.MediaTypeImageIndex, Manifests: []ocispec.Descriptor{
		{MediaType: ocispec.MediaTypeImageManifest, Digest: manifestDigest, Size: int64(len(manifestData)), Platform: &ocispec.Platform{OS: "linux", Architecture: "amd64"}},
		{MediaType: ocispec.MediaTypeImageManifest, Digest: digest.FromString("arm64"), Size: 1, Platform: &ocispec.Platform{OS: "linux", Architecture: "arm64"}},
	}}
	index.SchemaVersion = 2
	indexData := mustJSON(index)
	indexDigest := digest.FromBytes(indexData)

	id := configDigest.Encoded()
	bigData := map[string][]byte{
		"manifest-" + indexDigest.String():    indexData,
		"manifest-" + manifestDigest.String(): manifestData,
		"manifest":                            manifestData,
		configDigest.String():                 config,
	}
	stored := crio.StoredImage{
		ID:             id,
		Digest:         indexDigest,
		Names:          []string{"ghcr.io/example/app:v1"},
		BigDataSizes:   map[string]int64{},
		BigDataDigests: map[string]digest.Digest{},
		Created:        created,
	}
	dir := filepath.Join(storage.ImagesDir(), id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for key, data := range bigData {
		name := key
		if key != "manifest" {
			name = "=" + base64.StdEncoding.EncodeToString([]byte(key))
		}
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
		stored.BigDataNames = append(stored.BigDataNames, key)
		stored.BigDataSizes[key] = int64(len(data))
		stored.BigDataDigests[key] = digest.FromBytes(data)
	}
	if err := os.WriteFile(filepath.Join(storage.ImagesDir(), "images.json"), mustJSON([]crio.StoredImage{stored}), 0o600); err != nil {
		t.Fatal(err)
	}
	return id, indexDigest, manifestDigest
}

func TestCRIORecords(t *testing.T) {
	storage := &crio.Storage{Root: t.TempDir(), Driver: "overlay"}
	pulled := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	id, indexDigest, manifestDigest := storeImage(t, storage, pulled)
	stored, err := storage.Images()
	if err != nil {
		t.Fatal(err)
	}
	provider := storage.Provider(stored)

	// CRI-O lists a repo digest for every manifest digest of the image.
	listed := []*runtimeapi.Image{
		{
			Id:       id,
			RepoTags: []string{"ghcr.io/example/app:v1"},
			RepoDigests: []string{
				"ghcr.io/example/app@" + indexDigest.String(),
				"ghcr.io/example/app@" + manifestDigest.String(),
			},
			Pinned: true,
		},
		// An OCI artifact or an image removed since listing.
		{Id: digest.FromString("artifact").Encoded(), RepoTags: []string{"ghcr.io/example/artifact:v1"}},
	}
	records, inUse := crioRecords(listed, stored, provider, map[string]bool{"ghcr.io/example/app:v1": true})
	if len(records) != 4 {
		t.Fatalf("records = %+v, want ID, tag and two digest records", records)
	}
	result, err := inventory.Collect(context.Background(), records, provider, inventory.Options{
		Platform: platforms.Only(ocispec.Platform{OS: "linux", Architecture: "amd64"}),
		InUse:    inUse,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("expected one image, got %d", len(result))
	}
	image := result[indexDigest.String()]
	if image == nil || image.Index == nil || len(image.Index.Platforms) != 2 {
		t.Fatalf("image = %+v, want keyed by the index", image)
	}
	if !slices.Equal(image.RepoTags, []string{"ghcr.io/example/app:v1"}) {
		t.Errorf("repoTags = %v", image.RepoTags)
	}
	if want := slices.Sorted(slices.Values(listed[0].RepoDigests)); !slices.Equal(image.RepoDigests, want) {
		t.Errorf("repoDigests = %v, want %v", image.RepoDigests, want)
	}
	if !image.InUse || !image.Pinned || !image.PulledAt.Equal(pulled) {
		t.Errorf("inUse %v pinned %v pulledAt %v", image.InUse, image.Pinned, image.PulledAt)
	}
	if len(image.Manifests) != 1 || image.Manifests[0].Digest != manifestDigest.String() ||
		image.Manifests[0].Labels["org.opencontainers.image.version"] != "1.2.3" || len(image.Manifests[0].Layers) != 1 {
		t.Errorf("manifests = %+v", image.Manifests)
	}

	// Containers created from the image ID mark it in use too.
	_, inUse = crioRecords(listed, stored, provider, map[string]bool{id: true})
	if !inUse["ghcr.io/example/app:v1"] {
		t.Errorf("inUse = %v, want every record of the image", inUse)
	}
	_, inUse = crioRecords(listed, stored, provider, nil)
	if len(inUse) != 0 {
		t.Errorf("inUse = %v, want none", inUse)
	}
}
