package inventory

import (
	"bytes"
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/plugins/content/local"
	"github.com/containerd/platforms"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func writeJSON(t *testing.T, store content.Store, mediaType string, value any) ocispec.Descriptor {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	desc := ocispec.Descriptor{MediaType: mediaType, Digest: digest.FromBytes(data), Size: int64(len(data))}
	if err := content.WriteBlob(context.Background(), store, desc.Digest.String(), bytes.NewReader(data), desc); err != nil {
		t.Fatal(err)
	}
	return desc
}

func platformImage(t *testing.T, store content.Store, arch string, created time.Time) ocispec.Descriptor {
	t.Helper()
	config := ocispec.Image{
		Created:  &created,
		Platform: ocispec.Platform{OS: "linux", Architecture: arch},
		Config: ocispec.ImageConfig{
			User:         "1000",
			Env:          []string{"TOKEN=secret"},
			Entrypoint:   []string{"/app"},
			ExposedPorts: map[string]struct{}{"8080/tcp": {}, "443/tcp": {}},
			Labels:       map[string]string{"org.opencontainers.image.source": "https://github.com/example/app"},
		},
		RootFS: ocispec.RootFS{Type: "layers", DiffIDs: []digest.Digest{digest.FromString(arch + "-diff-0"), digest.FromString(arch + "-diff-1")}},
	}
	configDesc := writeJSON(t, store, ocispec.MediaTypeImageConfig, config)
	manifest := ocispec.Manifest{
		MediaType: ocispec.MediaTypeImageManifest,
		Config:    configDesc,
		Layers: []ocispec.Descriptor{
			{MediaType: ocispec.MediaTypeImageLayerGzip, Digest: digest.FromString(arch + "-layer-0"), Size: 100},
			{MediaType: ocispec.MediaTypeImageLayerGzip, Digest: digest.FromString(arch + "-layer-1"), Size: 200},
		},
		Annotations: map[string]string{"org.opencontainers.image.revision": "abc123"},
	}
	manifest.SchemaVersion = 2
	desc := writeJSON(t, store, ocispec.MediaTypeImageManifest, manifest)
	desc.Platform = &ocispec.Platform{OS: "linux", Architecture: arch}
	return desc
}

func TestCollectMergesRecordsByConfigDigest(t *testing.T) {
	ctx := context.Background()
	store, err := local.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	created := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	amd64 := platformImage(t, store, "amd64", created)
	arm64 := platformImage(t, store, "arm64", created)
	attestation := writeJSON(t, store, ocispec.MediaTypeImageManifest, ocispec.Manifest{MediaType: ocispec.MediaTypeImageManifest})
	attestation.Platform = &ocispec.Platform{OS: "unknown", Architecture: "unknown"}
	index := ocispec.Index{MediaType: ocispec.MediaTypeImageIndex, Manifests: []ocispec.Descriptor{arm64, attestation, amd64}}
	index.SchemaVersion = 2
	indexDesc := writeJSON(t, store, ocispec.MediaTypeImageIndex, index)

	var amd64Manifest ocispec.Manifest
	if err := readJSON(ctx, store, amd64, &amd64Manifest); err != nil {
		t.Fatal(err)
	}
	configDigest := amd64Manifest.Config.Digest.String()
	pulled := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	records := []images.Image{
		{Name: "ghcr.io/example/app:v1", Target: indexDesc, CreatedAt: pulled.Add(time.Minute)},
		{Name: "ghcr.io/example/app@" + indexDesc.Digest.String(), Target: indexDesc, CreatedAt: pulled.Add(time.Minute)},
		{Name: configDigest, Target: indexDesc, CreatedAt: pulled.Add(time.Hour)},
		// Pulled by platform manifest digest: merged into the index image.
		{Name: "ghcr.io/example/app@" + amd64.Digest.String(), Target: amd64, CreatedAt: pulled, Labels: map[string]string{PinnedLabel: "pinned"}},
		{Name: "ghcr.io/example/broken:v1", Target: ocispec.Descriptor{MediaType: ocispec.MediaTypeImageManifest, Digest: digest.FromString("missing"), Size: 7}},
	}

	// The container references the merged platform manifest record.
	inUse := map[string]bool{"ghcr.io/example/app@" + amd64.Digest.String(): true}
	result, err := Collect(ctx, records, store, Options{Platform: platforms.Only(ocispec.Platform{OS: "linux", Architecture: "amd64"}), InUse: inUse})
	if err == nil {
		t.Fatal("expected an error for the record with missing content")
	}
	if len(result) != 1 {
		t.Fatalf("expected one image, got %d", len(result))
	}
	image := result[indexDesc.Digest.String()]
	if image == nil {
		t.Fatalf("image %s missing", indexDesc.Digest)
	}
	if image.Index == nil || image.Index.Digest != indexDesc.Digest.String() {
		t.Fatalf("index = %+v", image.Index)
	}
	var archs []string
	for _, entry := range image.Index.Platforms {
		archs = append(archs, entry.Architecture)
	}
	if !slices.Equal(archs, []string{"arm64", "amd64"}) {
		t.Errorf("index platforms = %v, attestation must be excluded", archs)
	}
	if want := []string{"ghcr.io/example/app:v1"}; !slices.Equal(image.RepoTags, want) {
		t.Errorf("repoTags = %v, want %v", image.RepoTags, want)
	}
	if want := []string{"ghcr.io/example/app@" + amd64.Digest.String(), "ghcr.io/example/app@" + indexDesc.Digest.String()}; !slices.Equal(image.RepoDigests, slices.Sorted(slices.Values(want))) {
		t.Errorf("repoDigests = %v, want %v", image.RepoDigests, want)
	}
	if !image.PulledAt.Equal(pulled) {
		t.Errorf("pulledAt = %v, want %v", image.PulledAt, pulled)
	}
	if !image.InUse {
		t.Error("image used through a merged record must be in use")
	}
	if !image.Pinned {
		t.Error("image pinned through a merged record must be pinned")
	}
	if len(image.Manifests) != 1 {
		t.Fatalf("expected one manifest, got %+v", image.Manifests)
	}
	manifest := image.Manifests[0]
	if manifest.Digest != amd64.Digest.String() || manifest.ConfigDigest != configDigest || manifest.Architecture != "amd64" {
		t.Errorf("resolved manifest %s config %s for %s", manifest.Digest, manifest.ConfigDigest, manifest.Architecture)
	}
	if manifest.Env != nil {
		t.Errorf("env recorded without RecordEnv: %v", manifest.Env)
	}
	if want := []string{"443/tcp", "8080/tcp"}; !slices.Equal(manifest.ExposedPorts, want) {
		t.Errorf("exposedPorts = %v, want %v", manifest.ExposedPorts, want)
	}
	if manifest.Labels["org.opencontainers.image.source"] == "" || manifest.Annotations["org.opencontainers.image.revision"] != "abc123" {
		t.Errorf("labels %v annotations %v", manifest.Labels, manifest.Annotations)
	}
	if len(manifest.Layers) != 2 || manifest.Layers[1].DiffID != digest.FromString("amd64-diff-1").String() || manifest.Layers[1].Size != 200 {
		t.Errorf("layers = %+v", manifest.Layers)
	}
	if manifest.Size != amd64Manifest.Config.Size+300 {
		t.Errorf("size = %d", manifest.Size)
	}
	if manifest.Created == nil || !manifest.Created.Equal(created) {
		t.Errorf("created = %v", manifest.Created)
	}
}

func TestCollectKeysSinglePlatformPullsByManifest(t *testing.T) {
	store, err := local.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	arm64 := platformImage(t, store, "arm64", time.Now())
	// An arm64 manifest pulled directly on an amd64 node stays separate.
	amd64 := platformImage(t, store, "amd64", time.Now())
	records := []images.Image{
		{Name: "example/app:arm", Target: arm64},
		{Name: "example/app:amd", Target: amd64},
	}
	result, err := Collect(context.Background(), records, store, Options{Platform: platforms.Only(ocispec.Platform{OS: "linux", Architecture: "amd64"})})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 2 {
		t.Fatalf("expected two images, got %d", len(result))
	}
	image := result[arm64.Digest.String()]
	if image == nil || image.InUse || image.Pinned || image.Index != nil || len(image.Manifests) != 1 || image.Manifests[0].Digest != arm64.Digest.String() {
		t.Errorf("image = %+v", image)
	}
}

func TestCollectRecordsEnvWhenEnabled(t *testing.T) {
	store, err := local.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	desc := platformImage(t, store, "amd64", time.Now())
	desc.Platform = nil
	result, err := Collect(context.Background(), []images.Image{{Name: "example/app:v1", Target: desc}}, store,
		Options{Platform: platforms.Only(ocispec.Platform{OS: "linux", Architecture: "amd64"}), RecordEnv: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, image := range result {
		if !slices.Equal(image.Manifests[0].Env, []string{"TOKEN=secret"}) {
			t.Errorf("env = %v", image.Manifests[0].Env)
		}
	}
}

func TestCollectDetectsGenericMediaTypes(t *testing.T) {
	store, err := local.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	amd64 := platformImage(t, store, "amd64", time.Now())
	index := ocispec.Index{Manifests: []ocispec.Descriptor{amd64}}
	index.SchemaVersion = 2
	indexDesc := writeJSON(t, store, "application/octet-stream", index)
	manifestDesc := amd64
	manifestDesc.MediaType = "application/octet-stream"
	manifestDesc.Platform = nil
	records := []images.Image{
		{Name: "example/app:index", Target: indexDesc},
		{Name: "example/app:manifest", Target: manifestDesc},
	}
	result, err := Collect(context.Background(), records, store, Options{Platform: platforms.Only(ocispec.Platform{OS: "linux", Architecture: "amd64"})})
	if err != nil {
		t.Fatal(err)
	}
	image := result[indexDesc.Digest.String()]
	if image == nil || image.Index == nil || image.Index.MediaType != ocispec.MediaTypeImageIndex {
		t.Fatalf("index image = %+v", image)
	}
	// The manifest record is listed by the index and merged into it.
	if len(result) != 1 || !slices.Equal(image.RepoTags, []string{"example/app:index", "example/app:manifest"}) {
		t.Errorf("result = %+v", result)
	}
	if image.Manifests[0].MediaType != ocispec.MediaTypeImageManifest {
		t.Errorf("manifest media type = %s", image.Manifests[0].MediaType)
	}
}
