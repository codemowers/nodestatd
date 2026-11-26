package crio

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func TestBigDataBaseName(t *testing.T) {
	for key, want := range map[string]string{
		"manifest": "manifest",
		"manifest-sha256:7250955b2354cc6ad3548b428628e441e34625caa39dd64906e85adf369e1942": "=bWFuaWZlc3Qtc2hhMjU2OjcyNTA5NTViMjM1NGNjNmFkMzU0OGI0Mjg2MjhlNDQxZTM0NjI1Y2FhMzlkZDY0OTA2ZTg1YWRmMzY5ZTE5NDI=",
		"sha256:0010f63d26952bd3960fac5513c64a1cf7d6299f31fa7b54ac1b36633ee04333":          "=c2hhMjU2OjAwMTBmNjNkMjY5NTJiZDM5NjBmYWM1NTEzYzY0YTFjZjdkNjI5OWYzMWZhN2I1NGFjMWIzNjYzM2VlMDQzMzM=",
	} {
		if got := bigDataBaseName(key); got != want {
			t.Errorf("bigDataBaseName(%q) = %q, want %q", key, got, want)
		}
	}
}

func TestStorageProvider(t *testing.T) {
	root := t.TempDir()
	storage := &Storage{Root: root, Driver: "overlay"}
	id := "873ed75102791e5b0b8a7fcd41606c92fcec98d56d05ead4ac5131650004c136"
	manifest := []byte(`{"schemaVersion":2}`)
	dgst := digest.FromBytes(manifest)
	if err := os.MkdirAll(filepath.Join(storage.ImagesDir(), id), 0o700); err != nil {
		t.Fatal(err)
	}
	key := "manifest-" + dgst.String()
	if err := os.WriteFile(filepath.Join(storage.ImagesDir(), id, bigDataBaseName(key)), manifest, 0o600); err != nil {
		t.Fatal(err)
	}
	images := `[{"id":"` + id + `","big-data-names":["` + key + `","manifest"],` +
		`"big-data-sizes":{"` + key + `":19,"manifest":19},` +
		`"big-data-digests":{"` + key + `":"` + dgst.String() + `","manifest":"` + dgst.String() + `"},` +
		`"created":"2026-01-02T03:04:05Z"}]`
	if err := os.WriteFile(filepath.Join(storage.ImagesDir(), "images.json"), []byte(images), 0o600); err != nil {
		t.Fatal(err)
	}

	stored, err := storage.Images()
	if err != nil {
		t.Fatal(err)
	}
	image := stored[id]
	if image == nil || image.Digest != dgst || !image.Created.Equal(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)) {
		t.Fatalf("stored image = %+v, want digest defaulted to the manifest digest", image)
	}
	provider := storage.Provider(stored)
	data, err := content.ReadBlob(context.Background(), provider, ocispec.Descriptor{Digest: dgst})
	if err != nil || string(data) != string(manifest) {
		t.Fatalf("ReadBlob = %q, %v", data, err)
	}
	if _, err := provider.ReaderAt(context.Background(), ocispec.Descriptor{Digest: digest.FromString("missing")}); err == nil {
		t.Fatal("ReaderAt of a missing blob succeeded")
	}
}

func TestWatch(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	changed := make(chan struct{}, 10)
	done := make(chan error, 1)
	go func() { done <- watch(ctx, []string{dir}, func() { changed <- struct{}{} }) }()

	// Retry until the watch is in place; the lock file is not a change.
	deadline := time.After(5 * time.Second)
	for i := 0; ; i++ {
		if err := os.WriteFile(filepath.Join(dir, "images.lock"), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		tmp := filepath.Join(dir, ".tmp-images.json")
		if err := os.WriteFile(tmp, []byte("[]"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(tmp, filepath.Join(dir, "images.json")); err != nil {
			t.Fatal(err)
		}
		select {
		case <-changed:
		case <-time.After(50 * time.Millisecond):
			continue
		case <-deadline:
			t.Fatal("no change reported")
		}
		break
	}
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("watch returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("watch did not stop")
	}
}
