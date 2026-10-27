package publisher

import (
	"context"
	"io"
	"log/slog"
	"reflect"
	"slices"
	"testing"

	"github.com/codemowers/nodestatd/internal/images/inventory"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

func newClient(objects ...runtime.Object) *dynamicfake.FakeDynamicClient {
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{Resource: Kind + "List"}, objects...)
}

func newPublisher(client *dynamicfake.FakeDynamicClient, node string, registered ...string) *Publisher {
	lister := func(context.Context) (map[string]types.UID, error) {
		uids := map[string]types.UID{}
		for _, name := range registered {
			uids[name] = types.UID("uid-" + name)
		}
		return uids, nil
	}
	return New(client, lister, node, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func get(t *testing.T, client *dynamicfake.FakeDynamicClient, name string) *unstructured.Unstructured {
	t.Helper()
	obj, err := client.Resource(Resource).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get %s: %v", name, err)
	}
	return obj
}

func manifest(digest, arch string, labels map[string]string) inventory.Manifest {
	return inventory.Manifest{
		Digest:       digest,
		ConfigDigest: digest + "-config",
		OS:           "linux",
		Architecture: arch,
		User:         "app",
		Entrypoint:   []string{"/app"},
		Labels:       labels,
		Layers:       []inventory.Layer{{Digest: digest + "-layer", Size: 10}},
		Size:         20,
	}
}

var index = &inventory.Index{Digest: "sha256:index", MediaType: "application/vnd.oci.image.index.v1+json"}

func TestCompactEncoding(t *testing.T) {
	doc := newDocument()
	doc.setNode("node-a", "uid-a", &inventory.Image{
		Digest:    "sha256:index",
		Index:     index,
		Manifests: []inventory.Manifest{manifest("sha256:amd64", "amd64", map[string]string{"version": "1.0", "created": "a"})},
		RepoTags:  []string{"example/app:1.0", "example/app:latest"},
	})
	doc.setNode("node-b", "uid-b", &inventory.Image{
		Digest:    "sha256:index",
		Index:     index,
		Manifests: []inventory.Manifest{manifest("sha256:arm64", "arm64", map[string]string{"version": "1.0", "created": "b"})},
		RepoTags:  []string{"example/app:1.0"},
	})
	doc.setNode("node-c", "uid-c", &inventory.Image{
		Pinned:      true,
		Digest:      "sha256:index",
		Index:       index,
		Manifests:   []inventory.Manifest{manifest("sha256:amd64", "amd64", map[string]string{"version": "1.0", "created": "a"})},
		RepoDigests: []string{"example/app@sha256:index"},
	})
	data, owners := doc.encode()

	if data["user"] != "app" || !reflect.DeepEqual(data["entrypoint"], []any{"/app"}) {
		t.Errorf("shared fields not hoisted: %v", data)
	}
	if !reflect.DeepEqual(data["labels"], map[string]any{"version": "1.0"}) {
		t.Errorf("labels = %v", data["labels"])
	}
	manifests := data["manifests"].([]any)
	if len(manifests) != 2 {
		t.Fatalf("manifests = %v", manifests)
	}
	amd64 := manifests[0].(map[string]any)
	for _, field := range []string{"user", "entrypoint"} {
		if _, ok := amd64[field]; ok {
			t.Errorf("hoisted field %s kept in manifest", field)
		}
	}
	if !reflect.DeepEqual(amd64["labels"], map[string]any{"created": "a"}) || amd64["architecture"] != "amd64" || amd64["layers"] == nil {
		t.Errorf("amd64 manifest = %v", amd64)
	}
	if !reflect.DeepEqual(data["repoTags"], []any{"example/app:1.0", "example/app:latest"}) {
		t.Errorf("repoTags = %v", data["repoTags"])
	}
	nodes := data["nodes"].([]any)
	nodeA, nodeB, nodeC := nodes[0].(map[string]any), nodes[1].(map[string]any), nodes[2].(map[string]any)
	if _, ok := nodeA["repoTags"]; ok {
		t.Errorf("node-a repoTags equal the union and must be omitted: %v", nodeA)
	}
	if !reflect.DeepEqual(nodeB["repoTags"], []any{"example/app:1.0"}) {
		t.Errorf("node-b repoTags = %v", nodeB["repoTags"])
	}
	if nodeC["pinned"] != true || nodeA["pinned"] != nil {
		t.Errorf("pinned flags: a=%v c=%v", nodeA["pinned"], nodeC["pinned"])
	}
	if !reflect.DeepEqual(nodeC["repoTags"], []any{}) {
		t.Errorf("node-c without tags needs an explicit empty list: %v", nodeC)
	}
	if !reflect.DeepEqual(nodeB["manifests"], []any{"sha256:arm64"}) {
		t.Errorf("node-b manifests = %v", nodeB["manifests"])
	}
	for _, node := range nodes {
		round := decode(&unstructured.Unstructured{Object: data})
		name := node.(map[string]any)["name"].(string)
		if got, want := round.nodes[name].manifests, doc.nodes[name].manifests; !slices.Equal(got, want) {
			t.Errorf("%s manifests = %v, want %v", name, got, want)
		}
	}
	if len(owners) != 3 || owners[1].Name != "node-b" || owners[1].UID != "uid-b" {
		t.Errorf("owners = %+v", owners)
	}

	// Decoding the compact form restores every node's view.
	obj := &unstructured.Unstructured{Object: data}
	obj.SetOwnerReferences(owners)
	round := decode(obj)
	if !reflect.DeepEqual(round.manifests, doc.manifests) {
		t.Errorf("manifests changed in round trip:\n%v\n%v", round.manifests, doc.manifests)
	}
	for name, node := range doc.nodes {
		got := round.nodes[name]
		if !slices.Equal(got.repoTags, sortedUnique(slices.Clone(node.repoTags))) || !slices.Equal(got.repoDigests, sortedUnique(slices.Clone(node.repoDigests))) {
			t.Errorf("%s references = %v %v, want %v %v", name, got.repoTags, got.repoDigests, node.repoTags, node.repoDigests)
		}
	}
	if !reflect.DeepEqual(round.owners, doc.owners) {
		t.Errorf("owners = %v", round.owners)
	}

	// Removing node-b drops its manifest, and everything left is shared.
	round.removeNode("node-b")
	data, _ = round.encode()
	if len(data["manifests"].([]any)) != 1 || !reflect.DeepEqual(data["labels"], map[string]any{"version": "1.0", "created": "a"}) {
		t.Errorf("after removing node-b: %v", data)
	}
	// Nodes holding every manifest omit the list.
	for _, node := range data["nodes"].([]any) {
		if _, ok := node.(map[string]any)["manifests"]; ok {
			t.Errorf("node entry keeps manifests: %v", node)
		}
	}
}

func status(data map[string]any) map[string]any {
	status, _ := data["status"].(map[string]any)
	return status
}

func TestDerive(t *testing.T) {
	data := map[string]any{
		"labels": map[string]any{"org.opencontainers.image.source": "https://github.com/example/app"},
	}
	derive(data, "")
	if status(data)["image"] != nil || status(data)["source"] != "https://github.com/example/app" {
		t.Errorf("status = %v", status(data))
	}
	data["repoDigests"] = []any{"example/app@sha256:abc"}
	derive(data, "")
	if status(data)["image"] != "example/app@sha256:abc" {
		t.Errorf("image falls back to the digest reference: %v", status(data))
	}
	data["repoTags"] = []any{"example/app:1.0"}
	derive(data, "https://github.com/example/fork")
	if status(data)["image"] != "example/app:1.0" || status(data)["source"] != "https://github.com/example/fork" {
		t.Errorf("tags and spec.source must win: %v", status(data))
	}
}

func TestDeriveSource(t *testing.T) {
	for _, tc := range []struct {
		name   string
		data   map[string]any
		source string
	}{
		{"annotation beats inherited label", map[string]any{
			"annotations": map[string]any{"org.opencontainers.image.source": "https://github.com/docker-library/python.git#688a0b86:3.11/slim-trixie"},
			"labels":      map[string]any{"org.opencontainers.image.source": "https://github.com/GoogleContainerTools/distroless"},
		}, "https://github.com/docker-library/python"},
		{"label-schema", map[string]any{"labels": map[string]any{"org.label-schema.vcs-url": "https://github.com/opensearch-project/OpenSearch"}}, "https://github.com/opensearch-project/OpenSearch"},
		{"source_repository", map[string]any{"labels": map[string]any{"source_repository": "git@github.com:sapcc/mosquitto-exporter.git"}}, "https://github.com/sapcc/mosquitto-exporter"},
		{"forge url trimmed", map[string]any{"annotations": map[string]any{"org.opencontainers.image.url": "https://github.com/osscontainertools/kaniko/actions/runs/1"}}, "https://github.com/osscontainertools/kaniko"},
		{"gitlab subgroup", map[string]any{"labels": map[string]any{"org.opencontainers.image.url": "https://gitlab.com/group/sub/project/-/tree/main"}}, "https://gitlab.com/group/sub/project"},
		{"non-forge url ignored", map[string]any{"labels": map[string]any{"org.opencontainers.image.url": "https://hub.docker.com/_/python", "org.label-schema.url": "https://opensearch.org"}}, ""},
		{"non-url source ignored", map[string]any{"labels": map[string]any{"org.opencontainers.image.source": "unknown"}}, ""},
		{"non-forge source kept", map[string]any{"labels": map[string]any{"org.opencontainers.image.source": "https://git.example.com/team/app.git"}}, "https://git.example.com/team/app"},
	} {
		derive(tc.data, "")
		if got, _ := status(tc.data)["source"].(string); got != tc.source {
			t.Errorf("%s: source = %q, want %q", tc.name, got, tc.source)
		}
	}
}

func TestDeriveVersion(t *testing.T) {
	labels := func(version string) map[string]any {
		return map[string]any{"org.opencontainers.image.version": version}
	}
	for _, tc := range []struct {
		name    string
		data    map[string]any
		version string
	}{
		{"annotation beats label", map[string]any{"annotations": labels("v1.16.1"), "labels": labels("24.04")}, "v1.16.1"},
		{"label consistent with tag", map[string]any{"labels": labels("1.27.3"), "repoTags": []any{"docker.io/library/nginx:1.27-alpine", "docker.io/library/nginx:1.27.3-alpine"}}, "1.27.3"},
		{"label consistent with floating tag", map[string]any{"labels": labels("1.27.3"), "repoTags": []any{"docker.io/library/nginx:1.27"}}, "1.27.3"},
		{"inherited label loses to tag", map[string]any{"labels": labels("24.04"), "repoTags": []any{"example/app:v2.1.0"}}, "v2.1.0"},
		{"most specific tag", map[string]any{"repoTags": []any{"example/app:1", "example/app:1.2.3", "example/app:1.2", "example/app:latest"}}, "1.2.3"},
		{"label-schema", map[string]any{"labels": map[string]any{"org.label-schema.version": "7.10.2"}, "repoTags": []any{"example/app:7.10"}}, "7.10.2"},
		{"label without version tag", map[string]any{"labels": labels("0.1.0"), "repoTags": []any{"example/app:sha-00cde6b"}}, ""},
		{"registry port is not a tag", map[string]any{"repoTags": []any{"registry.local:5000/app"}}, ""},
		{"non-version values ignored", map[string]any{"labels": labels("main"), "repoTags": []any{"example/app:latest", "example/app:sha-1a2b3c"}}, ""},
	} {
		derive(tc.data, "")
		if got, _ := status(tc.data)["version"].(string); got != tc.version {
			t.Errorf("%s: version = %q, want %q", tc.name, got, tc.version)
		}
	}
}

func TestDeriveIgnoresPins(t *testing.T) {
	data := map[string]any{"nodes": []any{map[string]any{"name": "a", "pinned": true}}}
	derive(data, "")
	if _, ok := status(data)["pinned"]; ok {
		t.Error("pins are reported per node only")
	}
}

func TestDeriveUnused(t *testing.T) {
	data := map[string]any{"nodes": []any{map[string]any{"name": "a", "unused": true}, map[string]any{"name": "b"}}}
	derive(data, "")
	if _, ok := status(data)["unused"]; ok {
		t.Error("image used on node b must not be unused")
	}
	data = map[string]any{"nodes": []any{map[string]any{"name": "a", "unused": true}, map[string]any{"name": "b", "unused": true}}}
	derive(data, "")
	if status(data)["unused"] != true {
		t.Errorf("unused = %v", status(data))
	}
}

func TestUpdatePreservesSpec(t *testing.T) {
	ctx := context.Background()
	client := newClient()
	a := newPublisher(client, "node-a", "node-a")
	image := &inventory.Image{Digest: "sha256:abc", Manifests: []inventory.Manifest{manifest("sha256:abc", "amd64", nil)}}
	if err := a.Resync(ctx, map[string]*inventory.Image{image.Digest: image}); err != nil {
		t.Fatal(err)
	}
	obj := get(t, client, "sha256-abc")
	obj.Object["spec"] = map[string]any{"source": "https://github.com/example/app", "pin": map[string]any{"policy": "WhileInUse"}}
	if _, err := client.Resource(Resource).Update(ctx, obj, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	image.RepoTags = []string{"example/app:v2"}
	if err := a.Resync(ctx, map[string]*inventory.Image{image.Digest: image}); err != nil {
		t.Fatal(err)
	}
	obj = get(t, client, "sha256-abc")
	if policy, _, _ := unstructured.NestedString(obj.Object, "spec", "pin", "policy"); policy != "WhileInUse" || status(obj.Object)["source"] != "https://github.com/example/app" || status(obj.Object)["image"] != "example/app:v2" {
		t.Errorf("object = %v", obj.Object)
	}
}

func TestUpdateKeepsOtherOwners(t *testing.T) {
	ctx := context.Background()
	client := newClient()
	a := newPublisher(client, "node-a", "node-a", "node-b")
	image := &inventory.Image{Digest: "sha256:abc", Manifests: []inventory.Manifest{manifest("sha256:abc", "amd64", nil)}}
	if err := a.Resync(ctx, map[string]*inventory.Image{image.Digest: image}); err != nil {
		t.Fatal(err)
	}
	obj := get(t, client, "sha256-abc")
	controller := true
	other := metav1.OwnerReference{APIVersion: "example.com/v1", Kind: "Policy", Name: "keep", UID: "uid-policy", Controller: &controller}
	obj.SetOwnerReferences(append(obj.GetOwnerReferences(), other))
	if _, err := client.Resource(Resource).Update(ctx, obj, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	// An unchanged contribution does not rewrite the object.
	client.ClearActions()
	if err := a.Resync(ctx, map[string]*inventory.Image{image.Digest: image}); err != nil {
		t.Fatal(err)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "update" {
			t.Errorf("unexpected update: %v", action)
		}
	}

	b := newPublisher(client, "node-b", "node-a", "node-b")
	if err := b.Resync(ctx, map[string]*inventory.Image{image.Digest: image}); err != nil {
		t.Fatal(err)
	}
	var kinds []string
	for _, ref := range get(t, client, "sha256-abc").GetOwnerReferences() {
		kinds = append(kinds, ref.Kind+"/"+ref.Name)
	}
	if !slices.Equal(kinds, []string{"Node/node-a", "Policy/keep", "Node/node-b"}) {
		t.Errorf("owner references = %v", kinds)
	}
}

func TestPublishersShareObjects(t *testing.T) {
	ctx := context.Background()
	client := newClient()
	a := newPublisher(client, "node-a", "node-a", "node-b")
	b := newPublisher(client, "node-b", "node-a", "node-b")
	amd64 := &inventory.Image{Digest: "sha256:index", Index: index, RepoTags: []string{"example/app:1.0"},
		Manifests: []inventory.Manifest{manifest("sha256:amd64", "amd64", map[string]string{"version": "1.0"})}}
	arm64 := &inventory.Image{Digest: "sha256:index", Index: index, RepoTags: []string{"example/app:1.0", "example/app:latest"},
		Manifests: []inventory.Manifest{manifest("sha256:arm64", "arm64", map[string]string{"version": "1.0"})}}
	if err := a.Resync(ctx, map[string]*inventory.Image{amd64.Digest: amd64}); err != nil {
		t.Fatal(err)
	}
	if err := b.Resync(ctx, map[string]*inventory.Image{arm64.Digest: arm64}); err != nil {
		t.Fatal(err)
	}
	obj := get(t, client, "sha256-index")
	if got := entryNodes(obj); !slices.Equal(got, []string{"node-a", "node-b"}) {
		t.Errorf("nodes = %v", got)
	}
	if len(obj.GetOwnerReferences()) != 2 || len(slice(obj.Object["manifests"])) != 2 {
		t.Errorf("object = %v", obj.Object)
	}

	// An unchanged event-driven sync does not touch the cluster.
	client.ClearActions()
	if err := a.Sync(ctx, map[string]*inventory.Image{amd64.Digest: amd64}); err != nil {
		t.Fatal(err)
	}
	if len(client.Actions()) != 0 {
		t.Errorf("unexpected actions: %v", client.Actions())
	}

	// node-b dropping the image removes its manifest, tag and owner reference.
	if err := b.Sync(ctx, map[string]*inventory.Image{}); err != nil {
		t.Fatal(err)
	}
	obj = get(t, client, "sha256-index")
	if got := entryNodes(obj); !slices.Equal(got, []string{"node-a"}) {
		t.Errorf("nodes = %v", got)
	}
	if tags, _, _ := unstructured.NestedStringSlice(obj.Object, "repoTags"); !slices.Equal(tags, []string{"example/app:1.0"}) {
		t.Errorf("repoTags = %v", tags)
	}
	if len(obj.GetOwnerReferences()) != 1 || len(slice(obj.Object["manifests"])) != 1 {
		t.Errorf("object = %v", obj.Object)
	}

	// The last node deletes the object.
	if err := a.Sync(ctx, map[string]*inventory.Image{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Resource(Resource).Get(ctx, "sha256-index", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("expected deletion, got %v", err)
	}
}

func TestResyncRemovesStaleNodes(t *testing.T) {
	ctx := context.Background()
	client := newClient()
	image := func(digest string) *inventory.Image {
		return &inventory.Image{Digest: digest, Manifests: []inventory.Manifest{manifest(digest, "amd64", nil)}}
	}
	gone := newPublisher(client, "node-gone", "node-gone", "node-a")
	if err := gone.Resync(ctx, map[string]*inventory.Image{"sha256:shared": image("sha256:shared"), "sha256:orphan": image("sha256:orphan")}); err != nil {
		t.Fatal(err)
	}
	a := newPublisher(client, "node-a", "node-gone", "node-a")
	if err := a.Resync(ctx, map[string]*inventory.Image{"sha256:shared": image("sha256:shared"), "sha256:old": image("sha256:old")}); err != nil {
		t.Fatal(err)
	}

	// node-gone leaves the cluster and node-a no longer has sha256:old.
	a = newPublisher(client, "node-a", "node-a")
	if err := a.Resync(ctx, map[string]*inventory.Image{"sha256:shared": image("sha256:shared")}); err != nil {
		t.Fatal(err)
	}
	list, err := client.Resource(Resource).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, obj := range list.Items {
		names = append(names, obj.GetName())
		if nodes := entryNodes(&obj); !slices.Equal(nodes, []string{"node-a"}) {
			t.Errorf("%s nodes = %v", obj.GetName(), nodes)
		}
	}
	if !slices.Equal(names, []string{"sha256-shared"}) {
		t.Errorf("objects = %v", names)
	}
}

func TestResyncRefusesUnregisteredNode(t *testing.T) {
	if err := newPublisher(newClient(), "node-a", "node-b").Resync(context.Background(), nil); err == nil {
		t.Fatal("expected an error when the publisher's node is not listed")
	}
}

func TestApplyRequiresNodeUID(t *testing.T) {
	client := newClient()
	image := &inventory.Image{Digest: "sha256:abc"}
	if err := newPublisher(client, "node-a", "node-a").Sync(context.Background(), map[string]*inventory.Image{image.Digest: image}); err == nil {
		t.Fatal("expected an error before the node UID is known")
	}
	if len(client.Actions()) != 0 {
		t.Errorf("unexpected actions: %v", client.Actions())
	}
}

func TestUpdateRetriesConflicts(t *testing.T) {
	ctx := context.Background()
	client := newClient()
	a := newPublisher(client, "node-a", "node-a", "node-b")
	image := &inventory.Image{Digest: "sha256:abc", Manifests: []inventory.Manifest{manifest("sha256:abc", "amd64", nil)}}
	if err := a.Resync(ctx, map[string]*inventory.Image{image.Digest: image}); err != nil {
		t.Fatal(err)
	}
	conflicts := 2
	client.PrependReactor("update", "containerimages", func(k8stesting.Action) (bool, runtime.Object, error) {
		if conflicts == 0 {
			return false, nil, nil
		}
		conflicts--
		return true, nil, apierrors.NewConflict(Resource.GroupResource(), "sha256-abc", nil)
	})
	b := newPublisher(client, "node-b", "node-a", "node-b")
	if err := b.Resync(ctx, map[string]*inventory.Image{image.Digest: image}); err != nil {
		t.Fatal(err)
	}
	if got := entryNodes(get(t, client, "sha256-abc")); !slices.Equal(got, []string{"node-a", "node-b"}) {
		t.Errorf("nodes = %v", got)
	}
}
