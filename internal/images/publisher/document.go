package publisher

import (
	"encoding/json"
	"net/url"
	"reflect"
	"slices"
	"sort"
	stdstrings "strings"
	"time"

	"github.com/codemowers/nodestatd/internal/images/inventory"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// Manifest fields hoisted to the top level when every manifest has the same
// value. Maps are hoisted key by key. Platform-specific fields such as
// architecture and layers always stay in the manifest.
var (
	hoistedFields = []string{"author", "created", "user", "workingDir", "stopSignal", "entrypoint", "cmd", "env", "exposedPorts", "volumes"}
	hoistedMaps   = []string{"labels", "annotations"}
	// dataKeys are the top-level fields nodestatd manages.
	dataKeys = append([]string{"index", "manifests", "repoTags", "repoDigests", "nodes", "status"}, append(hoistedFields, hoistedMaps...)...)
)

// document is the expanded form of a ContainerImage: manifests carry every
// field and each node lists all of its references.
type document struct {
	index     map[string]any
	manifests map[string]map[string]any
	nodes     map[string]*nodeEntry
	// owners maps node names to the UIDs of their owner references.
	owners map[string]types.UID
}

type nodeEntry struct {
	manifests   []string
	repoTags    []string
	repoDigests []string
	pulledAt    string
	// unused marks images no container on the node references.
	unused bool
	// pinned marks images the node's containerd reports as pinned.
	pinned bool
}

func newDocument() *document {
	return &document{manifests: map[string]map[string]any{}, nodes: map[string]*nodeEntry{}, owners: map[string]types.UID{}}
}

// decode expands a stored object. Fields absent from a manifest inherit the
// top-level value; node entries without references inherit the top-level
// union.
func decode(obj *unstructured.Unstructured) *document {
	doc := newDocument()
	if index, ok := obj.Object["index"].(map[string]any); ok {
		doc.index = runtime.DeepCopyJSON(index)
	}
	for _, item := range slice(obj.Object["manifests"]) {
		stored, ok := item.(map[string]any)
		if !ok {
			continue
		}
		manifest := runtime.DeepCopyJSON(stored)
		for _, field := range hoistedFields {
			if value, ok := obj.Object[field]; ok {
				if _, own := manifest[field]; !own {
					manifest[field] = runtime.DeepCopyJSONValue(value)
				}
			}
		}
		for _, field := range hoistedMaps {
			common, _ := obj.Object[field].(map[string]any)
			if len(common) == 0 {
				continue
			}
			merged, _ := manifest[field].(map[string]any)
			if merged == nil {
				merged = map[string]any{}
			}
			for key, value := range common {
				if _, own := merged[key]; !own {
					merged[key] = value
				}
			}
			manifest[field] = merged
		}
		if digest, ok := manifest["digest"].(string); ok {
			doc.manifests[digest] = manifest
		}
	}
	unionTags, _, _ := unstructured.NestedStringSlice(obj.Object, "repoTags")
	unionDigests, _, _ := unstructured.NestedStringSlice(obj.Object, "repoDigests")
	for _, item := range slice(obj.Object["nodes"]) {
		entry, _ := item.(map[string]any)
		name, _ := entry["name"].(string)
		if name == "" {
			continue
		}
		node := &nodeEntry{}
		var found bool
		// Entries without manifests hold every manifest of the object.
		if node.manifests, found, _ = unstructured.NestedStringSlice(entry, "manifests"); !found {
			node.manifests = sortedKeys(doc.manifests)
		}
		node.pulledAt, _, _ = unstructured.NestedString(entry, "pulledAt")
		node.unused, _, _ = unstructured.NestedBool(entry, "unused")
		node.pinned, _, _ = unstructured.NestedBool(entry, "pinned")
		if node.repoTags, found, _ = unstructured.NestedStringSlice(entry, "repoTags"); !found {
			node.repoTags = slices.Clone(unionTags)
		}
		if node.repoDigests, found, _ = unstructured.NestedStringSlice(entry, "repoDigests"); !found {
			node.repoDigests = slices.Clone(unionDigests)
		}
		doc.nodes[name] = node
	}
	for _, ref := range obj.GetOwnerReferences() {
		if ref.APIVersion == "v1" && ref.Kind == "Node" {
			doc.owners[ref.Name] = ref.UID
		}
	}
	return doc
}

// setNode replaces a node's contribution. The node's manifest descriptions
// replace stored ones with the same digest.
func (doc *document) setNode(name string, uid types.UID, image *inventory.Image) {
	entry := &nodeEntry{repoTags: image.RepoTags, repoDigests: image.RepoDigests, unused: !image.InUse, pinned: image.Pinned}
	for _, manifest := range image.Manifests {
		doc.manifests[manifest.Digest] = manifestObject(manifest)
		entry.manifests = append(entry.manifests, manifest.Digest)
	}
	if image.Index != nil {
		doc.index = indexObject(image.Index)
	}
	if !image.PulledAt.IsZero() {
		entry.pulledAt = image.PulledAt.UTC().Format(time.RFC3339)
	}
	doc.nodes[name] = entry
	doc.owners[name] = uid
}

func (doc *document) removeNode(name string) {
	delete(doc.nodes, name)
	delete(doc.owners, name)
}

// encode returns the compact top-level fields and owner references. Manifests
// no node references are dropped.
func (doc *document) encode() (map[string]any, []metav1.OwnerReference) {
	out := map[string]any{}
	if doc.index != nil {
		out["index"] = doc.index
	}

	referenced := map[string]bool{}
	var unionTags, unionDigests []string
	for _, node := range doc.nodes {
		for _, digest := range node.manifests {
			referenced[digest] = true
		}
		unionTags = append(unionTags, node.repoTags...)
		unionDigests = append(unionDigests, node.repoDigests...)
	}
	unionTags, unionDigests = sortedUnique(unionTags), sortedUnique(unionDigests)

	var manifests []map[string]any
	for _, digest := range sortedKeys(doc.manifests) {
		if referenced[digest] {
			manifests = append(manifests, runtime.DeepCopyJSON(doc.manifests[digest]))
		}
	}
	hoist(out, manifests)
	items := make([]any, len(manifests))
	encoded := make([]string, len(manifests))
	for i, manifest := range manifests {
		items[i] = manifest
		encoded[i], _ = manifest["digest"].(string)
	}
	out["manifests"] = items
	setStrings(out, "repoTags", unionTags)
	setStrings(out, "repoDigests", unionDigests)

	var nodes []any
	var owners []metav1.OwnerReference
	for _, name := range sortedKeys(doc.nodes) {
		node := doc.nodes[name]
		entry := map[string]any{"name": name}
		if held := sortedUnique(slices.Clone(node.manifests)); !slices.Equal(held, encoded) {
			setStrings(entry, "manifests", held)
		}
		setString(entry, "pulledAt", node.pulledAt)
		if node.unused {
			entry["unused"] = true
		}
		if node.pinned {
			entry["pinned"] = true
		}
		// An explicit, possibly empty, list overrides the inherited union.
		if tags := sortedUnique(slices.Clone(node.repoTags)); !slices.Equal(tags, unionTags) {
			entry["repoTags"] = anySlice(tags)
		}
		if digests := sortedUnique(slices.Clone(node.repoDigests)); !slices.Equal(digests, unionDigests) {
			entry["repoDigests"] = anySlice(digests)
		}
		nodes = append(nodes, entry)
		if uid := doc.owners[name]; uid != "" {
			owners = append(owners, metav1.OwnerReference{APIVersion: "v1", Kind: "Node", Name: name, UID: uid})
		}
	}
	out["nodes"] = nodes
	return out, owners
}

// sourceKeys lists where the source repository is looked up, in order.
// Manifest annotations come first: unlike config labels they are not
// inherited from base images. URL fields count only when they point at a
// code forge.
var sourceKeys = []struct {
	field, key string
	forgeOnly  bool
}{
	{"annotations", "org.opencontainers.image.source", false},
	{"labels", "org.opencontainers.image.source", false},
	{"labels", "org.label-schema.vcs-url", false},
	{"labels", "source_repository", false},
	{"annotations", "org.opencontainers.image.url", true},
	{"labels", "org.opencontainers.image.url", true},
	{"labels", "org.label-schema.url", true},
}

// forges maps code hosting sites to the number of path segments naming a
// repository; zero keeps the path up to GitLab's /-/ separator.
var forges = map[string]int{"github.com": 2, "codeberg.org": 2, "bitbucket.org": 2, "gitlab.com": 0}

// derive sets status: the image reference, preferring tags over digest
// references, the source repository, preferring specSource over image
// metadata, and whether no node uses the image.
func derive(data map[string]any, specSource string) {
	status := map[string]any{}
	for _, key := range []string{"repoTags", "repoDigests"} {
		if refs := slice(data[key]); len(refs) > 0 {
			status["image"] = refs[0]
			break
		}
	}
	source := specSource
	for _, candidate := range sourceKeys {
		if source != "" {
			break
		}
		values, _ := data[candidate.field].(map[string]any)
		raw, _ := values[candidate.key].(string)
		source = normalizeSource(raw, candidate.forgeOnly)
	}
	setString(status, "source", source)
	if allNodes(data, "unused") {
		status["unused"] = true
	}
	if len(status) > 0 {
		data["status"] = status
	}
}

// allNodes reports whether every node entry sets flag.
func allNodes(data map[string]any, flag string) bool {
	nodes := slice(data["nodes"])
	for _, item := range nodes {
		entry, _ := item.(map[string]any)
		if set, _ := entry[flag].(bool); !set {
			return false
		}
	}
	return len(nodes) > 0
}

// normalizeSource turns a source reference into a repository URL, dropping
// git revisions and subdirectories (#rev:dir) and .git suffixes, and trimming
// forge URLs to the repository. It returns "" for values that are not HTTP
// URLs, or not forge URLs when forgeOnly is set.
func normalizeSource(raw string, forgeOnly bool) string {
	raw, _, _ = stdstrings.Cut(stdstrings.TrimSpace(raw), "#")
	if rest, ok := stdstrings.CutPrefix(raw, "git@"); ok {
		host, path, _ := stdstrings.Cut(rest, ":")
		raw = "https://" + host + "/" + path
	}
	parsed, err := url.Parse(raw)
	if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.Host == "" {
		return ""
	}
	host := stdstrings.ToLower(parsed.Host)
	path := stdstrings.Trim(stdstrings.TrimSuffix(stdstrings.Trim(parsed.Path, "/"), ".git"), "/")
	segments, forge := forges[host]
	switch {
	case forge && segments > 0:
		parts := stdstrings.Split(path, "/")
		if len(parts) < segments {
			return ""
		}
		path = stdstrings.Join(parts[:segments], "/")
	case forge:
		path, _, _ = stdstrings.Cut(path, "/-/")
		if path == "" {
			return ""
		}
	case forgeOnly:
		return ""
	}
	if path == "" {
		return parsed.Scheme + "://" + host
	}
	return parsed.Scheme + "://" + host + "/" + stdstrings.TrimSuffix(path, ".git")
}

// hoist moves values every manifest shares to out.
func hoist(out map[string]any, manifests []map[string]any) {
	if len(manifests) == 0 {
		return
	}
	for _, field := range hoistedFields {
		value, ok := manifests[0][field]
		if !ok {
			continue
		}
		shared := true
		for _, manifest := range manifests[1:] {
			if other, ok := manifest[field]; !ok || !reflect.DeepEqual(other, value) {
				shared = false
				break
			}
		}
		if shared {
			out[field] = value
			for _, manifest := range manifests {
				delete(manifest, field)
			}
		}
	}
	for _, field := range hoistedMaps {
		first, _ := manifests[0][field].(map[string]any)
		common := map[string]any{}
		for key, value := range first {
			shared := true
			for _, manifest := range manifests[1:] {
				other, _ := manifest[field].(map[string]any)
				if v, ok := other[key]; !ok || !reflect.DeepEqual(v, value) {
					shared = false
					break
				}
			}
			if shared {
				common[key] = value
			}
		}
		if len(common) == 0 {
			continue
		}
		out[field] = common
		for _, manifest := range manifests {
			own, _ := manifest[field].(map[string]any)
			for key := range common {
				delete(own, key)
			}
			if len(own) == 0 {
				delete(manifest, field)
			}
		}
	}
}

// sameContent reports whether obj already holds data and owners.
func sameContent(obj *unstructured.Unstructured, data map[string]any, owners []metav1.OwnerReference) bool {
	current := map[string]any{}
	for _, key := range dataKeys {
		if value, ok := obj.Object[key]; ok {
			current[key] = value
		}
	}
	return canonical(current) == canonical(data) && canonical(obj.GetOwnerReferences()) == canonical(owners)
}

// mergeOwners replaces the Node owner references in existing with nodes,
// keeping owner references of other kinds that someone else added. Existing
// references keep their positions so unchanged objects compare equal; new
// Node references are appended.
func mergeOwners(existing, nodes []metav1.OwnerReference) []metav1.OwnerReference {
	wanted := map[string]metav1.OwnerReference{}
	for _, ref := range nodes {
		wanted[ref.Name] = ref
	}
	var merged []metav1.OwnerReference
	for _, ref := range existing {
		if ref.APIVersion != "v1" || ref.Kind != "Node" {
			merged = append(merged, ref)
			continue
		}
		if node, ok := wanted[ref.Name]; ok {
			merged = append(merged, node)
			delete(wanted, ref.Name)
		}
	}
	for _, ref := range nodes {
		if _, ok := wanted[ref.Name]; ok {
			merged = append(merged, ref)
		}
	}
	return merged
}

func canonical(value any) string {
	data, _ := json.Marshal(value)
	return string(data)
}

func indexObject(index *inventory.Index) map[string]any {
	out := map[string]any{"digest": index.Digest}
	setString(out, "mediaType", index.MediaType)
	setMap(out, "annotations", index.Annotations)
	if len(index.Platforms) > 0 {
		var entries []any
		for _, entry := range index.Platforms {
			// Only Linux platforms are recorded; os is therefore implied.
			if entry.OS != "linux" {
				continue
			}
			platform := map[string]any{"digest": entry.Digest}
			setString(platform, "mediaType", entry.MediaType)
			setString(platform, "architecture", entry.Architecture)
			setString(platform, "variant", entry.Variant)
			entries = append(entries, platform)
		}
		if len(entries) > 0 {
			out["platforms"] = entries
		}
	}
	return out
}

func manifestObject(manifest inventory.Manifest) map[string]any {
	out := map[string]any{"digest": manifest.Digest, "configDigest": manifest.ConfigDigest}
	setString(out, "mediaType", manifest.MediaType)
	setString(out, "architecture", manifest.Architecture)
	setString(out, "variant", manifest.Variant)
	if manifest.Created != nil {
		out["created"] = manifest.Created.UTC().Format(time.RFC3339)
	}
	setString(out, "author", manifest.Author)
	setMap(out, "labels", manifest.Labels)
	setMap(out, "annotations", manifest.Annotations)
	setString(out, "user", manifest.User)
	setString(out, "workingDir", manifest.WorkingDir)
	setString(out, "stopSignal", manifest.StopSignal)
	setStrings(out, "entrypoint", manifest.Entrypoint)
	setStrings(out, "cmd", manifest.Cmd)
	setStrings(out, "env", manifest.Env)
	setStrings(out, "exposedPorts", manifest.ExposedPorts)
	setStrings(out, "volumes", manifest.Volumes)
	if len(manifest.Layers) > 0 {
		layers := make([]any, 0, len(manifest.Layers))
		for _, layer := range manifest.Layers {
			entry := map[string]any{"digest": layer.Digest, "size": layer.Size}
			setString(entry, "diffID", layer.DiffID)
			setString(entry, "mediaType", layer.MediaType)
			layers = append(layers, entry)
		}
		out["layers"] = layers
	}
	out["size"] = manifest.Size
	return out
}

func slice(value any) []any {
	items, _ := value.([]any)
	return items
}

func anySlice(values []string) []any {
	out := make([]any, len(values))
	for i, value := range values {
		out[i] = value
	}
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedUnique(values []string) []string {
	sort.Strings(values)
	return slices.Compact(values)
}

func setString(m map[string]any, key, value string) {
	if value != "" {
		m[key] = value
	}
}

func setStrings(m map[string]any, key string, values []string) {
	if len(values) > 0 {
		m[key] = anySlice(values)
	}
}

func setMap(m map[string]any, key string, values map[string]string) {
	if len(values) == 0 {
		return
	}
	out := make(map[string]any, len(values))
	for k, v := range values {
		out[k] = v
	}
	m[key] = out
}
