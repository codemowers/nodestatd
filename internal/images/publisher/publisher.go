// Package publisher records node image inventories as ContainerImage objects.
//
// One cluster-scoped object exists per digest an image was pulled by: the
// index for multi-platform images, otherwise the platform manifest. Instances
// update objects by read-modify-write under resourceVersion preconditions:
// each node replaces its own contribution (node entry, the manifests it holds
// and its Node owner reference) and re-encodes the object compactly. Manifest
// fields every manifest shares are stored once at the top level, and node
// entries list references only when they differ from the top-level union.
// The last node deletes the object; the garbage collector deletes objects
// whose owner Nodes are all gone.
package publisher

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/codemowers/nodestatd/internal/images/inventory"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/util/retry"
)

const (
	Group        = "codemowers.cloud"
	Version      = "v1alpha1"
	Kind         = "ContainerImage"
	apiVersion   = Group + "/" + Version
	fieldManager = "nodestatd"
)

var Resource = schema.GroupVersionResource{Group: Group, Version: Version, Resource: "containerimages"}

// conflictBackoff spaces retries when nodes update the same object at once,
// as every node does for common images at startup.
var conflictBackoff = wait.Backoff{Steps: 8, Duration: 50 * time.Millisecond, Factor: 2, Jitter: 0.5}

// NodeLister returns the UIDs of nodes currently in the cluster by name.
type NodeLister func(ctx context.Context) (map[string]types.UID, error)

type Publisher struct {
	client dynamic.ResourceInterface
	nodes  NodeLister
	node   string
	// nodeUID is learned during Resync and required for owner references.
	nodeUID types.UID
	log     *slog.Logger
	// applied maps image digests to the hash of this node's last written
	// contribution.
	applied map[string]string
}

func New(client dynamic.Interface, nodes NodeLister, node string, log *slog.Logger) *Publisher {
	return &Publisher{
		client:  client.Resource(Resource),
		nodes:   nodes,
		node:    node,
		log:     log,
		applied: map[string]string{},
	}
}

// Sync writes changed local images and removes this node from images it no
// longer has.
func (p *Publisher) Sync(ctx context.Context, local map[string]*inventory.Image) error {
	var errs []error
	for _, image := range local {
		if err := p.apply(ctx, image, nil, false); err != nil {
			errs = append(errs, err)
		}
	}
	for digest := range p.applied {
		if local[digest] != nil {
			continue
		}
		if err := p.update(ctx, ObjectName(digest), nil, func(doc *document) { doc.removeNode(p.node) }); err != nil {
			errs = append(errs, err)
			continue
		}
		delete(p.applied, digest)
	}
	return errors.Join(errs...)
}

// Resync reconciles against the cluster: it writes every local image whose
// object differs, removes this node from images it no longer has and removes
// nodes that no longer exist.
func (p *Publisher) Resync(ctx context.Context, local map[string]*inventory.Image) error {
	nodes, err := p.nodes(ctx)
	if err != nil {
		return fmt.Errorf("list nodes: %w", err)
	}
	uid, ok := nodes[p.node]
	if !ok {
		return fmt.Errorf("node %q is not registered in the cluster", p.node)
	}
	// A re-registered node has a new UID, which changes every contribution.
	p.nodeUID = uid
	list, err := p.client.List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list %s: %w", Resource.Resource, err)
	}
	var errs []error
	listed := map[string]*unstructured.Unstructured{}
	for i := range list.Items {
		obj := &list.Items[i]
		var stale []string
		for _, node := range entryNodes(obj) {
			if (node == p.node && local[objectDigest(obj)] == nil) || (node != p.node && nodes[node] == "") {
				stale = append(stale, node)
			}
		}
		if len(stale) == 0 {
			listed[obj.GetName()] = obj
			continue
		}
		err := p.update(ctx, obj.GetName(), obj, func(doc *document) {
			for _, node := range stale {
				doc.removeNode(node)
			}
		})
		if err != nil {
			errs = append(errs, err)
		}
	}
	for digest, image := range local {
		if err := p.apply(ctx, image, listed[ObjectName(digest)], true); err != nil {
			errs = append(errs, err)
		}
	}
	for digest := range p.applied {
		if local[digest] == nil {
			delete(p.applied, digest)
		}
	}
	return errors.Join(errs...)
}

// apply writes this node's contribution for image. Unless force is set, an
// unchanged contribution is skipped without reading the object.
func (p *Publisher) apply(ctx context.Context, image *inventory.Image, current *unstructured.Unstructured, force bool) error {
	if p.nodeUID == "" {
		return fmt.Errorf("apply %s: node UID unknown until a resync succeeds", ObjectName(image.Digest))
	}
	hash, err := contributionHash(p.nodeUID, image)
	if err != nil {
		return err
	}
	if !force && p.applied[image.Digest] == hash {
		return nil
	}
	err = p.update(ctx, ObjectName(image.Digest), current, func(doc *document) { doc.setNode(p.node, p.nodeUID, image) })
	if err != nil {
		return err
	}
	p.applied[image.Digest] = hash
	return nil
}

// update reads an object (or starts from current), applies mutate to its
// expanded form and writes the compact result, deleting the object when no
// nodes remain. Writes carry the read resourceVersion and are retried on
// conflict with a fresh read.
func (p *Publisher) update(ctx context.Context, name string, current *unstructured.Unstructured, mutate func(*document)) error {
	err := retry.OnError(conflictBackoff, func(err error) bool {
		return apierrors.IsConflict(err) || apierrors.IsAlreadyExists(err)
	}, func() error {
		obj := current
		current = nil
		if obj == nil {
			var err error
			obj, err = p.client.Get(ctx, name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				obj = nil
			} else if err != nil {
				return err
			}
		}
		doc := newDocument()
		if obj != nil {
			doc = decode(obj)
		}
		mutate(doc)

		if len(doc.nodes) == 0 {
			if obj == nil {
				return nil
			}
			uid, rv := obj.GetUID(), obj.GetResourceVersion()
			err := p.client.Delete(ctx, name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}})
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}

		data, owners := doc.encode()
		var specSource string
		if obj != nil {
			specSource, _, _ = unstructured.NestedString(obj.Object, "spec", "source")
		}
		derive(data, specSource)
		if obj == nil {
			created := &unstructured.Unstructured{Object: data}
			created.SetAPIVersion(apiVersion)
			created.SetKind(Kind)
			created.SetName(name)
			created.SetOwnerReferences(owners)
			_, err := p.client.Create(ctx, created, metav1.CreateOptions{FieldManager: fieldManager})
			return err
		}
		owners = mergeOwners(obj.GetOwnerReferences(), owners)
		if sameContent(obj, data, owners) {
			return nil
		}
		updated := obj.DeepCopy()
		for _, key := range dataKeys {
			delete(updated.Object, key)
		}
		for key, value := range data {
			updated.Object[key] = value
		}
		updated.SetOwnerReferences(owners)
		_, err := p.client.Update(ctx, updated, metav1.UpdateOptions{FieldManager: fieldManager})
		return err
	})
	if err != nil {
		return fmt.Errorf("update %s: %w", name, err)
	}
	return nil
}

// ObjectName maps a digest such as sha256:abc to sha256-abc.
func ObjectName(digest string) string {
	return strings.Replace(digest, ":", "-", 1)
}

func objectDigest(obj *unstructured.Unstructured) string {
	return strings.Replace(obj.GetName(), "-", ":", 1)
}

func entryNodes(obj *unstructured.Unstructured) []string {
	var names []string
	for _, item := range slice(obj.Object["nodes"]) {
		if entry, ok := item.(map[string]any); ok {
			if name, ok := entry["name"].(string); ok {
				names = append(names, name)
			}
		}
	}
	return names
}

func contributionHash(uid types.UID, image *inventory.Image) (string, error) {
	data, err := json.Marshal(struct {
		UID   types.UID
		Image *inventory.Image
	}{uid, image})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}
