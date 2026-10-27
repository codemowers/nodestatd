package publisher

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/util/retry"
)

var (
	ImageRepositoryResource  = schema.GroupVersionResource{Group: Group, Version: Version, Resource: "imagerepositories"}
	SourceRepositoryResource = schema.GroupVersionResource{Group: Group, Version: Version, Resource: "sourcerepositories"}
)

// aggregate is a cluster-scoped kind owned by the ContainerImages it groups:
// ImageRepository by image repository, SourceRepository by source URL. Its
// identity is a spec field; its name is derived from that identity. The
// garbage collector removes owner references of deleted ContainerImages and
// deletes aggregates left without owners; publishers move references when an
// image's repositories or source change.
type aggregate struct {
	client dynamic.ResourceInterface
	kind   string
	// field is the spec field holding the identity.
	field string
}

// AggregateName maps an identity such as quay.io/jetstack/cert-manager-controller
// or https://github.com/cert-manager/cert-manager to a DNS label: its last
// path segment, sanitized and truncated, followed by the first 10 hex digits
// of the identity's SHA-256. The hash is always present because joining
// segments with hyphens would be ambiguous.
func AggregateName(identity string) string {
	_, rest, found := strings.Cut(identity, "://")
	if !found {
		rest = identity
	}
	segments := strings.Split(strings.Trim(rest, "/"), "/")
	var prefix strings.Builder
	for _, r := range strings.ToLower(segments[len(segments)-1]) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			prefix.WriteRune(r)
		} else if s := prefix.String(); s != "" && !strings.HasSuffix(s, "-") {
			prefix.WriteByte('-')
		}
	}
	name := strings.Trim(prefix.String(), "-")
	if len(name) > 52 {
		name = strings.TrimRight(name[:52], "-")
	}
	if name == "" {
		name = "repo"
	}
	sum := sha256.Sum256([]byte(identity))
	return name + "-" + hex.EncodeToString(sum[:])[:10]
}

// ImageRepository returns the canonical repository of an image reference,
// without tag or digest: docker.io/library/nginx for nginx:1.27. It returns ""
// for references without a repository path.
func ImageRepository(ref string) string {
	name, _, _ := strings.Cut(ref, "@")
	if i := strings.LastIndex(name, ":"); i > strings.LastIndex(name, "/") {
		name = name[:i]
	}
	host, path, found := strings.Cut(name, "/")
	if !found || (!strings.ContainsAny(host, ".:") && host != "localhost") {
		host, path = "docker.io", name
	}
	host = strings.ToLower(host)
	if host == "index.docker.io" || host == "registry-1.docker.io" {
		host = "docker.io"
	}
	path = strings.ToLower(strings.Trim(path, "/"))
	if path == "" {
		return ""
	}
	if host == "docker.io" && !strings.Contains(path, "/") {
		path = "library/" + path
	}
	return host + "/" + path
}

// imageRepositories lists the canonical repositories of a ContainerImage's
// top-level references.
func imageRepositories(data map[string]any) []string {
	var repos []string
	for _, key := range []string{"repoTags", "repoDigests"} {
		for _, item := range slice(data[key]) {
			if ref, ok := item.(string); ok {
				if repo := ImageRepository(ref); repo != "" {
					repos = append(repos, repo)
				}
			}
		}
	}
	return sortedUnique(repos)
}

func imageSource(data map[string]any) string {
	source, _, _ := unstructured.NestedString(data, "status", "source")
	return source
}

// override returns the first ImageRepository spec.source among repos, as of
// the last resync.
func (p *Publisher) override(repos []string) string {
	var source string
	for _, repo := range repos {
		value := p.overrides[repo]
		if value == "" {
			continue
		}
		if source == "" {
			source = value
		} else if value != source {
			p.log.Warn("conflicting ImageRepository source overrides", "repositories", repos, "using", source, "ignored", value)
		}
	}
	return source
}

// loadOverrides refreshes the ImageRepository source overrides.
func (p *Publisher) loadOverrides(ctx context.Context) error {
	list, err := p.imageRepos.client.List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list %s: %w", ImageRepositoryResource.Resource, err)
	}
	overrides := map[string]string{}
	for _, obj := range list.Items {
		repo, _, _ := unstructured.NestedString(obj.Object, "spec", "repository")
		if source := normalizeSource(nestedString(obj.Object, "spec", "source"), false); repo != "" && source != "" {
			overrides[repo] = source
		}
	}
	p.overrides = overrides
	return nil
}

// link moves an image's owner references after its object changed from
// before to after: it adds the image to the aggregates of its current
// repositories and source and removes it from those it left. Updates that
// keep both are skipped; resync repairs missing aggregates.
func (p *Publisher) link(ctx context.Context, before, after *unstructured.Unstructured) error {
	owner := imageOwner(after)
	var oldRepos, oldSources []string
	if before != nil {
		oldRepos = imageRepositories(before.Object)
		if source := imageSource(before.Object); source != "" {
			oldSources = []string{source}
		}
	}
	newRepos := imageRepositories(after.Object)
	var newSources []string
	if source := imageSource(after.Object); source != "" {
		newSources = []string{source}
	}
	if before != nil && slices.Equal(oldRepos, newRepos) && slices.Equal(oldSources, newSources) {
		return nil
	}
	var errs []error
	for _, step := range []struct {
		agg      *aggregate
		old, new []string
	}{{p.imageRepos, oldRepos, newRepos}, {p.sourceRepos, oldSources, newSources}} {
		for _, identity := range step.new {
			errs = append(errs, p.updateOwners(ctx, step.agg, identity, nil, func(owners map[types.UID]metav1.OwnerReference) {
				owners[owner.UID] = owner
			}))
		}
		for _, identity := range step.old {
			if slices.Contains(step.new, identity) {
				continue
			}
			errs = append(errs, p.updateOwners(ctx, step.agg, identity, nil, func(owners map[types.UID]metav1.OwnerReference) {
				delete(owners, owner.UID)
			}))
		}
	}
	return errors.Join(errs...)
}

// reconcileAggregates sets the ContainerImage owners of every aggregate from
// the current ContainerImages: missing aggregates are created, stale owner
// references removed and aggregates left without owners deleted.
func (p *Publisher) reconcileAggregates(ctx context.Context) error {
	images, err := p.client.List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list %s: %w", Resource.Resource, err)
	}
	wanted := map[*aggregate]map[string]map[types.UID]metav1.OwnerReference{p.imageRepos: {}, p.sourceRepos: {}}
	add := func(agg *aggregate, identity string, owner metav1.OwnerReference) {
		if wanted[agg][identity] == nil {
			wanted[agg][identity] = map[types.UID]metav1.OwnerReference{}
		}
		wanted[agg][identity][owner.UID] = owner
	}
	for i := range images.Items {
		obj := &images.Items[i]
		owner := imageOwner(obj)
		for _, repo := range imageRepositories(obj.Object) {
			add(p.imageRepos, repo, owner)
		}
		if source := imageSource(obj.Object); source != "" {
			add(p.sourceRepos, source, owner)
		}
	}
	var errs []error
	for _, agg := range []*aggregate{p.imageRepos, p.sourceRepos} {
		list, err := agg.client.List(ctx, metav1.ListOptions{})
		if err != nil {
			errs = append(errs, fmt.Errorf("list %s: %w", agg.kind, err))
			continue
		}
		existing := map[string]*unstructured.Unstructured{}
		for i := range list.Items {
			obj := &list.Items[i]
			identity := nestedString(obj.Object, "spec", agg.field)
			if identity == "" || obj.GetName() != AggregateName(identity) {
				continue
			}
			existing[identity] = obj
			if _, ok := wanted[agg][identity]; !ok {
				wanted[agg][identity] = nil
			}
		}
		for _, identity := range sortedKeys(wanted[agg]) {
			owners := wanted[agg][identity]
			errs = append(errs, p.updateOwners(ctx, agg, identity, existing[identity], func(current map[types.UID]metav1.OwnerReference) {
				clear(current)
				for uid, ref := range owners {
					current[uid] = ref
				}
			}))
		}
	}
	return errors.Join(errs...)
}

// updateOwners reads the aggregate for identity (or starts from current),
// applies mutate to its ContainerImage owner references and writes the
// result: creating the aggregate when owners remain and deleting it when it
// has no owners left. Other fields and owners of other kinds are preserved.
func (p *Publisher) updateOwners(ctx context.Context, agg *aggregate, identity string, current *unstructured.Unstructured, mutate func(map[types.UID]metav1.OwnerReference)) error {
	name := AggregateName(identity)
	err := retry.OnError(conflictBackoff, func(err error) bool {
		return apierrors.IsConflict(err) || apierrors.IsAlreadyExists(err)
	}, func() error {
		obj := current
		current = nil
		if obj == nil {
			var err error
			obj, err = agg.client.Get(ctx, name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				obj = nil
			} else if err != nil {
				return err
			}
		}
		if obj != nil {
			if got := nestedString(obj.Object, "spec", agg.field); got != identity {
				return fmt.Errorf("name collision with spec.%s %q", agg.field, got)
			}
		}
		owners := map[types.UID]metav1.OwnerReference{}
		var others []metav1.OwnerReference
		if obj != nil {
			for _, ref := range obj.GetOwnerReferences() {
				if ref.APIVersion == apiVersion && ref.Kind == Kind {
					owners[ref.UID] = ref
				} else {
					others = append(others, ref)
				}
			}
		}
		mutate(owners)
		refs := append(others, sortedOwners(owners)...)

		if len(refs) == 0 {
			if obj == nil {
				return nil
			}
			uid, rv := obj.GetUID(), obj.GetResourceVersion()
			err := agg.client.Delete(ctx, name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}})
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if obj == nil {
			created := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{agg.field: identity}}}
			created.SetAPIVersion(apiVersion)
			created.SetKind(agg.kind)
			created.SetName(name)
			created.SetOwnerReferences(refs)
			_, err := agg.client.Create(ctx, created, metav1.CreateOptions{FieldManager: fieldManager})
			return err
		}
		if canonical(obj.GetOwnerReferences()) == canonical(refs) {
			return nil
		}
		updated := obj.DeepCopy()
		updated.SetOwnerReferences(refs)
		_, err := agg.client.Update(ctx, updated, metav1.UpdateOptions{FieldManager: fieldManager})
		return err
	})
	if err != nil {
		return fmt.Errorf("update %s %s: %w", agg.kind, name, err)
	}
	return nil
}

func imageOwner(obj *unstructured.Unstructured) metav1.OwnerReference {
	return metav1.OwnerReference{APIVersion: apiVersion, Kind: Kind, Name: obj.GetName(), UID: obj.GetUID()}
}

// sortedOwners orders owner references by name so unchanged sets compare
// equal.
func sortedOwners(owners map[types.UID]metav1.OwnerReference) []metav1.OwnerReference {
	refs := make([]metav1.OwnerReference, 0, len(owners))
	for _, ref := range owners {
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Name != refs[j].Name {
			return refs[i].Name < refs[j].Name
		}
		return refs[i].UID < refs[j].UID
	})
	return refs
}

func nestedString(obj map[string]any, fields ...string) string {
	value, _, _ := unstructured.NestedString(obj, fields...)
	return value
}
