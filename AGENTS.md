# nodestatd for agents

Design notes, data model and workflow for working on nodestatd. User-facing
deployment and usage live in [README.md](README.md); keep it free of
implementation detail.

## Layout

nodestatd runs as a DaemonSet and reports the state of each Kubernetes node.
It is built from modules sharing one container runtime connection, to
containerd or CRI-O; the images module is the first. netstatd's network monitoring is planned to join as a `net`
module, followed by listing the processes running in containers.

| Module | Package | Output | Requires |
| --- | --- | --- | --- |
| images | `internal/images` | `ContainerImage`, `ImageRepository` and `SourceRepository` objects | containerd socket, or CRI-O socket and image store |

`internal/containerd` holds the shared containerd client, event subscription
and container listing; `internal/crio` the CRI-O equivalents: a CRI client,
a reader for its containers/storage image store and a watch on that store; `internal/server` serves health endpoints, which later
share netstatd's metrics port. Keep module boundaries when adding features.

## Images module

The images module records the images held in each node's containerd or CRI-O
image store as cluster-scoped `ContainerImage` objects (`codemowers.io/v1alpha1`):
index and platform manifest digests, config digests, compressed layer digests
paired with diff IDs, config labels, manifest annotations, platform,
entrypoint, user and exposed ports. Driftmower MCP reads them for node-level
image facts.

Objects are named `sha256-<hex>` after the digest the image was pulled by: the
image index for multi-platform images, otherwise the platform manifest. `index`
lists the platforms the index offers; `manifests` holds the platform manifests
nodes actually have, so amd64 and arm64 nodes pulling the same index share one
object. A platform manifest pulled by its own digest is merged into a local
index that lists it.

Objects are stored compactly. Manifest fields every manifest shares (`labels`
and `annotations` key by key, `author`, `created`, `user`, `workingDir`,
`stopSignal`, `entrypoint`, `cmd`, `env`, `exposedPorts`, `volumes`) are kept
once at the top level and are absent from the manifests. `repoTags` and
`repoDigests` are the union of every reference nodes hold the image under; a
`nodes` entry lists its own references only when they differ from the union,
alongside the platform manifests that node holds. Absent fields inherit the
top-level value.

Node entries are marked `unused` when no container on the node, running or
exited, references the image, matching what kubelet image garbage collection
considers unused; `status.unused` is set when every node agrees. Node entries
are marked `pinned` when the runtime reports the image pinned on that node,
whether by its configuration (CRI-O's `pinned_images`), k3s image import or
another tool. Pins
nodestatd sets carry its own `codemowers.io/pinned` label, and it only ever
removes those. Only Linux platforms are recorded.

### Status

`status` summarizes the object for printer columns: `image` (first tag,
otherwise first digest reference), `source`, `version` and `unused`.

`status.source` is the `spec.source` of an ImageRepository the image is held
under, otherwise the first of the `org.opencontainers.image.source` manifest
annotation or label, `org.label-schema.vcs-url`, `source_repository`, or an
`org.opencontainers.image.url` on a code forge, normalized to a lowercase
repository URL without `.git` or trailing slash. Annotations come first
because labels are more often inherited from base images; an ImageRepository
override corrects values inherited either way, for every image of the
repository.

`status.version` is the version as published, such as `v1.16.1`: the
`org.opencontainers.image.version` manifest annotation, otherwise an
`org.opencontainers.image.version` or `org.label-schema.version` label that
agrees with a version-like tag (`1.27.3` agrees with `1.27`, `v1.27.3` and
`1.27.3-alpine`), otherwise the most specific such tag. Config labels are often
inherited from base images, so a label never sets the version alone; images
tagged only `latest`, branches or commits have none. `status.source` and
`status.version` are selectable fields.

### Writes

nodestatd updates objects by read-modify-write with resourceVersion
preconditions, retrying on conflict. Each instance expands the object, replaces
its node's contribution (entry, manifests and Node owner reference) and writes
the compact result only when it changed; fields nodestatd does not manage are
preserved. When an image leaves a node, the node's instance removes its
contribution, dropping manifests and references only that node reported; the
last node deletes the object. When a Node is deleted, the garbage collector
removes its owner reference and deletes objects with no remaining owners;
instances remove the deleted node's entries during resync.

Objects describe image store contents, not running containers. Pod `imageID`s
match entries in `repoDigests`.

### Image and source repositories

Each ContainerImage owns the cluster-scoped `ImageRepository` of every
repository it is held under (`spec.repository`, such as
`quay.io/jetstack/cert-manager-controller`, without tag or digest) and the
cluster-scoped `SourceRepository` of its `status.source` (`spec.url`). One
source usually has several image repositories, and a mirrored image several
ImageRepositories. Docker Hub references are canonical as
`docker.io/library/<name>` and registry hosts are lowercase.

Both are named after their identity: the last path segment, lowercased with
other characters replaced by hyphens and truncated to 52 characters, then a
hyphen and the first 10 hex digits of the identity's SHA-256, such as
`cert-manager-c311baa7dd` for `https://github.com/cert-manager/cert-manager`.
The hash is always present, since hyphen-joined segments would be ambiguous.
Identities are immutable selectable fields.

nodestatd adds an image's owner reference after writing the image and moves
it when the image's repositories or source change. The garbage collector
removes references of deleted ContainerImages and deletes aggregates once
their last owner is gone; nodestatd deletes aggregates it leaves without
owners. Each resync recomputes all owners from the ContainerImages, repairing
references concurrent writes moved out of order. An override in
`ImageRepository.spec.source` is preserved and applied at the next resync; it
lasts while images of the repository are on nodes. Other fields and owners of
other kinds are preserved.

### CRI-O

`images.Runtime` abstracts the runtime as containerd-style image records
resolved against a content provider, so inventory and publishing are shared.
For CRI-O, images, references and pins come from the CRI `ListImages` and
containers from `ListContainers`, so `repoDigests` hold exactly the references
kubelet reports as pod `imageID`s. CRI-O lists a repo digest for each manifest
digest of an image, so references to a platform manifest merge into the image
of its index. Manifests, indexes and configs are read from containers/storage
big data (`<driver>-images/<id>/`), which keeps the pulled index next to the
platform manifest; images missing from the store, such as OCI artifacts, are
skipped. CRI-O has no image events, so changes to the store's JSON files
trigger syncs. Docker schema 1 images, which containerd converts on pull and
CRI-O stores as pulled, are skipped. nodestatd cannot pin images on CRI-O.

## Configuration

The chart maps its values to these variables; each also has a command-line
flag.

| Variable | Default | Purpose |
| --- | --- | --- |
| `NODE_NAME` | `unset` | Required node name; the chart sets it from `spec.nodeName`. |
| `CONTAINER_RUNTIME` | `containerd` | `containerd` or `crio`. |
| `CONTAINERD_ADDRESS` | `/run/containerd/containerd.sock` | containerd socket. |
| `CONTAINERD_NAMESPACE` | `k8s.io` | containerd namespace holding Kubernetes containers and images. |
| `CRIO_ADDRESS` | `/run/crio/crio.sock` | CRI-O socket. |
| `CRIO_STORAGE_ROOT` | `/var/lib/containers/storage` | containers/storage graph root CRI-O pulls into. |
| `CRIO_STORAGE_DRIVER` | `overlay` | containers/storage driver; stores of other drivers in the graph root are ignored. |
| `KUBECONFIG` | `unset` | Kubeconfig path; in-cluster configuration when unset. |
| `HEALTH_ADDR` | `:5281` | `/healthz` checks the runtime; `/readyz` waits for each enabled module. |
| `LOG_LEVEL` | `info` | debug, info, warn or error. |
| `ENABLE_IMAGES` | `true` | Run the images module. |
| `IMAGES_RESYNC_INTERVAL` | `1h` | Full image reconciliation against the cluster. |
| `IMAGES_EVENT_DEBOUNCE` | `5s` | Delay before syncing after image and container events. |
| `IMAGES_RECORD_ENV` | `false` | Record config environment variables, which may contain baked-in credentials. |

## Development

```bash
go test ./...
go vet ./...
go build ./cmd/nodestatd
skaffold run    # uses skaffold.env; skaffold.ee-west-1.env for ee-west-1
# CRI-O test cluster, with the crio profile:
env $(xargs < skaffold.veebkolm.env) skaffold run
```

The chart installs the CRDs, the DaemonSet, its RBAC, and a read role for the
three kinds aggregated into `view`. The pod mounts the containerd socket, or
CRI-O's socket and read-only graph root, through `hostPath` volumes and runs
as UID 0 without capabilities, so its namespace must admit privileged pods and
`hostPath` volumes; Skaffold deploys it to
`kube-system`.

Driftmower's desktop install depends on the chart's values: it sets
`runtime=crio` when every node runs CRI-O, and only when `helm show values`
lists a top-level `runtime` key; otherwise it uses `containerd.socket` (the
k3s socket on k3s). It relies on the `crio.*` defaults. Keep these names and
defaults stable, or change Driftmower with them.

The image is a statically linked binary on `scratch`; the Docker build runs
the tests. GitHub Actions builds `linux/amd64` and `linux/arm64` on native
runners and publishes `ghcr.io/codemowers/nodestatd`: `latest` and branch
tags from `main`, version tags from `v*.*.*` Git tags, using the workflow's
`GITHUB_TOKEN`. Version tags also publish the chart as
`oci://ghcr.io/codemowers/charts/nodestatd`, with its version and appVersion
taken from the tag, so a chart version defaults to the matching image.

The API is v1alpha1: breaking changes need no migration path, and only current
Kubernetes releases are supported.
