# nodestatd

nodestatd runs as a DaemonSet and reports the state of each Kubernetes node.
It is built from modules sharing one containerd connection; the images module
is the first. netstatd's network monitoring is planned to join as a `net`
module, followed by listing the processes running in containers.

| Module | Package | Output | Requires |
| --- | --- | --- | --- |
| images | `internal/images` | `ContainerImage` objects | containerd socket |

`internal/containerd` holds the shared containerd client, event subscription
and container listing; `internal/server` serves health endpoints, which later
share netstatd's metrics port.

## Images module

The images module records the images held in each node's containerd image
store as cluster-scoped `ContainerImage` objects (`codemowers.io/v1alpha1`):
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
are marked `pinned` when containerd reports the image pinned on that node,
whether by containerd's configuration, k3s image import or another tool. Pins
nodestatd sets carry its own `codemowers.io/pinned` label, and it only ever
removes those. Only Linux platforms are recorded.

`status` summarizes the object for printer columns: `image` (first tag,
otherwise first digest reference), `source`, `version` and `unused`.
`status.source` is `spec.source` when set, otherwise the first of the
`org.opencontainers.image.source` manifest annotation or label,
`org.label-schema.vcs-url`, `source_repository`, or an
`org.opencontainers.image.url` on a code forge, normalized to the repository
URL. Annotations come first because labels are more often inherited from base
images; `spec.source` corrects values inherited either way.

`status.version` is the version as published, such as `v1.16.1`: the
`org.opencontainers.image.version` manifest annotation, otherwise an
`org.opencontainers.image.version` or `org.label-schema.version` label that
agrees with a version-like tag (`1.27.3` agrees with `1.27`, `v1.27.3` and
`1.27.3-alpine`), otherwise the most specific such tag. Config labels are often
inherited from base images, so a label never sets the version alone; images
tagged only `latest`, branches or commits have none. `status.source` and
`status.version` are selectable fields, so
`kubectl get cimg --field-selector status.source=<url>` lists the versions of
a source deployed on nodes.

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

```bash
kubectl get containerimages
kubectl get cimg sha256-<hex> -o yaml
```

## Examples

`examples/containerimage.yaml` shows a ContainerImage with every field
populated. `examples/warm-infra-images.yaml` is a MutatingAdmissionPolicy that
sets `spec.pin: {policy: WhileInUse, warm: true}` on ContainerImages of
infrastructure projects (cert-manager, Cilium, Traefik, CloudNativePG, k3s
components and others, matched by organization prefix), so pods evicted during
a drain start without pulling. nodestatd does not act on `spec.pin` yet.

## Development

```bash
go test ./...
go vet ./...
go build ./cmd/nodestatd
skaffold run    # uses skaffold.env; skaffold.ee-west-1.env for ee-west-1
```

The chart installs the CRD, the DaemonSet, its RBAC, and a
`containerimages` read role aggregated into `view`. The pod mounts the
containerd socket through a `hostPath` and runs as UID 0 without capabilities,
so its namespace must admit privileged pods and `hostPath` volumes; Skaffold
deploys it to `kube-system`.

The image is a statically linked binary on `scratch`; the Docker build runs
the tests. GitHub Actions builds `linux/amd64` and `linux/arm64` on native
runners and publishes `ghcr.io/codemowers/nodestatd`: `latest` and branch
tags from `main`, version tags from `v*.*.*` Git tags, using the workflow's
`GITHUB_TOKEN`. Version tags also publish the chart as
`oci://ghcr.io/codemowers/charts/nodestatd`, with its version and appVersion
taken from the tag, so a chart version defaults to the matching image.

## Environment variables

Each variable also has a command-line flag.

| Variable | Default | Purpose |
| --- | --- | --- |
| `NODE_NAME` | `unset` | Required node name; the chart sets it from `spec.nodeName`. |
| `CONTAINERD_ADDRESS` | `/run/containerd/containerd.sock` | containerd socket. |
| `CONTAINERD_NAMESPACE` | `k8s.io` | containerd namespace holding Kubernetes containers and images. |
| `KUBECONFIG` | `unset` | Kubeconfig path; in-cluster configuration when unset. |
| `HEALTH_ADDR` | `:5281` | `/healthz` checks containerd; `/readyz` waits for each enabled module. |
| `LOG_LEVEL` | `info` | debug, info, warn or error. |
| `ENABLE_IMAGES` | `true` | Run the images module. |
| `IMAGES_RESYNC_INTERVAL` | `1h` | Full image reconciliation against the cluster. |
| `IMAGES_EVENT_DEBOUNCE` | `5s` | Delay before syncing after containerd image and container events. |
| `IMAGES_RECORD_ENV` | `false` | Record config environment variables, which may contain baked-in credentials. |

## License

Copyright 2026 Codemowers OÜ. Licensed under the [Apache License 2.0](LICENSE).
