# nodestatd

nodestatd runs on every Kubernetes node and reports what the node holds. Today
it records the container images in each node's containerd image store, so you
can ask the cluster which images, versions and source repositories are present
on which nodes without logging in to them. Network monitoring and process
listing are planned as further modules.

It publishes three cluster-scoped kinds in `codemowers.io/v1alpha1`:

| Kind | Short name | One object per |
| --- | --- | --- |
| `ContainerImage` | `cimg` | image digest, shared by every node holding it |
| `ImageRepository` | `imgrepo` | image repository, such as `docker.io/library/nginx` |
| `SourceRepository` | `srcrepo` | source code repository the images were built from |

Objects come and go with the images: when the last node drops an image its
ContainerImage is deleted, and repositories without images are deleted with
it.

## Requirements

- Nodes running containerd: k3s, Talos, kubeadm and most managed clusters.
- Kubernetes 1.32 or newer.
- A namespace that admits privileged pods and `hostPath` volumes, such as
  `kube-system`. nodestatd mounts the containerd socket and runs as root,
  without extra capabilities.

## Install

The chart is published to GitHub's container registry with every release and
installs the CRDs, the DaemonSet and its RBAC:

```bash
helm install nodestatd oci://ghcr.io/codemowers/charts/nodestatd \
  --namespace kube-system --version <version>
```

On k3s, point it at k3s's containerd socket:

```bash
helm install nodestatd oci://ghcr.io/codemowers/charts/nodestatd \
  --namespace kube-system --version <version> \
  --set containerd.socket=/run/k3s/containerd/containerd.sock
```

The image defaults to `ghcr.io/codemowers/nodestatd` at the chart's version,
built for amd64 and arm64. The DaemonSet tolerates every taint, so it also runs
on control-plane nodes.

### Values

| Value | Default | Purpose |
| --- | --- | --- |
| `containerd.socket` | `/run/containerd/containerd.sock` | Host path of the containerd socket. k3s uses `/run/k3s/containerd/containerd.sock`. |
| `containerd.namespace` | `k8s.io` | containerd namespace holding Kubernetes images. |
| `images.enabled` | `true` | Record images. |
| `images.resyncInterval` | `1h` | Full reconciliation; image and container events trigger quicker updates. |
| `images.recordEnv` | `false` | Record image environment variables. They can hold credentials baked into images and become readable to anyone who can read ContainerImages. |
| `logLevel` | `info` | `debug`, `info`, `warn` or `error`. |
| `nodeSelector` | `{}` | Limit the nodes it runs on. |
| `tolerations` | all taints | |
| `resources` | 10m CPU, 32Mi requested; 128Mi limit | |

### Check it works

```bash
kubectl -n kube-system rollout status daemonset/nodestatd
kubectl get cimg
```

Objects appear within seconds of the pods starting. The `view` ClusterRole is
extended to read all three kinds, so anyone with `view` can query them.

## Usage

```bash
# Images on nodes, with version, source repository and whether any node uses them
kubectl get cimg
kubectl get cimg sha256-<hex> -o yaml

# Every version of a project present on nodes
kubectl get cimg --field-selector status.source=https://github.com/cert-manager/cert-manager

# Image and source repositories
kubectl get imgrepo
kubectl get srcrepo
kubectl get imgrepo --field-selector spec.repository=docker.io/library/nginx
```

Source repositories and versions come from the images' OCI annotations and
labels. When an image reports no source, or a wrong one inherited from its
base image, set it for the whole image repository:

```bash
kubectl patch imgrepo <name> --type merge \
  -p '{"spec":{"source":"https://github.com/example/project"}}'
```

The override applies at the next resync (hourly by default) and lasts as long
as images of the repository remain on nodes.

## Examples

- `examples/containerimage.yaml`: a ContainerImage with every field filled in.
- `examples/repositories.yaml`: the matching ImageRepository and
  SourceRepository.
- `examples/warm-infra-images.yaml`: a MutatingAdmissionPolicy marking images
  of infrastructure projects (cert-manager, Cilium, Traefik, CloudNativePG, k3s
  components and others) to be kept on nodes, so pods evicted during a drain
  start without pulling. nodestatd does not act on `spec.pin` yet.

## Uninstall

```bash
helm uninstall nodestatd --namespace kube-system
```

This also removes the CRDs and every object nodestatd created.

## Development

See [AGENTS.md](AGENTS.md) for the design, data model and build workflow.

## License

Copyright 2026 Codemowers OÜ. Licensed under the [Apache License 2.0](LICENSE).
