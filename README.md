# Node Labeler Operator

A Kubernetes operator that manages node labels based on high-level policies. It supports:
- **Static Labeling**: Apply defined labels to nodes matching a selector.
- **Auto-Detection**: Automatically detect and label nodes based on hardware and capabilities.
- **Idempotency**: Ensures labels are applied consistently and removed when policies change.
- **Cleanup**: Optionally removes labels when the policy is deleted.

## Features

### 1. Static Labeling
Apply a set of fixed labels to any node matching a `nodeSelector`.

```yaml
spec:
  nodeSelector:
    node-role.kubernetes.io/worker: ""
  labels:
    environment: production
    team: platform
```

### 2. Auto-Detection
Automatically detect node capabilities and apply labels. Supported detection types:

| Type | Description |
|------|-------------|
| `gpu` | Detects any GPU (NVIDIA or AMD) |
| `nvidia-gpu` | Detects NVIDIA GPUs |
| `amd-gpu` | Detects AMD GPUs |
| `memory` | Detects high-memory nodes |
| `cpu` | Detects high-CPU nodes |
| `unknown` | Detects ephemeral storage |
| `pods` | Detects high pod capacity |
| `sriov` | Detects SR-IOV network devices |
| `hugepages-2Mi` | Detects 2Mi hugepages |
| `arch`, `os` | Copies architecture/OS from node labels |
| `zone`, `region` | Copies topology info from node labels |
| `instance-type` | Copies instance type from node labels |
| `custom` | Detects any extended resource |

### 3. Usage Examples

#### GPU Detection
```yaml
apiVersion: nodelabel.nodelabel.io/v1alpha1
kind: NodeLabel
metadata:
  name: gpu-detection
spec:
  nodeSelector: {}
  autoDetect:
    - type: gpu
      labelKey: "capability/gpu"
      labelValue: "true"
    - type: nvidia-gpu
      labelKey: "nvidia-gpu-count"
      labelValueFromQuantity: true
```

#### High Resource Nodes
```yaml
apiVersion: nodelabel.nodelabel.io/v1alpha1
kind: NodeLabel
metadata:
  name: high-resource
spec:
  nodeSelector: {}
  autoDetect:
    - type: memory
      minQuantity: "64Gi"
      labelKey: "high-memory"
      labelValue: "true"
```

#### Topology Propagation
```yaml
apiVersion: nodelabel.nodelabel.io/v1alpha1
kind: NodeLabel
metadata:
  name: topology
spec:
  nodeSelector: {}
  autoDetect:
    - type: zone
      labelKey: "availability-zone"
      onlyIfPresent: true
```

## Installation

### Install with Helm (Recommended)

The easiest way to install the Node Labeler Operator is using Helm:

```bash
# Add the Helm repository
helm repo add node-labeler https://adefenwa7.github.io/nodelabeloperator/
helm repo update

# Install the operator
helm install node-labeler node-labeler/node-labeler-operator \
  --namespace node-labeler-system \
  --create-namespace
```

#### Install with Sample NodeLabel for Testing

```bash
helm install node-labeler node-labeler/node-labeler-operator \
  --namespace node-labeler-system \
  --create-namespace \
  --set sampleNodeLabel.enabled=true
```

#### Upgrade

```bash
helm upgrade node-labeler node-labeler/node-labeler-operator \
  --namespace node-labeler-system
```

#### Uninstall

```bash
helm uninstall node-labeler --namespace node-labeler-system
```

### Install with Kustomize

1. **Install CRDs**:
   ```sh
   make install
   ```

2. **Run Operator (Locally)**:
   ```sh
   make run
   ```

3. **Deploy to Cluster**:
   ```sh
   make docker-build docker-push IMG=<your-registry>/node-labeler:v1
   make deploy IMG=<your-registry>/node-labeler:v1
   ```

## Development

### Running Tests
```sh
make test
```

### Building
```sh
make build
```
