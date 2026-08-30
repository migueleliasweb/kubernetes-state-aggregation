# Kubernetes State Aggregation (KSA)

Your Global Kubernetes State, queryable.


## Goals

- Provide a globally aggregated snapshot of multiple Kubernetes clusters state.
- Provide an API-first queryable design, with a CLI tool to query and visualize the state.
- Leverage Postgres flexibility and speed to provide advanced queries and reporting on top of Kubernetes state.

---

## Features

- **Multi-Cluster Orchestration**: Connects to and syncs state from multiple Kubernetes API servers concurrently.
- **Dynamic Resource Discovery**: Uses low-level `client-go` controllers to automatically discover and watch API resources without pre-generated CRD structs, streaming data directly to the database to prevent memory explosion.
- **Automated State Reconciliation**: Seamlessly handles "missed deletes" (e.g., resources removed while the syncer was offline) by comparing the database against the cluster state on startup.
- **Unified Server Daemon**: Run the background sync worker, the gRPC state query service, or both within a single binary.
- **`ksactl` CLI**: Inspect aggregated cluster resources (`get`, `describe`) and render dependency graphs (`graph`) with formatted table, tree, YAML, and JSON outputs.
- **Flexible Filtering**: Global and per-cluster filter options for namespaces, excluded resources, and label selectors.
- **PostgreSQL Persistence**: Stores dynamic Kubernetes resource states, metadata, and JSON representations in a centralized PostgreSQL database.

---

## Example configuration

```yaml
global_filters:
  include_namespaces:
    - "*" # Include all namespaces
  exclude_resources:
    - "events"
    - "secrets"
    - "coordination.k8s.io/v1/leases"

clusters:
  - name: us1
    api_server: "https://127.0.0.1:6443"
    kubeconfig: "~/.kube/config"
    context: "kind-us1"
    disabled: false
```

## Running locally

The KSA server daemon (`cmd/server`) runs both the multi-cluster sync worker and the gRPC API server.

### Using `go run`

This runs the gRPC server and sync worker.

```bash
go run ./cmd/server --config config.yaml --db-url "postgres://postgres:yourpass@localhost:5432/ksa?sslmode=disable"
```

## Using the `ksactl` CLI

The `ksactl` CLI (`cmd/cli`) connects to the KSA gRPC API server to query and visualize aggregated cluster state.

### Commands & Examples

#### 1. List Resources (`ksactl get`)
```bash
# List all pods across all clusters
ksactl get pods

# Filter by namespace and cluster
ksactl get deployments -n default -c us1

# Output as YAML or JSON
ksactl get services -o yaml
```

#### 2. Describe Resource (`ksactl describe`)
```bash
# Describe a specific deployment
ksactl describe deployment my-app -n default -c us1
```

#### 3. Traverse Dependency Graph (`ksactl graph`)
```bash
# Render visual dependency graph for a root resource
ksactl graph deployment my-app -n default -c us1
```