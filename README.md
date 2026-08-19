# Kubernetes State Aggregation (KSA)

**Kubernetes State Aggregation (KSA)** is a high-performance multi-cluster state aggregation service designed to dynamically discover, watch, and synchronize state across multiple Kubernetes clusters into a central PostgreSQL database. Built with Go and `client-go` dynamic informers, KSA provides real-time state visibility with customizable namespace/resource filtering, structured JSON logging (`log/slog`), and automated schema management.

---

## Features

- **Multi-Cluster Orchestration**: Connects to and syncs state from multiple Kubernetes API servers concurrently.
- **Dynamic Resource Discovery**: Uses dynamic client-go informers to automatically discover and watch API resources without pre-generated CRD structs.
- **Flexible Filtering**: Global and per-cluster filter options for namespaces, excluded resources, and label selectors.
- **PostgreSQL Persistence**: Stores dynamic Kubernetes resource states, metadata, and JSON representations in a centralized PostgreSQL database.
- **Structured JSON Logging**: Powered by Go standard library `log/slog` with caller source locations (file and line numbers) and configurable log levels.

---

## Getting Started

### Prerequisites

- **Go**: `1.21+` (or standard Go toolchain)
- **PostgreSQL**: Local or remote PostgreSQL instance (e.g. `postgres://postgres:postgres@localhost:5432/ksa?sslmode=disable`)
- **Kubernetes Access**: Kubeconfig file(s) with access to target cluster(s)

---

## Configuration

Copy the example configuration file and customize it for your environment:

```bash
cp config.example.yaml config.yaml
```

Example configuration (`config.yaml`):

```yaml
global_filters:
  include_namespaces:
    - "default"
    - "kube-system"
  exclude_resources:
    - "events"

clusters:
  - name: us1
    api_server: "https://127.0.0.1:6443"
    kubeconfig: "~/.kube/config"
    context: "kind-us1"
    disabled: false
```

---

## Running the Sync Worker

### Using `go run`

```bash
go run ./cmd/sync --config config.yaml --db-url "postgres://postgres:postgres@localhost:5432/ksa?sslmode=disable"
```

### Building and Executing Binary

```bash
# Build the executable
go build -o bin/ksasync ./cmd/sync

# Run the sync worker
./bin/ksasync --config config.yaml --log-level info
```

---

## CLI Command Flags

| Flag | Short | Default | Description |
| :--- | :--- | :--- | :--- |
| `--config` | `-c` | `config.yaml` | Path to the KSA configuration file (YAML/JSON) |
| `--db-url` | `-d` | `postgres://postgres:postgres@localhost:5432/ksa?sslmode=disable` | PostgreSQL database connection URL |
| `--cluster` | `-l` | `""` | Optional cluster name to isolate sync execution to a single cluster |
| `--log-level` | `-v` | `info` | Log verbosity level (`debug`, `info`, `warn`, `error`) |
| `--help` | `-h` | — | Display help information for `ksasync` |

---

## Examples

### Run with Debug Logging enabled

```bash
go run ./cmd/sync -c config.yaml -v debug
```

### Isolate Execution to a Single Cluster (`us1`)

```bash
go run ./cmd/sync -c config.yaml -l us1
```

---

## Testing

Run all unit and integration tests across packages:

```bash
go test ./...
```
