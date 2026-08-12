# Kubernetes State Aggregation

This project allows Kubernetes administrators to sync the state of multiple Kubernetes API Servers to a centralised datastore. This allows administrators to have a global view across multiple clusters.

## High Level Design

- The KSA API will be compatible with `kubectl`.
    - Extra/incompatible endpoints will also be exposed and can be queried via `ksactl`.
- In the database, only the latest versions of reach resource is kept.
- Different datastores will be supported, starting with PostgreSQL.
- KSA can be configured to include/exclude certain resource types, namespaces and labels
    - More complex filters can be expected on future releases


## Components

### KSA API

Exposes a Kuberntes-compatible HTTP REST API with the aggregated data from all upstream Kubernetes API servers.

### Sync Worker

Syncs the remote state from various Kubernetes API Servers onto the datastore layer.

### KSA CLI

Interacts with the KSA API's endpoints that are not natively compatible with `kubectl`. This CLI is capable to perform complex aggregations between multiple aggregated Kubernetes API stores in the API.