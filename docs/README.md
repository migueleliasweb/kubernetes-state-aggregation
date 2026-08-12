# Kubernetes State Aggregation

This project allows Kubernetes administrators to sync the state of multiple Kubernetes API Servers to a centralised datastore. This allows administrators to have a global view across multiple clusters.

## High Level Design

- The KSA API will be compatible with `kubectl`.
    - Extra/incompatible endpoints will also be exposed and can be queried via `ksactl`.
- In the database, only the latest versions of reach resource is kept.
- Different datastores will be supported, starting with PostgreSQL.
- KSA can be configured to include/exclude certain resource types, namespaces and labels
    - More complex filters can be expected on future releases
