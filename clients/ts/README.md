# @migueleliasweb/ksa-client

TypeScript and JavaScript client for the Kubernetes State Aggregation (KSA) API, powered by [Connect-ES](https://connectrpc.com/docs/web/getting-started).

## Installation

### Via Git dependency in `package.json`

```bash
npm install github:migueleliasweb/kubernetes-state-aggregation#path:clients/ts
```

or in `package.json`:

```json
{
  "dependencies": {
    "@migueleliasweb/ksa-client": "github:migueleliasweb/kubernetes-state-aggregation#path:clients/ts"
  }
}
```

### Local path dependency

```json
{
  "dependencies": {
    "@migueleliasweb/ksa-client": "file:../kubernetes-state-aggregation/clients/ts"
  }
}
```

## Quick Start (Browser / React / Vue / Vanilla)

```typescript
import { createKSAClient, ResourceInfo } from "@migueleliasweb/ksa-client";

// Initialize client pointing to your KSA server
const client = createKSAClient("http://localhost:50051");

// Fetch dependency graph for a root resource
async function loadGraph() {
  const response = await client.fetchResourceGraph({
    root: new ResourceInfo({
      clusterName: "us-east-1",
      kind: "Deployment",
      namespace: "default",
      name: "frontend",
    }),
  });

  console.log("Graph resources:", response.items);
}

// List resources
async function listPods() {
  const response = await client.listResources({
    filter: new ResourceInfo({
      kind: "Pod",
      namespace: "default",
    }),
  });

  console.log("Found pods:", response.items);
}
```
