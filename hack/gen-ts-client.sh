#!/usr/bin/env bash
set -euo pipefail

# Change to the root of the project
cd "$(dirname "$0")/.."

echo "Step 1: Generating protobuf files..."
./hack/gen-proto.sh

echo "Step 2: Building TypeScript client..."
if command -v npm &> /dev/null; then
    echo "Using local Node/npm..."
    cd clients/ts
    npm install
    npm run build
    cd ../..
else
    echo "Node/npm not found locally, building TypeScript client via Docker (node:20-alpine)..."
    docker run --rm -v "$(pwd)/clients/ts:/app" -w /app node:20-alpine sh -c "npm install && npm run build"
fi

echo "TypeScript client generated and built successfully in clients/ts/dist"
