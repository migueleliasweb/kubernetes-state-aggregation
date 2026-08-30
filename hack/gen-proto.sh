#!/usr/bin/env bash
set -euo pipefail

# Change to the root of the project
cd "$(dirname "$0")/.."

mkdir -p pkg/api/v1

protoc \
  --proto_path=proto/v1 \
  --go_out=pkg/api/v1 \
  --go_opt=paths=source_relative \
  --go-grpc_out=pkg/api/v1 \
  --go-grpc_opt=paths=source_relative \
  proto/v1/state.proto

echo "Protobuf Go code generated successfully in pkg/api/v1"
