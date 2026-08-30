#!/usr/bin/env bash
set -euo pipefail

# Change to the root of the project
cd "$(dirname "$0")/.."

export PATH="$PATH:$HOME/go/bin"

if ! command -v buf &> /dev/null; then
    echo "buf could not be found, installing..."
    go install github.com/bufbuild/buf/cmd/buf@latest
fi

echo "Running buf generate..."
buf generate

echo "Protobuf Go and TypeScript code generated successfully"
