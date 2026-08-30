#!/usr/bin/env bash
set -e

# Change to the root of the project
cd "$(dirname "$0")/.."

echo "Starting PostgreSQL via docker-compose..."
docker-compose -f hack/docker-compose.yaml up -d

echo "Waiting for PostgreSQL to be ready..."
sleep 3 # Simple wait; adjust if Postgres takes longer to boot on your machine

echo "Starting KSA Server (Sync Worker & gRPC API)..."
export DB_URL="postgres://postgres:password@localhost:5432/ksa?sslmode=disable"

# Run the server natively so we can use local kubeconfig easily
go run ./cmd/server --config hack/config.yaml --db-url "${DB_URL}" --log-level debug --listen-addr :50051
