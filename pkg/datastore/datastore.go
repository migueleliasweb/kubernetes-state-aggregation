package datastore

import (
	"context"
	"encoding/json"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// ResourceRecord represents a row in the resources table.
type ResourceRecord struct {
	ClusterName     string          `json:"cluster_name"`
	GroupName       string          `json:"group_name"`
	Version         string          `json:"version"`
	Kind            string          `json:"kind"`
	Namespace       string          `json:"namespace"`
	Name            string          `json:"name"`
	UID             string          `json:"uid"`
	ResourceVersion string          `json:"resource_version"`
	Labels          json.RawMessage `json:"labels"`
	Manifest        json.RawMessage `json:"manifest"`
	UpdatedAt       time.Time       `json:"updated_at"`
}

// Syncer defines operations for synchronizing Kubernetes state to a datastore.
type Syncer interface {
	InitSchema(ctx context.Context) error

	UpsertResource(
		ctx context.Context,
		clusterName string,
		u *unstructured.Unstructured,
	) error

	DeleteResource(
		ctx context.Context,
		clusterName string,
		group string,
		version string,
		kind string,
		namespace string,
		name string,
	) error

	Close() error
}

// Fetcher defines operations for querying aggregated Kubernetes state.
type Fetcher interface {
	QueryResources(ctx context.Context) ([]ResourceRecord, error)
}
