package datastore

import (
	"context"
	"errors"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// ErrNotFound is returned when a requested resource does not exist in the datastore.
var ErrNotFound = errors.New("resource not found")

// Syncer defines operations for synchronizing Kubernetes state to a datastore.
type Syncer interface {
	// InitSchema initializes the datastore schema and indexes if they do not exist.
	InitSchema(ctx context.Context) error

	// UpsertResource inserts or updates a resource manifest and its metadata in the datastore.
	UpsertResource(
		ctx context.Context,
		clusterName string,
		u *unstructured.Unstructured, // All other information can be found within the unstructured object
	) error

	// DeleteResource removes a single resource identified by the provided ResourceInfo.
	DeleteResource(
		ctx context.Context,
		resourceInfo ResourceInfo,
	) error

	// ListResourceKeys queries basic identifiers for resources matching a specific cluster and GVK.
	ListResourceKeys(
		ctx context.Context,
		clusterName string,
		group string,
		version string,
		kind string,
	) ([]ResourceInfo, error)

	// ListAllResourceKeys queries all stored resource identifiers for a specific cluster regardless of GVK.
	ListAllResourceKeys(
		ctx context.Context,
		clusterName string,
	) ([]ResourceInfo, error)

	// ListClusters queries all distinct cluster names present in the datastore.
	ListClusters(ctx context.Context) ([]string, error)

	// DeleteCluster removes all stored resources for a cluster and returns the deleted count.
	DeleteCluster(
		ctx context.Context,
		clusterName string,
	) (int64, error)

	// BatchDeleteResources removes multiple resources in a single batch operation and returns the deleted count.
	BatchDeleteResources(
		ctx context.Context,
		resources []ResourceInfo,
	) (int64, error)

	// Close closes the underlying datastore connection pool or resources.
	Close() error
}

// Fetcher defines operations for querying aggregated Kubernetes state.
type Fetcher interface {
	// FetchResourceGraph queries for a whole resource graph starting from a rootResourceInfo.
	//
	// The traversal is controlled by the WalkAction returned by the callback, allowing callers to
	// include/exclude nodes from the returned collection and control whether children are traversed.
	// Returning an error from the callback will stop the traversal and return the error.
	FetchResourceGraph(
		ctx context.Context,
		rootResourceInfo ResourceInfo,
		callback ResourceCallback,
	) (*UniqueResourceCollection, error)

	// GetResource queries for a single resource matching the given ResourceInfo.
	// Returns ErrNotFound if no match is found, or an error if multiple resources match.
	GetResource(
		ctx context.Context,
		info ResourceInfo,
	) (*ResourceRecord, error)

	// ListResources queries for all resources matching the specified non-empty fields in filter.
	ListResources(
		ctx context.Context,
		filter ResourceInfo,
	) ([]ResourceRecord, error)
}
