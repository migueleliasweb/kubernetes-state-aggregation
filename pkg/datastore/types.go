package datastore

import (
	"context"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Syncer defines operations for synchronizing Kubernetes state to a datastore.
type Syncer interface {
	InitSchema(ctx context.Context) error

	UpsertResource(
		ctx context.Context,
		clusterName string,
		u *unstructured.Unstructured, // All other information can be found within the unstructured object
	) error

	DeleteResource(
		ctx context.Context,
		resourceInfo ResourceInfo,
	) error

	ListResourceKeys(
		ctx context.Context,
		clusterName string,
		group string,
		version string,
		kind string,
	) ([]ResourceInfo, error)

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
}
