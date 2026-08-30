package datastore

import (
	"context"
	"encoding/json"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// ResourceRecord represents a row in the resources table.
type ResourceRecord struct {
	Annotations     json.RawMessage            `json:"annotations,omitzero"`
	ClusterName     string                     `json:"clusterName,omitzero"`
	GroupName       string                     `json:"groupName,omitzero"`
	Kind            string                     `json:"kind,omitzero"`
	Labels          json.RawMessage            `json:"labels,omitzero"`
	Manifest        json.RawMessage            `json:"manifest,omitzero"`
	Name            string                     `json:"name,omitzero"`
	Namespace       string                     `json:"namespace,omitzero"`
	RawObject       *unstructured.Unstructured `json:"rawObject,omitzero"`
	ResourceVersion string                     `json:"resourceVersion,omitzero"`
	UID             string                     `json:"uid,omitzero"`
	UpdatedAt       time.Time                  `json:"updatedAt,omitzero"`
	Version         string                     `json:"version,omitzero"`
}

// GetResourceInfo Derives a ResourceInfo from a given ResourceRecord
func (rr *ResourceRecord) GetResourceInfo() ResourceInfo {
	return ResourceInfo{
		ClusterName:     rr.ClusterName,
		Group:           rr.GroupName,
		Kind:            rr.Kind,
		Name:            rr.Name,
		Namespace:       rr.Namespace,
		ResourceVersion: rr.ResourceVersion,
		UID:             rr.UID,
		Version:         rr.Version,
	}
}

// ResourceInfo Defines the base information to determine the
// uniqueness of a given resource in KSA.
type ResourceInfo struct {
	ClusterName     string `json:"clusterName,omitzero"`
	Group           string `json:"group,omitzero"`
	Kind            string `json:"kind,omitzero"`
	Name            string `json:"name,omitzero"`
	Namespace       string `json:"namespace,omitzero"`
	ResourceVersion string `json:"resourceVersion,omitzero"`
	UID             string `json:"uid,omitzero"`
	Version         string `json:"version,omitzero"`
}

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

// WalkAction represents the action to take after visiting a node in the resource graph.
type WalkAction string

const (
	// ActionInclude adds the node to the collection and continues traversing its children.
	ActionInclude WalkAction = "Include"
	// ActionExclude does not add the node to the collection, but continues traversing its children.
	ActionExclude WalkAction = "Exclude"
	// ActionIncludeAndSkipChildren adds the node to the collection, but stops traversing its children.
	ActionIncludeAndSkipChildren WalkAction = "IncludeAndSkipChildren"
	// ActionExcludeAndSkipChildren does not add the node to the collection, and stops traversing its children.
	ActionExcludeAndSkipChildren WalkAction = "ExcludeAndSkipChildren"
	// ActionStop stops the entire traversal immediately.
	ActionStop WalkAction = "Stop"
)

// ResourceCallback is a function called for each node in a resource graph.
type ResourceCallback func(resourceInfo ResourceRecord) (action WalkAction, err error)

// ResourceKey is a strictly-typed string for preventing cross-cluster uniqueness collisions.
type ResourceKey string

// UniqueResourceCollection stores a collection of resources, ensuring no duplicates.
type UniqueResourceCollection struct {
	items []ResourceRecord
	seen  map[ResourceKey]bool
}

// NewUniqueResourceCollection creates a new initialized UniqueResourceCollection.
func NewUniqueResourceCollection() *UniqueResourceCollection {
	return &UniqueResourceCollection{
		items: []ResourceRecord{},
		seen:  map[ResourceKey]bool{},
	}
}

// GetResourceKey creates a composite key from a cluster name and a UID to prevent cross-cluster collisions.
func GetResourceKey(clusterName, uid string) ResourceKey {
	return ResourceKey(clusterName + "/" + uid)
}

// Add appends a ResourceRecord to the collection if it hasn't been added yet (deduped by UID).
// Returns true if the resource was successfully added (was unique).
func (c *UniqueResourceCollection) Add(r ResourceRecord) bool {
	key := GetResourceKey(r.ClusterName, r.UID)
	if c.seen[key] {
		return false
	}
	c.seen[key] = true
	c.items = append(c.items, r)
	return true
}

// Items returns the slice of unique resources in the order they were added.
func (c *UniqueResourceCollection) Items() []ResourceRecord {
	return c.items
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
