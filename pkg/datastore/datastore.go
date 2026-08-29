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
		clusterName string,
		resourceInfo ResourceInfo,
	) error

	Close() error
}

// Fetcher defines operations for querying aggregated Kubernetes state.
type Fetcher interface {
	// Queries for a whole resource graph starting from the rootResourceInfo.
	//
	// The callback argument can be used both as a processing callback but also
	// as a filter (simply by ignoring certain resources).
	//
	// Returning an error from the callback will stop the traversal.
	//
	// Returning done=true will prevent the traversal from going deeper into the resource graph.
	QueryResourceGraph(
		ctx context.Context,
		rootResourceInfo ResourceInfo,
		callback func(resourceInfo ResourceRecord) (done bool, err error),
	) error
}
