package memory

import (
	"context"
	"errors"
	"testing"

	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/datastore"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestMemorySyncerOperations(t *testing.T) {
	ctx := context.Background()
	var syncer datastore.Syncer = NewSyncer()

	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "",
		Version: "v1",
		Kind:    "Pod",
	})
	u.SetNamespace("default")
	u.SetName("test-pod")
	u.SetUID("12345")
	u.SetResourceVersion("1")

	if err := syncer.UpsertResource(ctx, "cluster-1", u); err != nil {
		t.Fatalf("failed to upsert resource: %v", err)
	}

	fetcher := syncer.(datastore.Fetcher)
	retrieved, err := fetcher.GetResource(ctx, datastore.ResourceInfo{
		ClusterName: "cluster-1",
		Kind:        "Pod",
		Name:        "test-pod",
		Namespace:   "default",
	})
	if err != nil {
		t.Fatalf("expected resource to be found in store: %v", err)
	}

	if retrieved.Name != "test-pod" {
		t.Errorf("expected pod name test-pod, got %s", retrieved.Name)
	}

	list, err := fetcher.ListResources(ctx, datastore.ResourceInfo{
		Kind: "Pod",
	})
	if err != nil || len(list) != 1 {
		t.Fatalf("expected 1 listed pod, got %d (err: %v)", len(list), err)
	}

	if err := syncer.DeleteResource(
		ctx,
		datastore.ResourceInfo{
			ClusterName: "cluster-1",
			Group:       "",
			Version:     "v1",
			Kind:        "Pod",
			Namespace:   "default",
			Name:        "test-pod",
		},
	); err != nil {
		t.Fatalf("failed to delete resource: %v", err)
	}

	_, err = fetcher.GetResource(ctx, datastore.ResourceInfo{
		ClusterName: "cluster-1",
		Kind:        "Pod",
		Name:        "test-pod",
		Namespace:   "default",
	})
	if !errors.Is(err, datastore.ErrNotFound) {
		t.Errorf("expected ErrNotFound after deletion, got %v", err)
	}
}
