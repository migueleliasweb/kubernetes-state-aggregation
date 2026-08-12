package db

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestMockStoreOperations(t *testing.T) {
	ctx := context.Background()
	store := NewMockStore()

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

	if err := store.UpsertResource(ctx, "cluster-1", u); err != nil {
		t.Fatalf("failed to upsert resource: %v", err)
	}

	retrieved := store.GetResource("cluster-1", "", "v1", "Pod", "default", "test-pod")
	if retrieved == nil {
		t.Fatalf("expected resource to be found in store")
	}
	if retrieved.GetName() != "test-pod" {
		t.Errorf("expected pod name test-pod, got %s", retrieved.GetName())
	}

	if err := store.DeleteResource(ctx, "cluster-1", "", "v1", "Pod", "default", "test-pod"); err != nil {
		t.Fatalf("failed to delete resource: %v", err)
	}

	if store.GetResource("cluster-1", "", "v1", "Pod", "default", "test-pod") != nil {
		t.Errorf("expected resource to be deleted from store")
	}
}
