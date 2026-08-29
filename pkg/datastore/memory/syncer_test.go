package memory

import (
	"context"
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

	memSyncer := syncer.(*Syncer)
	retrieved := memSyncer.GetResource(
		"cluster-1",
		"",
		"v1",
		"Pod",
		"default",
		"test-pod",
	)

	if retrieved == nil {
		t.Fatalf("expected resource to be found in store")
	}

	if retrieved.GetName() != "test-pod" {
		t.Errorf("expected pod name test-pod, got %s", retrieved.GetName())
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

	if memSyncer.GetResource(
		"cluster-1",
		"",
		"v1",
		"Pod",
		"default",
		"test-pod",
	) != nil {
		t.Errorf("expected resource to be deleted from store")
	}
}
