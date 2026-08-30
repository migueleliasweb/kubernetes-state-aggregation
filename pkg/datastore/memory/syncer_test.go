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

func TestMemorySyncerCleanupOperations(t *testing.T) {
	ctx := context.Background()
	syncer := NewSyncer()

	pod1 := &unstructured.Unstructured{}
	pod1.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"})
	pod1.SetNamespace("default")
	pod1.SetName("pod-1")
	pod1.SetUID("uid-1")

	pod2 := &unstructured.Unstructured{}
	pod2.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"})
	pod2.SetNamespace("kube-system")
	pod2.SetName("pod-2")
	pod2.SetUID("uid-2")

	deploy1 := &unstructured.Unstructured{}
	deploy1.SetGroupVersionKind(schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"})
	deploy1.SetNamespace("default")
	deploy1.SetName("dep-1")
	deploy1.SetUID("uid-3")

	_ = syncer.UpsertResource(ctx, "cluster-a", pod1)
	_ = syncer.UpsertResource(ctx, "cluster-a", pod2)
	_ = syncer.UpsertResource(ctx, "cluster-b", deploy1)

	// Test ListClusters
	clusters, err := syncer.ListClusters(ctx)
	if err != nil {
		t.Fatalf("ListClusters failed: %v", err)
	}
	if len(clusters) != 2 {
		t.Errorf("expected 2 clusters, got %d", len(clusters))
	}

	// Test ListAllResourceKeys
	keysA, err := syncer.ListAllResourceKeys(ctx, "cluster-a")
	if err != nil {
		t.Fatalf("ListAllResourceKeys failed: %v", err)
	}
	if len(keysA) != 2 {
		t.Errorf("expected 2 keys in cluster-a, got %d", len(keysA))
	}

	// Test BatchDeleteResources
	deleted, err := syncer.BatchDeleteResources(ctx, []datastore.ResourceInfo{
		{
			ClusterName: "cluster-a",
			Group:       "",
			Version:     "v1",
			Kind:        "Pod",
			Namespace:   "kube-system",
			Name:        "pod-2",
		},
	})
	if err != nil {
		t.Fatalf("BatchDeleteResources failed: %v", err)
	}
	if deleted != 1 {
		t.Errorf("expected 1 deleted, got %d", deleted)
	}

	keysAAfter, _ := syncer.ListAllResourceKeys(ctx, "cluster-a")
	if len(keysAAfter) != 1 {
		t.Errorf("expected 1 key remaining in cluster-a, got %d", len(keysAAfter))
	}

	// Test DeleteCluster
	deletedClusterB, err := syncer.DeleteCluster(ctx, "cluster-b")
	if err != nil {
		t.Fatalf("DeleteCluster failed: %v", err)
	}
	if deletedClusterB != 1 {
		t.Errorf("expected 1 resource deleted for cluster-b, got %d", deletedClusterB)
	}

	keysB, _ := syncer.ListAllResourceKeys(ctx, "cluster-b")
	if len(keysB) != 0 {
		t.Errorf("expected 0 keys in cluster-b, got %d", len(keysB))
	}
}
