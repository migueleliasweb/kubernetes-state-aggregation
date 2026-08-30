package sync

import (
	"context"
	"testing"

	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/config"
	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/datastore/memory"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestRunStartupCleanup(t *testing.T) {
	ctx := context.Background()
	store := memory.NewSyncer()

	// 1. Setup initial resources in store
	// Cluster "us-east":
	// - pod in default (keep)
	// - pod in kube-system (should be purged by exclude_namespaces)
	// - event in default (should be purged by exclude_resources)
	// - clusterrole (cluster-scoped, should be purged if include_cluster_scoped is false)
	podUsDefault := &unstructured.Unstructured{}
	podUsDefault.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"})
	podUsDefault.SetNamespace("default")
	podUsDefault.SetName("web")
	podUsDefault.SetUID("uid-1")

	podUsKubeSystem := &unstructured.Unstructured{}
	podUsKubeSystem.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"})
	podUsKubeSystem.SetNamespace("kube-system")
	podUsKubeSystem.SetName("coredns")
	podUsKubeSystem.SetUID("uid-2")

	eventUs := &unstructured.Unstructured{}
	eventUs.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Event"})
	eventUs.SetNamespace("default")
	eventUs.SetName("pod-event")
	eventUs.SetUID("uid-3")

	crUs := &unstructured.Unstructured{}
	crUs.SetGroupVersionKind(schema.GroupVersionKind{Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "ClusterRole"})
	crUs.SetName("admin")
	crUs.SetUID("uid-4")

	// Cluster "disabled-cluster":
	// - pod in default (should be purged entirely)
	podDisabled := &unstructured.Unstructured{}
	podDisabled.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"})
	podDisabled.SetNamespace("default")
	podDisabled.SetName("old-pod")
	podDisabled.SetUID("uid-5")

	// Cluster "removed-cluster":
	// - pod in default (should be purged entirely)
	podRemoved := &unstructured.Unstructured{}
	podRemoved.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"})
	podRemoved.SetNamespace("default")
	podRemoved.SetName("stale-pod")
	podRemoved.SetUID("uid-6")

	_ = store.UpsertResource(ctx, "us-east", podUsDefault)
	_ = store.UpsertResource(ctx, "us-east", podUsKubeSystem)
	_ = store.UpsertResource(ctx, "us-east", eventUs)
	_ = store.UpsertResource(ctx, "us-east", crUs)
	_ = store.UpsertResource(ctx, "disabled-cluster", podDisabled)
	_ = store.UpsertResource(ctx, "removed-cluster", podRemoved)

	// 2. Configure active clusters and filters
	cfg := &config.Config{
		GlobalFilters: config.FilterConfig{
			ExcludeNamespaces:    []string{"kube-system"},
			ExcludeResources:     []string{"events"},
			IncludeClusterScoped: false,
		},
		Clusters: []config.ClusterConfig{
			{
				Name:     "us-east",
				Disabled: false,
			},
			{
				Name:     "disabled-cluster",
				Disabled: true,
			},
		},
	}

	// 3. Run startup cleanup
	if err := RunStartupCleanup(ctx, cfg, store, ""); err != nil {
		t.Fatalf("RunStartupCleanup failed: %v", err)
	}

	// 4. Verify results
	// Disabled and removed clusters should be purged completely
	clusters, err := store.ListClusters(ctx)
	if err != nil {
		t.Fatalf("ListClusters failed: %v", err)
	}
	if len(clusters) != 1 || clusters[0] != "us-east" {
		t.Errorf("expected only us-east cluster in datastore, got %v", clusters)
	}

	// Check remaining keys in us-east
	keysUs, err := store.ListAllResourceKeys(ctx, "us-east")
	if err != nil {
		t.Fatalf("ListAllResourceKeys failed: %v", err)
	}

	if len(keysUs) != 1 {
		t.Fatalf("expected exactly 1 remaining resource in us-east, got %d: %+v", len(keysUs), keysUs)
	}

	if keysUs[0].Name != "web" || keysUs[0].Namespace != "default" || keysUs[0].Kind != "Pod" {
		t.Errorf("unexpected remaining resource: %+v", keysUs[0])
	}
}
