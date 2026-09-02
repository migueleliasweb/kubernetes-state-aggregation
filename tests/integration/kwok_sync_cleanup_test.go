package integration

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/migueleliasweb/kubernetes-state-aggregation/pkg/api/v1"
	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/api/v1/v1connect"
	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/config"
	ksaServer "github.com/migueleliasweb/kubernetes-state-aggregation/pkg/server"
	ksaSync "github.com/migueleliasweb/kubernetes-state-aggregation/pkg/sync"
	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/testenv"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

func TestKWOKStartupCleanupAndSync(t *testing.T) {
	if os.Getenv("KSA_INTEGRATION_TEST") != "true" {
		t.Skip("Skipping KWOK integration test; set KSA_INTEGRATION_TEST=true to run")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// 1. Provision clusters, seed workloads, and launch PostgreSQL via testenv.Setup
	suffix := rand.Intn(100000)
	c1Name := fmt.Sprintf("ksa-kwok-c1-%d", suffix)
	c2Name := fmt.Sprintf("ksa-kwok-c2-%d", suffix)

	scaleMode := "default"
	if os.Getenv("KSA_TEST_SCALE") == "benchmark" {
		scaleMode = "benchmark"
	}

	tmpDir := t.TempDir()

	env, err := testenv.Setup(ctx, testenv.SetupOptions{
		Clusters:         []string{c1Name, c2Name},
		Seed:             true,
		Scale:            scaleMode,
		OutputConfigPath: filepath.Join(tmpDir, "kwok-config.yaml"),
	})
	if err != nil {
		t.Fatalf("Failed to setup test environment: %v", err)
	}

	store := env.Store

	var apiServer *ksaServer.Server
	var apiAddr string

	t.Cleanup(func() {
		if t.Failed() {
			if apiAddr != "" {
				env.PrintDebugInfo(t, fmt.Sprintf("KSA Server API Address: %s", apiAddr))
			} else {
				env.PrintDebugInfo(t)
			}

			return
		}

		if apiServer != nil {
			apiServer.GracefulStop()
		}

		if err := env.Teardown(context.Background()); err != nil {
			t.Logf("Teardown warning: %v", err)
		}
	})

	// 3. PHASE 1: Initial Sync with both clusters enabled
	t.Log("Starting Phase 1: Initial multi-cluster sync...")

	cfgPhase1 := &config.Config{
		GlobalFilters: config.FilterConfig{
			IncludeClusterScoped: true,
		},
		Clusters: []config.ClusterConfig{
			{
				Name:       c1Name,
				Kubeconfig: env.Clusters[c1Name].KubeconfigPath,
				Disabled:   false,
			},
			{
				Name:       c2Name,
				Kubeconfig: env.Clusters[c2Name].KubeconfigPath,
				Disabled:   false,
			},
		},
	}

	mgrCtx1, mgrCancel1 := context.WithCancel(ctx)

	manager1 := ksaSync.NewManager(
		cfgPhase1,
		store,
		"",
	)

	go func() {
		_ = manager1.Start(mgrCtx1)
	}()

	// Wait for datastore to populate both clusters
	assertEventually(
		t,
		30*time.Second,
		500*time.Millisecond,
		"Phase 1: Datastore contains both clusters with synced resources",
		func() bool {
			clusters, err := store.ListClusters(ctx)
			if err != nil || len(clusters) < 2 {
				return false
			}

			keys1, err1 := store.ListAllResourceKeys(ctx, c1Name)
			keys2, err2 := store.ListAllResourceKeys(ctx, c2Name)

			return err1 == nil && err2 == nil && len(keys1) > 50 && len(keys2) > 50
		},
	)

	// Start KSA API server and assert API queryability
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to start listener: %v", err)
	}

	apiAddr = lis.Addr().String()
	apiServer = ksaServer.NewServer(
		store,
		lis,
	)

	go func() {
		_ = apiServer.Serve()
	}()

	apiClient := v1connect.NewStateServiceClient(
		http.DefaultClient,
		fmt.Sprintf("http://%s", apiAddr),
	)

	// Query via API client
	listResp, err := apiClient.ListResources(
		ctx,
		connect.NewRequest(&v1.ListResourcesRequest{
			Filter: &v1.ResourceInfo{
				ClusterName: c1Name,
				Kind:        "Pod",
				Namespace:   "prod",
			},
		}),
	)
	if err != nil {
		t.Fatalf("API ListResources failed in Phase 1: %v", err)
	}

	if len(listResp.Msg.GetItems()) == 0 {
		t.Fatalf("Expected prod pods via API in Phase 1, got 0")
	}

	t.Logf("Phase 1 verified: found %d pods in %s/prod via API", len(listResp.Msg.GetItems()), c1Name)

	// 6. PHASE 2: Reconfigure filters and disable cluster 2
	t.Log("Starting Phase 2: Stopping sync manager and mutating configuration...")

	mgrCancel1()
	time.Sleep(500 * time.Millisecond)

	cfgPhase2 := &config.Config{
		GlobalFilters: config.FilterConfig{
			ExcludeNamespaces:    []string{"kube-system", "monitoring"},
			ExcludeResources:     []string{"configmaps"},
			IncludeClusterScoped: false,
		},
		Clusters: []config.ClusterConfig{
			{
				Name:       c1Name,
				Kubeconfig: env.Clusters[c1Name].KubeconfigPath,
				Disabled:   false,
			},
			{
				Name:       c2Name,
				Kubeconfig: env.Clusters[c2Name].KubeconfigPath,
				Disabled:   true, // Disabled cluster
			},
		},
	}

	// 7. PHASE 3: Restart Sync Manager and verify Pre-Cleanup
	t.Log("Starting Phase 3: Restarting sync manager to trigger RunStartupCleanup...")

	mgrCtx2, mgrCancel2 := context.WithCancel(ctx)
	defer mgrCancel2()

	manager2 := ksaSync.NewManager(
		cfgPhase2,
		store,
		"",
	)

	go func() {
		_ = manager2.Start(mgrCtx2)
	}()

	// Assert that cluster 2 was completely purged
	assertEventually(
		t,
		15*time.Second,
		500*time.Millisecond,
		"Phase 3: Disabled cluster 2 is purged from datastore",
		func() bool {
			clusters, err := store.ListClusters(ctx)
			if err != nil {
				return false
			}

			for _, cl := range clusters {
				if cl == c2Name {
					return false
				}
			}

			return true
		},
	)

	// Assert that excluded resources in cluster 1 were pruned
	assertEventually(
		t,
		15*time.Second,
		500*time.Millisecond,
		"Phase 3: Excluded namespaces, kinds, and cluster-scoped resources are pruned from cluster 1",
		func() bool {
			keys, err := store.ListAllResourceKeys(ctx, c1Name)
			if err != nil || len(keys) == 0 {
				return false
			}

			for _, k := range keys {
				// No kube-system or monitoring
				if k.Namespace == "kube-system" || k.Namespace == "monitoring" {
					return false
				}

				// No cluster-scoped resources
				if k.Namespace == "" {
					return false
				}

				// No configmaps
				if k.Kind == "ConfigMap" {
					return false
				}
			}

			return true
		},
	)

	// Verify via API that pruned state matches
	listKubeSystemResp, err := apiClient.ListResources(
		ctx,
		connect.NewRequest(&v1.ListResourcesRequest{
			Filter: &v1.ResourceInfo{
				ClusterName: c1Name,
				Namespace:   "kube-system",
			},
		}),
	)
	if err != nil {
		t.Fatalf("API query failed in Phase 3: %v", err)
	}

	if len(listKubeSystemResp.Msg.GetItems()) != 0 {
		t.Fatalf("Expected 0 kube-system resources after cleanup, got %d", len(listKubeSystemResp.Msg.GetItems()))
	}

	listProdResp, err := apiClient.ListResources(
		ctx,
		connect.NewRequest(&v1.ListResourcesRequest{
			Filter: &v1.ResourceInfo{
				ClusterName: c1Name,
				Namespace:   "prod",
				Kind:        "Pod",
			},
		}),
	)
	if err != nil {
		t.Fatalf("API query for prod pods failed in Phase 3: %v", err)
	}

	if len(listProdResp.Msg.GetItems()) == 0 {
		t.Fatalf("Expected valid prod pods to remain intact after cleanup, got 0")
	}

	t.Logf("Phase 3 cleanup verified: %d prod pods intact, 0 kube-system resources, cluster 2 purged", len(listProdResp.Msg.GetItems()))

	// 8. Test live synchronization of a new resource
	t.Log("Testing live dynamic synchronization after pre-cleanup...")

	dynClient1, err := dynamic.NewForConfig(env.Clusters[c1Name].RESTConfig)
	if err != nil {
		t.Fatalf("Failed to create dynamic client for %s: %v", c1Name, err)
	}

	newPod := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"name":      "live-sync-test-pod",
				"namespace": "prod",
				"labels": map[string]any{
					"live-test": "true",
				},
			},
			"spec": map[string]any{
				"containers": []any{
					map[string]any{
						"name":  "live-container",
						"image": "nginx:alpine",
					},
				},
			},
		},
	}

	podGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}

	_, err = dynClient1.Resource(podGVR).Namespace("prod").Create(ctx, newPod, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create live sync test pod: %v", err)
	}

	assertEventually(
		t,
		15*time.Second,
		500*time.Millisecond,
		"Live sync: new pod appears via API",
		func() bool {
			getResp, err := apiClient.GetResource(
				ctx,
				connect.NewRequest(&v1.GetResourceRequest{
					Info: &v1.ResourceInfo{
						ClusterName: c1Name,
						Namespace:   "prod",
						Kind:        "Pod",
						Name:        "live-sync-test-pod",
					},
				}),
			)

			return err == nil && getResp.Msg.GetRecord() != nil && getResp.Msg.GetRecord().GetName() == "live-sync-test-pod"
		},
	)

	t.Log("All KWOK integration test phases succeeded!")
}

func assertEventually(
	t *testing.T,
	timeout time.Duration,
	interval time.Duration,
	description string,
	condition func() bool,
) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if condition() {
			return
		}

		time.Sleep(interval)
	}

	t.Fatalf("Timed out waiting for condition: %s", description)
}
