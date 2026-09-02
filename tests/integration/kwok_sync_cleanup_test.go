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
	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/datastore/postgres"
	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/kwok"
	ksaServer "github.com/migueleliasweb/kubernetes-state-aggregation/pkg/server"
	ksaSync "github.com/migueleliasweb/kubernetes-state-aggregation/pkg/sync"
	tcPostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
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

	// 1. Provision clusters and seed workloads via centralized kwok.SetupClusters
	suffix := rand.Intn(100000)
	c1Name := fmt.Sprintf("ksa-kwok-c1-%d", suffix)
	c2Name := fmt.Sprintf("ksa-kwok-c2-%d", suffix)

	scaleMode := "default"
	if os.Getenv("KSA_TEST_SCALE") == "benchmark" {
		scaleMode = "benchmark"
	}

	tmpDir := t.TempDir()

	env, err := kwok.SetupClusters(ctx, kwok.SetupOptions{
		Clusters:         []string{c1Name, c2Name},
		Seed:             true,
		Scale:            scaleMode,
		OutputConfigPath: filepath.Join(tmpDir, "kwok-config.yaml"),
	})
	if err != nil {
		t.Fatalf("Failed to setup KWOK test clusters: %v", err)
	}

	// 2. Setup PostgreSQL datastore (via testcontainers or existing DB_URL)
	dbURL := os.Getenv("DB_URL")
	var pgContainer *tcPostgres.PostgresContainer

	if dbURL == "" {
		t.Log("Starting ephemeral PostgreSQL container via testcontainers...")

		container, err := tcPostgres.Run(
			ctx,
			"postgres:15-alpine",
			tcPostgres.WithDatabase("ksa"),
			tcPostgres.WithUsername("postgres"),
			tcPostgres.WithPassword("password"),
			tcPostgres.BasicWaitStrategies(),
		)
		if err != nil {
			t.Fatalf("Failed to start PostgreSQL testcontainer: %v", err)
		}

		pgContainer = container

		connStr, err := container.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			t.Fatalf("Failed to get connection string for PostgreSQL testcontainer: %v", err)
		}

		dbURL = connStr
	}

	t.Logf("Connecting to PostgreSQL: %s", dbURL)

	store, err := postgres.NewPGSyncer(dbURL)
	if err != nil {
		t.Fatalf("Failed to connect to Postgres: %v", err)
	}

	if err := store.InitSchema(ctx); err != nil {
		t.Fatalf("Failed to init Postgres schema: %v", err)
	}

	var apiServer *ksaServer.Server
	var apiAddr string

	t.Cleanup(func() {
		if t.Failed() {
			t.Log("\n=================================================================")
			t.Log("⚠️ TEST FAILED: Preserving test environment for inspection!")
			t.Log("=================================================================")
			t.Logf("PostgreSQL DB_URL: %s", dbURL)
			if apiAddr != "" {
				t.Logf("KSA Server API Address: %s", apiAddr)
			}
			t.Logf("KSA Config Path: %s", env.ConfigPath)
			t.Log("To inspect KWOK clusters with kubectl:")
			t.Logf("  export KUBECONFIG=%s:%s", env.Clusters[c1Name].KubeconfigPath, env.Clusters[c2Name].KubeconfigPath)
			t.Log("  kubectl get nodes")
			t.Log("  kubectl get pods -A")
			t.Log("=================================================================")

			return
		}

		if apiServer != nil {
			apiServer.GracefulStop()
		}

		_ = store.Close()

		_ = kwok.TeardownClusters(context.Background(), kwok.TeardownOptions{
			Clusters:         []string{c1Name, c2Name},
			OutputConfigPath: env.ConfigPath,
		})

		if pgContainer != nil {
			_ = pgContainer.Terminate(context.Background())
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
