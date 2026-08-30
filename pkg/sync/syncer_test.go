package sync

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/config"
	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/datastore/memory"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	fakediscovery "k8s.io/client-go/discovery/fake"
	fakedynamic "k8s.io/client-go/dynamic/fake"
	testingk8s "k8s.io/client-go/testing"
)

func TestClusterSyncerWithFakeClients(t *testing.T) {
	scheme := runtime.NewScheme()

	podGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}

	initialPod := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]interface{}{
				"namespace":       "default",
				"name":            "pod-1",
				"uid":             "pod-1-uid",
				"resourceVersion": "100",
			},
		},
	}

	// Make fake dynamic client
	fakeDynClient := fakedynamic.NewSimpleDynamicClient(scheme, initialPod)

	// In client-go v0.31.0+, Reflector defaults to using streaming WatchLists instead of traditional List+Watch.
	// fakedynamic's FakeWatcher does not correctly simulate WatchList semantics (e.g. emitting Bookmark events),
	// which causes cache.WaitForCacheSync to block indefinitely.
	// We inject a reactor here that forces Reflector to fall back to the standard List+Watch by rejecting SendInitialEvents.
	fakeDynClient.PrependWatchReactor("*", func(action testingk8s.Action) (handled bool, ret watch.Interface, err error) {
		if watchAction, ok := action.(testingk8s.WatchActionImpl); ok {
			if watchAction.ListOptions.SendInitialEvents != nil && *watchAction.ListOptions.SendInitialEvents {
				return true, nil, fmt.Errorf("fake client does not support watch lists")
			}
		}
		return false, nil, nil
	})

	fakeDiscovery := &fakediscovery.FakeDiscovery{
		Fake: &testingk8s.Fake{
			Resources: []*metav1.APIResourceList{
				{
					GroupVersion: "v1",
					APIResources: []metav1.APIResource{
						{Name: "pods", Namespaced: true, Verbs: metav1.Verbs{"get", "list", "watch"}},
					},
				},
			},
		},
	}

	mockStore := memory.NewSyncer()

	clusterCfg := config.ClusterConfig{
		Name: "us1",
	}
	filters := config.FilterConfig{
		IncludeNamespaces: []string{"default"},
	}

	syncer := NewClusterSyncer(clusterCfg, filters, mockStore, fakeDynClient, fakeDiscovery)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = syncer.Start(ctx)
	}()

	// Wait for cache sync using a custom timeout wrapper to prevent test hanging if it fails
	syncedCh := make(chan bool)
	go func() {
		// We use a small polling loop internally in case the internal controllers aren't registered yet
		for i := 0; i < 50; i++ {
			if syncer.HasSynced() {
				syncedCh <- true
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		syncedCh <- false
	}()

	select {
	case synced := <-syncedCh:
		if !synced {
			t.Fatalf("syncer failed to sync cache within 5 seconds")
		}
	case <-time.After(6 * time.Second):
		t.Fatalf("timed out waiting for cache sync (HasSynced blocked indefinitely)")
	}

	res := mockStore.GetResource("us1", "", "v1", "Pod", "default", "pod-1")
	if res == nil {
		t.Fatalf("expected pod-1 to be synced to mock store")
	}
	if res.GetName() != "pod-1" {
		t.Errorf("expected pod name pod-1, got %s", res.GetName())
	}

	// Create new pod dynamically
	newPod := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]interface{}{
				"namespace":       "default",
				"name":            "pod-2",
				"uid":             "pod-2-uid",
				"resourceVersion": "101",
			},
		},
	}

	_, err := fakeDynClient.Resource(podGVR).Namespace("default").Create(ctx, newPod, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create pod-2 in fake dynamic client: %v", err)
	}

	// Wait for pod-2 to appear in the mock store
	foundPod2 := false
	for i := 0; i < 20; i++ {
		res2 := mockStore.GetResource("us1", "", "v1", "Pod", "default", "pod-2")
		if res2 != nil {
			foundPod2 = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !foundPod2 {
		t.Fatalf("expected pod-2 to be synced to mock store after create event")
	}

	// Delete pod-1
	err = fakeDynClient.Resource(podGVR).Namespace("default").Delete(ctx, "pod-1", metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("failed to delete pod-1: %v", err)
	}

	// Wait for pod-1 to disappear from the mock store
	deletedPod1 := false
	for i := 0; i < 20; i++ {
		if mockStore.GetResource("us1", "", "v1", "Pod", "default", "pod-1") == nil {
			deletedPod1 = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !deletedPod1 {
		t.Errorf("expected pod-1 to be deleted from mock store")
	}
}

// The controller test can be flaky with fakedynamic.
// We test the logic of filtering instead.
func TestClusterSyncerFilters(t *testing.T) {
	filters := config.FilterConfig{
		IncludeNamespaces:    []string{"default"},
		IncludeClusterScoped: true,
	}

	if !filters.MatchesNamespace("default") {
		t.Errorf("expected default to match")
	}

	if filters.MatchesNamespace("kube-system") {
		t.Errorf("expected kube-system to not match")
	}

	if !filters.MatchesNamespace("") {
		t.Errorf("expected cluster scoped (empty namespace) to match when IncludeClusterScoped is true")
	}
}
