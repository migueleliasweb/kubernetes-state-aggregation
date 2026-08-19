package sync

import (
	"context"
	"testing"
	"time"

	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/config"
	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/datastore/memory"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
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

	fakeDynClient := fakedynamic.NewSimpleDynamicClient(scheme, initialPod)

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

	go func() {
		_ = syncer.Start(ctx)
	}()

	// Allow informers to initialize and process initial add event
	time.Sleep(200 * time.Millisecond)

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

	time.Sleep(200 * time.Millisecond)

	res2 := mockStore.GetResource("us1", "", "v1", "Pod", "default", "pod-2")
	if res2 == nil {
		t.Fatalf("expected pod-2 to be synced to mock store after create event")
	}

	// Delete pod-1
	err = fakeDynClient.Resource(podGVR).Namespace("default").Delete(ctx, "pod-1", metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("failed to delete pod-1: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	if mockStore.GetResource("us1", "", "v1", "Pod", "default", "pod-1") != nil {
		t.Errorf("expected pod-1 to be deleted from mock store")
	}

	cancel()
}
