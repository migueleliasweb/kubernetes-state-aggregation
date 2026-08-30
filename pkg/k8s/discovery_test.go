package k8s

import (
	"testing"

	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/config"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	fakediscovery "k8s.io/client-go/discovery/fake"
	testingk8s "k8s.io/client-go/testing"
)

func TestDiscoverWatchableResources(t *testing.T) {
	fakeDiscovery := &fakediscovery.FakeDiscovery{
		Fake: &testingk8s.Fake{
			Resources: []*metav1.APIResourceList{
				{
					GroupVersion: "v1",
					APIResources: []metav1.APIResource{
						{Name: "pods", Kind: "Pod", Namespaced: true, Verbs: metav1.Verbs{"get", "list", "watch"}},
						{Name: "pods/status", Kind: "Pod", Namespaced: true, Verbs: metav1.Verbs{"get", "patch"}},
						{Name: "events", Kind: "Event", Namespaced: true, Verbs: metav1.Verbs{"get", "list", "watch"}},
					},
				},
				{
					GroupVersion: "apps/v1",
					APIResources: []metav1.APIResource{
						{Name: "deployments", Kind: "Deployment", Namespaced: true, Verbs: metav1.Verbs{"get", "list", "watch"}},
					},
				},
			},
		},
	}

	filters := config.FilterConfig{
		ExcludeResources: []string{"events"},
	}

	resources, err := DiscoverWatchableResources(fakeDiscovery, filters)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resources) != 2 {
		t.Fatalf("expected 2 resources (pods and deployments), got %d", len(resources))
	}

	expectedMap := map[schema.GroupVersionResource]string{
		{Group: "", Version: "v1", Resource: "pods"}:            "Pod",
		{Group: "apps", Version: "v1", Resource: "deployments"}: "Deployment",
	}

	for _, res := range resources {
		expectedKind, exists := expectedMap[res.GVR]
		if !exists {
			t.Errorf("unexpected GVR discovered: %v", res.GVR)
		}
		if res.Kind != expectedKind {
			t.Errorf("expected kind %s for %v, got %s", expectedKind, res.GVR, res.Kind)
		}
		if !res.Namespaced {
			t.Errorf("expected %v to be namespaced", res.GVR)
		}
	}
}
