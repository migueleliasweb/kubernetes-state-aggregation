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
						{Name: "pods", Namespaced: true, Verbs: metav1.Verbs{"get", "list", "watch"}},
						{Name: "pods/status", Namespaced: true, Verbs: metav1.Verbs{"get", "patch"}},
						{Name: "events", Namespaced: true, Verbs: metav1.Verbs{"get", "list", "watch"}},
					},
				},
				{
					GroupVersion: "apps/v1",
					APIResources: []metav1.APIResource{
						{Name: "deployments", Namespaced: true, Verbs: metav1.Verbs{"get", "list", "watch"}},
					},
				},
			},
		},
	}

	filters := config.FilterConfig{
		ExcludeResources: []string{"events"},
	}

	gvrs, err := DiscoverWatchableResources(fakeDiscovery, filters)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(gvrs) != 2 {
		t.Fatalf("expected 2 GVRs (pods and deployments), got %d", len(gvrs))
	}

	expectedMap := map[schema.GroupVersionResource]bool{
		{Group: "", Version: "v1", Resource: "pods"}:            true,
		{Group: "apps", Version: "v1", Resource: "deployments"}: true,
	}

	for _, gvr := range gvrs {
		if !expectedMap[gvr] {
			t.Errorf("unexpected GVR discovered: %v", gvr)
		}
	}
}
