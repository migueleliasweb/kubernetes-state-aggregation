package k8s

import (
	"fmt"
	"strings"

	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/config"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
)

// DiscoveredResource represents a watchable Kubernetes API resource discovered from the server.
type DiscoveredResource struct {
	GVR        schema.GroupVersionResource
	Kind       string
	Namespaced bool
}

// DiscoverWatchableResources queries the API server for preferred API resources and filters watchable GVRs.
func DiscoverWatchableResources(
	discoveryClient discovery.DiscoveryInterface,
	filters config.FilterConfig,
) ([]DiscoveredResource, error) {
	apiResourceLists, err := discoveryClient.ServerPreferredResources()
	if err != nil || len(apiResourceLists) == 0 {
		var err2 error
		_, apiResourceLists, err2 = discoveryClient.ServerGroupsAndResources()
		if err2 != nil && len(apiResourceLists) == 0 {
			if err != nil {
				return nil, fmt.Errorf("failed to discover server resources: %w", err)
			}
			return nil, fmt.Errorf("failed to discover server resources: %w", err2)
		}
	}

	var resources []DiscoveredResource
	for _, apiList := range apiResourceLists {
		gv, err := schema.ParseGroupVersion(apiList.GroupVersion)
		if err != nil {
			continue
		}

		for _, apiRes := range apiList.APIResources {
			// Subresources (e.g. pods/status, deployments/scale) contain a slash
			if strings.Contains(apiRes.Name, "/") {
				continue
			}

			// Must support the "watch" verb
			if !hasVerb(apiRes.Verbs, "watch") {
				continue
			}

			// Format resource strings for filtering check (e.g. "pods", "v1/pods", "apps/v1/deployments")
			gvrStr := apiRes.Name
			fullGVRStr := fmt.Sprintf("%s/%s", gv.String(), apiRes.Name)

			if !filters.MatchesResource(gvrStr) || !filters.MatchesResource(fullGVRStr) {
				continue
			}

			resources = append(resources, DiscoveredResource{
				GVR: schema.GroupVersionResource{
					Group:    gv.Group,
					Version:  gv.Version,
					Resource: apiRes.Name,
				},
				Kind:       apiRes.Kind,
				Namespaced: apiRes.Namespaced,
			})
		}
	}

	return resources, nil
}

func hasVerb(verbs metav1.Verbs, target string) bool {
	for _, v := range verbs {
		if v == target {
			return true
		}
	}
	return false
}
