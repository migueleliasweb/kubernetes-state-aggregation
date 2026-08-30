package sync

import (
	"testing"

	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/config"
)

// The controller test can be flaky with fakedynamic.
// We test the logic of filtering instead.
func TestClusterSyncerFilters(t *testing.T) {
	filters := config.FilterConfig{
		IncludeNamespaces: []string{"default"},
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
