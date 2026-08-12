package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigAndFiltering(t *testing.T) {
	content := `
global_filters:
  include_namespaces: ["default", "kube-*"]
  exclude_namespaces: ["kube-system"]
  exclude_resources: ["events", "coordination.k8s.io/v1/leases"]

clusters:
  - name: us1
    api_server: "https://us1.k8s.local"
    kubeconfig: "/path/to/us1.kubeconfig"
  - name: eu1
    api_server: "https://eu1.k8s.local"
    filters:
      include_namespaces: ["production"]
`
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write temp config: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if len(cfg.Clusters) != 2 {
		t.Fatalf("expected 2 clusters, got %d", len(cfg.Clusters))
	}

	us1Filters := cfg.GetEffectiveFilters("us1")
	if !us1Filters.MatchesNamespace("default") {
		t.Errorf("us1 expected default to match namespace filters")
	}
	if !us1Filters.MatchesNamespace("kube-public") {
		t.Errorf("us1 expected kube-public to match namespace filters")
	}
	if us1Filters.MatchesNamespace("kube-system") {
		t.Errorf("us1 expected kube-system to be excluded")
	}
	if us1Filters.MatchesNamespace("random-ns") {
		t.Errorf("us1 expected random-ns to not match include rules")
	}

	if us1Filters.MatchesResource("events") {
		t.Errorf("us1 expected events to be excluded")
	}
	if !us1Filters.MatchesResource("pods") {
		t.Errorf("us1 expected pods to be included")
	}

	eu1Filters := cfg.GetEffectiveFilters("eu1")
	if !eu1Filters.MatchesNamespace("production") {
		t.Errorf("eu1 expected production namespace to match")
	}
	if eu1Filters.MatchesNamespace("default") {
		t.Errorf("eu1 expected default namespace to be excluded by cluster override")
	}
}

func TestConfigValidation(t *testing.T) {
	invalidYaml := `
clusters:
  - name: us1
  - name: us1
`
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "invalid.yaml")
	if err := os.WriteFile(configPath, []byte(invalidYaml), 0644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	_, err := LoadConfig(configPath)
	if err == nil {
		t.Errorf("expected error for duplicate cluster name, got nil")
	}
}
