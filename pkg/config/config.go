package config

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// FilterConfig holds namespace, resource, and label selection rules.
type FilterConfig struct {
	IncludeNamespaces    []string `yaml:"include_namespaces" json:"include_namespaces"`
	ExcludeNamespaces    []string `yaml:"exclude_namespaces" json:"exclude_namespaces"`
	IncludeResources     []string `yaml:"include_resources" json:"include_resources"`
	ExcludeResources     []string `yaml:"exclude_resources" json:"exclude_resources"`
	LabelSelector        string   `yaml:"label_selector" json:"label_selector"`
	IncludeClusterScoped bool     `yaml:"include_cluster_scoped" json:"include_cluster_scoped"`
}

// ClusterConfig defines connection details and filters for a single remote cluster.
type ClusterConfig struct {
	Name       string       `yaml:"name" json:"name"`
	APIServer  string       `yaml:"api_server" json:"api_server"`
	Kubeconfig string       `yaml:"kubeconfig" json:"kubeconfig"`
	Context    string       `yaml:"context" json:"context"`
	Disabled   bool         `yaml:"disabled" json:"disabled"`
	Filters    FilterConfig `yaml:"filters" json:"filters"`
}

// Config is the root configuration structure for KSA sync worker.
type Config struct {
	GlobalFilters FilterConfig    `yaml:"global_filters" json:"global_filters"`
	Clusters      []ClusterConfig `yaml:"clusters" json:"clusters"`
}

// LoadConfig reads and parses a YAML/JSON configuration file.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config YAML/JSON: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &cfg, nil
}

// Validate ensures basic configuration rules are met.
func (c *Config) Validate() error {
	if len(c.Clusters) == 0 {
		return fmt.Errorf("at least one cluster must be configured")
	}
	names := map[string]bool{}
	for i, cluster := range c.Clusters {
		if strings.TrimSpace(cluster.Name) == "" {
			return fmt.Errorf("cluster at index %d missing name", i)
		}
		if names[cluster.Name] {
			return fmt.Errorf("duplicate cluster name: %s", cluster.Name)
		}
		names[cluster.Name] = true
	}
	return nil
}

// GetEffectiveFilters combines global filters and cluster-specific filters.
func (c *Config) GetEffectiveFilters(clusterName string) FilterConfig {
	for _, cl := range c.Clusters {
		if cl.Name == clusterName {
			eff := c.GlobalFilters
			if len(cl.Filters.IncludeNamespaces) > 0 {
				eff.IncludeNamespaces = cl.Filters.IncludeNamespaces
			}
			if len(cl.Filters.ExcludeNamespaces) > 0 {
				eff.ExcludeNamespaces = cl.Filters.ExcludeNamespaces
			}
			if len(cl.Filters.IncludeResources) > 0 {
				eff.IncludeResources = cl.Filters.IncludeResources
			}
			if len(cl.Filters.ExcludeResources) > 0 {
				eff.ExcludeResources = cl.Filters.ExcludeResources
			}
			if cl.Filters.LabelSelector != "" {
				eff.LabelSelector = cl.Filters.LabelSelector
			}

			// We OR the boolean flag so if either global or cluster specifies true, it's true.
			// Or we could just override it if we had a pointer, but for a boolean, OR is usually desired
			// if true is the opt-in behavior. Actually, just overriding it if set is hard without a pointer.
			// Let's OR it, so setting it to true at global level applies to all, and setting it at cluster level
			// applies to that cluster.
			if cl.Filters.IncludeClusterScoped {
				eff.IncludeClusterScoped = true
			}

			return eff
		}
	}
	return c.GlobalFilters
}

// MatchesNamespace checks if a namespace matches the include/exclude filter rules.
func (f *FilterConfig) MatchesNamespace(ns string) bool {
	// If the resource is cluster-scoped (empty namespace) and the flag is true, include it.
	if ns == "" && f.IncludeClusterScoped {
		return true
	}

	if len(f.ExcludeNamespaces) > 0 {
		for _, pattern := range f.ExcludeNamespaces {
			if matchPattern(pattern, ns) {
				return false
			}
		}
	}
	if len(f.IncludeNamespaces) > 0 {
		for _, pattern := range f.IncludeNamespaces {
			if matchPattern(pattern, ns) {
				return true
			}
		}
		return false
	}
	return true
}

// MatchesResource checks if a resource string matches include/exclude rules.
func (f *FilterConfig) MatchesResource(resourceStr string) bool {
	if len(f.ExcludeResources) > 0 {
		for _, pattern := range f.ExcludeResources {
			if matchPattern(pattern, resourceStr) {
				return false
			}
		}
	}
	if len(f.IncludeResources) > 0 {
		for _, pattern := range f.IncludeResources {
			if matchPattern(pattern, resourceStr) {
				return true
			}
		}
		return false
	}
	return true
}

func matchPattern(pattern, target string) bool {
	if pattern == "*" || pattern == target {
		return true
	}
	if strings.Contains(pattern, "*") {
		regexPattern := "^" + strings.ReplaceAll(regexp.QuoteMeta(pattern), "\\*", ".*") + "$"
		matched, err := regexp.MatchString(regexPattern, target)
		if err == nil && matched {
			return true
		}
	}
	return false
}
