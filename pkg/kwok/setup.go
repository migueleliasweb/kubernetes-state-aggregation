package kwok

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// SetupOptions defines parameters for spinning up and seeding test clusters.
type SetupOptions struct {
	Clusters         []string     `json:"clusters" yaml:"clusters"`
	Seed             bool         `json:"seed" yaml:"seed"`
	Scale            string       `json:"scale" yaml:"scale"`
	ScaleConfig      *ScaleConfig `json:"scale_config,omitempty" yaml:"scale_config,omitempty"`
	OutputConfigPath string       `json:"output_config_path" yaml:"output_config_path"`
	KubeconfigDir    string       `json:"kubeconfig_dir" yaml:"kubeconfig_dir"`
}

// TeardownOptions defines parameters for tearing down test clusters.
type TeardownOptions struct {
	Clusters         []string `json:"clusters" yaml:"clusters"`
	OutputConfigPath string   `json:"output_config_path" yaml:"output_config_path"`
	KubeconfigDir    string   `json:"kubeconfig_dir" yaml:"kubeconfig_dir"`
}

type ksaClusterEntry struct {
	Name       string `yaml:"name"`
	Kubeconfig string `yaml:"kubeconfig"`
	Disabled   bool   `yaml:"disabled"`
}

type ksaFilterConfig struct {
	IncludeClusterScoped bool     `yaml:"include_cluster_scoped"`
	ExcludeNamespaces    []string `yaml:"exclude_namespaces"`
	ExcludeResources     []string `yaml:"exclude_resources"`
}

type ksaConfigFile struct {
	GlobalFilters ksaFilterConfig   `yaml:"global_filters"`
	Clusters      []ksaClusterEntry `yaml:"clusters"`
}

// SetupClusters provisions KWOK clusters, exports kubeconfigs, optionally seeds workloads, and writes KSA config.
func SetupClusters(
	ctx context.Context,
	opts SetupOptions,
) error {
	mgr, err := NewClusterManager()
	if err != nil {
		return err
	}

	if len(opts.Clusters) == 0 {
		opts.Clusters = []string{"ksa-kwok-us-east", "ksa-kwok-eu-west"}
	}

	if opts.OutputConfigPath == "" {
		opts.OutputConfigPath = "hack/kwok-config.yaml"
	}

	if opts.KubeconfigDir == "" {
		opts.KubeconfigDir = filepath.Join(filepath.Dir(opts.OutputConfigPath), ".kwok-configs")
	}

	if err := os.MkdirAll(opts.KubeconfigDir, 0o755); err != nil {
		return fmt.Errorf("failed to create kubeconfig directory %s: %w", opts.KubeconfigDir, err)
	}

	var clusterEntries []ksaClusterEntry

	for _, clusterName := range opts.Clusters {
		slog.Info(
			"Setting up KWOK cluster",
			"cluster", clusterName,
		)

		if err := mgr.CreateCluster(ctx, clusterName); err != nil {
			slog.Warn(
				"Cluster creation warning (may already exist)",
				"cluster", clusterName,
				"err", err,
			)
		}

		kubeconfigPath := filepath.Join(opts.KubeconfigDir, fmt.Sprintf("%s.kubeconfig", clusterName))

		data, err := mgr.GetKubeconfig(ctx, clusterName)
		if err != nil {
			return fmt.Errorf("failed to get kubeconfig for %s: %w", clusterName, err)
		}

		if err := os.WriteFile(kubeconfigPath, data, 0o600); err != nil {
			return fmt.Errorf("failed to write kubeconfig for %s: %w", clusterName, err)
		}

		clusterEntries = append(clusterEntries, ksaClusterEntry{
			Name:       clusterName,
			Kubeconfig: kubeconfigPath,
			Disabled:   false,
		})

		if opts.Seed {
			slog.Info(
				"Seeding mock workloads into cluster",
				"cluster", clusterName,
				"scale", opts.Scale,
			)

			restCfg, err := mgr.GetRESTConfig(ctx, clusterName)
			if err != nil {
				return fmt.Errorf("failed to get rest config for %s: %w", clusterName, err)
			}

			seeder, err := NewWorkloadSeeder(restCfg)
			if err != nil {
				return fmt.Errorf("failed to initialize seeder for %s: %w", clusterName, err)
			}

			var scaleCfg ScaleConfig
			if opts.ScaleConfig != nil {
				scaleCfg = *opts.ScaleConfig
			} else if opts.Scale == "benchmark" {
				scaleCfg = BenchmarkScaleConfig()
			} else {
				scaleCfg = DefaultScaleConfig()
			}

			if err := seeder.Seed(ctx, scaleCfg); err != nil {
				return fmt.Errorf("failed to seed cluster %s: %w", clusterName, err)
			}
		}
	}

	// Generate KSA configuration file
	cfg := ksaConfigFile{
		GlobalFilters: ksaFilterConfig{
			IncludeClusterScoped: true,
			ExcludeNamespaces:    []string{},
			ExcludeResources:     []string{},
		},
		Clusters: clusterEntries,
	}

	outBytes, err := yaml.Marshal(&cfg)
	if err != nil {
		return fmt.Errorf("failed to marshal KSA config: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(opts.OutputConfigPath), 0o755); err != nil {
		return fmt.Errorf("failed to create output config directory: %w", err)
	}

	if err := os.WriteFile(opts.OutputConfigPath, outBytes, 0o644); err != nil {
		return fmt.Errorf("failed to write KSA config file to %s: %w", opts.OutputConfigPath, err)
	}

	slog.Info(
		"Test clusters successfully configured",
		"configPath", opts.OutputConfigPath,
		"clustersCount", len(opts.Clusters),
	)

	return nil
}

// TeardownClusters deletes the specified KWOK clusters and cleans up generated configuration files.
func TeardownClusters(
	ctx context.Context,
	opts TeardownOptions,
) error {
	mgr, err := NewClusterManager()
	if err != nil {
		return err
	}

	if len(opts.Clusters) == 0 {
		opts.Clusters = []string{"ksa-kwok-us-east", "ksa-kwok-eu-west"}
	}

	for _, clusterName := range opts.Clusters {
		slog.Info(
			"Tearing down KWOK cluster",
			"cluster", clusterName,
		)

		if err := mgr.DeleteCluster(ctx, clusterName); err != nil {
			slog.Warn(
				"Failed to delete cluster (may not exist)",
				"cluster", clusterName,
				"err", err,
			)
		}
	}

	if opts.KubeconfigDir != "" {
		_ = os.RemoveAll(opts.KubeconfigDir)
	}

	if opts.OutputConfigPath != "" {
		_ = os.Remove(opts.OutputConfigPath)
	}

	slog.Info(
		"Test clusters teardown complete",
		"clustersCount", len(opts.Clusters),
	)

	return nil
}
