package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/kwok"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func newTestClusterCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "testcluster",
		Short: "Manage KWOK simulated test clusters for development and UI testing",
	}

	cmd.AddCommand(newTestClusterUpCmd())
	cmd.AddCommand(newTestClusterDownCmd())

	return cmd
}

func newTestClusterUpCmd() *cobra.Command {
	var (
		clusters     []string
		seed         bool
		scale        string
		outputConfig string
		configPath   string
	)

	cmd := &cobra.Command{
		Use:   "up",
		Short: "Spin up KWOK test clusters, export kubeconfigs, seed workloads, and generate KSA config",
		RunE: func(cmd *cobra.Command, args []string) error {
			opts := kwok.SetupOptions{
				Clusters:         clusters,
				Seed:             seed,
				Scale:            scale,
				OutputConfigPath: outputConfig,
			}

			if configPath != "" {
				data, err := os.ReadFile(configPath)
				if err != nil {
					return fmt.Errorf("failed to read config file %s: %w", configPath, err)
				}

				if err := yaml.Unmarshal(data, &opts); err != nil {
					return fmt.Errorf("failed to parse config file %s: %w", configPath, err)
				}
			}

			ctx := context.Background()

			return kwok.SetupClusters(ctx, opts)
		},
	}

	cmd.Flags().StringSliceVar(
		&clusters,
		"clusters",
		[]string{"ksa-kwok-us-east", "ksa-kwok-eu-west"},
		"Comma-separated list of KWOK cluster names to create",
	)
	cmd.Flags().BoolVar(
		&seed,
		"seed",
		true,
		"Whether to populate the clusters with mock workloads",
	)
	cmd.Flags().StringVar(
		&scale,
		"scale",
		"default",
		"Workload scale mode: default (~500 resources) or benchmark (~5,000+ resources)",
	)
	cmd.Flags().StringVar(
		&outputConfig,
		"output-config",
		"hack/kwok-config.yaml",
		"Path to write the generated KSA configuration YAML",
	)
	cmd.Flags().StringVarP(
		&configPath,
		"config",
		"c",
		"",
		"Optional path to a YAML configuration file to override options",
	)

	return cmd
}

func newTestClusterDownCmd() *cobra.Command {
	var (
		clusters     []string
		outputConfig string
		configPath   string
	)

	cmd := &cobra.Command{
		Use:   "down",
		Short: "Tear down KWOK test clusters and clean up generated configs",
		RunE: func(cmd *cobra.Command, args []string) error {
			opts := kwok.TeardownOptions{
				Clusters:         clusters,
				OutputConfigPath: outputConfig,
			}

			if configPath != "" {
				data, err := os.ReadFile(configPath)
				if err != nil {
					return fmt.Errorf("failed to read config file %s: %w", configPath, err)
				}

				var fileOpts kwok.SetupOptions
				if err := yaml.Unmarshal(data, &fileOpts); err != nil {
					return fmt.Errorf("failed to parse config file %s: %w", configPath, err)
				}

				if len(fileOpts.Clusters) > 0 {
					opts.Clusters = fileOpts.Clusters
				}

				if fileOpts.OutputConfigPath != "" {
					opts.OutputConfigPath = fileOpts.OutputConfigPath
				}
			}

			if opts.OutputConfigPath != "" {
				opts.KubeconfigDir = filepath.Join(filepath.Dir(opts.OutputConfigPath), ".kwok-configs")
			}

			ctx := context.Background()

			return kwok.TeardownClusters(ctx, opts)
		},
	}

	cmd.Flags().StringSliceVar(
		&clusters,
		"clusters",
		[]string{"ksa-kwok-us-east", "ksa-kwok-eu-west"},
		"Comma-separated list of KWOK cluster names to delete",
	)
	cmd.Flags().StringVar(
		&outputConfig,
		"output-config",
		"hack/kwok-config.yaml",
		"Path to the generated KSA configuration YAML to remove",
	)
	cmd.Flags().StringVarP(
		&configPath,
		"config",
		"c",
		"",
		"Optional path to a YAML configuration file to determine cluster names",
	)

	return cmd
}
