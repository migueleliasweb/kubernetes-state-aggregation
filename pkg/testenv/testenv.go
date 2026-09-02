package testenv

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/datastore"
	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/datastore/postgres"
	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/kwok"
	"github.com/testcontainers/testcontainers-go"
	tcPostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

const (
	PostgresLabelKey   = "app"
	PostgresLabelValue = "ksa-test-postgres"
)

// SetupOptions specifies configuration for initializing a test environment.
type SetupOptions struct {
	Clusters         []string          `json:"clusters" yaml:"clusters"`
	Seed             bool              `json:"seed" yaml:"seed"`
	Scale            string            `json:"scale" yaml:"scale"`
	ScaleConfig      *kwok.ScaleConfig `json:"scale_config,omitempty" yaml:"scale_config,omitempty"`
	OutputConfigPath string            `json:"output_config_path" yaml:"output_config_path"`
	KubeconfigDir    string            `json:"kubeconfig_dir" yaml:"kubeconfig_dir"`
	DBURL            string            `json:"db_url" yaml:"db_url"`
}

// StoreBackend represents a datastore that supports both synchronization and query fetching.
type StoreBackend interface {
	datastore.Syncer
	datastore.Fetcher
	io.Closer
}

// Environment represents an orchestrated test environment with KWOK clusters and PostgreSQL datastore.
type Environment struct {
	Store             StoreBackend
	DBURL             string
	ConfigPath        string
	Clusters          map[string]kwok.ClusterInfo
	PostgresContainer *tcPostgres.PostgresContainer
	KwokEnv           *kwok.ClusterEnvironment
}

// Setup provisions a unified test environment with KWOK clusters and a PostgreSQL datastore.
// On boot, it automatically performs a pre-cleanup of any lingering KWOK clusters or PostgreSQL
// testcontainers left over from previous runs.
func Setup(
	ctx context.Context,
	opts SetupOptions,
) (*Environment, error) {
	// 1. Pre-cleanup lingering resources from prior runs
	if err := CleanupLeftovers(ctx); err != nil {
		slog.Warn(
			"Failed to clean up leftover test resources on boot",
			"err", err,
		)
	}

	// 2. Setup PostgreSQL datastore
	dbURL := opts.DBURL
	if dbURL == "" {
		dbURL = os.Getenv("DB_URL")
	}

	var pgContainer *tcPostgres.PostgresContainer

	if dbURL == "" {
		slog.Info("Starting ephemeral PostgreSQL container via testcontainers...")

		container, err := tcPostgres.Run(
			ctx,
			"postgres:15-alpine",
			tcPostgres.WithDatabase("ksa"),
			tcPostgres.WithUsername("postgres"),
			tcPostgres.WithPassword("password"),
			tcPostgres.BasicWaitStrategies(),
			testcontainers.WithLabels(map[string]string{
				PostgresLabelKey: PostgresLabelValue,
			}),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to start PostgreSQL testcontainer: %w", err)
		}

		pgContainer = container

		connStr, err := container.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			_ = container.Terminate(ctx)

			return nil, fmt.Errorf("failed to get connection string for PostgreSQL testcontainer: %w", err)
		}

		dbURL = connStr
	}

	slog.Info(
		"Connecting to PostgreSQL datastore",
		"db_url", dbURL,
	)

	store, err := postgres.NewPGSyncer(dbURL)
	if err != nil {
		if pgContainer != nil {
			_ = pgContainer.Terminate(ctx)
		}

		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	if err := store.InitSchema(ctx); err != nil {
		_ = store.Close()

		if pgContainer != nil {
			_ = pgContainer.Terminate(ctx)
		}

		return nil, fmt.Errorf("failed to init Postgres schema: %w", err)
	}

	// Purge any existing data in the datastore to guarantee a clean slate
	existingClusters, err := store.ListClusters(ctx)
	if err == nil {
		for _, cluster := range existingClusters {
			if _, delErr := store.DeleteCluster(ctx, cluster); delErr != nil {
				slog.Warn(
					"Failed to delete existing cluster from store during setup",
					"cluster", cluster,
					"err", delErr,
				)
			}
		}
	}

	// 3. Setup KWOK clusters
	if opts.OutputConfigPath == "" {
		opts.OutputConfigPath = filepath.Join(os.TempDir(), "kwok-config.yaml")
	}

	kwokEnv, err := kwok.SetupClusters(ctx, kwok.SetupOptions{
		Clusters:         opts.Clusters,
		Seed:             opts.Seed,
		Scale:            opts.Scale,
		ScaleConfig:      opts.ScaleConfig,
		OutputConfigPath: opts.OutputConfigPath,
		KubeconfigDir:    opts.KubeconfigDir,
	})
	if err != nil {
		_ = store.Close()

		if pgContainer != nil {
			_ = pgContainer.Terminate(ctx)
		}

		return nil, fmt.Errorf("failed to setup KWOK clusters: %w", err)
	}

	return &Environment{
		Store:             store,
		DBURL:             dbURL,
		ConfigPath:        kwokEnv.ConfigPath,
		Clusters:          kwokEnv.Clusters,
		PostgresContainer: pgContainer,
		KwokEnv:           kwokEnv,
	}, nil
}

// Teardown cleanly shuts down and removes all resources associated with the environment.
func (e *Environment) Teardown(ctx context.Context) error {
	var errs []string

	// Teardown KWOK clusters
	if e.KwokEnv != nil && len(e.Clusters) > 0 {
		var clusterNames []string

		for name := range e.Clusters {
			clusterNames = append(clusterNames, name)
		}

		if err := kwok.TeardownClusters(ctx, kwok.TeardownOptions{
			Clusters:         clusterNames,
			OutputConfigPath: e.ConfigPath,
			KubeconfigDir:    e.KwokEnv.KubeconfigDir,
		}); err != nil {
			errs = append(errs, fmt.Sprintf("kwok teardown: %v", err))
		}
	}

	// Close datastore connection
	if e.Store != nil {
		if closer, ok := e.Store.(io.Closer); ok {
			if err := closer.Close(); err != nil {
				errs = append(errs, fmt.Sprintf("store close: %v", err))
			}
		}
	}

	// Terminate PostgreSQL container
	if e.PostgresContainer != nil {
		if err := e.PostgresContainer.Terminate(ctx); err != nil {
			errs = append(errs, fmt.Sprintf("postgres terminate: %v", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("teardown errors: %s", strings.Join(errs, "; "))
	}

	return nil
}

// PrintDebugInfo logs inspection commands for KWOK clusters and PostgreSQL when a test fails.
func (e *Environment) PrintDebugInfo(
	t *testing.T,
	extraInfo ...string,
) {
	t.Helper()

	t.Log("\n=================================================================")
	t.Log("⚠️ TEST FAILED: Preserving test environment for inspection!")
	t.Log("=================================================================")
	t.Logf("PostgreSQL DB_URL: %s", e.DBURL)

	for _, info := range extraInfo {
		if info != "" {
			t.Log(info)
		}
	}

	t.Logf("KSA Config Path: %s", e.ConfigPath)

	var kubeconfigs []string
	for _, c := range e.Clusters {
		kubeconfigs = append(kubeconfigs, c.KubeconfigPath)
	}

	if len(kubeconfigs) > 0 {
		t.Log("To inspect KWOK clusters with kubectl:")
		t.Logf("  export KUBECONFIG=%s", strings.Join(kubeconfigs, ":"))
		t.Log("  kubectl get nodes")
		t.Log("  kubectl get pods -A")
	}

	t.Log("=================================================================")
}

// CleanupLeftovers removes any lingering KWOK clusters and PostgreSQL testcontainers from prior runs.
func CleanupLeftovers(ctx context.Context) error {
	// 1. Remove lingering KWOK clusters
	if err := kwok.CleanupExistingClusters(ctx, "ksa-kwok-", "ksa-"); err != nil {
		slog.Warn(
			"Failed to cleanup existing KWOK clusters",
			"err", err,
		)
	}

	// 2. Remove lingering PostgreSQL testcontainers
	if err := CleanupLingeringContainers(ctx); err != nil {
		slog.Warn(
			"Failed to cleanup lingering PostgreSQL testcontainers",
			"err", err,
		)
	}

	return nil
}

// CleanupLingeringContainers searches for and removes Docker containers matching PostgresLabelKey=PostgresLabelValue.
func CleanupLingeringContainers(ctx context.Context) error {
	dockerPath, err := exec.LookPath("docker")
	if err != nil {
		return nil
	}

	cmd := exec.CommandContext(
		ctx,
		dockerPath,
		"ps",
		"-a",
		"--filter", fmt.Sprintf("label=%s=%s", PostgresLabelKey, PostgresLabelValue),
		"-q",
	)

	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to query docker for lingering containers: %w", err)
	}

	ids := strings.Fields(string(out))

	for _, id := range ids {
		slog.Info(
			"Cleaning up lingering KSA testcontainer",
			"container_id", id,
		)

		rmCmd := exec.CommandContext(
			ctx,
			dockerPath,
			"rm",
			"-f",
			id,
		)

		if rmErr := rmCmd.Run(); rmErr != nil {
			slog.Warn(
				"Failed to remove lingering container",
				"container_id", id,
				"err", rmErr,
			)
		}
	}

	return nil
}
