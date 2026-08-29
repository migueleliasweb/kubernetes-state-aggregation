package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/config"
	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/datastore/postgres"
	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/sync"
	"github.com/spf13/cobra"
)

var (
	configPath    string
	clusterFilter string
	dbURL         string
	logLevel      string
)

func newRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ksasync",
		Short: "Kubernetes State Aggregation (KSA) Sync Worker",
		Long:  "Syncs remote state from multiple Kubernetes API Servers onto the central PostgreSQL datastore layer.",
		RunE: func(cmd *cobra.Command, args []string) error {
			var level slog.Level
			if err := level.UnmarshalText([]byte(logLevel)); err != nil {
				return fmt.Errorf("invalid log level %q: %w", logLevel, err)
			}

			handler := slog.NewJSONHandler(
				os.Stdout,
				&slog.HandlerOptions{
					AddSource: true,
					Level:     level,
				},
			)
			logger := slog.New(handler)
			slog.SetDefault(logger)

			slog.Info("Starting Kubernetes State Aggregation (KSA) Sync Worker...")

			ctx, stop := signal.NotifyContext(
				context.Background(),
				os.Interrupt,
				syscall.SIGTERM,
			)
			defer stop()

			cfg, err := config.LoadConfig(configPath)
			if err != nil {
				slog.Error(
					"Failed to load configuration file",
					"path", configPath,
					"err", err,
				)

				os.Exit(1)
			}

			store, err := postgres.NewPGSyncer(dbURL)
			if err != nil {
				slog.Error(
					"Failed to connect to database",
					"err", err,
				)

				os.Exit(1)
			}
			defer store.Close()

			if err := store.InitSchema(ctx); err != nil {
				slog.Error(
					"Failed to initialize database schema",
					"err", err,
				)

				os.Exit(1)
			}

			slog.Info("Database schema initialized successfully")

			manager := sync.NewManager(
				cfg,
				store,
				clusterFilter,
			)

			if err := manager.Start(ctx); err != nil {
				slog.Error(
					"Sync Manager stopped with error",
					"err", err,
				)

				os.Exit(1)
			}

			return nil
		},
	}

	cmd.Flags().StringVarP(
		&configPath,
		"config",
		"c",
		"config.yaml",
		"Path to the KSA sync worker configuration YAML/JSON file",
	)
	cmd.Flags().StringVarP(
		&clusterFilter,
		"cluster",
		"l",
		"",
		"Optional cluster name to isolate sync execution to a single cluster",
	)
	cmd.Flags().StringVarP(
		&dbURL,
		"db-url",
		"d",
		"postgres://postgres:postgres@localhost:5432/ksa?sslmode=disable",
		"PostgreSQL database connection URL",
	)
	cmd.Flags().StringVarP(
		&logLevel,
		"log-level",
		"v",
		"info",
		"Log level (debug, info, warn, error)",
	)

	return cmd
}

func main() {
	if err := newRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
