package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/config"
	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/db"
	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/sync"
	"github.com/spf13/cobra"
)

var (
	configPath    string
	clusterFilter string
	dbURL         string
)

func newRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ksasync",
		Short: "Kubernetes State Aggregation (KSA) Sync Worker",
		Long:  "Syncs remote state from multiple Kubernetes API Servers onto the central PostgreSQL datastore layer.",
		RunE: func(cmd *cobra.Command, args []string) error {
			log.Println("Starting Kubernetes State Aggregation (KSA) Sync Worker...")

			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			cfg, err := config.LoadConfig(configPath)
			if err != nil {
				log.Fatalf("Failed to load configuration file %s: %v", configPath, err)
			}

			store, err := db.NewStore(dbURL)
			if err != nil {
				log.Fatalf("Failed to connect to database: %v", err)
			}
			defer store.Close()

			if err := store.InitSchema(ctx); err != nil {
				log.Fatalf("Failed to initialize database schema: %v", err)
			}
			log.Println("Database schema initialized successfully")

			manager := sync.NewManager(cfg, store, clusterFilter)
			if err := manager.Start(ctx); err != nil {
				log.Fatalf("Sync Manager stopped with error: %v", err)
			}
			return nil
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "config.yaml", "Path to the KSA sync worker configuration YAML/JSON file")
	cmd.Flags().StringVarP(&clusterFilter, "cluster", "l", "", "Optional cluster name to isolate sync execution to a single cluster")
	cmd.Flags().StringVarP(&dbURL, "db-url", "d", "postgres://postgres:postgres@localhost:5432/ksa?sslmode=disable", "PostgreSQL database connection URL")

	return cmd
}

func main() {
	if err := newRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
