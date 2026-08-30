package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/config"
	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/datastore/postgres"
	ksaServer "github.com/migueleliasweb/kubernetes-state-aggregation/pkg/server"
	ksaSync "github.com/migueleliasweb/kubernetes-state-aggregation/pkg/sync"
	"github.com/spf13/cobra"
)

var (
	configPath         string
	clusterFilter      string
	dbURL              string
	logLevel           string
	listenAddr         string
	enableSync         bool
	enableAPI          bool
	corsAllowedOrigins []string
)

func newRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "server",
		Short: "Kubernetes State Aggregation (KSA) Unified Server Daemon",
		Long:  "Runs the KSA remote state sync worker and the gRPC query API server.",
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

			if !enableSync && !enableAPI {
				return fmt.Errorf("at least one of --enable-sync or --enable-api must be enabled")
			}

			slog.Info(
				"Starting Kubernetes State Aggregation (KSA) Server...",
				"enableSync", enableSync,
				"enableAPI", enableAPI,
				"listenAddr", listenAddr,
			)

			ctx, stop := signal.NotifyContext(
				context.Background(),
				os.Interrupt,
				syscall.SIGTERM,
			)
			defer stop()

			store, err := postgres.NewPGSyncer(dbURL)
			if err != nil {
				slog.Error(
					"Failed to connect to database",
					"err", err,
				)

				return err
			}
			defer store.Close()

			if err := store.InitSchema(ctx); err != nil {
				slog.Error(
					"Failed to initialize database schema",
					"err", err,
				)

				return err
			}

			slog.Info("Database schema initialized successfully")

			var wg sync.WaitGroup

			// Start gRPC API Server
			if enableAPI {
				lis, err := net.Listen("tcp", listenAddr)
				if err != nil {
					slog.Error(
						"Failed to listen on gRPC address",
						"addr", listenAddr,
						"err", err,
					)

					return err
				}

				srv := ksaServer.NewServer(
					store,
					lis,
					ksaServer.WithAllowedOrigins(corsAllowedOrigins),
				)

				wg.Add(1)
				go func() {
					defer wg.Done()

					slog.Info(
						"gRPC & Connect API server listening",
						"addr", listenAddr,
					)

					if err := srv.Serve(); err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, http.ErrServerClosed) {
						slog.Error(
							"API server encountered error",
							"err", err,
						)
					}
				}()

				// Handle graceful shutdown for API Server
				wg.Add(1)
				go func() {
					defer wg.Done()

					<-ctx.Done()

					slog.Info("Stopping API server gracefully...")

					srv.GracefulStop()
				}()
			}

			// Start KSA Sync Worker
			if enableSync {
				cfg, err := config.LoadConfig(configPath)
				if err != nil {
					slog.Error(
						"Failed to load configuration file",
						"path", configPath,
						"err", err,
					)

					return err
				}

				manager := ksaSync.NewManager(
					cfg,
					store,
					clusterFilter,
				)

				wg.Add(1)
				go func() {
					defer wg.Done()

					slog.Info("Starting sync manager...")

					if err := manager.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
						slog.Error(
							"Sync Manager stopped with error",
							"err", err,
						)
					}
				}()
			}

			// Wait for termination signal or component exit
			<-ctx.Done()
			slog.Info("Shutdown signal received, waiting for workers to finish...")

			wg.Wait()
			slog.Info("KSA Server stopped cleanly")

			return nil
		},
	}

	cmd.Flags().StringVarP(
		&configPath,
		"config",
		"c",
		"config.yaml",
		"Path to the KSA configuration YAML/JSON file",
	)
	cmd.Flags().StringVarP(
		&clusterFilter,
		"cluster",
		"l",
		"",
		"Optional cluster name to isolate sync execution to a single cluster",
	)

	defaultDBURL := os.Getenv("DB_URL")
	if defaultDBURL == "" {
		defaultDBURL = "postgres://postgres:password@localhost:5432/ksa?sslmode=disable"
	}

	cmd.Flags().StringVarP(
		&dbURL,
		"db-url",
		"d",
		defaultDBURL,
		"PostgreSQL database connection URL",
	)
	cmd.Flags().StringVarP(
		&logLevel,
		"log-level",
		"v",
		"info",
		"Log level (debug, info, warn, error)",
	)
	cmd.Flags().StringVar(
		&listenAddr,
		"listen-addr",
		":50051",
		"gRPC API server listen address",
	)
	cmd.Flags().StringSliceVar(
		&corsAllowedOrigins,
		"cors-allowed-origins",
		[]string{"*"},
		"Allowed CORS origins for Connect/gRPC-Web web requests",
	)
	cmd.Flags().BoolVar(
		&enableSync,
		"enable-sync",
		true,
		"Enable the Kubernetes sync worker",
	)
	cmd.Flags().BoolVar(
		&enableAPI,
		"enable-api",
		true,
		"Enable the gRPC query API server",
	)

	return cmd
}

func main() {
	if err := newRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
