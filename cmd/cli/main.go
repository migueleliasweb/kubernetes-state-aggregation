package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/spf13/cobra"
)

var (
	dbURL    string
	logLevel string
)

func newRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ksactl",
		Short: "Kubernetes State Aggregation (KSA) CLI",
		Long:  "CLI tool to interact with the KSA datastore.",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
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

			return nil
		},
	}

	defaultDBURL := os.Getenv("DB_URL")
	if defaultDBURL == "" {
		defaultDBURL = "postgres://postgres:password@localhost:5432/ksa?sslmode=disable"
	}

	cmd.PersistentFlags().StringVarP(
		&dbURL,
		"db-url",
		"d",
		defaultDBURL,
		"PostgreSQL database connection URL",
	)
	cmd.PersistentFlags().StringVarP(
		&logLevel,
		"log-level",
		"v",
		"error",
		"Log level (debug, info, warn, error)",
	)

	cmd.AddCommand(newGraphCmd())

	return cmd
}

func main() {
	if err := newRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
