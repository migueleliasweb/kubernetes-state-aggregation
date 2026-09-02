package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/spf13/cobra"
)

var logLevel string

func newRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ksadev",
		Short: "Kubernetes State Aggregation (KSA) Developer CLI",
		Long:  "Developer CLI for managing local testing clusters, simulated state, and dev environments.",
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

	cmd.PersistentFlags().StringVarP(
		&logLevel,
		"log-level",
		"v",
		"info",
		"Log level (debug, info, warn, error)",
	)

	cmd.AddCommand(newTestClusterCmd())

	return cmd
}

func main() {
	if err := newRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
