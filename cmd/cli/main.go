package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/spf13/cobra"
)

var (
	serverAddr string
	logLevel   string
)

func newRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ksactl",
		Short: "Kubernetes State Aggregation (KSA) CLI",
		Long:  "CLI tool to interact with the KSA API.",
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

	defaultServerAddr := os.Getenv("KSA_API_ADDR")
	if defaultServerAddr == "" {
		defaultServerAddr = "127.0.0.1:50051"
	}

	cmd.PersistentFlags().StringVarP(
		&serverAddr,
		"server",
		"s",
		defaultServerAddr,
		"KSA gRPC API server address",
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
