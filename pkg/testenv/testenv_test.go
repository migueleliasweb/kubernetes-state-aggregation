package testenv

import (
	"context"
	"testing"
)

func TestCleanupLingeringContainers(t *testing.T) {
	ctx := context.Background()

	// Running cleanup on a clean system should succeed without error
	if err := CleanupLingeringContainers(ctx); err != nil {
		t.Fatalf("unexpected error during CleanupLingeringContainers: %v", err)
	}
}

func TestCleanupLeftovers(t *testing.T) {
	ctx := context.Background()

	// Running CleanupLeftovers should execute without returning an error
	if err := CleanupLeftovers(ctx); err != nil {
		t.Fatalf("unexpected error during CleanupLeftovers: %v", err)
	}
}
