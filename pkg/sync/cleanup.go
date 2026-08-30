package sync

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/config"
	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/datastore"
)

// RunStartupCleanup purges removed/disabled cluster data and cleans up resources
// that no longer match the effective filter rules for each cluster.
func RunStartupCleanup(
	ctx context.Context,
	cfg *config.Config,
	store datastore.Syncer,
	targetCluster string,
) error {
	slog.Info("Running startup datastore consistency cleanup...")

	// 1. Purge disabled or removed clusters from datastore
	activeClusterMap := map[string]bool{}
	for _, cluster := range cfg.Clusters {
		if !cluster.Disabled {
			activeClusterMap[cluster.Name] = true
		}
	}

	existingClusters, err := store.ListClusters(ctx)
	if err != nil {
		return fmt.Errorf("failed to list clusters from datastore during cleanup: %w", err)
	}

	for _, clusterName := range existingClusters {
		if !activeClusterMap[clusterName] {
			deletedCount, err := store.DeleteCluster(ctx, clusterName)
			if err != nil {
				slog.Error(
					"failed to purge removed/disabled cluster from datastore",
					"cluster", clusterName,
					"err", err,
				)

				continue
			}

			slog.Info(
				"Purged disabled or removed cluster from datastore",
				"cluster", clusterName,
				"deleted_count", deletedCount,
			)
		}
	}

	// 2. Clean up invalid/excluded resources for configured active clusters
	for _, cluster := range cfg.Clusters {
		if cluster.Disabled {
			continue
		}

		if targetCluster != "" && cluster.Name != targetCluster {
			continue
		}

		effectiveFilters := cfg.GetEffectiveFilters(cluster.Name)

		keys, err := store.ListAllResourceKeys(ctx, cluster.Name)
		if err != nil {
			slog.Error(
				"failed to list resource keys for cluster cleanup",
				"cluster", cluster.Name,
				"err", err,
			)

			continue
		}

		var toDelete []datastore.ResourceInfo

		for _, key := range keys {
			// Check namespace filter
			if key.Namespace == "" {
				if !effectiveFilters.IncludeClusterScoped {
					toDelete = append(toDelete, key)
					continue
				}
			} else if !effectiveFilters.MatchesNamespace(key.Namespace) {
				toDelete = append(toDelete, key)
				continue
			}

			// Check GVK/Resource filter
			if !effectiveFilters.MatchesGVK(key.Group, key.Version, key.Kind) {
				toDelete = append(toDelete, key)
				continue
			}
		}

		if len(toDelete) > 0 {
			deletedCount, err := store.BatchDeleteResources(ctx, toDelete)
			if err != nil {
				slog.Error(
					"failed to delete inconsistent resources during startup cleanup",
					"cluster", cluster.Name,
					"count", len(toDelete),
					"err", err,
				)

				continue
			}

			slog.Info(
				"Pruned inconsistent resources from datastore at startup",
				"cluster", cluster.Name,
				"pruned_count", deletedCount,
			)

			for _, r := range toDelete {
				slog.Debug(
					"Pruned resource",
					"cluster", r.ClusterName,
					"group", r.Group,
					"version", r.Version,
					"kind", r.Kind,
					"namespace", r.Namespace,
					"name", r.Name,
				)
			}
		}
	}

	slog.Info("Startup datastore consistency cleanup finished")

	return nil
}
