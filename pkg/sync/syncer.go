package sync

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/config"
	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/db"
	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/k8s"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
)

// ClusterSyncer watches a single Kubernetes cluster and syncs state to the store.
type ClusterSyncer struct {
	clusterCfg config.ClusterConfig
	filters    config.FilterConfig
	store      db.Store
	dynClient  dynamic.Interface
	discClient discovery.DiscoveryInterface
}

// NewClusterSyncer creates a ClusterSyncer instance.
func NewClusterSyncer(
	clusterCfg config.ClusterConfig,
	filters config.FilterConfig,
	store db.Store,
	dynClient dynamic.Interface,
	discClient discovery.DiscoveryInterface,
) *ClusterSyncer {
	return &ClusterSyncer{
		clusterCfg: clusterCfg,
		filters:    filters,
		store:      store,
		dynClient:  dynClient,
		discClient: discClient,
	}
}

// Start begins dynamic resource discovery, sets up informers, and blocks until ctx is cancelled.
func (cs *ClusterSyncer) Start(ctx context.Context) error {
	slog.Info(
		"Starting cluster syncer...",
		"cluster",
		cs.clusterCfg.Name,
	)

	gvrs, err := k8s.DiscoverWatchableResources(
		cs.discClient,
		cs.filters,
	)

	if err != nil {
		return fmt.Errorf("[%s] dynamic discovery failed: %w", cs.clusterCfg.Name, err)
	}

	slog.Info(
		"Discovered watchable GVRs",
		"cluster",
		cs.clusterCfg.Name,
		"count",
		len(gvrs),
	)

	factory := dynamicinformer.NewDynamicSharedInformerFactory(
		cs.dynClient,
		0,
	)

	for _, gvr := range gvrs {
		informer := factory.ForResource(gvr).Informer()

		_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				u, ok := obj.(*unstructured.Unstructured)
				if !ok {
					return
				}

				if !cs.filters.MatchesNamespace(u.GetNamespace()) {
					return
				}

				if err := cs.store.UpsertResource(
					ctx,
					cs.clusterCfg.Name,
					u,
				); err != nil {
					slog.Error(
						"error upserting resource",
						"cluster", cs.clusterCfg.Name,
						"namespace", u.GetNamespace(),
						"name", u.GetName(),
						"resource", gvr.Resource,
						"err", err,
					)
				}
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				u, ok := newObj.(*unstructured.Unstructured)
				if !ok {
					return
				}

				if !cs.filters.MatchesNamespace(u.GetNamespace()) {
					return
				}

				if err := cs.store.UpsertResource(
					ctx,
					cs.clusterCfg.Name,
					u,
				); err != nil {
					slog.Error(
						"error updating resource",
						"cluster",
						cs.clusterCfg.Name,
						"namespace",
						u.GetNamespace(),
						"name",
						u.GetName(),
						"resource",
						gvr.Resource,
						"error",
						err,
					)
				}
			},
			DeleteFunc: func(obj interface{}) {
				var u *unstructured.Unstructured
				var ok bool

				u, ok = obj.(*unstructured.Unstructured)
				if !ok {
					tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
					if !ok {
						return
					}

					u, ok = tombstone.Obj.(*unstructured.Unstructured)
					if !ok {
						return
					}
				}

				gvk := u.GroupVersionKind()

				if err := cs.store.DeleteResource(
					ctx,
					cs.clusterCfg.Name,
					gvk.Group,
					gvk.Version,
					gvk.Kind,
					u.GetNamespace(),
					u.GetName(),
				); err != nil {
					slog.Error(
						"error deleting resource",
						"cluster",
						cs.clusterCfg.Name,
						"namespace",
						u.GetNamespace(),
						"name",
						u.GetName(),
						"resource",
						gvr.Resource,
						"error",
						err,
					)
				}
			},
		})

		if err != nil {
			slog.Warn(
				"failed to add event handler for GVR",
				"cluster",
				cs.clusterCfg.Name,
				"gvr",
				gvr.String(),
				"error",
				err,
			)
		}
	}

	factory.Start(ctx.Done())

	synced := factory.WaitForCacheSync(ctx.Done())

	for gvr, isSynced := range synced {
		if !isSynced {
			slog.Warn(
				"cache sync failed for GVR",
				"cluster",
				cs.clusterCfg.Name,
				"gvr",
				gvr.String(),
			)
		}
	}

	slog.Info(
		"Cluster syncer active and watching resources",
		"cluster",
		cs.clusterCfg.Name,
	)

	<-ctx.Done()

	slog.Info(
		"Cluster syncer shutting down",
		"cluster",
		cs.clusterCfg.Name,
	)

	return nil
}
