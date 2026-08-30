package sync

import (
	"context"
	"fmt"
	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/config"
	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/datastore"
	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/k8s"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
	"log/slog"
	"sync"
)

// ClusterSyncer watches a single Kubernetes cluster and syncs state to the store.
type ClusterSyncer struct {
	clusterCfg config.ClusterConfig
	filters    config.FilterConfig
	store      datastore.Syncer
	dynClient  dynamic.Interface
	discClient discovery.DiscoveryInterface
}

// NewClusterSyncer creates a ClusterSyncer instance.
func NewClusterSyncer(
	clusterCfg config.ClusterConfig,
	filters config.FilterConfig,
	store datastore.Syncer,
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

// Start begins dynamic resource discovery, sets up custom informers, and blocks until ctx is cancelled.
func (cs *ClusterSyncer) Start(ctx context.Context) error {
	slog.Info(
		"Starting cluster syncer...",
		"cluster", cs.clusterCfg.Name,
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
		"cluster", cs.clusterCfg.Name,
		"count", len(gvrs),
	)

	var wg sync.WaitGroup

	for _, gvr := range gvrs {
		wg.Add(1)
		go func(gvr schema.GroupVersionResource) {
			defer wg.Done()

			// 1. Create the DirectStore
			// We pass "" as kind for now because GVR doesn't strictly have Kind.
			// The datastore will match the Group/Version.
			directStore := NewDirectStore(cs.clusterCfg.Name, gvr, cs.store)
			if err := directStore.InitializeKeys(ctx, ""); err != nil {
				slog.Error("failed to initialize keys for GVR", "cluster", cs.clusterCfg.Name, "gvr", gvr.String(), "err", err)
				return
			}

			// 2. Create the ListerWatcher
			lw := &cache.ListWatch{
				ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
					return cs.dynClient.Resource(gvr).List(ctx, options)
				},
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					return cs.dynClient.Resource(gvr).Watch(ctx, options)
				},
			}

			// 3. We wrap the DirectStore in a namespace-filtering EventHandler
			handler := cache.ResourceEventHandlerFuncs{
				AddFunc: func(obj interface{}) {
					u, ok := obj.(*unstructured.Unstructured)
					if ok && cs.filters.MatchesNamespace(u.GetNamespace()) {
						if err := directStore.Add(obj); err != nil {
							slog.Error("error adding resource", "cluster", cs.clusterCfg.Name, "gvr", gvr.String(), "name", u.GetName(), "err", err)
						}
					}
				},
				UpdateFunc: func(oldObj, newObj interface{}) {
					u, ok := newObj.(*unstructured.Unstructured)
					if ok && cs.filters.MatchesNamespace(u.GetNamespace()) {
						if err := directStore.Update(newObj); err != nil {
							slog.Error("error updating resource", "cluster", cs.clusterCfg.Name, "gvr", gvr.String(), "name", u.GetName(), "err", err)
						}
					}
				},
				DeleteFunc: func(obj interface{}) {
					// Namespace filtering is skipped on delete since we want to delete what we tracked.
					if err := directStore.Delete(obj); err != nil {
						slog.Error("error deleting resource", "cluster", cs.clusterCfg.Name, "gvr", gvr.String(), "err", err)
					}
				},
			}

			// 4. Create the Controller directly.
			// The DeltaFIFO uses directStore as its KeyLister. This is the crucial part that enables
			// the FIFO to generate Deleted deltas for items that are in the directStore but missing
			// from the initial list.
			fifo := cache.NewDeltaFIFOWithOptions(cache.DeltaFIFOOptions{
				KeyFunction:  cache.MetaNamespaceKeyFunc,
				KnownObjects: directStore,
			})

			cfg := &cache.Config{
				Queue:            fifo,
				ListerWatcher:    lw,
				ObjectType:       &unstructured.Unstructured{},
				FullResyncPeriod: 0,
				Process: func(obj interface{}, isInInitialList bool) error {
					// We pass the deltas to the default processDeltas-like handling.
					// Actually, cache.ProcessDeltas is available, but wait, it expects a cache.Store
					// and updates it BEFORE calling handlers. Since our handler DOES the update,
					// we just want to call the handler.
					for _, d := range obj.(cache.Deltas) {
						switch d.Type {
						case cache.Sync, cache.Replaced, cache.Added, cache.Updated:
							// For updates/adds, just call UpdateFunc or AddFunc.
							// The handler itself will write to the DirectStore.
							// Wait, if it's a new item, call Add. If exists, call Update.
							// Let's check directStore to see if it exists.
							_, exists, _ := directStore.Get(d.Object)
							if exists {
								handler.OnUpdate(nil, d.Object)
							} else {
								handler.OnAdd(d.Object, isInInitialList)
							}
						case cache.Deleted:
							handler.OnDelete(d.Object)
						}
					}
					return nil
				},
			}

			controller := cache.New(cfg)

			// Start the controller
			controller.Run(ctx.Done())

		}(gvr)
	}

	slog.Info(
		"Cluster syncer active and watching resources",
		"cluster", cs.clusterCfg.Name,
	)

	<-ctx.Done()
	wg.Wait()

	slog.Info(
		"Cluster syncer shutting down",
		"cluster", cs.clusterCfg.Name,
	)

	return nil
}
