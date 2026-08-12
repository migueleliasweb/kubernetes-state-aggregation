package sync

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/config"
	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/db"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Manager orchestrates multiple ClusterSyncer instances.
type Manager struct {
	cfg           *config.Config
	store         db.Store
	targetCluster string
}

// NewManager creates a new multi-cluster Manager.
func NewManager(cfg *config.Config, store db.Store, targetCluster string) *Manager {
	return &Manager{
		cfg:           cfg,
		store:         store,
		targetCluster: targetCluster,
	}
}

// Start launches syncers for selected/configured clusters in goroutines and blocks until ctx is done.
func (m *Manager) Start(ctx context.Context) error {
	var wg sync.WaitGroup

	activeCount := 0
	for _, cluster := range m.cfg.Clusters {
		if cluster.Disabled {
			log.Printf("[%s] Cluster is disabled, skipping", cluster.Name)
			continue
		}

		if m.targetCluster != "" && cluster.Name != m.targetCluster {
			log.Printf("[%s] Skipping cluster (target filter is set to %s)", cluster.Name, m.targetCluster)
			continue
		}

		activeCount++
		wg.Add(1)

		clusterCopy := cluster
		go func(c config.ClusterConfig) {
			defer wg.Done()

			effectiveFilters := m.cfg.GetEffectiveFilters(c.Name)

			restCfg, err := buildRESTConfig(c)
			if err != nil {
				log.Printf("[%s] error building kubeconfig: %v", c.Name, err)
				return
			}

			dynClient, err := dynamic.NewForConfig(restCfg)
			if err != nil {
				log.Printf("[%s] error creating dynamic client: %v", c.Name, err)
				return
			}

			discClient, err := discovery.NewDiscoveryClientForConfig(restCfg)
			if err != nil {
				log.Printf("[%s] error creating discovery client: %v", c.Name, err)
				return
			}

			syncer := NewClusterSyncer(c, effectiveFilters, m.store, dynClient, discClient)
			if err := syncer.Start(ctx); err != nil {
				log.Printf("[%s] syncer stopped with error: %v", c.Name, err)
			}
		}(clusterCopy)
	}

	if activeCount == 0 {
		return fmt.Errorf("no active clusters match execution parameters")
	}

	log.Printf("Sync Manager running with %d active cluster syncers", activeCount)
	<-ctx.Done()
	log.Println("Sync Manager waiting for cluster syncers to finish...")
	wg.Wait()
	log.Println("Sync Manager shutdown complete")
	return nil
}

func buildRESTConfig(c config.ClusterConfig) (*rest.Config, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if c.Kubeconfig != "" {
		loadingRules.ExplicitPath = c.Kubeconfig
	}

	overrides := &clientcmd.ConfigOverrides{}
	if c.APIServer != "" {
		overrides.ClusterInfo.Server = c.APIServer
	}
	if c.Context != "" {
		overrides.CurrentContext = c.Context
	}

	clientCfg := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, overrides)
	restCfg, err := clientCfg.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to build REST client config: %w", err)
	}

	return restCfg, nil
}
