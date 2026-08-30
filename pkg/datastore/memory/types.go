package memory

import (
	"sync"
	"time"

	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/datastore"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Build-time interface check.
var _ datastore.Syncer = (*Syncer)(nil)
var _ datastore.Fetcher = (*Syncer)(nil)

type resourceItem struct {
	clusterName string
	u           *unstructured.Unstructured
	updatedAt   time.Time
}

// Syncer provides an in-memory implementation of datastore.Syncer and datastore.Fetcher for testing and development.
type Syncer struct {
	mu        sync.RWMutex
	resources map[string]*resourceItem
}
