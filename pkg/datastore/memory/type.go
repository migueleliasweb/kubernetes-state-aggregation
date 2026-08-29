package memory

import (
	"sync"

	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/datastore"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Build-time interface check.
var _ datastore.Syncer = (*Syncer)(nil)

// Syncer provides an in-memory implementation of datastore.Syncer for testing and development.
type Syncer struct {
	mu        sync.RWMutex
	resources map[string]*unstructured.Unstructured
}
