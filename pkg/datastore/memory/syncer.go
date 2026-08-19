package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/datastore"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

var _ datastore.Syncer = (*Syncer)(nil)

// Syncer provides an in-memory implementation of datastore.Syncer for testing and development.
type Syncer struct {
	mu        sync.RWMutex
	resources map[string]*unstructured.Unstructured
}

// NewSyncer initializes an empty in-memory Syncer.
func NewSyncer() *Syncer {
	return &Syncer{
		resources: make(map[string]*unstructured.Unstructured),
	}
}

func (s *Syncer) InitSchema(ctx context.Context) error {
	return nil
}

func (s *Syncer) getKey(
	clusterName string,
	group string,
	version string,
	kind string,
	namespace string,
	name string,
) string {
	return fmt.Sprintf("%s/%s/%s/%s/%s/%s", clusterName, group, version, kind, namespace, name)
}

func (s *Syncer) UpsertResource(
	ctx context.Context,
	clusterName string,
	u *unstructured.Unstructured,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	gvk := u.GroupVersionKind()
	key := s.getKey(
		clusterName,
		gvk.Group,
		gvk.Version,
		gvk.Kind,
		u.GetNamespace(),
		u.GetName(),
	)

	s.resources[key] = u.DeepCopy()

	return nil
}

func (s *Syncer) DeleteResource(
	ctx context.Context,
	clusterName string,
	group string,
	version string,
	kind string,
	namespace string,
	name string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := s.getKey(
		clusterName,
		group,
		version,
		kind,
		namespace,
		name,
	)

	delete(s.resources, key)

	return nil
}

// GetResource retrieves a resource by its key from the in-memory store.
func (s *Syncer) GetResource(
	clusterName string,
	group string,
	version string,
	kind string,
	namespace string,
	name string,
) *unstructured.Unstructured {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := s.getKey(
		clusterName,
		group,
		version,
		kind,
		namespace,
		name,
	)

	return s.resources[key]
}

func (s *Syncer) Close() error {
	return nil
}
