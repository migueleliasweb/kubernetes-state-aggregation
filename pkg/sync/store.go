package sync

import (
	"context"
	"fmt"
	"sync"

	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/datastore"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
)

// Build-time interface check.
var _ cache.Store = &DirectStore{}

// DirectStore is a custom client-go cache.Store that bypasses in-memory caching
// and writes directly to a datastore.Syncer. It retains only the resource keys
// in memory to satisfy the requirements of a cache.DeltaFIFO and correctly emit
// deleted events for items that disappear while the syncer is offline.
type DirectStore struct {
	clusterName string
	gvr         schema.GroupVersionResource
	db          datastore.Syncer

	mu   sync.RWMutex
	keys map[string]struct{}
}

// NewDirectStore creates a new DirectStore and initializes its in-memory key map
// by querying the datastore for currently known resources.
func NewDirectStore(
	clusterName string,
	gvr schema.GroupVersionResource,
	db datastore.Syncer,
) *DirectStore {
	return &DirectStore{
		clusterName: clusterName,
		gvr:         gvr,
		db:          db,
		keys:        make(map[string]struct{}),
	}
}

// InitializeKeys loads existing keys from the DB.
// We call this after we have extracted the Kind from the first object, or we pass Kind if known.
func (s *DirectStore) InitializeKeys(ctx context.Context, kind string) error {
	keys, err := s.db.ListResourceKeys(ctx, s.clusterName, s.gvr.Group, s.gvr.Version, kind)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range keys {
		// Use client-go MetaNamespaceKeyFunc format: namespace/name or just name
		key := k.Name
		if k.Namespace != "" {
			key = k.Namespace + "/" + k.Name
		}
		s.keys[key] = struct{}{}
	}
	return nil
}

func (s *DirectStore) Add(obj interface{}) error {
	return s.Update(obj)
}

func (s *DirectStore) Update(obj interface{}) error {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return fmt.Errorf("expected unstructured, got %T", obj)
	}

	if err := s.db.UpsertResource(context.Background(), s.clusterName, u); err != nil {
		return err
	}

	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err == nil {
		s.mu.Lock()
		s.keys[key] = struct{}{}
		s.mu.Unlock()
	}

	return nil
}

func (s *DirectStore) Delete(obj interface{}) error {
	var u *unstructured.Unstructured
	var ok bool

	u, ok = obj.(*unstructured.Unstructured)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return fmt.Errorf("expected unstructured or tombstone, got %T", obj)
		}

		u, ok = tombstone.Obj.(*unstructured.Unstructured)
		if !ok {
			return fmt.Errorf("tombstone contained unknown object %T", tombstone.Obj)
		}
	}

	gvk := u.GroupVersionKind()
	info := datastore.ResourceInfo{
		ClusterName:     s.clusterName,
		Group:           gvk.Group,
		Version:         gvk.Version,
		Kind:            gvk.Kind,
		Namespace:       u.GetNamespace(),
		Name:            u.GetName(),
		ResourceVersion: u.GetResourceVersion(),
		UID:             string(u.GetUID()),
	}

	if err := s.db.DeleteResource(context.Background(), info); err != nil {
		return err
	}

	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err == nil {
		s.mu.Lock()
		delete(s.keys, key)
		s.mu.Unlock()
	}

	return nil
}

func (s *DirectStore) List() []interface{} {
	// Not needed by our controller implementation
	return nil
}

func (s *DirectStore) ListKeys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.keys))
	for k := range s.keys {
		keys = append(keys, k)
	}
	return keys
}

func (s *DirectStore) Get(obj interface{}) (item interface{}, exists bool, err error) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		return nil, false, err
	}
	return s.GetByKey(key)
}

func (s *DirectStore) GetByKey(key string) (item interface{}, exists bool, err error) {
	s.mu.RLock()
	_, exists = s.keys[key]
	s.mu.RUnlock()

	// Returning exists=true satisfies processDeltas so it can decide between Add/Update.
	// The item itself is not needed because our EventHandler doesn't use the 'old' object.
	if exists {
		return struct{}{}, true, nil
	}
	return nil, false, nil
}

func (s *DirectStore) Replace(list []interface{}, resourceVersion string) error {
	s.mu.Lock()
	newKeys := make(map[string]struct{})
	for _, obj := range list {
		key, err := cache.MetaNamespaceKeyFunc(obj)
		if err == nil {
			newKeys[key] = struct{}{}
		}
	}
	s.keys = newKeys
	s.mu.Unlock()

	return nil
}

func (s *DirectStore) Resync() error {
	return nil
}

func (s *DirectStore) LastStoreSyncResourceVersion() string {
	return ""
}

func (s *DirectStore) Bookmark(rv string) {
	// No-op
}
