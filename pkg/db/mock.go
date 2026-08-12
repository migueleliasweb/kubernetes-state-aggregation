package db

import (
	"context"
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// MockStore provides an in-memory implementation of Store for unit testing.
type MockStore struct {
	mu        sync.RWMutex
	Resources map[string]*unstructured.Unstructured
}

// NewMockStore initializes an empty MockStore.
func NewMockStore() *MockStore {
	return &MockStore{
		Resources: make(map[string]*unstructured.Unstructured),
	}
}

func (m *MockStore) InitSchema(ctx context.Context) error {
	return nil
}

func (m *MockStore) getKey(clusterName, group, version, kind, namespace, name string) string {
	return fmt.Sprintf("%s/%s/%s/%s/%s/%s", clusterName, group, version, kind, namespace, name)
}

func (m *MockStore) UpsertResource(ctx context.Context, clusterName string, u *unstructured.Unstructured) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	gvk := u.GroupVersionKind()
	key := m.getKey(clusterName, gvk.Group, gvk.Version, gvk.Kind, u.GetNamespace(), u.GetName())
	m.Resources[key] = u.DeepCopy()
	return nil
}

func (m *MockStore) DeleteResource(ctx context.Context, clusterName, group, version, kind, namespace, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := m.getKey(clusterName, group, version, kind, namespace, name)
	delete(m.Resources, key)
	return nil
}

func (m *MockStore) GetResource(clusterName, group, version, kind, namespace, name string) *unstructured.Unstructured {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := m.getKey(clusterName, group, version, kind, namespace, name)
	return m.Resources[key]
}

func (m *MockStore) Close() error {
	return nil
}
