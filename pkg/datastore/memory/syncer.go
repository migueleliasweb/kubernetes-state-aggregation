package memory

import (
	"context"
	"fmt"
	"strings"

	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/datastore"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// NewSyncer initializes an empty in-memory Syncer.
func NewSyncer() *Syncer {
	return &Syncer{
		resources: map[string]*unstructured.Unstructured{},
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
	resourceInfo datastore.ResourceInfo,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := s.getKey(
		resourceInfo.ClusterName,
		resourceInfo.Group,
		resourceInfo.Version,
		resourceInfo.Kind,
		resourceInfo.Namespace,
		resourceInfo.Name,
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

func (s *Syncer) ListResourceKeys(
	ctx context.Context,
	clusterName string,
	group string,
	version string,
	kind string,
) ([]datastore.ResourceInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var keys []datastore.ResourceInfo
	var prefix string
	if kind != "" {
		prefix = fmt.Sprintf("%s/%s/%s/%s/", clusterName, group, version, kind)
	} else {
		// Just match the version prefix
		prefix = fmt.Sprintf("%s/%s/%s/", clusterName, group, version)
	}

	for key, u := range s.resources {
		if strings.HasPrefix(key, prefix) {
			gvk := u.GroupVersionKind()
			keys = append(keys, datastore.ResourceInfo{
				ClusterName:     clusterName,
				Group:           group,
				Version:         version,
				Kind:            gvk.Kind,
				Namespace:       u.GetNamespace(),
				Name:            u.GetName(),
				UID:             string(u.GetUID()),
				ResourceVersion: u.GetResourceVersion(),
			})
		}
	}

	return keys, nil
}

func (s *Syncer) Close() error {
	return nil
}
