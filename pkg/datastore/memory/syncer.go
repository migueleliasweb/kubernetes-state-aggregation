package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/datastore"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// NewSyncer initializes an empty in-memory Syncer.
func NewSyncer() *Syncer {
	return &Syncer{
		resources: map[string]*resourceItem{},
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

	s.resources[key] = &resourceItem{
		clusterName: clusterName,
		u:           u.DeepCopy(),
		updatedAt:   time.Now().UTC(),
	}

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

func (s *Syncer) toRecord(item *resourceItem) (datastore.ResourceRecord, error) {
	u := item.u
	gvk := u.GroupVersionKind()

	manifestBytes, err := json.Marshal(u.Object)
	if err != nil {
		return datastore.ResourceRecord{}, err
	}

	labelsBytes, err := json.Marshal(u.GetLabels())
	if err != nil {
		return datastore.ResourceRecord{}, err
	}

	annotationsBytes, err := json.Marshal(u.GetAnnotations())
	if err != nil {
		return datastore.ResourceRecord{}, err
	}

	return datastore.ResourceRecord{
		Annotations:     annotationsBytes,
		ClusterName:     item.clusterName,
		GroupName:       gvk.Group,
		Kind:            gvk.Kind,
		Labels:          labelsBytes,
		Manifest:        manifestBytes,
		Name:            u.GetName(),
		Namespace:       u.GetNamespace(),
		RawObject:       u,
		ResourceVersion: u.GetResourceVersion(),
		UID:             string(u.GetUID()),
		UpdatedAt:       item.updatedAt,
		Version:         gvk.Version,
	}, nil
}

// GetResource queries for a single resource matching the given ResourceInfo.
func (s *Syncer) GetResource(
	ctx context.Context,
	info datastore.ResourceInfo,
) (*datastore.ResourceRecord, error) {
	records, err := s.ListResources(ctx, info)
	if err != nil {
		return nil, err
	}

	if len(records) == 0 {
		return nil, datastore.ErrNotFound
	}

	if len(records) > 1 {
		return nil, fmt.Errorf("multiple resources (%d) found matching query", len(records))
	}

	return &records[0], nil
}

// ListResources queries all resources matching the specified non-empty fields in filter.
func (s *Syncer) ListResources(
	ctx context.Context,
	filter datastore.ResourceInfo,
) ([]datastore.ResourceRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var records []datastore.ResourceRecord

	for _, item := range s.resources {
		u := item.u
		gvk := u.GroupVersionKind()

		if filter.UID != "" && string(u.GetUID()) != filter.UID {
			continue
		}
		if filter.ClusterName != "" && item.clusterName != filter.ClusterName {
			continue
		}
		if filter.Group != "" && gvk.Group != filter.Group {
			continue
		}
		if filter.Version != "" && gvk.Version != filter.Version {
			continue
		}
		if filter.Kind != "" && gvk.Kind != filter.Kind {
			continue
		}
		if filter.Namespace != "" && u.GetNamespace() != filter.Namespace {
			continue
		}
		if filter.Name != "" && u.GetName() != filter.Name {
			continue
		}

		rec, err := s.toRecord(item)
		if err != nil {
			return nil, err
		}

		records = append(records, rec)
	}

	return records, nil
}

// FetchResourceGraph queries for a whole resource graph starting from a rootResourceInfo.
func (s *Syncer) FetchResourceGraph(
	ctx context.Context,
	rootResourceInfo datastore.ResourceInfo,
	callback datastore.ResourceCallback,
) (*datastore.UniqueResourceCollection, error) {
	collection := datastore.NewUniqueResourceCollection()

	roots, err := s.ListResources(ctx, rootResourceInfo)
	if err != nil {
		return nil, err
	}

	for _, rec := range roots {
		action, err := callback(rec)
		if err != nil {
			return nil, err
		}

		if action == datastore.ActionStop {
			return collection, nil
		}

		if action == datastore.ActionInclude || action == datastore.ActionIncludeAndSkipChildren {
			collection.Add(rec)
		}
	}

	return collection, nil
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
		prefix = fmt.Sprintf("%s/%s/%s/", clusterName, group, version)
	}

	for key, item := range s.resources {
		if strings.HasPrefix(key, prefix) {
			u := item.u
			gvk := u.GroupVersionKind()
			keys = append(keys, datastore.ResourceInfo{
				ClusterName:     clusterName,
				Group:           group,
				Kind:            gvk.Kind,
				Name:            u.GetName(),
				Namespace:       u.GetNamespace(),
				ResourceVersion: u.GetResourceVersion(),
				UID:             string(u.GetUID()),
				Version:         version,
			})
		}
	}

	return keys, nil
}

func (s *Syncer) ListAllResourceKeys(
	ctx context.Context,
	clusterName string,
) ([]datastore.ResourceInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var keys []datastore.ResourceInfo
	prefix := clusterName + "/"

	for key, item := range s.resources {
		if strings.HasPrefix(key, prefix) {
			u := item.u
			gvk := u.GroupVersionKind()
			keys = append(keys, datastore.ResourceInfo{
				ClusterName:     clusterName,
				Group:           gvk.Group,
				Kind:            gvk.Kind,
				Name:            u.GetName(),
				Namespace:       u.GetNamespace(),
				ResourceVersion: u.GetResourceVersion(),
				UID:             string(u.GetUID()),
				Version:         gvk.Version,
			})
		}
	}

	return keys, nil
}

func (s *Syncer) ListClusters(ctx context.Context) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	clusterSet := map[string]bool{}
	for _, item := range s.resources {
		clusterSet[item.clusterName] = true
	}

	clusters := make([]string, 0, len(clusterSet))
	for cl := range clusterSet {
		clusters = append(clusters, cl)
	}

	return clusters, nil
}

func (s *Syncer) DeleteCluster(
	ctx context.Context,
	clusterName string,
) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var count int64
	prefix := clusterName + "/"

	for key := range s.resources {
		if strings.HasPrefix(key, prefix) {
			delete(s.resources, key)
			count++
		}
	}

	return count, nil
}

func (s *Syncer) BatchDeleteResources(
	ctx context.Context,
	resources []datastore.ResourceInfo,
) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var count int64
	for _, r := range resources {
		key := s.getKey(
			r.ClusterName,
			r.Group,
			r.Version,
			r.Kind,
			r.Namespace,
			r.Name,
		)
		if _, exists := s.resources[key]; exists {
			delete(s.resources, key)
			count++
		}
	}

	return count, nil
}

func (s *Syncer) Close() error {
	return nil
}
