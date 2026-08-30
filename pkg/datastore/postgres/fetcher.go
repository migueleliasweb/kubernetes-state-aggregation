package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/datastore"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func (s *PG) fetchResourceRecords(
	ctx context.Context,
	query string,
	args ...any,
) ([]datastore.ResourceRecord, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []datastore.ResourceRecord
	for rows.Next() {
		var rec datastore.ResourceRecord
		var labelsJSON, manifestJSON []byte

		err := rows.Scan(
			&rec.ClusterName,
			&rec.GroupName,
			&rec.Version,
			&rec.Kind,
			&rec.Namespace,
			&rec.Name,
			&rec.UID,
			&rec.ResourceVersion,
			&labelsJSON,
			&manifestJSON,
			&rec.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}

		rec.Labels = labelsJSON
		rec.Manifest = manifestJSON
		records = append(records, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return records, nil
}

// GetResource queries for a single resource matching the given ResourceInfo.
func (s *PG) GetResource(
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
		return nil, fmt.Errorf("multiple resources (%d) found matching query; specify --cluster or --namespace to disambiguate", len(records))
	}

	return &records[0], nil
}

// ListResources queries resources table filtering by any non-empty field in filter.
func (s *PG) ListResources(
	ctx context.Context,
	filter datastore.ResourceInfo,
) ([]datastore.ResourceRecord, error) {
	baseQuery := `
		SELECT cluster_name, group_name, version, kind, namespace, name, uid, resource_version, labels, manifest, updated_at
		FROM resources
		WHERE 1=1
	`
	var args []any
	argID := 1

	if filter.UID != "" {
		baseQuery += fmt.Sprintf(" AND uid = $%d", argID)
		args = append(args, filter.UID)
		argID++
	}

	if filter.ClusterName != "" {
		baseQuery += fmt.Sprintf(" AND cluster_name = $%d", argID)
		args = append(args, filter.ClusterName)
		argID++
	}

	if filter.Group != "" {
		baseQuery += fmt.Sprintf(" AND group_name = $%d", argID)
		args = append(args, filter.Group)
		argID++
	}

	if filter.Version != "" {
		baseQuery += fmt.Sprintf(" AND version = $%d", argID)
		args = append(args, filter.Version)
		argID++
	}

	if filter.Kind != "" {
		baseQuery += fmt.Sprintf(" AND kind = $%d", argID)
		args = append(args, filter.Kind)
		argID++
	}

	if filter.Namespace != "" {
		baseQuery += fmt.Sprintf(" AND namespace = $%d", argID)
		args = append(args, filter.Namespace)
		argID++
	}

	if filter.Name != "" {
		baseQuery += fmt.Sprintf(" AND name = $%d", argID)
		args = append(args, filter.Name)
		argID++
	}

	baseQuery += " ORDER BY cluster_name, namespace, name"

	records, err := s.fetchResourceRecords(ctx, baseQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list resources: %w", err)
	}

	return records, nil
}

// FetchResourceGraph fetches a graph of related resources starting from rootResourceInfo.
// It resolves relationships iteratively using breadth-first search.
func (s *PG) FetchResourceGraph(
	ctx context.Context,
	rootResourceInfo datastore.ResourceInfo,
	callback datastore.ResourceCallback,
) (*datastore.UniqueResourceCollection, error) {
	collection := datastore.NewUniqueResourceCollection()

	// 1. Fetch root node(s)
	var rootRecords []datastore.ResourceRecord
	var err error

	baseQuery := `
		SELECT cluster_name, group_name, version, kind, namespace, name, uid, resource_version, labels, manifest, updated_at
		FROM resources
		WHERE 1=1
	`
	var args []any
	argID := 1

	if rootResourceInfo.ClusterName != "" {
		baseQuery += fmt.Sprintf(" AND cluster_name = $%d", argID)
		args = append(args, rootResourceInfo.ClusterName)
		argID++
	}
	if rootResourceInfo.Group != "" {
		baseQuery += fmt.Sprintf(" AND group_name = $%d", argID)
		args = append(args, rootResourceInfo.Group)
		argID++
	}
	if rootResourceInfo.Version != "" {
		baseQuery += fmt.Sprintf(" AND version = $%d", argID)
		args = append(args, rootResourceInfo.Version)
		argID++
	}

	if rootResourceInfo.Namespace != "" {
		baseQuery += fmt.Sprintf(" AND namespace = $%d", argID)
		args = append(args, rootResourceInfo.Namespace)
		argID++
	}

	baseQuery += fmt.Sprintf(" AND kind = $%d AND name = $%d", argID, argID+1)
	args = append(args, rootResourceInfo.Kind, rootResourceInfo.Name)

	rootRecords, err = s.fetchResourceRecords(ctx, baseQuery, args...)

	if err != nil {
		return nil, fmt.Errorf("failed to fetch root resource(s): %w", err)
	}
	if len(rootRecords) == 0 {
		return collection, nil
	}

	// Queue for BFS
	queue := rootRecords
	visited := map[datastore.ResourceKey]bool{}

	for len(queue) > 0 {
		var nextLayer []datastore.ResourceRecord

		// Group parent and children UIDs by cluster
		parentsByCluster := map[string][]string{}
		childrenByCluster := map[string][]string{}

		for _, rec := range queue {
			vKey := datastore.GetResourceKey(rec.ClusterName, rec.UID)
			if visited[vKey] {
				continue
			}
			visited[vKey] = true

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

			if action == datastore.ActionIncludeAndSkipChildren || action == datastore.ActionExcludeAndSkipChildren {
				continue
			}

			// We need to fetch related resources (upwards and downwards) within the SAME cluster.
			// Upwards: extract ownerReferences from manifest
			if rec.Manifest != nil {
				var u unstructured.Unstructured
				if err := json.Unmarshal(rec.Manifest, &u.Object); err == nil {
					for _, ownerRef := range u.GetOwnerReferences() {
						pKey := datastore.GetResourceKey(rec.ClusterName, string(ownerRef.UID))
						if !visited[pKey] {
							parentsByCluster[rec.ClusterName] = append(parentsByCluster[rec.ClusterName], string(ownerRef.UID))
						}
					}
				}
			}

			// Downwards: search for resources that list this UID as an ownerReference
			childrenByCluster[rec.ClusterName] = append(childrenByCluster[rec.ClusterName], rec.UID)
		}

		// Batch fetch parents by UIDs (per cluster)
		for cName, uids := range parentsByCluster {
			if len(uids) == 0 {
				continue
			}
			placeholders := make([]string, len(uids))
			args := make([]any, len(uids)+1)
			args[0] = cName
			for i, uid := range uids {
				placeholders[i] = fmt.Sprintf("$%d", i+2)
				args[i+1] = uid
			}

			pQuery := fmt.Sprintf(`
				SELECT cluster_name, group_name, version, kind, namespace, name, uid, resource_version, labels, manifest, updated_at
				FROM resources
				WHERE cluster_name = $1 AND uid IN (%s)
			`, strings.Join(placeholders, ","))

			parents, err := s.fetchResourceRecords(ctx, pQuery, args...)
			if err != nil {
				return nil, fmt.Errorf("failed to fetch parent resources in cluster %s: %w", cName, err)
			}
			nextLayer = append(nextLayer, parents...)
		}

		// Batch fetch children using JSONB array containment (per cluster)
		for cName, uids := range childrenByCluster {
			for _, uid := range uids {
				cQuery := `
					SELECT cluster_name, group_name, version, kind, namespace, name, uid, resource_version, labels, manifest, updated_at
					FROM resources
					WHERE cluster_name = $1 AND manifest @> $2::jsonb
				`
				ownerRefQuery := fmt.Sprintf(`{"metadata":{"ownerReferences":[{"uid":"%s"}]}}`, uid)

				children, err := s.fetchResourceRecords(ctx, cQuery, cName, ownerRefQuery)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch children for %s in cluster %s: %w", uid, cName, err)
				}
				nextLayer = append(nextLayer, children...)
			}
		}

		queue = nextLayer
	}

	return collection, nil
}
