package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/datastore"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// NewPGSyncer connects to PostgreSQL using the provided connection string.
func NewPGSyncer(dbURL string) (*PG, error) {
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &PG{db: db}, nil
}

// NewSyncerWithDB creates a Syncer wrapping an existing *sql.DB (useful for testing).
func NewSyncerWithDB(db *sql.DB) *PG {
	return &PG{db: db}
}

// InitSchema creates the resources table and associated GIN indexes if they do not exist.
func (s *PG) InitSchema(ctx context.Context) error {
	query := `
	CREATE TABLE IF NOT EXISTS resources (
		cluster_name     VARCHAR(255) NOT NULL,
		group_name       VARCHAR(255) NOT NULL DEFAULT '',
		version          VARCHAR(64)  NOT NULL,
		kind             VARCHAR(255) NOT NULL,
		namespace        VARCHAR(255) NOT NULL DEFAULT '',
		name             VARCHAR(255) NOT NULL,
		uid              VARCHAR(255) NOT NULL,
		resource_version VARCHAR(255) NOT NULL,
		labels           JSONB        NOT NULL DEFAULT '{}',
		manifest         JSONB        NOT NULL,
		updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
		PRIMARY KEY (cluster_name, group_name, version, kind, namespace, name)
	);

	CREATE INDEX IF NOT EXISTS idx_resources_manifest_gin ON resources USING gin (manifest);
	CREATE INDEX IF NOT EXISTS idx_resources_labels_gin ON resources USING gin (labels);
	`
	_, err := s.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to initialize schema: %w", err)
	}

	return nil
}

// UpsertResource inserts or updates a resource manifest in the datastore.
func (s *PG) UpsertResource(
	ctx context.Context,
	clusterName string,
	u *unstructured.Unstructured,
) error {
	gvk := u.GroupVersionKind()
	labels := u.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}

	labelsJSON, err := json.Marshal(labels)
	if err != nil {
		return fmt.Errorf("failed to marshal labels: %w", err)
	}

	manifestJSON, err := json.Marshal(u.Object)
	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %w", err)
	}

	query := `
	INSERT INTO resources (
		cluster_name, group_name, version, kind, namespace, name,
		uid, resource_version, labels, manifest, updated_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
	ON CONFLICT (cluster_name, group_name, version, kind, namespace, name)
	DO UPDATE SET
		uid = EXCLUDED.uid,
		resource_version = EXCLUDED.resource_version,
		labels = EXCLUDED.labels,
		manifest = EXCLUDED.manifest,
		updated_at = NOW();
	`

	_, err = s.db.ExecContext(
		ctx,
		query,
		clusterName,
		gvk.Group,
		gvk.Version,
		gvk.Kind,
		u.GetNamespace(),
		u.GetName(),
		string(u.GetUID()),
		u.GetResourceVersion(),
		string(labelsJSON),
		string(manifestJSON),
	)
	if err != nil {
		return fmt.Errorf(
			"failed to upsert resource %s/%s in cluster %s: %w",
			u.GetNamespace(),
			u.GetName(),
			clusterName,
			err,
		)
	}

	return nil
}

// DeleteResource removes a resource from the datastore.
func (s *PG) DeleteResource(
	ctx context.Context,
	resourceInfo datastore.ResourceInfo,
) error {
	query := `
	DELETE FROM resources
	WHERE cluster_name = $1
	  AND group_name = $2
	  AND version = $3
	  AND kind = $4
	  AND namespace = $5
	  AND name = $6;
	`
	_, err := s.db.ExecContext(
		ctx,
		query,
		resourceInfo.ClusterName,
		resourceInfo.Group,
		resourceInfo.Version,
		resourceInfo.Kind,
		resourceInfo.Namespace,
		resourceInfo.Name,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to delete resource %s/%s in cluster %s: %w",
			resourceInfo.Namespace,
			resourceInfo.Name,
			resourceInfo.ClusterName,
			err,
		)
	}

	return nil
}

// ListResourceKeys retrieves all basic resource identifiers for a given cluster and GVR.
func (s *PG) ListResourceKeys(
	ctx context.Context,
	clusterName string,
	group string,
	version string,
	kind string,
) ([]datastore.ResourceInfo, error) {
	query := `
	SELECT namespace, name, uid, resource_version, kind
	FROM resources
	WHERE cluster_name = $1
	  AND group_name = $2
	  AND version = $3
	`
	args := []interface{}{clusterName, group, version}

	if kind != "" {
		query += " AND kind = $4"
		args = append(args, kind)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list resource keys: %w", err)
	}
	defer rows.Close()

	var keys []datastore.ResourceInfo
	for rows.Next() {
		var info datastore.ResourceInfo
		info.ClusterName = clusterName
		info.Group = group
		info.Version = version
		info.Kind = kind

		if err := rows.Scan(
			&info.Namespace,
			&info.Name,
			&info.UID,
			&info.ResourceVersion,
			&info.Kind,
		); err != nil {
			return nil, fmt.Errorf("failed to scan resource key row: %w", err)
		}
		keys = append(keys, info)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating resource keys: %w", err)
	}

	return keys, nil
}

// ListAllResourceKeys retrieves all resource identifiers for a given cluster.
func (s *PG) ListAllResourceKeys(
	ctx context.Context,
	clusterName string,
) ([]datastore.ResourceInfo, error) {
	query := `
	SELECT group_name, version, kind, namespace, name, uid, resource_version
	FROM resources
	WHERE cluster_name = $1
	`
	rows, err := s.db.QueryContext(ctx, query, clusterName)
	if err != nil {
		return nil, fmt.Errorf("failed to list all resource keys: %w", err)
	}
	defer rows.Close()

	var keys []datastore.ResourceInfo
	for rows.Next() {
		var info datastore.ResourceInfo
		info.ClusterName = clusterName

		if err := rows.Scan(
			&info.Group,
			&info.Version,
			&info.Kind,
			&info.Namespace,
			&info.Name,
			&info.UID,
			&info.ResourceVersion,
		); err != nil {
			return nil, fmt.Errorf("failed to scan resource key row: %w", err)
		}

		keys = append(keys, info)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating resource keys: %w", err)
	}

	return keys, nil
}

// ListClusters retrieves all distinct cluster names present in the datastore.
func (s *PG) ListClusters(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT DISTINCT cluster_name FROM resources")
	if err != nil {
		return nil, fmt.Errorf("failed to list clusters: %w", err)
	}
	defer rows.Close()

	var clusters []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan cluster name: %w", err)
		}

		clusters = append(clusters, name)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating cluster names: %w", err)
	}

	return clusters, nil
}

// DeleteCluster removes all resources belonging to a cluster and returns the deleted row count.
func (s *PG) DeleteCluster(
	ctx context.Context,
	clusterName string,
) (int64, error) {
	res, err := s.db.ExecContext(
		ctx,
		"DELETE FROM resources WHERE cluster_name = $1",
		clusterName,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to delete cluster %s resources: %w", clusterName, err)
	}

	rowsAff, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return rowsAff, nil
}

// BatchDeleteResources removes a slice of resources within a single transaction.
func (s *PG) BatchDeleteResources(
	ctx context.Context,
	resources []datastore.ResourceInfo,
) (int64, error) {
	if len(resources) == 0 {
		return 0, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
	DELETE FROM resources
	WHERE cluster_name = $1
	  AND group_name = $2
	  AND version = $3
	  AND kind = $4
	  AND namespace = $5
	  AND name = $6;
	`)
	if err != nil {
		return 0, fmt.Errorf("failed to prepare delete statement: %w", err)
	}
	defer stmt.Close()

	var totalDeleted int64
	for _, r := range resources {
		res, err := stmt.ExecContext(
			ctx,
			r.ClusterName,
			r.Group,
			r.Version,
			r.Kind,
			r.Namespace,
			r.Name,
		)
		if err != nil {
			return 0, fmt.Errorf("failed to execute batch delete: %w", err)
		}

		rowsAff, err := res.RowsAffected()
		if err == nil {
			totalDeleted += rowsAff
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit batch delete transaction: %w", err)
	}

	return totalDeleted, nil
}

// Close closes the database connection pool.
func (s *PG) Close() error {
	return s.db.Close()
}
