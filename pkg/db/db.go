package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// ResourceRecord represents a row in the resources table.
type ResourceRecord struct {
	ClusterName     string          `json:"cluster_name"`
	GroupName       string          `json:"group_name"`
	Version         string          `json:"version"`
	Kind            string          `json:"kind"`
	Namespace       string          `json:"namespace"`
	Name            string          `json:"name"`
	UID             string          `json:"uid"`
	ResourceVersion string          `json:"resource_version"`
	Labels          json.RawMessage `json:"labels"`
	Manifest        json.RawMessage `json:"manifest"`
	UpdatedAt       time.Time       `json:"updated_at"`
}

// Store defines operations for managing aggregated Kubernetes state.
type Store interface {
	InitSchema(ctx context.Context) error
	UpsertResource(ctx context.Context, clusterName string, u *unstructured.Unstructured) error
	DeleteResource(ctx context.Context, clusterName, group, version, kind, namespace, name string) error
	Close() error
}

// DBStore implements Store backed by PostgreSQL.
type DBStore struct {
	db *sql.DB
}

// NewStore connects to PostgreSQL using the provided connection string.
func NewStore(dbURL string) (*DBStore, error) {
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &DBStore{db: db}, nil
}

// NewStoreWithDB creates a DBStore wrapping an existing *sql.DB (useful for testing).
func NewStoreWithDB(db *sql.DB) *DBStore {
	return &DBStore{db: db}
}

// InitSchema creates the resources table and associated GIN indexes if they do not exist.
func (s *DBStore) InitSchema(ctx context.Context) error {
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

	CREATE INDEX IF NOT EXISTS idx_resources_manifest_gin ON resources USING gin (manifest jsonb_path_ops);
	CREATE INDEX IF NOT EXISTS idx_resources_labels_gin ON resources USING gin (labels);
	`
	_, err := s.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to initialize schema: %w", err)
	}
	return nil
}

// UpsertResource inserts or updates a resource manifest in the datastore.
func (s *DBStore) UpsertResource(ctx context.Context, clusterName string, u *unstructured.Unstructured) error {
	gvk := u.GroupVersionKind()
	labels := u.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
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

	_, err = s.db.ExecContext(ctx, query,
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
		return fmt.Errorf("failed to upsert resource %s/%s in cluster %s: %w", u.GetNamespace(), u.GetName(), clusterName, err)
	}

	return nil
}

// DeleteResource removes a resource from the datastore.
func (s *DBStore) DeleteResource(ctx context.Context, clusterName, group, version, kind, namespace, name string) error {
	query := `
	DELETE FROM resources
	WHERE cluster_name = $1
	  AND group_name = $2
	  AND version = $3
	  AND kind = $4
	  AND namespace = $5
	  AND name = $6;
	`
	_, err := s.db.ExecContext(ctx, query, clusterName, group, version, kind, namespace, name)
	if err != nil {
		return fmt.Errorf("failed to delete resource %s/%s in cluster %s: %w", namespace, name, clusterName, err)
	}

	return nil
}

// Close closes the database connection pool.
func (s *DBStore) Close() error {
	return s.db.Close()
}
