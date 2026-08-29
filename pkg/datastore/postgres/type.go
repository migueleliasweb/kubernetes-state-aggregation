package postgres

import (
	"database/sql"

	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/datastore"
)

// Build-time interface checks.
var _ datastore.Syncer = (*PG)(nil)
var _ datastore.Fetcher = (*PG)(nil)

// PG implements datastore.PG backed by PostgreSQL.
type PG struct {
	db *sql.DB
}
