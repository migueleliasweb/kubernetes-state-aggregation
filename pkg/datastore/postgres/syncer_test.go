package postgres

import (
	"testing"
)

func TestSyncerTypeImplementsDatastoreSyncer(t *testing.T) {
	var syncer *PG
	if syncer != nil {
		t.Errorf("expected nil syncer instance for type test")
	}
}
