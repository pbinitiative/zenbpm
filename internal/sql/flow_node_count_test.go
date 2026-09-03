package sql

import (
	stdsql "database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

func TestGetFlowNodeCountReturnsZeroForMissingProcessInstance(t *testing.T) {
	db, err := stdsql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	_, err = db.ExecContext(t.Context(), `
		CREATE TABLE process_instance (
			key INTEGER PRIMARY KEY,
			flow_node_count INTEGER NOT NULL DEFAULT 0
		);
		INSERT INTO process_instance (key, flow_node_count) VALUES (1, 7);
	`)
	require.NoError(t, err)

	var count int64
	err = db.QueryRowContext(t.Context(), getFlowNodeCount, 2).Scan(&count)
	require.NoError(t, err)
	require.Zero(t, count)

	err = db.QueryRowContext(t.Context(), getFlowNodeCount, 1).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, int64(7), count)
}
