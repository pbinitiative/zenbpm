package sql

import (
	"context"
	stdsql "database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

func TestProcessInstanceFlowNodeCountMigrationUpAndDownPreservesExistingData(t *testing.T) {
	db, err := stdsql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx := t.Context()

	_, err = db.ExecContext(ctx, `
		CREATE TABLE process_instance (
			key INTEGER PRIMARY KEY,
			state INTEGER NOT NULL
		);
		CREATE TABLE incident (
			key INTEGER PRIMARY KEY,
			message TEXT NOT NULL
		);
		INSERT INTO process_instance (key, state) VALUES (1, 1), (2, 4);
		INSERT INTO incident (key, message) VALUES (10, 'existing incident');
	`)
	require.NoError(t, err)

	up, err := readMigrationFile(DefaultMigrationsDir, "0014_process_instance_flow_node_count.up.sql")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, string(up))
	require.NoError(t, err)

	var incidentType string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT incident_type FROM incident WHERE key = 10").Scan(&incidentType))
	require.Empty(t, incidentType)

	for _, key := range []int64{1, 2} {
		var count int64
		require.NoError(t, db.QueryRowContext(ctx, "SELECT flow_node_count FROM process_instance WHERE key = ?", key).Scan(&count))
		require.Zero(t, count)
	}
	_, err = db.ExecContext(ctx, "UPDATE process_instance SET flow_node_count = flow_node_count + 1 WHERE key = ?", 1)
	require.NoError(t, err)
	var count int64
	require.NoError(t, db.QueryRowContext(ctx, "SELECT flow_node_count FROM process_instance WHERE key = ?", 1).Scan(&count))
	require.Equal(t, int64(1), count)
	_, err = db.ExecContext(ctx, "UPDATE process_instance SET flow_node_count = 0 WHERE key = ?", 1)
	require.NoError(t, err)
	require.NoError(t, db.QueryRowContext(ctx, "SELECT flow_node_count FROM process_instance WHERE key = ?", 1).Scan(&count))
	require.Zero(t, count)

	down, err := readMigrationFile(DefaultMigrationsDir, "0014_process_instance_flow_node_count.down.sql")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, string(down))
	require.NoError(t, err)

	require.NotContains(t, sqliteColumnNames(t, ctx, db, "process_instance"), "flow_node_count")
	require.NotContains(t, sqliteColumnNames(t, ctx, db, "incident"), "incident_type")
	var processInstances, incidents int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM process_instance").Scan(&processInstances))
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM incident").Scan(&incidents))
	require.Equal(t, 2, processInstances)
	require.Equal(t, 1, incidents)
}

func sqliteColumnNames(t testing.TB, ctx context.Context, db *stdsql.DB, table string) map[string]struct{} {
	t.Helper()
	var query string
	switch table {
	case "process_instance":
		query = "PRAGMA table_info(process_instance)"
	case "incident":
		query = "PRAGMA table_info(incident)"
	default:
		t.Fatalf("unsupported table %q", table)
	}
	rows, err := db.QueryContext(ctx, query)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()

	columns := make(map[string]struct{})
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		require.NoError(t, rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey))
		columns[name] = struct{}{}
	}
	require.NoError(t, rows.Err())
	return columns
}
