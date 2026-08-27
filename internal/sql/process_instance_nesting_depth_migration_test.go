package sql

import (
	stdsql "database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

func TestNestingDepthMigrationBackfillsExistingHierarchy(t *testing.T) {
	db, err := stdsql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx := t.Context()

	_, err = db.ExecContext(ctx, `
		CREATE TABLE process_instance (
			key INTEGER PRIMARY KEY,
			parent_process_execution_token INTEGER
		);
		CREATE TABLE execution_token (
			key INTEGER PRIMARY KEY,
			process_instance_key INTEGER NOT NULL
		);

		INSERT INTO process_instance (key, parent_process_execution_token) VALUES
			(1, NULL),
			(2, 10),
			(3, 20),
			(4, NULL),
			(5, 999),
			(6, 30);
		INSERT INTO execution_token (key, process_instance_key) VALUES
			(10, 1),
			(20, 2),
			(30, 5);
	`)
	require.NoError(t, err)

	migration, err := readMigrationFile(DefaultMigrationsDir, "0013_process_instance_nesting_depth.up.sql")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, string(migration))
	require.NoError(t, err)

	rows, err := db.QueryContext(ctx, "SELECT key, nesting_depth FROM process_instance ORDER BY key")
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()

	actual := make(map[int64]int64)
	for rows.Next() {
		var key, nestingDepth int64
		require.NoError(t, rows.Scan(&key, &nestingDepth))
		actual[key] = nestingDepth
	}
	require.NoError(t, rows.Err())
	require.Equal(t, map[int64]int64{1: 0, 2: 1, 3: 2, 4: 0, 5: 1, 6: 2}, actual)
}
