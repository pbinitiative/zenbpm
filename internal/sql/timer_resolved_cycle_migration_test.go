package sql

import (
	stdsql "database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

func TestTimerResolvedCycleMigrationUpAndDownPreservesExistingData(t *testing.T) {
	db, err := stdsql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx := t.Context()

	_, err = db.ExecContext(ctx, `
		CREATE TABLE timer (
			key INTEGER PRIMARY KEY,
			element_id TEXT NOT NULL,
			state INTEGER NOT NULL,
			due_at INTEGER NOT NULL
		);
		INSERT INTO timer (key, element_id, state, due_at) VALUES (1, 'literal-cycle', 1, 1000), (2, 'duration', 2, 2000);
	`)
	require.NoError(t, err)

	up, err := readMigrationFile(DefaultMigrationsDir, "0021_timer_resolved_cycle.up.sql")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, string(up))
	require.NoError(t, err)

	// a timer created before the migration has no resolved cycle: its cycle is read from the model
	for _, key := range []int64{1, 2} {
		var resolvedCycle stdsql.NullString
		require.NoError(t, db.QueryRowContext(ctx, "SELECT resolved_cycle FROM timer WHERE key = ?", key).Scan(&resolvedCycle))
		require.False(t, resolvedCycle.Valid)
	}
	_, err = db.ExecContext(ctx, "INSERT INTO timer (key, element_id, state, due_at, resolved_cycle) VALUES (3, 'expression-cycle', 1, 3000, 'R3/PT1H')")
	require.NoError(t, err)
	var resolvedCycle string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT resolved_cycle FROM timer WHERE key = 3").Scan(&resolvedCycle))
	require.Equal(t, "R3/PT1H", resolvedCycle)

	down, err := readMigrationFile(DefaultMigrationsDir, "0021_timer_resolved_cycle.down.sql")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, string(down))
	require.NoError(t, err)

	rows, err := db.QueryContext(ctx, "SELECT name FROM pragma_table_info('timer')")
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var columns []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		columns = append(columns, name)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []string{"key", "element_id", "state", "due_at"}, columns)
	var timers int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM timer").Scan(&timers))
	require.Equal(t, 3, timers)
}
