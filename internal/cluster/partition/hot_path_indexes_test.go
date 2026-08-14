package partition

import (
	"strings"
	"testing"

	zensql "github.com/pbinitiative/zenbpm/internal/sql"
	"github.com/stretchr/testify/require"
)

var hotPathIndexNames = []string{
	"idx_decision_instance_process_instance_key",
	"idx_job_execution_token_state",
	"idx_timer_state_due_at",
	"idx_timer_execution_token_state",
	"idx_message_subscription_execution_token_state",
	"idx_error_subscription_execution_token_state",
	"idx_incident_execution_token",
	"idx_flow_element_instance_execution_token_created_at",
	"idx_execution_token_state",
	"idx_process_instance_state",
}

func TestHotPathIndexes(t *testing.T) {
	partition, conf, clientMgr, testStore, server := prepareTestSetup(t, false)
	t.Cleanup(func() {
		require.NoError(t, partition.Stop())
		require.NoError(t, server.Close())
	})

	db := newTestDB(t, partition, conf, clientMgr, testStore, "test-hot-path-indexes")

	t.Run("targeted queries use indexes", func(t *testing.T) {
		tests := []struct {
			name      string
			table     string
			index     string
			query     string
			arguments []any
		}{
			{
				name:      "decision instance history cleanup",
				table:     "decision_instance",
				index:     "idx_decision_instance_process_instance_key",
				query:     "DELETE FROM decision_instance WHERE process_instance_key IN (?)",
				arguments: []any{int64(1)},
			},
			{
				name:      "jobs by execution token and state",
				table:     "job",
				index:     "idx_job_execution_token_state",
				query:     "SELECT * FROM job WHERE execution_token = ? AND state IN (?, ?)",
				arguments: []any{int64(1), int64(1), int64(6)},
			},
			{
				name:      "timer polling by state and due date",
				table:     "timer",
				index:     "idx_timer_state_due_at",
				query:     "SELECT * FROM timer WHERE due_at < ? AND state = ?",
				arguments: []any{int64(1), int64(1)},
			},
			{
				name:      "timers by execution token and state",
				table:     "timer",
				index:     "idx_timer_execution_token_state",
				query:     "SELECT * FROM timer WHERE execution_token = ? AND state = ?",
				arguments: []any{int64(1), int64(1)},
			},
			{
				name:      "message subscriptions by execution token and state",
				table:     "message_subscription",
				index:     "idx_message_subscription_execution_token_state",
				query:     "SELECT * FROM message_subscription WHERE execution_token = ? AND state = ?",
				arguments: []any{int64(1), int64(1)},
			},
			{
				name:      "error subscriptions by execution token and state",
				table:     "error_subscription",
				index:     "idx_error_subscription_execution_token_state",
				query:     "SELECT * FROM error_subscription WHERE execution_token = ? AND state = ?",
				arguments: []any{int64(1), int64(1)},
			},
			{
				name:      "incidents by execution token",
				table:     "incident",
				index:     "idx_incident_execution_token",
				query:     "SELECT * FROM incident WHERE execution_token = ?",
				arguments: []any{int64(1)},
			},
			{
				name:      "flow element instances by execution token",
				table:     "flow_element_instance",
				index:     "idx_flow_element_instance_execution_token_created_at",
				query:     "SELECT * FROM flow_element_instance WHERE execution_token_key = ? ORDER BY created_at DESC LIMIT 1",
				arguments: []any{int64(1)},
			},
			{
				name:      "startup token recovery",
				table:     "execution_token",
				index:     "idx_execution_token_state",
				query:     "SELECT * FROM execution_token WHERE state = ?",
				arguments: []any{int64(1)},
			},
			{
				name:  "active process instance metrics",
				table: "process_instance",
				index: "idx_process_instance_state",
				query: "SELECT count(*) FROM process_instance WHERE state = 1",
			},
			{
				name:  "active process instance recovery",
				table: "process_instance",
				index: "idx_process_instance_state",
				query: "SELECT key FROM process_instance WHERE state IN (1, 8)",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				details := explainQueryPlan(t, db, tt.query, tt.arguments...)
				plan := strings.Join(details, "\n")

				require.Contains(t, plan, tt.index, "query plan:\n%s", plan)
				require.NotContains(t, plan, "SCAN "+tt.table, "query plan:\n%s", plan)
				if tt.index == "idx_flow_element_instance_execution_token_created_at" {
					require.NotContains(t, plan, "USE TEMP B-TREE FOR ORDER BY", "query plan:\n%s", plan)
				}
			})
		}
	})

	t.Run("rollback removes and forward migration restores indexes", func(t *testing.T) {
		migration := findMigration(t, "0012_hot_path_indexes.up.sql")
		requireIndexesExist(t, db, true)

		require.NoError(t, executeRollbackMigration(t.Context(), db, migration))
		requireIndexesExist(t, db, false)

		require.NoError(t, executeUpMigration(t.Context(), db, migration))
		requireIndexesExist(t, db, true)
	})
}

func explainQueryPlan(t *testing.T, db *DB, query string, arguments ...any) []string {
	t.Helper()

	rows, err := db.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+query, arguments...)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, rows.Close())
	})

	details := make([]string, 0, 2)
	for rows.Next() {
		var id, parent, notUsed int64
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &notUsed, &detail))
		details = append(details, detail)
	}
	require.NoError(t, rows.Err())
	require.NotEmpty(t, details)
	return details
}

func findMigration(t *testing.T, filename string) zensql.MigrationData {
	t.Helper()

	migrations, err := zensql.GetUpMigrations(zensql.DefaultMigrationsDir)
	require.NoError(t, err)
	for _, migration := range migrations {
		if migration.Filename == filename {
			return migration
		}
	}

	require.FailNow(t, "migration not found", filename)
	return zensql.MigrationData{}
}

func requireIndexesExist(t *testing.T, db *DB, expected bool) {
	t.Helper()

	for _, indexName := range hotPathIndexNames {
		var count int64
		err := db.QueryRowContext(
			t.Context(),
			"SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?",
			indexName,
		).Scan(&count)
		require.NoError(t, err)
		if expected {
			require.EqualValues(t, 1, count, indexName)
		} else {
			require.Zero(t, count, indexName)
		}
	}
}
