package partition

import (
	"regexp"
	"strings"
	"testing"

	zensql "github.com/pbinitiative/zenbpm/internal/sql"
	"github.com/stretchr/testify/require"
)

// hotPathIndexNames is the set of indexes added by migration 0012.
// Used by the rollback/restore sub-test to assert all of them are
// added on the up-migration and dropped on the down-migration.
var hotPathIndexNames = []string{
	"idx_decision_instance_process_instance_key",
	"idx_job_execution_token_state",
	"idx_timer_state_due_at",
	"idx_timer_execution_token_state",
	"idx_message_subscription_execution_token_state",
	"idx_error_subscription_execution_token_state",
	"idx_incident_execution_token",
	"idx_execution_token_state",
	"idx_process_instance_state",
}

// newIndexUseCase describes a query that should use a *new* (added in
// migration 0012) hot-path index. These cases verify the indexes themselves
// are wired up correctly.
type newIndexUseCase struct {
	name      string
	table     string
	index     string
	query     string
	arguments []any
}

// pinnedIndexUseCase describes a query that must use a *specific* older index
// via INDEXED BY. These are queries that filter on a narrow column
// (process_instance_key, process_definition_key, history_delete_sec, ...)
// which has its own dedicated index, and the query could otherwise be
// shadowed by a newer generic state-led index added in 0012.
//
// Production never runs ANALYZE, so SQLite's planner is effectively blind.
// We pin these queries explicitly to lock in the access path.
//
// How to extend: when adding a new query or new index that could cause
// shadowing, add a case below. The first matching EXPLAIN QUERY PLAN detail
// that contains "USING INDEX <index>" is what SQLite picked; the assertions
// ensure it stays that way.
type pinnedIndexUseCase struct {
	name      string
	// scanTargets lists every name SQLite could print in a SCAN line for this
	// query — the underlying table name plus any alias used in the FROM clause.
	// EXPLAIN prints the alias, not the table name, so both must be checked
	// (or a SCAN slip-through will silently pass).
	scanTargets []string
	index       string
	query       string
	arguments   []any
}

func TestHotPathIndexes(t *testing.T) {
	partition, conf, clientMgr, testStore, server := prepareTestSetup(t, false)
	t.Cleanup(func() {
		require.NoError(t, partition.Stop())
		require.NoError(t, server.Close())
	})

	db := newTestDB(t, partition, conf, clientMgr, testStore, "test-hot-path-indexes")

	t.Run("new index use cases (added in 0012)", func(t *testing.T) {
		tests := newIndexUseCases()

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				details := explainQueryPlan(t, db, tt.query, tt.arguments...)
				plan := strings.Join(details, "\n")

				// SQLite prints either "USING INDEX <name>" or "USING COVERING INDEX <name>"
				// (the latter when the index covers every column the query needs).
				require.Regexp(t, regexp.MustCompile(`USING (COVERING )?INDEX `+regexp.QuoteMeta(tt.index)+`\b`), plan, "query plan:\n%s", plan)
				require.NotContains(t, plan, "SCAN "+tt.table, "query plan:\n%s", plan)
			})
		}
	})

	t.Run("pinned index use cases (must use a specific older index)", func(t *testing.T) {
		tests := pinnedIndexUseCases()

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				details := explainQueryPlan(t, db, tt.query, tt.arguments...)
				plan := strings.Join(details, "\n")

				require.Regexp(t, regexp.MustCompile(`USING (COVERING )?INDEX `+regexp.QuoteMeta(tt.index)+`\b`), plan, "query plan:\n%s", plan)
				for _, target := range tt.scanTargets {
					require.NotContains(t, plan, "SCAN "+target, "query plan:\n%s", plan)
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

// newIndexUseCases returns the table of queries that should be served by an
// index introduced in migration 0012. The test asserts that no fallback
// table scan happens.
func newIndexUseCases() []newIndexUseCase {
	return []newIndexUseCase{
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
}

// pinnedIndexUseCases returns the table of queries that must use a specific
// older index via INDEXED BY. Adding a case here is the right way to defend
// against a future generic index accidentally shadowing a narrower one.
func pinnedIndexUseCases() []pinnedIndexUseCase {
	return []pinnedIndexUseCase{
		{
			// Without the hint SQLite picks idx_process_instance_state (added in 0012)
			// and scans every terminal instance on each cleanup pass.
			name:      "TTL cleanup uses partial cleanup index",
			scanTargets: []string{"pi", "parent_pi", "et", "process_instance", "execution_token"},
			index:     "idx_process_instance_cleanup",
			query: `SELECT pi.key
FROM process_instance AS pi INDEXED BY idx_process_instance_cleanup
    LEFT JOIN execution_token AS et ON pi.parent_process_execution_token = et.key
    LEFT JOIN process_instance AS parent_pi ON et.process_instance_key = parent_pi.key
WHERE pi.state IN (4, 6, 9)
    AND (et.key IS NULL OR parent_pi.state IN (4, 6, 9))
    AND pi.history_delete_sec < ?
ORDER BY pi.created_at DESC, pi.key DESC
LIMIT ?`,
			arguments: []any{int64(1), int64(100)},
		},
		{
			// Without the hint SQLite picks idx_timer_state_due_at and scans every
			// timer in that state across the partition instead of the few timers
			// for one process instance.
			name:      "process instance timers use FK index",
			scanTargets: []string{"timer"},
			index:     "idx_fk_timer_process_instance_key",
			query: `SELECT * FROM timer INDEXED BY idx_fk_timer_process_instance_key
WHERE process_instance_key = ? AND state = ?`,
			arguments: []any{int64(1), int64(1)},
		},
		{
			name:      "process definition timers use FK index",
			scanTargets: []string{"timer"},
			index:     "idx_fk_timer_process_definition_key",
			query: `SELECT * FROM timer INDEXED BY idx_fk_timer_process_definition_key
WHERE process_definition_key = ? AND state = ?`,
			arguments: []any{int64(1), int64(1)},
		},
		{
			name:      "process instance timers by element use FK index",
			scanTargets: []string{"timer"},
			index:     "idx_fk_timer_process_instance_key",
			query: `SELECT * FROM timer INDEXED BY idx_fk_timer_process_instance_key
WHERE process_instance_key = ? AND element_id = ? AND state = ?`,
			arguments: []any{int64(1), "elem", int64(1)},
		},
		{
			name:      "process definition timers by element use FK index",
			scanTargets: []string{"timer"},
			index:     "idx_fk_timer_process_definition_key",
			query: `SELECT * FROM timer INDEXED BY idx_fk_timer_process_definition_key
WHERE process_definition_key = ? AND process_instance_key IS NULL AND element_id = ? AND state = ?`,
			arguments: []any{int64(1), "elem", int64(1)},
		},
		{
			// The planner currently picks the FK index correctly, but pinning makes
			// the contract explicit and prevents the generic idx_execution_token_state
			// from shadowing it under different data distributions.
			name:      "tokens for process instance use FK index",
			scanTargets: []string{"execution_token"},
			index:     "idx_fk_execution_token_process_instance_key",
			query: `SELECT * FROM execution_token INDEXED BY idx_fk_execution_token_process_instance_key
WHERE process_instance_key = ? AND state IN (?, ?)`,
			arguments: []any{int64(1), int64(1), int64(2)},
		},
		{
			name:      "jobs in state for process instance use FK index",
			scanTargets: []string{"job"},
			index:     "idx_fk_job_process_instance_key",
			query: `SELECT * FROM job INDEXED BY idx_fk_job_process_instance_key
WHERE process_instance_key = ? AND state IN (?, ?)`,
			arguments: []any{int64(1), int64(1), int64(2)},
		},
		{
			name:      "message subscriptions for process instance use FK index",
			scanTargets: []string{"message_subscription"},
			index:     "idx_fk_message_subscription_process_instance_key",
			query: `SELECT * FROM message_subscription INDEXED BY idx_fk_message_subscription_process_instance_key
WHERE process_instance_key = ? AND state = ?`,
			arguments: []any{int64(1), int64(1)},
		},
		{
			name:      "error subscriptions for process instance use FK index",
			scanTargets: []string{"error_subscription"},
			index:     "idx_fk_error_subscription_process_instance_key",
			query: `SELECT * FROM error_subscription INDEXED BY idx_fk_error_subscription_process_instance_key
WHERE process_instance_key = ? AND state = ?`,
			arguments: []any{int64(1), int64(1)},
		},
		{
			// Without the hint SQLite picks idx_process_instance_state (added in 0012) and
			// scans every process instance in the active/ready states, instead of joining from
			// execution_token filtered by process_instance_key. The pin forces a join order
			// that probes child by parent_process_execution_token.
			name:      "active subprocess count uses FK join",
			scanTargets: []string{"child", "et", "process_instance", "execution_token"},
			index:     "idx_process_instance_parent_execution_token",
			query: `SELECT CAST(COUNT(*) AS INTEGER)
FROM process_instance AS child INDEXED BY idx_process_instance_parent_execution_token
    INNER JOIN execution_token AS et ON child.parent_process_execution_token = et.key
WHERE et.process_instance_key = ? AND child.process_type = ? AND child.state IN (?, ?)`,
			arguments: []any{int64(1), int64(1), int64(1), int64(8)},
		},
	}
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
