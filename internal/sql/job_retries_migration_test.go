package sql

import (
	stdsql "database/sql"
	"slices"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/require"
)

// jobRetriesMigration is the migration job-level retries arrived with.
const jobRetriesMigration = "0018_job_retries.up.sql"

// TestJobRetriesMigrationGivesExistingJobsOneRetry pins the compatibility
// policy of the upgrade to job-level retries (see docs/reference/jobs.md,
// "Jobs created before job-level retries"): every job stored before job-level
// retries existed gets one retry, whatever its state and its task definition,
// so that its first failure without an error code creates an incident as it
// did before. It starts with no attempts, no backoff and no policy of its own,
// which makes the engine's default backoff apply to it for as long as it
// lives. Changing this changes the behaviour of running instances at the
// upgrade, which is a separate design decision and a migration of its own.
//
// A migration which shipped is not edited, so the policy would change through
// a later one, a backfill from the task definitions for example. The jobs are
// therefore stored in the schema as it was before migration 0018, and the
// policy is read after that migration and every later one.
func TestJobRetriesMigrationGivesExistingJobsOneRetry(t *testing.T) {
	db, err := stdsql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	// every connection to :memory: is a database of its own
	db.SetMaxOpenConns(1)
	ctx := t.Context()

	migrations, err := GetUpMigrations(DefaultMigrationsDir)
	require.NoError(t, err)
	// the order a partition applies them in
	slices.SortFunc(migrations, func(a, b MigrationData) int {
		return strings.Compare(a.Filename, b.Filename)
	})
	upgrade := slices.IndexFunc(migrations, func(migration MigrationData) bool {
		return migration.Filename == jobRetriesMigration
	})
	require.Positive(t, upgrade, "%s is not among the migrations", jobRetriesMigration)
	apply := func(migrations Migrations) {
		t.Helper()
		for _, migration := range migrations {
			_, err := db.ExecContext(ctx, migration.SQL)
			require.NoError(t, err, migration.Filename)
		}
	}

	apply(migrations[:upgrade])

	// what a node older than 1.9.0 stored: an instance with one job in every
	// state, keyed by its state, and an open incident
	_, err = db.ExecContext(ctx, `
		INSERT INTO process_definition (key, version, bpmn_process_id, bpmn_data, bpmn_checksum, bpmn_process_name)
		VALUES (1, 1, 'charge', '<definitions/>', x'00', 'Charge');
		INSERT INTO process_instance (key, process_definition_key, created_at, state, variables, process_type)
		VALUES (2, 1, 1000, 1, '{}', 0);
		INSERT INTO incident (key, element_instance_key, element_id, process_instance_key, message, created_at, execution_token)
		VALUES (10, 3, 'charge-card', 2, 'card declined', 1000, 4);
	`)
	require.NoError(t, err)
	states := everyActivityState(t)
	for _, state := range states {
		_, err = db.ExecContext(ctx, `
			INSERT INTO job (key, element_instance_key, element_id, process_instance_key, type, state, created_at,
				input_variables, execution_token)
			VALUES (?, 3, 'charge-card', 2, 'charge-card', ?, 1000, '{}', 4)`, int64(state), int64(state))
		require.NoError(t, err, state)
	}

	apply(migrations[upgrade:])

	type migratedJob struct {
		retries              int64
		attempts             int64
		retryAt              stdsql.NullInt64
		lastFailureMessage   stdsql.NullString
		retryBackoff         stdsql.NullString
		retriesSetByOperator int64
		deliveryToken        int64
		failedDeliveryToken  int64
	}
	rows, err := db.QueryContext(ctx, `
		SELECT key, retries, attempts, retry_at, last_failure_message, retry_backoff,
			retries_set_by_operator, delivery_token, failed_delivery_token
		FROM job ORDER BY key`)
	require.NoError(t, err)

	migrated := make(map[runtime.ActivityState]migratedJob)
	for rows.Next() {
		var key int64
		var job migratedJob
		require.NoError(t, rows.Scan(&key, &job.retries, &job.attempts, &job.retryAt, &job.lastFailureMessage,
			&job.retryBackoff, &job.retriesSetByOperator, &job.deliveryToken, &job.failedDeliveryToken))
		migrated[runtime.ActivityState(key)] = job
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	// one retry, a fresh series, no backoff of its own, nothing an operator set, never delivered
	expected := make(map[runtime.ActivityState]migratedJob)
	for _, state := range states {
		expected[state] = migratedJob{retries: 1}
	}
	require.Equal(t, expected, migrated)

	var incidentJobKey stdsql.NullInt64
	require.NoError(t, db.QueryRowContext(ctx, "SELECT job_key FROM incident WHERE key = 10").Scan(&incidentJobKey))
	require.False(t, incidentJobKey.Valid, "an incident from before the upgrade names no job")

	var failures int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM job_failure").Scan(&failures))
	require.Zero(t, failures, "no failure history is made up for existing jobs")
}

// everyActivityState lists every state the runtime names, in the order of
// their values, so that a state added later is covered without a change here.
func everyActivityState(t *testing.T) []runtime.ActivityState {
	t.Helper()
	var states []runtime.ActivityState
	// the generated String() answers "ActivityState(n)" for a value without a name
	for state := runtime.ActivityStateActive; !strings.HasPrefix(state.String(), "ActivityState("); state++ {
		states = append(states, state)
	}
	require.Contains(t, states, runtime.ActivityStateWithdrawn)
	return states
}
