package sql

import (
	stdsql "database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/require"
)

// TestJobRetriesMigrationGivesExistingJobsOneRetry pins the compatibility
// policy of migration 0018 (see docs/reference/jobs.md, "Jobs created before
// job-level retries"): every job stored before job-level retries existed gets
// one retry, whatever its state and its task definition, so that its first
// failure without an error code creates an incident as it did before. It
// starts with no attempts, no backoff and no policy of its own, which makes
// the engine's default backoff apply to it for as long as it lives. Changing
// this changes the behaviour of running instances at the upgrade, which is a
// separate design decision and a migration of its own.
func TestJobRetriesMigrationGivesExistingJobsOneRetry(t *testing.T) {
	db, err := stdsql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx := t.Context()

	// the columns of job and incident the migration starts from which it reads;
	// state holds a runtime.ActivityState, as in the real schema
	_, err = db.ExecContext(ctx, `
		CREATE TABLE job (
			key INTEGER PRIMARY KEY,
			state INTEGER NOT NULL
		);
		CREATE TABLE incident (
			key INTEGER PRIMARY KEY
		);
		INSERT INTO incident (key) VALUES (10);
	`)
	require.NoError(t, err)
	// one job in every state, keyed by its state
	for state := runtime.ActivityStateActive; state <= runtime.ActivityStateWithdrawn; state++ {
		_, err = db.ExecContext(ctx, "INSERT INTO job (key, state) VALUES (?, ?)", int64(state), int64(state))
		require.NoError(t, err)
	}

	migration, err := readMigrationFile(DefaultMigrationsDir, "0018_job_retries.up.sql")
	require.NoError(t, err)
	require.NotEmpty(t, migration)
	_, err = db.ExecContext(ctx, string(migration))
	require.NoError(t, err)

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
	defer func() { require.NoError(t, rows.Close()) }()

	migrated := make(map[int64]migratedJob)
	for rows.Next() {
		var key int64
		var job migratedJob
		require.NoError(t, rows.Scan(&key, &job.retries, &job.attempts, &job.retryAt, &job.lastFailureMessage,
			&job.retryBackoff, &job.retriesSetByOperator, &job.deliveryToken, &job.failedDeliveryToken))
		migrated[key] = job
	}
	require.NoError(t, rows.Err())

	// one retry, a fresh series, no backoff of its own, nothing an operator set, never delivered
	expected := make(map[int64]migratedJob)
	for state := runtime.ActivityStateActive; state <= runtime.ActivityStateWithdrawn; state++ {
		expected[int64(state)] = migratedJob{retries: 1}
	}
	require.Equal(t, expected, migrated)

	var incidentJobKey stdsql.NullInt64
	require.NoError(t, db.QueryRowContext(ctx, "SELECT job_key FROM incident WHERE key = 10").Scan(&incidentJobKey))
	require.False(t, incidentJobKey.Valid, "an incident from before the upgrade names no job")

	var failures int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM job_failure").Scan(&failures))
	require.Zero(t, failures, "no failure history is made up for existing jobs")
}
