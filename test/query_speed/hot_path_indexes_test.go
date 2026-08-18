//go:build queryspeed

package queryspeed

import (
	"context"
	stdsql "database/sql"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	zensql "github.com/pbinitiative/zenbpm/internal/sql"
	rqdb "github.com/rqlite/rqlite/v10/db"
	"github.com/stretchr/testify/require"
)

const hotPathMigrationFilename = "0012_hot_path_indexes.up.sql"

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

func TestHotPathIndexQuerySpeed(t *testing.T) {
	rowCount := envInt(t, "ZENBPM_QUERY_SPEED_ROWS", 200_000)
	iterations := envInt(t, "ZENBPM_QUERY_SPEED_ITERATIONS", 9)
	require.GreaterOrEqual(t, rowCount, 10_000)
	require.GreaterOrEqual(t, iterations, 3)

	dbPath := t.TempDir() + "/hot-path-query-speed.db"
	driver := rqdb.DefaultDriver()
	db, err := stdsql.Open(driver.Name(), rqdb.MakeDSN(dbPath, false, true, true))
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(1)

	migration := applyMigrations(t, db)

	populateStarted := time.Now()
	populateHotPathTables(t, db, rowCount)
	populateDuration := time.Since(populateStarted)

	rollbackHotPathMigration(t, db, migration)
	require.NoError(t, execSQL(db, "ANALYZE"))

	targetOrdinal := (rowCount / 2 / 1_000) * 1_000
	targetToken := int64(1_000_000_000 + targetOrdinal)
	tests := hotPathSpeedQueries(rowCount, targetOrdinal, targetToken)

	withoutIndexes := measureQueries(t, db, tests, iterations)

	indexBuildStarted := time.Now()
	require.NoError(t, execSQL(db, migration.SQL))
	indexBuildDuration := time.Since(indexBuildStarted)
	requireHotPathIndexes(t, db, true)
	require.NoError(t, execSQL(db, "ANALYZE"))

	withIndexes := measureQueries(t, db, tests, iterations)

	t.Logf("populated %d rows in each of 9 hot-path tables (%s)", rowCount, populateDuration.Round(time.Millisecond))
	t.Logf("applied %s in %s", migration.Filename, indexBuildDuration.Round(time.Millisecond))
	t.Log("warm-cache median query times:")
	t.Logf("%-42s %12s %12s %10s %10s", "query", "down", "up", "speedup", "rows")
	for i, test := range tests {
		before := withoutIndexes[i]
		after := withIndexes[i]
		require.Equal(t, before.rows, after.rows, test.name)

		speedup := float64(before.median) / float64(after.median)
		t.Logf("%-42s %12s %12s %9.1fx %10d",
			test.name,
			before.median.Round(time.Microsecond),
			after.median.Round(time.Microsecond),
			speedup,
			before.rows,
		)
	}
}

type speedQuery struct {
	name      string
	statement string
	arguments []any
	mutation  bool
}

type queryMeasurement struct {
	median time.Duration
	rows   int
}

func hotPathSpeedQueries(rowCount, targetOrdinal int, targetToken int64) []speedQuery {
	const cleanupBatchSize = 1_000
	cleanupArguments := make([]any, cleanupBatchSize)
	cleanupStart := targetOrdinal - cleanupBatchSize/2
	for i := range cleanupArguments {
		cleanupArguments[i] = cleanupStart + i
	}
	cleanupPlaceholders := strings.TrimSuffix(strings.Repeat("?,", cleanupBatchSize), ",")

	return []speedQuery{
		{
			name:      "decision-instance history cleanup",
			statement: "DELETE FROM decision_instance WHERE process_instance_key IN (" + cleanupPlaceholders + ")",
			arguments: cleanupArguments,
			mutation:  true,
		},
		{
			name:      "jobs by execution token and state",
			statement: "SELECT * FROM job WHERE execution_token = ? AND state IN (?, ?, ?)",
			arguments: []any{targetToken, 1, 5, 6},
		},
		{
			name:      "timer polling by state and due date",
			statement: "SELECT * FROM timer WHERE due_at < ? AND state = ?",
			arguments: []any{rowCount / 2, 1},
		},
		{
			name:      "timers by execution token and state",
			statement: "SELECT * FROM timer WHERE execution_token = ? AND state = ?",
			arguments: []any{targetToken, 1},
		},
		{
			name:      "messages by execution token and state",
			statement: "SELECT * FROM message_subscription WHERE execution_token = ? AND state = ?",
			arguments: []any{targetToken, 1},
		},
		{
			name:      "errors by execution token and state",
			statement: "SELECT * FROM error_subscription WHERE execution_token = ? AND state = ?",
			arguments: []any{targetToken, 1},
		},
		{
			name:      "incidents by execution token",
			statement: "SELECT * FROM incident WHERE execution_token = ?",
			arguments: []any{targetToken},
		},
		{
			name:      "startup running-token recovery",
			statement: "SELECT * FROM execution_token WHERE state = ?",
			arguments: []any{1},
		},
		{
			name:      "active process-instance metric",
			statement: "SELECT count(*) FROM process_instance WHERE state = 1",
		},
		{
			name:      "active process-instance recovery",
			statement: "SELECT key FROM process_instance WHERE state IN (1, 8)",
		},
	}
}

func applyMigrations(t *testing.T, db *stdsql.DB) zensql.MigrationData {
	t.Helper()

	migrations, err := zensql.GetUpMigrations(zensql.DefaultMigrationsDir)
	require.NoError(t, err)

	var hotPathMigration zensql.MigrationData
	for _, migration := range migrations {
		require.NoError(t, execSQL(db, migration.SQL), migration.Filename)
		if migration.Filename == hotPathMigrationFilename {
			hotPathMigration = migration
		}
	}
	require.NotEmpty(t, hotPathMigration.Filename)
	return hotPathMigration
}

func rollbackHotPathMigration(t *testing.T, db *stdsql.DB, migration zensql.MigrationData) {
	t.Helper()

	rollback, err := zensql.GetRollbackMigration(zensql.DefaultMigrationsDir, migration.Filename)
	require.NoError(t, err)
	require.NotNil(t, rollback)
	require.NoError(t, execSQL(db, rollback.SQL))
	requireHotPathIndexes(t, db, false)
}

func requireHotPathIndexes(t *testing.T, db *stdsql.DB, expected bool) {
	t.Helper()

	for _, indexName := range hotPathIndexNames {
		var count int
		err := db.QueryRow(
			"SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?",
			indexName,
		).Scan(&count)
		require.NoError(t, err)
		if expected {
			require.Equal(t, 1, count, indexName)
		} else {
			require.Zero(t, count, indexName)
		}
	}
}

func populateHotPathTables(t *testing.T, db *stdsql.DB, rowCount int) {
	t.Helper()

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()

	statements := []struct {
		name string
		sql  string
		args []any
	}{
		{
			name: "process definition",
			sql:  `INSERT INTO process_definition(key, version, bpmn_process_id, bpmn_data, bpmn_checksum, bpmn_process_name) VALUES (1, 1, 'speed-test', '<process/>', X'00', 'speed-test')`,
		},
		{
			name: "DMN resource definition",
			sql:  `INSERT INTO dmn_resource_definition(key, version, dmn_resource_definition_id, dmn_data, dmn_checksum, dmn_definition_name) VALUES (1, 1, 'speed-test', '<definitions/>', X'00', 'speed-test')`,
		},
		{
			name: "decision definition",
			sql:  `INSERT INTO decision_definition(key, version, decision_id, version_tag, dmn_resource_definition_id, dmn_resource_definition_key) VALUES (1, 1, 'speed-test', 'v1', 'speed-test', 1)`,
		},
		{
			name: "process instances",
			sql: `WITH RECURSIVE seq(i) AS (
				VALUES(1) UNION ALL SELECT i + 1 FROM seq WHERE i < ?
			)
			INSERT INTO process_instance(key, process_definition_key, created_at, state, variables, process_type, start_element_id)
			SELECT i, 1, i, CASE WHEN i % 1000 = 0 THEN 1 ELSE 4 END, '{}', 1, 'start' FROM seq`,
			args: []any{rowCount},
		},
		{
			name: "execution tokens",
			sql: `WITH RECURSIVE seq(i) AS (
				VALUES(1) UNION ALL SELECT i + 1 FROM seq WHERE i < ?
			)
			INSERT INTO execution_token(key, element_instance_key, element_id, process_instance_key, state, created_at)
			SELECT 1000000000 + i, 1100000000 + i, 'task', i, CASE WHEN i % 1000 = 0 THEN 1 ELSE 3 END, i FROM seq`,
			args: []any{rowCount},
		},
		{
			name: "decision instances",
			sql: `WITH RECURSIVE seq(i) AS (
				VALUES(1) UNION ALL SELECT i + 1 FROM seq WHERE i < ?
			)
			INSERT INTO decision_instance(key, decision_id, created_at, output_variables, evaluated_decisions, dmn_resource_definition_key, decision_definition_key, process_instance_key)
			SELECT 2000000000 + i, 'speed-test', i, '{}', '[]', 1, 1, i FROM seq`,
			args: []any{rowCount},
		},
		{
			name: "jobs",
			sql: `WITH RECURSIVE seq(i) AS (
				VALUES(1) UNION ALL SELECT i + 1 FROM seq WHERE i < ?
			)
			INSERT INTO job(key, element_instance_key, element_id, process_instance_key, type, state, created_at, input_variables, output_variables, execution_token)
			SELECT 3000000000 + i, 1100000000 + i, 'task', i, 'speed-test', CASE WHEN i % 1000 = 0 THEN 1 ELSE 4 END, i, '{}', '{}', 1000000000 + i FROM seq`,
			args: []any{rowCount},
		},
		{
			name: "timers",
			sql: `WITH RECURSIVE seq(i) AS (
				VALUES(1) UNION ALL SELECT i + 1 FROM seq WHERE i < ?
			)
			INSERT INTO timer(key, element_instance_key, element_id, process_definition_key, process_instance_key, state, created_at, due_at, execution_token)
			SELECT 4000000000 + i, 1100000000 + i, 'timer', 1, i, CASE WHEN i % 100 = 0 THEN 1 ELSE 2 END, i, i, 1000000000 + i FROM seq`,
			args: []any{rowCount},
		},
		{
			name: "message subscriptions",
			sql: `WITH RECURSIVE seq(i) AS (
				VALUES(1) UNION ALL SELECT i + 1 FROM seq WHERE i < ?
			)
			INSERT INTO message_subscription(key, element_id, process_definition_key, process_instance_key, name, state, created_at, correlation_key, execution_token, type, element_instance_key)
			SELECT 5000000000 + i, 'message', 1, i, 'speed-test', CASE WHEN i % 1000 = 0 THEN 1 ELSE 4 END, i, 'correlation-' || i, 1000000000 + i, 1, 1100000000 + i FROM seq`,
			args: []any{rowCount},
		},
		{
			name: "error subscriptions",
			sql: `WITH RECURSIVE seq(i) AS (
				VALUES(1) UNION ALL SELECT i + 1 FROM seq WHERE i < ?
			)
			INSERT INTO error_subscription(key, element_instance_key, element_id, process_definition_key, process_instance_key, error_code, state, created_at, execution_token)
			SELECT 6000000000 + i, 1100000000 + i, 'error', 1, i, 'speed-test', CASE WHEN i % 1000 = 0 THEN 1 ELSE 2 END, i, 1000000000 + i FROM seq`,
			args: []any{rowCount},
		},
		{
			name: "incidents",
			sql: `WITH RECURSIVE seq(i) AS (
				VALUES(1) UNION ALL SELECT i + 1 FROM seq WHERE i < ?
			)
			INSERT INTO incident(key, element_instance_key, element_id, process_instance_key, message, created_at, execution_token)
			SELECT 7000000000 + i, 1100000000 + i, 'task', i, 'speed-test', i, 1000000000 + i FROM seq`,
			args: []any{rowCount},
		},
		{
			name: "flow element instances",
			sql: `WITH RECURSIVE seq(i) AS (
				VALUES(1) UNION ALL SELECT i + 1 FROM seq WHERE i < ?
			)
			INSERT INTO flow_element_instance(key, element_id, process_instance_key, execution_token_key, created_at, input_variables, output_variables, completed_at, element_type)
			SELECT 8000000000 + i, 'task', i, 1000000000 + (((i - 1) / 10) * 10 + 10), i, '{}', '{}', i, 'serviceTask' FROM seq`,
			args: []any{rowCount},
		},
	}

	for _, statement := range statements {
		_, err := tx.ExecContext(context.Background(), statement.sql, statement.args...)
		require.NoError(t, err, statement.name)
	}
	require.NoError(t, tx.Commit())
}

func measureQueries(t *testing.T, db *stdsql.DB, tests []speedQuery, iterations int) []queryMeasurement {
	t.Helper()

	measurements := make([]queryMeasurement, 0, len(tests))
	for _, test := range tests {
		_, _, err := measureQuery(db, test)
		require.NoError(t, err, test.name)

		samples := make([]time.Duration, 0, iterations)
		rowCount := -1
		for range iterations {
			duration, rows, err := measureQuery(db, test)
			samples = append(samples, duration)
			require.NoError(t, err, test.name)
			if rowCount < 0 {
				rowCount = rows
			} else {
				require.Equal(t, rowCount, rows, test.name)
			}
		}

		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		measurements = append(measurements, queryMeasurement{
			median: samples[len(samples)/2],
			rows:   rowCount,
		})
	}
	return measurements
}

func measureQuery(db *stdsql.DB, query speedQuery) (time.Duration, int, error) {
	if query.mutation {
		return measureMutation(db, query.statement, query.arguments...)
	}

	started := time.Now()
	rows, err := consumeQuery(db, query.statement, query.arguments...)
	return time.Since(started), rows, err
}

func measureMutation(db *stdsql.DB, statement string, arguments ...any) (time.Duration, int, error) {
	tx, err := db.Begin()
	if err != nil {
		return 0, 0, err
	}

	started := time.Now()
	result, execErr := tx.Exec(statement, arguments...)
	duration := time.Since(started)
	if execErr != nil {
		_ = tx.Rollback()
		return duration, 0, execErr
	}

	rowsAffected, rowsErr := result.RowsAffected()
	rollbackErr := tx.Rollback()
	if rowsErr != nil {
		return duration, 0, rowsErr
	}
	if rollbackErr != nil {
		return duration, 0, rollbackErr
	}
	return duration, int(rowsAffected), nil
}

func consumeQuery(db *stdsql.DB, statement string, arguments ...any) (int, error) {
	rows, err := db.Query(statement, arguments...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return 0, err
	}
	values := make([]any, len(columns))
	destinations := make([]any, len(columns))
	for i := range values {
		destinations[i] = &values[i]
	}

	rowCount := 0
	for rows.Next() {
		if err := rows.Scan(destinations...); err != nil {
			return 0, err
		}
		rowCount++
	}
	return rowCount, rows.Err()
}

func execSQL(db *stdsql.DB, statement string) error {
	_, err := db.Exec(statement)
	return err
}

func envInt(t *testing.T, name string, defaultValue int) int {
	t.Helper()

	value := os.Getenv(name)
	if value == "" {
		return defaultValue
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		t.Fatalf("%s must be an integer: %v", name, err)
	}
	return parsed
}
