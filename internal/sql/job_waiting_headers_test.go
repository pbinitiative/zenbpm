package sql

import (
	stdsql "database/sql"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetWaitingJobsQuarantinesMalformedHeaders pins the quarantine: a waiting
// job whose headers cannot be decoded into a string map is never returned, so it
// cannot take a batch slot or a SQL parameter and cannot starve later valid jobs.
func TestGetWaitingJobsQuarantinesMalformedHeaders(t *testing.T) {
	db, err := stdsql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx := t.Context()

	_, err = db.ExecContext(ctx, `
		CREATE TABLE job (
			key INTEGER PRIMARY KEY,
			element_instance_key INTEGER NOT NULL,
			element_id TEXT NOT NULL,
			process_instance_key INTEGER NOT NULL,
			type TEXT NOT NULL,
			state INTEGER NOT NULL,
			created_at INTEGER NOT NULL,
			input_variables TEXT NOT NULL,
			execution_token INTEGER NOT NULL,
			assignee TEXT,
			output_variables TEXT,
			element_type TEXT NOT NULL DEFAULT '',
			headers TEXT NOT NULL DEFAULT '{}'
		);
		INSERT INTO job (key, element_instance_key, element_id, process_instance_key, type, state, created_at, input_variables, execution_token, element_type, headers) VALUES
			(1, 1, 'e', 1, 'test-job', 1, 1, '{}', 1, 'serviceTask', '{not valid json'),
			(2, 1, 'e', 1, 'test-job', 1, 2, '{}', 1, 'serviceTask', '"a string"'),
			(3, 1, 'e', 1, 'test-job', 1, 3, '{}', 1, 'serviceTask', '{"retry":3}'),
			(4, 1, 'e', 1, 'test-job', 1, 4, '{}', 1, 'serviceTask', '{}'),
			(5, 1, 'e', 1, 'test-job', 1, 5, '{}', 1, 'serviceTask', '{"url":"https://example.com"}');
	`)
	require.NoError(t, err)

	// Mirror how sqlc renders the slice placeholders at run time; the caller
	// always passes at least one skip key, so the key_skip slice is a literal.
	query := strings.Replace(getWaitingJobs, "/*SLICE:type*/?", "?", 1)
	query = strings.Replace(query, "/*SLICE:key_skip*/?", "0", 1)

	rows, err := db.QueryContext(ctx, query, "test-job", 10)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rows.Close()) })

	columns, err := rows.Columns()
	require.NoError(t, err)

	var keys []int64
	for rows.Next() {
		values := make([]any, len(columns))
		dest := make([]any, len(columns))
		for i := range values {
			dest[i] = &values[i]
		}
		require.NoError(t, rows.Scan(dest...))
		for i, name := range columns {
			if name == "key" {
				keys = append(keys, values[i].(int64))
			}
		}
	}
	require.NoError(t, rows.Err())

	assert.Equal(t, []int64{4, 5}, keys, "only rows whose headers decode into a string map are returned, oldest first")
}
