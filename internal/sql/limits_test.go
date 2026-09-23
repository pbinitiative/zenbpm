package sql

import (
	stdsql "database/sql"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMaxQueryParametersIsTheBundledSQLiteLimit pins the constant to the
// limit SQLite is compiled with: a statement carrying that many parameters
// runs, one more fails to prepare.
func TestMaxQueryParametersIsTheBundledSQLiteLimit(t *testing.T) {
	db, err := stdsql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.ExecContext(t.Context(), "CREATE TABLE job (key INTEGER PRIMARY KEY)")
	require.NoError(t, err)

	var count int64
	err = db.QueryRowContext(t.Context(), keysInQuery(MaxQueryParameters), keyArguments(MaxQueryParameters)...).Scan(&count)
	require.NoError(t, err, "a statement with MaxQueryParameters parameters must run")
	assert.Zero(t, count)

	err = db.QueryRowContext(t.Context(), keysInQuery(MaxQueryParameters+1), keyArguments(MaxQueryParameters+1)...).Scan(&count)
	require.Error(t, err, "one parameter more must be refused, else the constant understates the limit")
	assert.Contains(t, err.Error(), "too many SQL variables")
}

func keysInQuery(parameters int) string {
	return "SELECT count(*) FROM job WHERE key IN (" + strings.Repeat(",?", parameters)[1:] + ")"
}

func keyArguments(parameters int) []any {
	args := make([]any, parameters)
	for i := range args {
		args[i] = int64(i)
	}
	return args
}
