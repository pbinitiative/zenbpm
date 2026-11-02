package partition

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPartitionStatementErrors(t *testing.T) {
	t.Run("reports SQL preparation failures from direct writes", func(t *testing.T) {
		partition, _, _, _, _ := prepareTestSetup(t, false)
		t.Cleanup(func() { require.NoError(t, partition.Stop()) })

		_, err := partition.DB.ExecContext(t.Context(), "INSERT INTO missing_table(id) VALUES(1)")

		require.ErrorContains(t, err, "no such table: missing_table")
	})

	t.Run("reports the rows a direct write affected", func(t *testing.T) {
		partition, _, _, _, _ := prepareTestSetup(t, false)
		t.Cleanup(func() { require.NoError(t, partition.Stop()) })
		ctx := t.Context()
		_, err := partition.DB.ExecContext(ctx, "CREATE TABLE rows_affected_test (id INTEGER PRIMARY KEY, name TEXT)")
		require.NoError(t, err)
		_, err = partition.DB.ExecContext(ctx, "INSERT INTO rows_affected_test(id, name) VALUES(1, 'a'), (2, 'b')")
		require.NoError(t, err)

		updated, err := partition.DB.ExecContext(ctx, "UPDATE rows_affected_test SET name = 'c'")
		require.NoError(t, err)
		untouched, err := partition.DB.ExecContext(ctx, "UPDATE rows_affected_test SET name = 'd' WHERE id = 3")
		require.NoError(t, err)

		rowsAffected, err := updated.RowsAffected()
		require.NoError(t, err)
		require.EqualValues(t, 2, rowsAffected)
		rowsAffected, err = untouched.RowsAffected()
		require.NoError(t, err)
		require.Zero(t, rowsAffected)
	})

	t.Run("rolls back a failed batch without running post-flush actions", func(t *testing.T) {
		partition, _, _, _, _ := prepareTestSetup(t, false)
		t.Cleanup(func() { require.NoError(t, partition.Stop()) })
		ctx := t.Context()
		_, err := partition.DB.ExecContext(ctx, "CREATE TABLE statement_errors_test (id INTEGER PRIMARY KEY)")
		require.NoError(t, err)

		batch := partition.DB.NewBatch().(*DBBatch)
		_, err = batch.ExecContext(ctx, "INSERT INTO statement_errors_test(id) VALUES(1)")
		require.NoError(t, err)
		_, err = batch.ExecContext(ctx, "INSERT INTO statement_errors_test(id) VALUES(1)")
		require.NoError(t, err)
		postFlushCalled := false
		batch.AddPostFlushAction(ctx, func() { postFlushCalled = true })

		err = batch.Flush(ctx)

		require.ErrorContains(t, err, "UNIQUE constraint failed")
		require.False(t, postFlushCalled)
		require.Zero(t, queryCount(t, partition.DB, "SELECT COUNT(*) FROM statement_errors_test"))
	})
}
