package partition

import (
	"testing"

	"github.com/rqlite/rqlite/v10/command/proto"
	"github.com/stretchr/testify/require"
)

func TestStatementErrorMessage(t *testing.T) {
	t.Run("returns no error for a nil response", func(t *testing.T) {
		require.Empty(t, statementErrorMessage(nil))
	})

	t.Run("returns no error for an empty response", func(t *testing.T) {
		require.Empty(t, statementErrorMessage(&proto.ExecuteQueryResponse{}))
	})

	t.Run("returns no error for a successful execute result", func(t *testing.T) {
		response := &proto.ExecuteQueryResponse{
			Result: &proto.ExecuteQueryResponse_E{
				E: &proto.ExecuteResult{RowsAffected: 1},
			},
		}

		require.Empty(t, statementErrorMessage(response))
	})

	t.Run("returns no error for a successful query result", func(t *testing.T) {
		response := &proto.ExecuteQueryResponse{
			Result: &proto.ExecuteQueryResponse_Q{
				Q: &proto.QueryRows{Columns: []string{"id"}},
			},
		}

		require.Empty(t, statementErrorMessage(response))
	})

	t.Run("returns a legacy top-level error", func(t *testing.T) {
		response := &proto.ExecuteQueryResponse{
			Result: &proto.ExecuteQueryResponse_Error{Error: "legacy SQL failure"},
		}

		require.Equal(t, "legacy SQL failure", statementErrorMessage(response))
	})

	t.Run("returns an execute result error", func(t *testing.T) {
		response := &proto.ExecuteQueryResponse{
			Result: &proto.ExecuteQueryResponse_E{
				E: &proto.ExecuteResult{Error: "execute SQL failure"},
			},
		}

		require.Equal(t, "execute SQL failure", statementErrorMessage(response))
	})

	t.Run("returns a query result error", func(t *testing.T) {
		response := &proto.ExecuteQueryResponse{
			Result: &proto.ExecuteQueryResponse_Q{
				Q: &proto.QueryRows{Error: "query SQL failure"},
			},
		}

		require.Equal(t, "query SQL failure", statementErrorMessage(response))
	})
}
