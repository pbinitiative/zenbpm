package bpmn

import (
	"bytes"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/require"
)

func TestSuppliedRunningTokenDiagnostics(t *testing.T) {
	t.Run("warns when a supplied running token was never persisted", func(t *testing.T) {
		var logs bytes.Buffer
		engine := NewEngine(
			EngineWithStorage(inmemory.NewStorage()),
			EngineWithLogger(hclog.New(&hclog.LoggerOptions{Output: &logs, Level: hclog.Warn})),
		)
		t.Cleanup(engine.Stop)

		tokens, err := engine.reloadSuppliedRunningTokens(t.Context(), 7, []runtime.ExecutionToken{
			{Key: 42, ProcessInstanceKey: 7, State: runtime.TokenStateRunning},
		})
		require.NoError(t, err)
		require.Empty(t, tokens)
		require.Contains(t, logs.String(), "supplied Running token was not persisted")
		require.Contains(t, logs.String(), "42")
	})

	t.Run("does not warn when another runner already completed the token", func(t *testing.T) {
		store := inmemory.NewStorage()
		var logs bytes.Buffer
		engine := NewEngine(
			EngineWithStorage(store),
			EngineWithLogger(hclog.New(&hclog.LoggerOptions{Output: &logs, Level: hclog.Warn})),
		)
		t.Cleanup(engine.Stop)
		require.NoError(t, store.SaveToken(t.Context(), runtime.ExecutionToken{
			Key: 42, ProcessInstanceKey: 7, State: runtime.TokenStateCompleted,
		}))

		tokens, err := engine.reloadSuppliedRunningTokens(t.Context(), 7, []runtime.ExecutionToken{
			{Key: 42, ProcessInstanceKey: 7, State: runtime.TokenStateRunning},
		})
		require.NoError(t, err)
		require.Empty(t, tokens)
		require.Empty(t, logs.String())
	})
}
