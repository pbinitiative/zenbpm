package partition

import (
	"context"
	ssql "database/sql"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/sql"
	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nullInt64 / nullString helpers keep the table-driven test cases concise.
func nullInt64(v int64) ssql.NullInt64    { return ssql.NullInt64{Int64: v, Valid: true} }
func nullString(v string) ssql.NullString { return ssql.NullString{String: v, Valid: true} }

func TestBuildIncident(t *testing.T) {
	baseIncident := sql.Incident{
		Key:                1,
		ElementInstanceKey: 11,
		ElementID:          "incident-element",
		ProcessInstanceKey: 101,
		Message:            "incident-message",
		CreatedAt:          12345,
		ExecutionToken:     1001,
	}

	t.Run("uses hydrated token when available", func(t *testing.T) {
		token := sql.ExecutionToken{
			Key:                1001,
			ElementInstanceKey: 22,
			ElementID:          "token-element",
			ProcessInstanceKey: 202,
			State:              int64(bpmnruntime.TokenStateFailed),
		}

		got := buildIncident(baseIncident, token)

		assert.Equal(t, int64(1001), got.Token.Key)
		assert.Equal(t, int64(22), got.Token.ElementInstanceKey)
		assert.Equal(t, "token-element", got.Token.ElementId)
		assert.Equal(t, int64(202), got.Token.ProcessInstanceKey)
		assert.Equal(t, bpmnruntime.TokenStateFailed, got.Token.State)
	})

	t.Run("falls back to incident fields when referenced token is missing", func(t *testing.T) {
		got := buildIncident(baseIncident, sql.ExecutionToken{})

		assert.Equal(t, int64(1001), got.Token.Key)
		assert.Equal(t, baseIncident.ElementInstanceKey, got.Token.ElementInstanceKey)
		assert.Equal(t, baseIncident.ElementID, got.Token.ElementId)
		assert.Equal(t, baseIncident.ProcessInstanceKey, got.Token.ProcessInstanceKey)
		assert.Equal(t, bpmnruntime.TokenState(0), got.Token.State)
	})

	t.Run("keeps tokenless incidents tokenless", func(t *testing.T) {
		incident := baseIncident
		incident.ExecutionToken = 0

		got := buildIncident(incident, sql.ExecutionToken{})

		assert.Equal(t, bpmnruntime.ExecutionToken{}, got.Token)
	})
}

func TestIncidentTokenKeysDeduplicatesAndSkipsZero(t *testing.T) {
	incidents := []sql.Incident{
		{ExecutionToken: 20},
		{ExecutionToken: 0},
		{ExecutionToken: 10},
		{ExecutionToken: 20},
		{ExecutionToken: 10},
	}

	assert.Equal(t, []int64{10, 20}, incidentTokenKeys(incidents))
}

func TestInflateDefinitionMessageSubscription(t *testing.T) {
	dbm := sql.MessageSubscription{
		Key:                  10,
		ElementID:            "elem-def",
		ProcessDefinitionKey: 200,
		Name:                 "msg-def",
		State:                int64(bpmnruntime.ActivityStateActive),
		CreatedAt:            99999,
		Type:                 int64(bpmnruntime.MessageSubscriptionTypeDefinition),
	}
	got, err := inflateDefinitionMessageSubscription(dbm)
	require.NoError(t, err)
	assert.Equal(t, int64(10), got.Key)
	assert.Equal(t, "msg-def", got.Name)
	assert.Equal(t, int64(200), got.ProcessDefinitionKey)
}

func TestInflateInstanceMessageSubscription(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		dbm := sql.MessageSubscription{
			Key:                  20,
			ElementID:            "elem-inst",
			ProcessDefinitionKey: 300,
			ProcessInstanceKey:   nullInt64(55),
			CorrelationKey:       nullString("ck-inst"),
			Name:                 "msg-inst",
			State:                int64(bpmnruntime.ActivityStateActive),
			CreatedAt:            11111,
		}
		got, err := inflateInstanceMessageSubscription(dbm)
		require.NoError(t, err)
		assert.Equal(t, int64(55), got.ProcessInstanceKey)
		assert.Equal(t, "ck-inst", got.CorrelationKey)
		assert.Equal(t, int64(20), got.Key)
	})

	t.Run("missing ProcessInstanceKey", func(t *testing.T) {
		dbm := sql.MessageSubscription{
			Key:            21,
			CorrelationKey: nullString("ck"),
			// ProcessInstanceKey is NULL
		}
		_, err := inflateInstanceMessageSubscription(dbm)
		assert.Error(t, err)
	})

	t.Run("missing CorrelationKey", func(t *testing.T) {
		dbm := sql.MessageSubscription{
			Key:                22,
			ProcessInstanceKey: nullInt64(1),
			// CorrelationKey is NULL
		}
		_, err := inflateInstanceMessageSubscription(dbm)
		assert.Error(t, err)
	})
}

func TestInflateTokenMessageSubscription(t *testing.T) {
	rq := &DB{}

	t.Run("valid with preloaded token", func(t *testing.T) {
		dbm := sql.MessageSubscription{
			Key:                  30,
			ElementID:            "elem-tok",
			ProcessDefinitionKey: 400,
			ProcessInstanceKey:   nullInt64(77),
			CorrelationKey:       nullString("ck-tok"),
			ExecutionToken:       nullInt64(888),
			Name:                 "msg-tok",
			State:                int64(bpmnruntime.ActivityStateActive),
			CreatedAt:            22222,
		}
		token := &bpmnruntime.ExecutionToken{
			Key:                888,
			ElementInstanceKey: 900,
			ElementId:          "tok-el",
			ProcessInstanceKey: 77,
			State:              bpmnruntime.TokenStateWaiting,
		}
		got, err := rq.inflateTokenMessageSubscription(context.Background(), dbm, token)
		require.NoError(t, err)
		assert.Equal(t, int64(77), got.ProcessInstanceKey)
		assert.Equal(t, "ck-tok", got.CorrelationKey)
		assert.Equal(t, int64(888), got.Token.Key)
	})

	t.Run("missing required params", func(t *testing.T) {
		dbm := sql.MessageSubscription{
			Key: 31,
			// ExecutionToken / CorrelationKey / ProcessInstanceKey all NULL
		}
		_, err := rq.inflateTokenMessageSubscription(context.Background(), dbm, nil)
		assert.Error(t, err)
	})
}

// TestInflateMessageSubscription_Branches exercises every branch of
// inflateMessageSubscription that does not require the database (Token branch
// is exercised via the messageToken-already-loaded fast path).
func TestInflateMessageSubscription_Branches(t *testing.T) {
	ctx := context.Background()
	rq := &DB{} // safe: Token branch with non-nil messageToken does not touch rq.Queries

	t.Run("Definition", func(t *testing.T) {
		dbm := sql.MessageSubscription{
			Key:                  1,
			ElementID:            "elem-def",
			ProcessDefinitionKey: 100,
			Name:                 "msg-def",
			State:                int64(bpmnruntime.ActivityStateActive),
			CreatedAt:            12345,
			Type:                 int64(bpmnruntime.MessageSubscriptionTypeDefinition),
		}
		got, err := rq.inflateMessageSubscription(ctx, dbm, nil)
		require.NoError(t, err)
		def, ok := got.(*bpmnruntime.DefinitionMessageSubscription)
		require.True(t, ok)
		assert.Equal(t, int64(1), def.Key)
		assert.Equal(t, "msg-def", def.Name)
	})

	t.Run("Instance", func(t *testing.T) {
		dbm := sql.MessageSubscription{
			Key:                  2,
			ElementID:            "elem-inst",
			ProcessDefinitionKey: 100,
			ProcessInstanceKey:   nullInt64(42),
			CorrelationKey:       nullString("ck"),
			Name:                 "msg-inst",
			State:                int64(bpmnruntime.ActivityStateActive),
			CreatedAt:            12345,
			Type:                 int64(bpmnruntime.MessageSubscriptionTypeInstance),
		}
		got, err := rq.inflateMessageSubscription(ctx, dbm, nil)
		require.NoError(t, err)
		inst, ok := got.(*bpmnruntime.InstanceMessageSubscription)
		require.True(t, ok)
		assert.Equal(t, int64(42), inst.ProcessInstanceKey)
		assert.Equal(t, "ck", inst.CorrelationKey)
	})

	t.Run("Instance missing required params", func(t *testing.T) {
		dbm := sql.MessageSubscription{
			Key:  3,
			Type: int64(bpmnruntime.MessageSubscriptionTypeInstance),
			// ProcessInstanceKey / CorrelationKey are NULL → must error
		}
		_, err := rq.inflateMessageSubscription(ctx, dbm, nil)
		assert.Error(t, err)
	})

	t.Run("Token with preloaded token", func(t *testing.T) {
		dbm := sql.MessageSubscription{
			Key:                  4,
			ElementID:            "elem-tok",
			ProcessDefinitionKey: 100,
			ProcessInstanceKey:   nullInt64(7),
			CorrelationKey:       nullString("ck-tok"),
			ExecutionToken:       nullInt64(999),
			Name:                 "msg-tok",
			State:                int64(bpmnruntime.ActivityStateActive),
			CreatedAt:            12345,
			Type:                 int64(bpmnruntime.MessageSubscriptionTypeToken),
		}
		token := &bpmnruntime.ExecutionToken{
			Key:                999,
			ElementInstanceKey: 1000,
			ElementId:          "tok-elem",
			ProcessInstanceKey: 7,
			State:              bpmnruntime.TokenStateWaiting,
		}
		got, err := rq.inflateMessageSubscription(ctx, dbm, token)
		require.NoError(t, err)
		tokSub, ok := got.(*bpmnruntime.TokenMessageSubscription)
		require.True(t, ok)
		assert.Equal(t, int64(7), tokSub.ProcessInstanceKey)
		assert.Equal(t, "ck-tok", tokSub.CorrelationKey)
		assert.Equal(t, int64(999), tokSub.Token.Key)
	})

	t.Run("Token missing required params", func(t *testing.T) {
		dbm := sql.MessageSubscription{
			Key:  5,
			Type: int64(bpmnruntime.MessageSubscriptionTypeToken),
			// ExecutionToken / CorrelationKey / ProcessInstanceKey are NULL → must error
		}
		_, err := rq.inflateMessageSubscription(ctx, dbm, nil)
		assert.Error(t, err)
	})

	t.Run("Unknown type returns error", func(t *testing.T) {
		dbm := sql.MessageSubscription{
			Key:  6,
			Type: 999,
		}
		_, err := rq.inflateMessageSubscription(ctx, dbm, nil)
		assert.Error(t, err)
	})
}
