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
