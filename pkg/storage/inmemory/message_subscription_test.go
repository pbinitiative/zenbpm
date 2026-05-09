package inmemory_test

import (
	"context"
	"testing"
	"time"

	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTokenSub(key, tokenKey, piKey int64, name, correlationKey string, state bpmnruntime.ActivityState) *bpmnruntime.TokenMessageSubscription {
	return &bpmnruntime.TokenMessageSubscription{
		Token:              bpmnruntime.ExecutionToken{Key: tokenKey},
		ProcessInstanceKey: piKey,
		CorrelationKey:     correlationKey,
		MessageSubscriptionData: bpmnruntime.MessageSubscriptionData{
			Key:                  key,
			ElementId:            "elem",
			Name:                 name,
			State:                state,
			ProcessDefinitionKey: 1000,
			CreatedAt:            time.Now(),
		},
	}
}

func newInstanceSub(key, piKey int64, name, correlationKey string, state bpmnruntime.ActivityState) *bpmnruntime.InstanceMessageSubscription {
	return &bpmnruntime.InstanceMessageSubscription{
		ProcessInstanceKey: piKey,
		CorrelationKey:     correlationKey,
		MessageSubscriptionData: bpmnruntime.MessageSubscriptionData{
			Key:                  key,
			ElementId:            "elem-inst",
			Name:                 name,
			State:                state,
			ProcessDefinitionKey: 1000,
			CreatedAt:            time.Now(),
		},
	}
}

func newDefinitionSub(key int64, name string, state bpmnruntime.ActivityState) *bpmnruntime.DefinitionMessageSubscription {
	return &bpmnruntime.DefinitionMessageSubscription{
		MessageSubscriptionData: bpmnruntime.MessageSubscriptionData{
			Key:                  key,
			ElementId:            "elem-def",
			Name:                 name,
			State:                state,
			ProcessDefinitionKey: 1000,
			CreatedAt:            time.Now(),
		},
	}
}

func TestFindTokenMessageSubscriptions(t *testing.T) {
	ctx := context.Background()

	t.Run("returns only TokenMessageSubscriptions matching tokenKey and state", func(t *testing.T) {
		store := inmemory.NewStorage()

		sub1 := newTokenSub(1, 100, 200, "msg-a", "ck-a", bpmnruntime.ActivityStateActive)
		sub2 := newTokenSub(2, 100, 200, "msg-b", "ck-b", bpmnruntime.ActivityStateActive)
		sub3 := newTokenSub(3, 999, 200, "msg-c", "ck-c", bpmnruntime.ActivityStateActive)    // different token
		sub4 := newTokenSub(4, 100, 200, "msg-d", "ck-d", bpmnruntime.ActivityStateCompleted) // wrong state

		require.NoError(t, store.SaveMessageSubscription(ctx, sub1))
		require.NoError(t, store.SaveMessageSubscription(ctx, sub2))
		require.NoError(t, store.SaveMessageSubscription(ctx, sub3))
		require.NoError(t, store.SaveMessageSubscription(ctx, sub4))

		result, err := store.FindTokenMessageSubscriptions(ctx, 100, bpmnruntime.ActivityStateActive)
		require.NoError(t, err)
		assert.Len(t, result, 2)
	})

	t.Run("skips non-TokenMessageSubscription types", func(t *testing.T) {
		store := inmemory.NewStorage()

		instSub := newInstanceSub(10, 200, "msg-inst", "ck", bpmnruntime.ActivityStateActive)
		defSub := newDefinitionSub(11, "msg-def", bpmnruntime.ActivityStateActive)

		require.NoError(t, store.SaveMessageSubscription(ctx, instSub))
		require.NoError(t, store.SaveMessageSubscription(ctx, defSub))

		result, err := store.FindTokenMessageSubscriptions(ctx, 0, bpmnruntime.ActivityStateActive)
		require.NoError(t, err)
		assert.Empty(t, result)
	})

	t.Run("returns empty when no match", func(t *testing.T) {
		store := inmemory.NewStorage()

		sub := newTokenSub(1, 100, 200, "msg", "ck", bpmnruntime.ActivityStateActive)
		require.NoError(t, store.SaveMessageSubscription(ctx, sub))

		result, err := store.FindTokenMessageSubscriptions(ctx, 999, bpmnruntime.ActivityStateActive)
		require.NoError(t, err)
		assert.Empty(t, result)
	})
}

func TestFindProcessInstanceMessageSubscriptions(t *testing.T) {
	ctx := context.Background()

	t.Run("returns TokenMessageSubscription and InstanceMessageSubscription for the given processInstanceKey", func(t *testing.T) {
		store := inmemory.NewStorage()

		tokenSub := newTokenSub(1, 100, 200, "msg-token", "ck-t", bpmnruntime.ActivityStateActive)
		instSub := newInstanceSub(2, 200, "msg-inst", "ck-i", bpmnruntime.ActivityStateActive)
		otherTokenSub := newTokenSub(3, 101, 999, "msg-other", "ck-o", bpmnruntime.ActivityStateActive)

		require.NoError(t, store.SaveMessageSubscription(ctx, tokenSub))
		require.NoError(t, store.SaveMessageSubscription(ctx, instSub))
		require.NoError(t, store.SaveMessageSubscription(ctx, otherTokenSub))

		result, err := store.FindProcessInstanceMessageSubscriptions(ctx, 200, bpmnruntime.ActivityStateActive)
		require.NoError(t, err)
		assert.Len(t, result, 2)
	})

	t.Run("skips DefinitionMessageSubscription", func(t *testing.T) {
		store := inmemory.NewStorage()

		defSub := newDefinitionSub(11, "msg-def", bpmnruntime.ActivityStateActive)
		require.NoError(t, store.SaveMessageSubscription(ctx, defSub))

		result, err := store.FindProcessInstanceMessageSubscriptions(ctx, 0, bpmnruntime.ActivityStateActive)
		require.NoError(t, err)
		assert.Empty(t, result)
	})

	t.Run("filters by state", func(t *testing.T) {
		store := inmemory.NewStorage()

		activeSub := newTokenSub(1, 100, 200, "msg-a", "ck-a", bpmnruntime.ActivityStateActive)
		completedSub := newTokenSub(2, 101, 200, "msg-b", "ck-b", bpmnruntime.ActivityStateCompleted)

		require.NoError(t, store.SaveMessageSubscription(ctx, activeSub))
		require.NoError(t, store.SaveMessageSubscription(ctx, completedSub))

		result, err := store.FindProcessInstanceMessageSubscriptions(ctx, 200, bpmnruntime.ActivityStateActive)
		require.NoError(t, err)
		assert.Len(t, result, 1)
	})
}

func TestFindMessageSubscriptionByName(t *testing.T) {
	ctx := context.Background()

	t.Run("finds by name with matching correlationKey", func(t *testing.T) {
		store := inmemory.NewStorage()
		ck := "my-correlation-key"

		sub := newTokenSub(1, 100, 200, "order-placed", ck, bpmnruntime.ActivityStateActive)
		require.NoError(t, store.SaveMessageSubscription(ctx, sub))

		result, err := store.FindMessageSubscriptionByName(ctx, "order-placed", &ck, bpmnruntime.ActivityStateActive)
		require.NoError(t, err)
		assert.Equal(t, int64(1), result.MessageSubscription().Key)
	})

	t.Run("finds by name with nil correlationKey when subscription has empty correlationKey", func(t *testing.T) {
		store := inmemory.NewStorage()

		sub := newTokenSub(1, 100, 200, "order-placed", "", bpmnruntime.ActivityStateActive)
		require.NoError(t, store.SaveMessageSubscription(ctx, sub))

		result, err := store.FindMessageSubscriptionByName(ctx, "order-placed", nil, bpmnruntime.ActivityStateActive)
		require.NoError(t, err)
		assert.Equal(t, int64(1), result.MessageSubscription().Key)
	})

	t.Run("does not find when correlationKey differs", func(t *testing.T) {
		store := inmemory.NewStorage()
		ck := "wrong-key"

		sub := newTokenSub(1, 100, 200, "order-placed", "correct-key", bpmnruntime.ActivityStateActive)
		require.NoError(t, store.SaveMessageSubscription(ctx, sub))

		result, err := store.FindMessageSubscriptionByName(ctx, "order-placed", &ck, bpmnruntime.ActivityStateActive)
		assert.ErrorIs(t, err, storage.ErrNotFound)
		assert.Nil(t, result)
	})

	t.Run("does not find when state differs", func(t *testing.T) {
		store := inmemory.NewStorage()
		ck := "ck"

		sub := newTokenSub(1, 100, 200, "order-placed", ck, bpmnruntime.ActivityStateCompleted)
		require.NoError(t, store.SaveMessageSubscription(ctx, sub))

		result, err := store.FindMessageSubscriptionByName(ctx, "order-placed", &ck, bpmnruntime.ActivityStateActive)
		assert.ErrorIs(t, err, storage.ErrNotFound)
		assert.Nil(t, result)
	})

	t.Run("returns ErrNotFound when no subscription exists", func(t *testing.T) {
		store := inmemory.NewStorage()
		ck := "ck"

		result, err := store.FindMessageSubscriptionByName(ctx, "missing", &ck, bpmnruntime.ActivityStateActive)
		assert.ErrorIs(t, err, storage.ErrNotFound)
		assert.Nil(t, result)
	})

	t.Run("finds DefinitionMessageSubscription with nil correlationKey", func(t *testing.T) {
		store := inmemory.NewStorage()

		sub := newDefinitionSub(7, "process-start-message", bpmnruntime.ActivityStateActive)
		require.NoError(t, store.SaveMessageSubscription(ctx, sub))

		result, err := store.FindMessageSubscriptionByName(ctx, "process-start-message", nil, bpmnruntime.ActivityStateActive)
		require.NoError(t, err)
		assert.Equal(t, int64(7), result.MessageSubscription().Key)
	})
}

func TestSaveMessageSubscription_DuplicateCheck(t *testing.T) {
	ctx := context.Background()

	t.Run("rejects duplicate active subscription with same name and correlationKey", func(t *testing.T) {
		store := inmemory.NewStorage()

		sub1 := newTokenSub(1, 100, 200, "order-placed", "ck-1", bpmnruntime.ActivityStateActive)
		sub2 := newTokenSub(2, 101, 200, "order-placed", "ck-1", bpmnruntime.ActivityStateActive)

		require.NoError(t, store.SaveMessageSubscription(ctx, sub1))
		err := store.SaveMessageSubscription(ctx, sub2)
		assert.Error(t, err)
	})

	t.Run("allows different name with same correlationKey", func(t *testing.T) {
		store := inmemory.NewStorage()

		sub1 := newTokenSub(1, 100, 200, "order-placed", "ck-1", bpmnruntime.ActivityStateActive)
		sub2 := newTokenSub(2, 101, 200, "order-shipped", "ck-1", bpmnruntime.ActivityStateActive)

		require.NoError(t, store.SaveMessageSubscription(ctx, sub1))
		require.NoError(t, store.SaveMessageSubscription(ctx, sub2))
	})

	t.Run("allows same name/correlationKey when existing is not active", func(t *testing.T) {
		store := inmemory.NewStorage()

		sub1 := newTokenSub(1, 100, 200, "order-placed", "ck-1", bpmnruntime.ActivityStateCompleted)
		sub2 := newTokenSub(2, 101, 200, "order-placed", "ck-1", bpmnruntime.ActivityStateActive)

		require.NoError(t, store.SaveMessageSubscription(ctx, sub1))
		require.NoError(t, store.SaveMessageSubscription(ctx, sub2))
	})

	t.Run("allows updating existing subscription by same key", func(t *testing.T) {
		store := inmemory.NewStorage()

		sub1 := newTokenSub(1, 100, 200, "order-placed", "ck-1", bpmnruntime.ActivityStateActive)
		require.NoError(t, store.SaveMessageSubscription(ctx, sub1))

		// Save again with same key (update scenario)
		sub1Updated := newTokenSub(1, 100, 200, "order-placed", "ck-1", bpmnruntime.ActivityStateCompleted)
		require.NoError(t, store.SaveMessageSubscription(ctx, sub1Updated))

		result, err := store.FindMessageSubscriptionByKey(ctx, 1, bpmnruntime.ActivityStateCompleted)
		require.NoError(t, err)
		assert.Equal(t, bpmnruntime.ActivityStateCompleted, result.MessageSubscription().State)
	})
}

func TestFindMessageSubscriptionByKey(t *testing.T) {
	ctx := context.Background()

	t.Run("finds existing subscription by key and state", func(t *testing.T) {
		store := inmemory.NewStorage()

		sub := newTokenSub(42, 100, 200, "order-placed", "ck", bpmnruntime.ActivityStateActive)
		require.NoError(t, store.SaveMessageSubscription(ctx, sub))

		result, err := store.FindMessageSubscriptionByKey(ctx, 42, bpmnruntime.ActivityStateActive)
		require.NoError(t, err)
		assert.Equal(t, int64(42), result.MessageSubscription().Key)
	})

	t.Run("returns ErrNotFound for wrong state", func(t *testing.T) {
		store := inmemory.NewStorage()

		sub := newTokenSub(42, 100, 200, "order-placed", "ck", bpmnruntime.ActivityStateActive)
		require.NoError(t, store.SaveMessageSubscription(ctx, sub))

		_, err := store.FindMessageSubscriptionByKey(ctx, 42, bpmnruntime.ActivityStateCompleted)
		assert.ErrorIs(t, err, storage.ErrNotFound)
	})

	t.Run("returns ErrNotFound for missing key", func(t *testing.T) {
		store := inmemory.NewStorage()

		_, err := store.FindMessageSubscriptionByKey(ctx, 999, bpmnruntime.ActivityStateActive)
		assert.ErrorIs(t, err, storage.ErrNotFound)
	})
}
