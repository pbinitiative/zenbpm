package appcontext

import (
	"context"

	"github.com/pbinitiative/zenbpm/internal/cluster/types"
)

type historyTTLKeyType struct{}
type businessKeyKeyType struct{}
type processInstanceKeyKeyType struct{}
type elementInstanceKeyKeyType struct{}

var historyTTLKey = historyTTLKeyType{}
var businessKeyKey = businessKeyKeyType{}
var processInstanceKey = processInstanceKeyKeyType{}
var elementInstanceKey = elementInstanceKeyKeyType{}

func WithHistoryTTL(ctx context.Context, ttl types.TTL) context.Context {
	return context.WithValue(ctx, historyTTLKey, ttl)
}

func HistoryTTLFromContext(ctx context.Context) (types.TTL, bool) {
	ttl, ok := ctx.Value(historyTTLKey).(types.TTL)
	return ttl, ok
}

func WithBusinessKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, businessKeyKey, key)
}

func BusinessKeyFromContext(ctx context.Context) (string, bool) {
	businessKey, ok := ctx.Value(businessKeyKey).(string)
	return businessKey, ok
}

func WithProcessInstanceKey(ctx context.Context, key int64) context.Context {
	return context.WithValue(ctx, processInstanceKey, key)
}

func ProcessInstanceKeyFromContext(ctx context.Context) (int64, bool) {
	key, ok := ctx.Value(processInstanceKey).(int64)
	return key, ok
}

func WithElementInstanceKey(ctx context.Context, key int64) context.Context {
	return context.WithValue(ctx, elementInstanceKey, key)
}

func ElementInstanceKeyFromContext(ctx context.Context) (int64, bool) {
	key, ok := ctx.Value(elementInstanceKey).(int64)
	return key, ok
}
