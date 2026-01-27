package appcontext

import (
	"context"

	"github.com/pbinitiative/zenbpm/internal/cluster/types"
)

type historyTTLKeyType struct{}
type businessKeyKeyType struct{}
type processInstanceKeyKeyType struct{}

var historyTTLKey = historyTTLKeyType{}
var businessKeyKey = businessKeyKeyType{}
var processInstanceKey = processInstanceKeyKeyType{}

func WithHistoryTTL(ctx context.Context, ttl types.TTL) context.Context {
	return context.WithValue(ctx, historyTTLKey, ttl)
}

func HistoryTTLFromContext(ctx context.Context) (types.TTL, bool) {
	ttl, ok := ctx.Value(historyTTLKey).(types.TTL)
	return ttl, ok
}

func WithBusinessKey(ctx context.Context, businessKey string) context.Context {
	return context.WithValue(ctx, businessKeyKey, businessKey)
}

func BusinessKeyFromContext(ctx context.Context) (string, bool) {
	businessKey, ok := ctx.Value(businessKeyKey).(string)
	return businessKey, ok
}

func WithProcessInstanceKey(ctx context.Context, processInstanceKey int64) context.Context {
	return context.WithValue(ctx, processInstanceKey, processInstanceKey)
}

func ProcessInstanceKeyFromContext(ctx context.Context) (int64, bool) {
	processInstanceKey, ok := ctx.Value(processInstanceKey).(int64)
	return processInstanceKey, ok
}
