package appcontext

import (
	"context"

	"github.com/pbinitiative/zenbpm/internal/cluster/types"
)

type historyTTLKeyType struct{}

var historyTTLKey = historyTTLKeyType{}

func WithHistoryTTL(ctx context.Context, ttl types.TTL) context.Context {
	return context.WithValue(ctx, historyTTLKey, ttl)
}

func HistoryTTLFromContext(ctx context.Context) (types.TTL, bool) {
	ttl, ok := ctx.Value(historyTTLKey).(types.TTL)
	return ttl, ok
}
