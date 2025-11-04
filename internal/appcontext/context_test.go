package appcontext

import (
	"context"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/types"
	"github.com/stretchr/testify/assert"
)

func TestHistoryTTL(t *testing.T) {
	ctx := context.Background()
	ttl, err := types.ParseTTL("5d")
	assert.NoError(t, err)
	ctx = WithHistoryTTL(ctx, ttl)

	valFromCtx, found := HistoryTTLFromContext(ctx)
	assert.True(t, found)
	assert.Equal(t, ttl, valFromCtx)

	valFromCtx, found = HistoryTTLFromContext(context.Background())
	assert.False(t, found)
	assert.Equal(t, types.TTL(0), valFromCtx)
}
