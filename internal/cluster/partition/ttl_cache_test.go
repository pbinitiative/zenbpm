package partition

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTTLCacheExpiresEntriesOnRead(t *testing.T) {
	now := time.Date(2026, time.July, 14, 0, 0, 0, 0, time.UTC)
	cache, err := newTTLCache[string, int](2, time.Minute)
	require.NoError(t, err)
	cache.now = func() time.Time { return now }
	cache.Add("key", 42)

	value, ok := cache.Get("key")
	require.True(t, ok, "expected the entry to be cached")
	assert.Equal(t, 42, value)

	now = now.Add(time.Minute)
	_, ok = cache.Get("key")
	assert.False(t, ok, "expected an expired cache miss")
}

func TestTTLCacheUsesLRUEviction(t *testing.T) {
	cache, err := newTTLCache[string, int](2, 0)
	require.NoError(t, err)
	cache.Add("first", 1)
	cache.Add("second", 2)
	_, ok := cache.Get("first")
	require.True(t, ok, "expected first entry to exist")
	cache.Add("third", 3)

	_, ok = cache.Get("second")
	assert.False(t, ok, "expected least-recently-used entry to be evicted")

	value, ok := cache.Get("first")
	assert.True(t, ok, "expected first entry to remain")
	assert.Equal(t, 1, value)

	value, ok = cache.Get("third")
	assert.True(t, ok, "expected third entry to remain")
	assert.Equal(t, 3, value)
}

func TestTTLCacheZeroSizeIsUnbounded(t *testing.T) {
	cache, err := newTTLCache[int, int](0, 0)
	require.NoError(t, err)
	for key := range 100 {
		cache.Add(key, key)
	}
	for key := range 100 {
		value, ok := cache.Get(key)
		require.True(t, ok, "expected key %d to remain", key)
		require.Equal(t, key, value)
	}
}

func TestTTLCacheZeroSizeReapsExpiredEntriesOnAdd(t *testing.T) {
	now := time.Date(2026, time.July, 14, 0, 0, 0, 0, time.UTC)
	cache, err := newTTLCache[int, int](0, time.Minute)
	require.NoError(t, err)
	cache.now = func() time.Time { return now }
	for key := range 100 {
		cache.Add(key, key)
	}

	now = now.Add(time.Minute)
	cache.Add(100, 100)
	assert.Equal(t, 1, cache.cache.Len(), "expected expired entries to be reaped on Add")

	value, ok := cache.Get(100)
	assert.True(t, ok, "expected new entry to remain")
	assert.Equal(t, 100, value)
}

func TestTTLCacheReapsExpiredEntriesBeforeLRUEviction(t *testing.T) {
	now := time.Date(2026, time.July, 14, 0, 0, 0, 0, time.UTC)
	cache, err := newTTLCache[string, int](2, 10*time.Second)
	require.NoError(t, err)
	cache.now = func() time.Time { return now }
	cache.Add("expires-first", 1)
	now = now.Add(5 * time.Second)
	cache.Add("still-live", 2)
	now = now.Add(4 * time.Second)
	_, ok := cache.Get("expires-first")
	require.True(t, ok, "expected first entry to still be live before its deadline")

	now = now.Add(2 * time.Second)
	cache.Add("new", 3)

	value, ok := cache.Get("still-live")
	assert.True(t, ok, "expected live entry to remain")
	assert.Equal(t, 2, value)

	_, ok = cache.Get("expires-first")
	assert.False(t, ok, "expected expired entry to have been reaped")
}

func TestTTLCacheRejectsNegativeSize(t *testing.T) {
	cache, err := newTTLCache[int, int](-1, time.Minute)
	assert.Error(t, err, "expected negative size to fail")
	assert.Nil(t, cache)
}
