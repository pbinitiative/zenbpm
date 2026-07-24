package partition

import (
	"math"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2/simplelru"
)

type ttlCacheEntry[V any] struct {
	value     V
	expiresAt time.Time
}

// ttlCache expires entries when they are read and opportunistically sweeps
// expired entries on writes. Positive-size caches remain size-bounded; a zero
// size preserves the previous unlimited-cache behavior without retaining an
// ever-growing set of expired entries while writes continue.
//
// This replaces expirable.LRU from hashicorp/golang-lru/v2 v2.0.7, whose
// cleanup goroutine cannot be stopped. Upstream main already provides
// expirable.LRU.Close, but that fix has not been released yet. Once it is part
// of a stable release, consider switching back to the upstream implementation.
type ttlCache[K comparable, V any] struct {
	mu    sync.Mutex
	cache *simplelru.LRU[K, ttlCacheEntry[V]]
	ttl   time.Duration
	now   func() time.Time
	// nextExpiration avoids scanning the cache on every Add while still
	// reclaiming expired, unread entries when writes continue.
	nextExpiration time.Time
}

func newTTLCache[K comparable, V any](size int, ttl time.Duration) (*ttlCache[K, V], error) {
	if size == 0 {
		size = math.MaxInt
	}
	cache, err := simplelru.NewLRU[K, ttlCacheEntry[V]](size, nil)
	if err != nil {
		return nil, err
	}
	return &ttlCache[K, V]{
		cache: cache,
		ttl:   ttl,
		now:   time.Now,
	}, nil
}

func (c *ttlCache[K, V]) Add(key K, value V) bool {
	now := c.now()
	entry := ttlCacheEntry[V]{value: value}
	if c.ttl > 0 {
		entry.expiresAt = now.Add(c.ttl)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.nextExpiration.IsZero() && !now.Before(c.nextExpiration) {
		c.removeExpiredLocked(now)
	}
	evicted := c.cache.Add(key, entry)
	if !entry.expiresAt.IsZero() && (c.nextExpiration.IsZero() || entry.expiresAt.Before(c.nextExpiration)) {
		c.nextExpiration = entry.expiresAt
	}
	return evicted
}

func (c *ttlCache[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.cache.Get(key)
	if !ok {
		var zero V
		return zero, false
	}
	if !entry.expiresAt.IsZero() && !c.now().Before(entry.expiresAt) {
		c.cache.Remove(key)
		var zero V
		return zero, false
	}
	return entry.value, true
}

// removeExpiredLocked sweeps the whole cache while holding c.mu. That is fine
// for the process and decision definition caches this type was written for,
// where the number of entries stays small; reconsider the sweep before reusing
// ttlCache for a large or write heavy cache.
func (c *ttlCache[K, V]) removeExpiredLocked(now time.Time) {
	c.nextExpiration = time.Time{}
	for _, key := range c.cache.Keys() {
		entry, ok := c.cache.Peek(key)
		if !ok || entry.expiresAt.IsZero() {
			continue
		}
		if !now.Before(entry.expiresAt) {
			c.cache.Remove(key)
			continue
		}
		if c.nextExpiration.IsZero() || entry.expiresAt.Before(c.nextExpiration) {
			c.nextExpiration = entry.expiresAt
		}
	}
}
