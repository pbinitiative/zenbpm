package store

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/safego"
	"github.com/stretchr/testify/assert"
)

type captureLogger struct {
	mu       sync.Mutex
	errCalls int
}

func (c *captureLogger) Error(msg string, args ...interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errCalls++
}

func (c *captureLogger) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.errCalls
}

func TestObserverGoroutinePanicRecovered(t *testing.T) {
	logger := &captureLogger{}
	done := make(chan struct{})

	observer := func(ctx context.Context) {
		defer close(done)
		panic("boom in observer")
	}

	safego.Go("cluster-state-change-observer", logger, func() {
		observer(context.Background())
	})

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("observer goroutine did not run")
	}

	assert.Eventually(t, func() bool {
		return logger.count() == 1
	}, time.Second, 10*time.Millisecond, "panic should be recovered and logged")
}
