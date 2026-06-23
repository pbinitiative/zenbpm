package bpmn

import (
	"bytes"
	"context"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func TestTimerWaiterRecoversPanic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var logBuf syncBuffer
	logger := hclog.New(&hclog.LoggerOptions{
		Name:   "timer-manager-test",
		Level:  hclog.Error,
		Output: &logBuf,
	})

	tm := &timerManager{
		pollTimerDelay:  10 * time.Minute,
		mu:              &sync.RWMutex{},
		ctx:             ctx,
		ctxCancelFunc:   cancel,
		ch:              make(chan runtime.Timer),
		logger:          logger,
		waitingTimers:   []waitingTimer{},
		waitingTimersWg: &sync.WaitGroup{},
	}

	close(tm.ch)

	tm.addWaitingTimer(runtime.Timer{
		Key:   rand.Int63(),
		DueAt: time.Now().Add(-1 * time.Second),
	})

	done := make(chan struct{})
	go func() {
		tm.waitingTimersWg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("waitingTimersWg.Wait() did not return: Done() was not called after panic")
	}

	assert.Eventually(t, func() bool {
		return strings.Contains(logBuf.String(), "timer-waiter")
	}, 2*time.Second, 20*time.Millisecond,
		"safego should have recovered the panic and logged the goroutine name")
}
