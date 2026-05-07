package safego

import (
	"context"
	"strings"
	"sync"
	"testing"
)

type testLogger struct {
	mu   sync.Mutex
	msgs []string
}

func (l *testLogger) Error(msg string, args ...interface{}) {
	l.mu.Lock()
	l.msgs = append(l.msgs, msg)
	l.mu.Unlock()
}

func (l *testLogger) contains(substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, m := range l.msgs {
		if strings.Contains(m, substr) {
			return true
		}
	}
	return false
}

func TestGo_PanicIsRecoveredAndLogged(t *testing.T) {
	logger := &testLogger{}

	var wg sync.WaitGroup
	wg.Add(1)

	Go(context.Background(), "test-panic", logger, func() {
		defer wg.Done()
		panic("boom")
	})

	wg.Wait()

	if !logger.contains("safego: panic in test-panic") {
		t.Errorf("expected panic log message, got: %v", logger.msgs)
	}
	if !logger.contains("boom") {
		t.Errorf("expected panic value in log, got: %v", logger.msgs)
	}
	if !logger.contains("goroutine") {
		t.Errorf("expected stack trace in log, got: %v", logger.msgs)
	}
}

func TestGo_NormalExecutionCompletes(t *testing.T) {
	logger := &testLogger{}

	var wg sync.WaitGroup
	wg.Add(1)

	called := false
	Go(context.Background(), "test-normal", logger, func() {
		defer wg.Done()
		called = true
	})

	wg.Wait()

	if !called {
		t.Error("expected fn to be called")
	}
}

func TestGo_PanicDoesNotBlockCaller(t *testing.T) {
	logger := &testLogger{}

	var wg sync.WaitGroup
	wg.Add(2)

	completed := false

	Go(context.Background(), "panicking", logger, func() {
		defer wg.Done()
		panic("intentional panic")
	})

	Go(context.Background(), "normal", logger, func() {
		defer wg.Done()
		completed = true
	})

	wg.Wait()

	if !completed {
		t.Error("normal goroutine should complete even when another panics")
	}
}
