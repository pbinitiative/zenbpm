package safego

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

type logEntry struct {
	msg  string
	args []interface{}
}

type testLogger struct {
	mu      sync.Mutex
	entries []logEntry
	errored chan struct{}
}

func newTestLogger() *testLogger {
	return &testLogger{
		errored: make(chan struct{}, 16),
	}
}

func (l *testLogger) Error(msg string, args ...interface{}) {
	l.mu.Lock()
	l.entries = append(l.entries, logEntry{msg: msg, args: args})
	l.mu.Unlock()
	select {
	case l.errored <- struct{}{}:
	default:
	}
}

func (l *testLogger) waitForError(t *testing.T) {
	t.Helper()
	select {
	case <-l.errored:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for error log; entries: %v", l.snapshot())
	}
}

func (l *testLogger) snapshot() []logEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]logEntry, len(l.entries))
	copy(out, l.entries)
	return out
}

// containsMsg checks whether any log entry message contains substr.
func (l *testLogger) containsMsg(substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range l.entries {
		if strings.Contains(e.msg, substr) {
			return true
		}
	}
	return false
}

// containsArg checks whether any log entry has an arg whose string representation contains substr.
func (l *testLogger) containsArg(substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range l.entries {
		for _, a := range e.args {
			if strings.Contains(fmt.Sprintf("%v", a), substr) {
				return true
			}
		}
	}
	return false
}

func TestGo_PanicIsRecoveredAndLogged(t *testing.T) {
	logger := newTestLogger()

	Go("test-panic", logger, func() {
		panic("boom")
	})

	logger.waitForError(t)

	if !logger.containsMsg("safego: panic in test-panic") {
		t.Errorf("expected panic log message, got: %v", logger.snapshot())
	}
	if !logger.containsArg("boom") {
		t.Errorf("expected panic value in args, got: %v", logger.snapshot())
	}
	if !logger.containsArg("goroutine") {
		t.Errorf("expected stack trace in args, got: %v", logger.snapshot())
	}
}

func TestGo_NormalExecutionCompletes(t *testing.T) {
	logger := newTestLogger()

	var wg sync.WaitGroup
	wg.Add(1)

	called := false
	Go("test-normal", logger, func() {
		defer wg.Done()
		called = true
	})

	wg.Wait()

	if !called {
		t.Error("expected fn to be called")
	}
}

func TestGo_PanicDoesNotBlockCaller(t *testing.T) {
	logger := newTestLogger()

	var wg sync.WaitGroup
	wg.Add(1)

	completed := false

	Go("panicking", logger, func() {
		panic("intentional panic")
	})

	Go("normal", logger, func() {
		defer wg.Done()
		completed = true
	})

	wg.Wait()
	logger.waitForError(t)

	if !completed {
		t.Error("normal goroutine should complete even when another panics")
	}
}

func TestRun_PanicIsRecoveredAndReturnedAsError(t *testing.T) {
	logger := newTestLogger()

	err := Run("test-run-panic", logger, func() error {
		panic("run-boom")
	})

	if err == nil {
		t.Fatal("expected non-nil error from panic recovery")
	}
	if !strings.Contains(err.Error(), "run-boom") {
		t.Errorf("expected panic value in error, got: %v", err)
	}
	if !logger.containsMsg("safego: panic in test-run-panic") {
		t.Errorf("expected panic log message, got: %v", logger.snapshot())
	}
	if !logger.containsArg("goroutine") {
		t.Errorf("expected stack trace in log args, got: %v", logger.snapshot())
	}
}

func TestRun_NormalExecutionReturnsError(t *testing.T) {
	logger := newTestLogger()
	sentinel := errors.New("normal error")

	err := Run("test-run-normal", logger, func() error {
		return sentinel
	})

	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel error, got: %v", err)
	}
	if entries := logger.snapshot(); len(entries) != 0 {
		t.Errorf("expected no log entries for normal error, got: %v", entries)
	}
}

func TestRun_NilReturnOnSuccess(t *testing.T) {
	logger := newTestLogger()

	err := Run("test-run-success", logger, func() error {
		return nil
	})

	if err != nil {
		t.Errorf("expected nil error on success, got: %v", err)
	}
}
