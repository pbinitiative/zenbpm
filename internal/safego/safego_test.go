package safego

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

type logEntry struct {
	msg  string
	args []interface{}
}

type testLogger struct {
	mu      sync.Mutex
	entries []logEntry
}

func (l *testLogger) Error(msg string, args ...interface{}) {
	l.mu.Lock()
	l.entries = append(l.entries, logEntry{msg: msg, args: args})
	l.mu.Unlock()
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
	logger := &testLogger{}

	var wg sync.WaitGroup
	wg.Add(1)

	Go("test-panic", logger, func() {
		defer wg.Done()
		panic("boom")
	})

	wg.Wait()

	if !logger.containsMsg("safego: panic in test-panic") {
		t.Errorf("expected panic log message, got: %v", logger.entries)
	}
	if !logger.containsArg("boom") {
		t.Errorf("expected panic value in args, got: %v", logger.entries)
	}
	if !logger.containsArg("goroutine") {
		t.Errorf("expected stack trace in args, got: %v", logger.entries)
	}
}

func TestGo_NormalExecutionCompletes(t *testing.T) {
	logger := &testLogger{}

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
	logger := &testLogger{}

	var wg sync.WaitGroup
	wg.Add(2)

	completed := false

	Go("panicking", logger, func() {
		defer wg.Done()
		panic("intentional panic")
	})

	Go("normal", logger, func() {
		defer wg.Done()
		completed = true
	})

	wg.Wait()

	if !completed {
		t.Error("normal goroutine should complete even when another panics")
	}
}

func TestRun_PanicIsRecoveredAndReturnedAsError(t *testing.T) {
	logger := &testLogger{}

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
		t.Errorf("expected panic log message, got: %v", logger.entries)
	}
	if !logger.containsArg("goroutine") {
		t.Errorf("expected stack trace in log args, got: %v", logger.entries)
	}
}

func TestRun_NormalExecutionReturnsError(t *testing.T) {
	logger := &testLogger{}
	sentinel := errors.New("normal error")

	err := Run("test-run-normal", logger, func() error {
		return sentinel
	})

	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel error, got: %v", err)
	}
	if len(logger.entries) != 0 {
		t.Errorf("expected no log entries for normal error, got: %v", logger.entries)
	}
}

func TestRun_NilReturnOnSuccess(t *testing.T) {
	logger := &testLogger{}

	err := Run("test-run-success", logger, func() error {
		return nil
	})

	if err != nil {
		t.Errorf("expected nil error on success, got: %v", err)
	}
}
