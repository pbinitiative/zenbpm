package safego

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// logArgs returns a flat slice of all variadic args recorded across all entries.
func (l *testLogger) logArgs() []interface{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	var all []interface{}
	for _, e := range l.entries {
		all = append(all, e.args...)
	}
	return all
}

// entryCount returns the number of recorded log entries.
func (l *testLogger) entryCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

// panicValue scans args for the value stored under the key "panic".
// The Logger contract is: args is a flat key–value sequence.
func (l *testLogger) panicValue() interface{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range l.entries {
		for i := 0; i+1 < len(e.args); i += 2 {
			if k, ok := e.args[i].(string); ok && k == "panic" {
				return e.args[i+1]
			}
		}
	}
	return nil
}

// stackValue scans args for the value stored under the key "stack".
func (l *testLogger) stackValue() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range l.entries {
		for i := 0; i+1 < len(e.args); i += 2 {
			if k, ok := e.args[i].(string); ok && k == "stack" {
				return fmt.Sprintf("%v", e.args[i+1])
			}
		}
	}
	return ""
}

// awaitEntries blocks until logger has at least n entries or 2 s have elapsed.
// It yields the scheduler on each iteration so the recovery goroutine can run.
func awaitEntries(l *testLogger, n int) {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if l.entryCount() >= n {
			return
		}
		runtime.Gosched()
		time.Sleep(time.Millisecond)
	}
}

func TestGo_ConcurrentPanicsDoNotRaceOnLogger(t *testing.T) {
	const goroutines = 50
	logger := &testLogger{}

	for i := range goroutines {
		name := fmt.Sprintf("worker-%d", i)
		Go(name, logger, func() {
			panic(fmt.Sprintf("concurrent panic %d", i))
		})
	}
	
	awaitEntries(logger, goroutines)
	
	if got := logger.entryCount(); got != goroutines {
		t.Errorf("expected %d log entries (one per panic), got %d", goroutines, got)
	}
}

func TestGo_NilPanicIsRecoveredAndLogged(t *testing.T) {
	logger := &testLogger{}

	Go("nil-panic", logger, func() {
		panic(nil)
	})

	awaitEntries(logger, 1)

	if logger.entryCount() == 0 {
		t.Fatal("panic(nil) was silently swallowed — no log entry produced")
	}
	if !logger.containsMsg("safego: panic in nil-panic") {
		t.Errorf("expected panic log message, got: %v", logger.entries)
	}
	if stack := logger.stackValue(); !strings.Contains(stack, "goroutine") {
		t.Errorf("expected stack trace in log, got: %q", stack)
	}
}

func TestRun_NilPanicReturnsNonNilError(t *testing.T) {
	logger := &testLogger{}

	err := Run("nil-panic-run", logger, func() error {
		panic(nil)
	})
	
	if err == nil {
		t.Fatal("expected non-nil error from panic(nil) recovery under Go 1.21+")
	}
}

func TestGo_RuntimeIndexOutOfBoundsPanicIsRecovered(t *testing.T) {
	logger := &testLogger{}

	Go("oob", logger, func() {
		s := []int{}
		_ = s[0]
	})

	awaitEntries(logger, 1)

	if logger.entryCount() == 0 {
		t.Fatal("runtime OOB panic was not logged")
	}
	pv := logger.panicValue()
	if pv == nil {
		t.Fatal("expected panic value in log args, got nil")
	}
	pvStr := fmt.Sprintf("%v", pv)
	if !strings.Contains(pvStr, "index out of range") {
		t.Errorf("expected 'index out of range' in panic value, got: %q", pvStr)
	}
}

func TestGo_NilPointerDereferencePanicIsRecovered(t *testing.T) {
	type inner struct{ n int }
	logger := &testLogger{}

	Go("nil-deref", logger, func() {
		var p *inner
		_ = p.n
	})

	awaitEntries(logger, 1)

	if logger.entryCount() == 0 {
		t.Fatal("nil-deref panic was not logged")
	}
	pv := logger.panicValue()
	pvStr := fmt.Sprintf("%v", pv)
	if !strings.Contains(pvStr, "nil pointer") {
		t.Errorf("expected 'nil pointer' in panic value, got: %q", pvStr)
	}
}

type panicStruct struct {
	Code    int
	Message string
}

func (p panicStruct) Error() string {
	return fmt.Sprintf("panicStruct{Code:%d, Message:%q}", p.Code, p.Message)
}

func TestGo_PanicWithStructValueIsLoggedCorrectly(t *testing.T) {
	logger := &testLogger{}

	Go("struct-panic", logger, func() {
		panic(panicStruct{Code: 42, Message: "domain failure"})
	})

	awaitEntries(logger, 1)

	pv := logger.panicValue()
	if pv == nil {
		t.Fatal("expected panic value in log, got nil")
	}
	ps, ok := pv.(panicStruct)
	if !ok {
		t.Fatalf("expected panicStruct in log args, got %T: %v", pv, pv)
	}
	if ps.Code != 42 || ps.Message != "domain failure" {
		t.Errorf("panic struct fields mangled: %+v", ps)
	}
}

func TestGo_PanicWithErrorValueIsLoggedCorrectly(t *testing.T) {
	sentinel := errors.New("sentinel error payload")
	logger := &testLogger{}

	Go("error-panic", logger, func() {
		panic(sentinel)
	})

	awaitEntries(logger, 1)

	pv := logger.panicValue()
	if pv == nil {
		t.Fatal("expected panic value in log, got nil")
	}
	pvErr, ok := pv.(error)
	if !ok {
		t.Fatalf("expected error type in log args, got %T: %v", pv, pv)
	}
	if !errors.Is(pvErr, sentinel) {
		t.Errorf("expected sentinel error, got: %v", pvErr)
	}
}

func TestRun_PanicWithStructValueReturnedInError(t *testing.T) {
	logger := &testLogger{}

	err := Run("struct-panic-run", logger, func() error {
		panic(panicStruct{Code: 7, Message: "oops"})
	})

	if err == nil {
		t.Fatal("expected non-nil error")
	}
	msg := err.Error()

	if !strings.Contains(msg, "oops") {
		t.Errorf("error message should contain struct representation, got: %q", msg)
	}
}

func TestGo_StackTraceContainsCaller(t *testing.T) {
	logger := &testLogger{}

	Go("stack-depth", logger, func() {
		triggerableDeepPanic()
	})

	awaitEntries(logger, 1)

	stack := logger.stackValue()
	if stack == "" {
		t.Fatal("no stack trace in log")
	}
	if !strings.Contains(stack, "triggerableDeepPanic") {
		t.Errorf("stack trace missing the direct panicking frame 'triggerableDeepPanic':\n%s", stack)
	}
}

func triggerableDeepPanic() {
	panic("deep panic for stack trace test")
}

func TestRun_StackTraceContainsCallerFrame(t *testing.T) {
	logger := &testLogger{}

	err := Run("stack-depth-run", logger, func() error {
		triggerableDeepPanic()
		return nil
	})

	if err == nil {
		t.Fatal("expected non-nil error")
	}
	// The error string includes the stack trace (see Run implementation).
	if !strings.Contains(err.Error(), "triggerableDeepPanic") {
		t.Errorf("error string missing direct panic frame:\n%s", err.Error())
	}
	stack := logger.stackValue()
	if !strings.Contains(stack, "triggerableDeepPanic") {
		t.Errorf("log stack trace missing direct panic frame:\n%s", stack)
	}
}

func TestDefaultLogger_UsesCorrectSlogKeysForPanic(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	prev := slog.Default()
	slog.SetDefault(slog.New(handler))
	t.Cleanup(func() { slog.SetDefault(prev) })

	DefaultLogger.Error("safego: panic in probe", "panic", "test-value", "stack", "goroutine 99 [running]:\n")

	output := buf.String()

	if !strings.Contains(output, `panic=test-value`) {
		t.Errorf("expected 'panic=test-value' in slog text output, got: %q", output)
	}
	if !strings.Contains(output, `stack=`) {
		t.Errorf("expected 'stack=' key in slog text output, got: %q", output)
	}
	if !strings.Contains(output, "safego: panic in probe") {
		t.Errorf("expected message in slog text output, got: %q", output)
	}
}

func TestDefaultLogger_IsAssignableToLoggerInterface(t *testing.T) {
	var _ Logger = DefaultLogger
}

func TestGo_LogEntryHasExactlyPanicAndStackKeys(t *testing.T) {
	logger := &testLogger{}

	Go("key-structure", logger, func() {
		panic("structured")
	})

	awaitEntries(logger, 1)

	if logger.entryCount() != 1 {
		t.Fatalf("expected 1 log entry, got %d", logger.entryCount())
	}

	logger.mu.Lock()
	entry := logger.entries[0]
	logger.mu.Unlock()

	args := entry.args
	if len(args) != 4 {
		t.Fatalf("expected exactly 4 args (2 key-value pairs), got %d: %v", len(args), args)
	}

	wantKeys := []string{"panic", "stack"}
	for i, wantKey := range wantKeys {
		k, ok := args[i*2].(string)
		if !ok {
			t.Errorf("arg[%d] should be a string key, got %T: %v", i*2, args[i*2], args[i*2])
			continue
		}
		if k != wantKey {
			t.Errorf("arg[%d] key: want %q, got %q", i*2, wantKey, k)
		}
	}
}

func TestRun_LogEntryHasExactlyPanicAndStackKeys(t *testing.T) {
	logger := &testLogger{}

	_ = Run("key-structure-run", logger, func() error {
		panic("structured-run")
	})

	if logger.entryCount() != 1 {
		t.Fatalf("expected 1 log entry, got %d", logger.entryCount())
	}

	logger.mu.Lock()
	entry := logger.entries[0]
	logger.mu.Unlock()

	if len(entry.args) != 4 {
		t.Fatalf("expected exactly 4 args, got %d: %v", len(entry.args), entry.args)
	}
}

func TestRun_DeferredPanicInsideFnIsRecovered(t *testing.T) {
	logger := &testLogger{}

	err := Run("deferred-panic", logger, func() error {
		defer func() {
			panic("panic inside deferred cleanup")
		}()
		return nil
	})

	if err == nil {
		t.Fatal("expected non-nil error from deferred panic inside fn")
	}
	if !strings.Contains(err.Error(), "panic inside deferred cleanup") {
		t.Errorf("error should mention deferred panic value, got: %q", err.Error())
	}
	if logger.entryCount() == 0 {
		t.Error("expected deferred panic to be logged")
	}
}

func TestRun_PanicOverridesPartialNamedReturn(t *testing.T) {
	logger := &testLogger{}
	originalErr := errors.New("original error before panic")

	err := Run("override-return", logger, func() error {
		defer func() {
			panic("override panic")
		}()
		return originalErr
	})

	if err == nil {
		t.Fatal("expected non-nil error")
	}
	if errors.Is(err, originalErr) {
		t.Errorf("expected panic-derived error to override original error, but got original: %v", err)
	}
	if !strings.Contains(err.Error(), "override panic") {
		t.Errorf("expected panic value in error, got: %q", err.Error())
	}
}

func TestGo_NoGoroutineLeakAfterPanic(t *testing.T) {
	logger := &testLogger{}

	// Let the runtime settle before counting.
	runtime.Gosched()
	before := runtime.NumGoroutine()

	Go("leak-check", logger, func() {
		panic("leak test panic")
	})

	awaitEntries(logger, 1)
	runtime.Gosched()
	time.Sleep(5 * time.Millisecond)

	after := runtime.NumGoroutine()
	
	if after > before+1 {
		t.Errorf("goroutine count grew from %d to %d after panic — possible leak", before, after)
	}
}

func TestGo_FnIsCalledExactlyOnce(t *testing.T) {
	logger := &testLogger{}
	var callCount atomic.Int64

	Go("once", logger, func() {
		callCount.Add(1)
		panic("call-count panic")
	})

	awaitEntries(logger, 1)

	if n := callCount.Load(); n != 1 {
		t.Errorf("expected fn to be called exactly once, called %d times", n)
	}
}

func TestRun_FnIsCalledExactlyOnce(t *testing.T) {
	logger := &testLogger{}
	var callCount atomic.Int64

	_ = Run("once-run", logger, func() error {
		callCount.Add(1)
		panic("call-count run panic")
	})

	if n := callCount.Load(); n != 1 {
		t.Errorf("expected fn to be called exactly once, called %d times", n)
	}
}

func TestRun_ErrorMessageContainsNameAndStack(t *testing.T) {
	logger := &testLogger{}
	const goroutineName = "critical-worker"

	err := Run(goroutineName, logger, func() error {
		namedPanicTrigger()
		return nil
	})

	if err == nil {
		t.Fatal("expected non-nil error")
	}
	msg := err.Error()

	checks := []struct {
		substr string
		label  string
	}{
		{goroutineName, "goroutine name"},
		{"named panic value", "panic value"},
		{"namedPanicTrigger", "stack frame"},
	}
	for _, c := range checks {
		if !strings.Contains(msg, c.substr) {
			t.Errorf("error message missing %s (%q):\n%s", c.label, c.substr, msg)
		}
	}
}

func namedPanicTrigger() {
	panic("named panic value")
}
