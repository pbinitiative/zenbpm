package script

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeRunner struct{}

func (fakeRunner) Runner() {}

type fakeFactory struct{}

func (fakeFactory) NewRunner() Runner { return fakeRunner{} }

func TestNewRunnerPool_StartsMinRunners(t *testing.T) {
	pool := NewRunnerPool(fakeFactory{}, 4, 2)
	defer pool.Stop()

	require.NotNil(t, pool)
	assert.Equal(t, 2, pool.activeRunnersCount)
}

func TestNewRunnerPool_PanicGuardOnBadSizes(t *testing.T) {
	assert.Panics(t, func() {
		NewRunnerPool(fakeFactory{}, 1, 2)
	})
}

type recordingLogger struct {
	mu      sync.Mutex
	msgs    []string
	errored chan struct{}
}

func newRecordingLogger() *recordingLogger {
	return &recordingLogger{errored: make(chan struct{}, 4)}
}

func (l *recordingLogger) Error(msg string, args ...interface{}) {
	l.mu.Lock()
	l.msgs = append(l.msgs, msg)
	l.mu.Unlock()
	select {
	case l.errored <- struct{}{}:
	default:
	}
}

func (l *recordingLogger) waitForError(t *testing.T) {
	t.Helper()
	select {
	case <-l.errored:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for recovered-panic log")
	}
}

func TestRunnerPoolCleanup_PanicIsRecovered(t *testing.T) {
	logger := newRecordingLogger()
	
	done := make(chan struct{})
	startCleanupLoop(logger, func() {
		defer close(done)
		panic("cleanup boom")
	})

	logger.waitForError(t)
	<-done

	logger.mu.Lock()
	defer logger.mu.Unlock()
	require.NotEmpty(t, logger.msgs)
	assert.Contains(t, logger.msgs[0], "panic in script-pool-cleanup")
}
