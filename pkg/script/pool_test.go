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

func TestDrainIdleRunners_DiscardsRunnersAboveMin(t *testing.T) {
	pool := NewRunnerPool(fakeFactory{}, 5, 1)
	defer pool.Stop()
	
	for i := 0; i < 3; i++ {
		pool.activeRunnersMu.Lock()
		pool.pool <- pool.runnerFactory.NewRunner()
		pool.activeRunnersCount++
		pool.activeRunnersMu.Unlock()
	}
	require.Equal(t, 4, pool.activeRunnersCount)
	require.Equal(t, 4, len(pool.pool))

	pool.drainIdleRunners()

	assert.Equal(t, 1, len(pool.pool), "should drain down to minVmPoolSize")
	assert.Equal(t, 1, pool.activeRunnersCount)
}

func TestDrainIdleRunners_EmptyPoolDoesNotBlock(t *testing.T) {
	pool := NewRunnerPool(fakeFactory{}, 5, 1)
	defer pool.Stop()

	<-pool.pool
	require.Equal(t, 0, len(pool.pool))

	done := make(chan struct{})
	go func() {
		defer close(done)
		pool.drainIdleRunners()
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("drainIdleRunners blocked on an empty pool — possible deadlock")
	}
}

func TestDrainIdleRunners_DoesNotDeadlockOnConcurrentTake(t *testing.T) {
	pool := NewRunnerPool(fakeFactory{}, 5, 1)
	defer pool.Stop()

	pool.activeRunnersMu.Lock()
	pool.pool <- pool.runnerFactory.NewRunner()
	pool.activeRunnersCount++
	pool.activeRunnersMu.Unlock()
	require.Equal(t, 2, len(pool.pool))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 2; i++ {
			select {
			case <-pool.pool:
			default:
			}
		}
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		pool.drainIdleRunners()
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("drainIdleRunners deadlocked under concurrent take")
	}
	wg.Wait()

	require.True(t, pool.activeRunnersMu.TryLock(), "activeRunnersMu still held after drain — mutex leaked")
	pool.activeRunnersMu.Unlock()
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
