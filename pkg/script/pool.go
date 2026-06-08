package script

import (
	"context"
	"sync"
	"time"

	"github.com/pbinitiative/zenbpm/internal/safego"
)

type Runner interface {
	Runner()
}

type RunnerFactory interface {
	NewRunner() Runner
}

type RunnerPool struct {
	ctx                context.Context
	cancel             context.CancelFunc
	pool               chan Runner
	runnerFactory      RunnerFactory
	activeRunnersCount int
	activeRunnersMu    *sync.Mutex
	maxVmPoolSize      int // max amount of active runners
	minVmPoolSize      int // min amount of active runners
}

func NewRunnerPool(runnerFactory RunnerFactory, maxVmPoolSize int, minVmPoolSize int) *RunnerPool {
	if maxVmPoolSize < minVmPoolSize {
		panic("vm pool max size is smaller than vm pool min size")
	}

	ctx, cancel := context.WithCancel(context.Background())

	runtime := RunnerPool{
		ctx:                ctx,
		cancel:             cancel,
		pool:               make(chan Runner, maxVmPoolSize),
		runnerFactory:      runnerFactory,
		activeRunnersCount: 0,
		activeRunnersMu:    &sync.Mutex{},
		maxVmPoolSize:      maxVmPoolSize,
		minVmPoolSize:      minVmPoolSize,
	}

	//start min amount of runners
	for i := 0; i < minVmPoolSize; i++ {
		runtime.activeRunnersMu.Lock()
		runtime.pool <- runtime.runnerFactory.NewRunner()
		runtime.activeRunnersCount++
		runtime.activeRunnersMu.Unlock()
	}

	//cleanup runners every 10 minutes
	//should clean runners only when they are not being used
	startCleanupLoop(safego.DefaultLogger, runtime.cleanupLoop)
	return &runtime
}

// startCleanupLoop runs body in a panic-recovering goroutine. body is expected
// to block until it should exit (e.g. on ctx.Done()). Extracted as a seam so
// tests can verify panic recovery without waiting on the ticker.
func startCleanupLoop(logger safego.Logger, body func()) {
	safego.Go("script-pool-cleanup", logger, body)
}

// cleanupLoop periodically discards idle runners above the minimum pool size
// until the pool's context is cancelled.
func (r *RunnerPool) cleanupLoop() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.drainIdleRunners()
		case <-r.ctx.Done():
			return
		}
	}
}

// drainIdleRunners discards idle runners above the minimum pool size. It never
// blocks on the channel: the receive is non-blocking (select/default), so a
// runner taken concurrently by GetRunnerFromPool between the size check and the
// receive simply ends the drain instead of stalling the goroutine while holding
// the mutex.
func (r *RunnerPool) drainIdleRunners() {
	for len(r.pool) > r.minVmPoolSize {
		select {
		case <-r.pool:
			r.activeRunnersMu.Lock()
			r.activeRunnersCount--
			r.activeRunnersMu.Unlock()
		default:
			return
		}
	}
}

func (r *RunnerPool) Stop() {
	r.cancel()
}

func (r *RunnerPool) GetRunnerFromPool() Runner {
	var runner Runner
	select {
	case runner = <-r.pool:
	default:
		r.activeRunnersMu.Lock()
		if r.activeRunnersCount < r.maxVmPoolSize {
			runner = r.runnerFactory.NewRunner()
			r.activeRunnersCount++
		}
		r.activeRunnersMu.Unlock()
		if runner == nil {
			runner = <-r.pool
		}
	}
	return runner
}

func (r *RunnerPool) ReturnRunnerToPool(runner Runner) {
	select {
	case r.pool <- runner:
	default:
		//delete runner if pool is full
		r.activeRunnersMu.Lock()
		r.activeRunnersCount--
		r.activeRunnersMu.Unlock()
	}
}
