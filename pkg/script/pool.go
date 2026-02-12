package script

import (
	"context"
	"sync"
	"time"
)

type Runner interface {
	Runner()
}

type RunnerFactory interface {
	NewRunner() Runner
}

type RunnerPool struct {
	pool               chan Runner
	runnerFactory      RunnerFactory
	activeRunnersCount int
	activeRunnersMu    *sync.Mutex
	maxVmPoolSize      int // max amount of active runners
	minVmPoolSize      int // min amount of active runners
}

func NewRunnerPool(ctx context.Context, runnerFactory RunnerFactory, maxVmPoolSize int, minVmPoolSize int) *RunnerPool {
	if maxVmPoolSize < minVmPoolSize {
		panic("vm pool min size is smaller than vm pool max size")
	}

	runtime := RunnerPool{
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
	go func() {
		ticker := time.NewTicker(10 * time.Minute)
		for {
			select {
			case <-ticker.C:
				if len(runtime.pool) > minVmPoolSize {
					for i := minVmPoolSize; i < len(runtime.pool); {
						runtime.activeRunnersMu.Lock()
						<-runtime.pool
						runtime.activeRunnersCount--
						runtime.activeRunnersMu.Unlock()
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return &runtime
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
