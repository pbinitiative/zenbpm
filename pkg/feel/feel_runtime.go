package feel

import (
	_ "embed"
	"sync"
	"time"
)

// max amount of active runners
const maxVmPoolSize = 10

// min amount of active runners
const minVmPoolSize = 2

type FeelinRuntime struct {
	pool               chan *Runner
	activeRunnersCount int32
	activeRunnersMu    *sync.Mutex
}

func NewFeelinRuntime() *FeelinRuntime {
	if maxVmPoolSize < minVmPoolSize {
		panic("vm pool min size is smaller than vm pool max size")
	}

	runtime := FeelinRuntime{
		pool:               make(chan *Runner, maxVmPoolSize),
		activeRunnersCount: 0,
		activeRunnersMu:    &sync.Mutex{},
	}

	//start min amount of runners
	for i := 0; i < minVmPoolSize; i++ {
		runtime.activeRunnersMu.Lock()
		runtime.pool <- newRunner()
		runtime.activeRunnersCount++
		runtime.activeRunnersMu.Unlock()
	}

	//cleanup runners every 10 minutes
	//should clean runners only when they are not being used
	go func() {
		time.Sleep(10 * time.Minute)
		if len(runtime.pool) > minVmPoolSize {
			for i := minVmPoolSize; i < len(runtime.pool); {
				runtime.activeRunnersMu.Lock()
				<-runtime.pool
				runtime.activeRunnersCount--
				runtime.activeRunnersMu.Unlock()
			}
		}
	}()

	return &runtime
}

func (r *FeelinRuntime) getRunnerFromPool() *Runner {
	var runner *Runner
	select {
	case runner = <-r.pool:
	default:
		r.activeRunnersMu.Lock()
		if r.activeRunnersCount < maxVmPoolSize {
			runner = newRunner()
			r.activeRunnersCount++
		}
		r.activeRunnersMu.Unlock()
		if runner == nil {
			runner = <-r.pool
		}
	}
	return runner
}

func (r *FeelinRuntime) returnRunnerToPool(runner *Runner) {
	select {
	case r.pool <- runner:
	default:
		//delete runner if pool is full
		r.activeRunnersMu.Lock()
		r.activeRunnersCount--
		r.activeRunnersMu.Unlock()
	}
}

func (r *FeelinRuntime) UnaryTest(expression string, variableContext map[string]any) (bool, error) {
	var runner = r.getRunnerFromPool()
	defer r.returnRunnerToPool(runner)

	return (*runner.unaryTest)(expression, variableContext)
}

func (r *FeelinRuntime) Evaluate(expression string, variableContext map[string]any) (any, error) {
	var runner = r.getRunnerFromPool()
	defer r.returnRunnerToPool(runner)

	return (*runner.evalFunc)(expression, variableContext)
}
