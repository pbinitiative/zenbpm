package bpmn

import (
	"context"
	"math/rand"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
)

type timeManagerTester struct {
	mu              sync.Mutex
	generatedTimers []runtime.Timer
	processedTimers []runtime.Timer
}

func (t *timeManagerTester) generateTimers(ctx context.Context, end time.Time) ([]runtime.Timer, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	timersToGenerate := int64(3)
	now := time.Now()
	diff := int64(now.Sub(end))
	timers := make([]runtime.Timer, timersToGenerate)
	for i := range timersToGenerate {
		timerDuration := time.Duration((diff / timersToGenerate) * i)
		timers[i] = runtime.Timer{
			Key:       rand.Int63(),
			CreatedAt: now,
			DueAt:     now.Add(timerDuration),
			Duration:  timerDuration,
		}
	}
	t.generatedTimers = append(t.generatedTimers, timers...)
	return timers, nil
}

func (t *timeManagerTester) processTimer(ctx context.Context, timer runtime.Timer) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.processedTimers = append(t.processedTimers, timer)
}

func TestTimerManagerLoadsAndFiresTimers(t *testing.T) {
	tester := timeManagerTester{
		generatedTimers: []runtime.Timer{},
		processedTimers: []runtime.Timer{},
		mu:              sync.Mutex{},
	}
	tm := newTimerManager(tester.processTimer, tester.generateTimers, 500*time.Millisecond)
	tm.start()
	defer tm.stop()
	now := time.Now()
	assert.Eventually(t, func() bool {
		if len(tester.generatedTimers) > 0 {
			now = time.Now()
			return true
		}
		return false
	}, 2*time.Second, 100*time.Millisecond, "timers should be generated in time")

	assert.Eventually(t, func() bool {
		for _, timerToFire := range tester.generatedTimers {
			if timerToFire.DueAt.After(now) {
				continue
			}
			if timerToFire.DueAt.Before(now) {
				if !slices.Contains(tester.processedTimers, timerToFire) {
					return false
				}
			}
		}
		return true
	}, 2*time.Second, 100*time.Millisecond, "processed timers did not contain timer that should be fired")

	// verify that timers that should have fired fired
	assert.NotEmpty(t, tester.generatedTimers)
	assert.NotEmpty(t, tester.processedTimers)
	for _, timerToFire := range tester.generatedTimers {
		if timerToFire.DueAt.Before(now) {
			assert.Contains(t, tester.processedTimers, timerToFire, "processed timers did not contain timer that should be fired")
		}
	}
}

func TestTimerManagerIgnoresDuplicateTimers(t *testing.T) {
	tester := timeManagerTester{
		generatedTimers: []runtime.Timer{},
		processedTimers: []runtime.Timer{},
		mu:              sync.Mutex{},
	}
	tm := newTimerManager(
		tester.processTimer,
		func(ctx context.Context, end time.Time) ([]runtime.Timer, error) { return nil, nil },
		1*time.Second,
	)
	tm.start()
	defer tm.stop()
	duplicate := runtime.Timer{
		ElementId:            "1",
		Key:                  rand.Int63(),
		ProcessDefinitionKey: rand.Int63(),
		ProcessInstanceKey:   rand.Int63(),
		TimerState:           runtime.TimerStateCreated,
		CreatedAt:            time.Now().Add(-1 * time.Second),
		DueAt:                time.Now(),
		Duration:             2 * time.Second,
		Token: runtime.ExecutionToken{
			Key: rand.Int63(),
		},
	}
	tm.registerTimer(duplicate)
	tm.registerTimer(duplicate)
	time.Sleep(2 * time.Second)

	// verify that the timer fired exactly once
	assert.NotEmpty(t, tester.processedTimers)
	count := 0
	for _, timerToFire := range tester.processedTimers {
		if timerToFire.EqualTo(duplicate) {
			count++
		}
	}
	assert.Equal(t, 1, count, "Duplicate timer should fire exactly once")
}

func TestTimerManagerFiresHistoricTimers(t *testing.T) {
	tester := timeManagerTester{
		generatedTimers: []runtime.Timer{},
		processedTimers: []runtime.Timer{},
		mu:              sync.Mutex{},
	}
	tm := newTimerManager(
		tester.processTimer,
		func(ctx context.Context, end time.Time) ([]runtime.Timer, error) { return nil, nil },
		1*time.Second,
	)
	tm.start()
	defer tm.stop()
	timer := runtime.Timer{
		ElementId:            "1",
		Key:                  rand.Int63(),
		ProcessDefinitionKey: rand.Int63(),
		ProcessInstanceKey:   rand.Int63(),
		TimerState:           runtime.TimerStateCreated,
		CreatedAt:            time.Now().Add(-24 * time.Hour),
		DueAt:                time.Now().Add(-12 * time.Hour),
		Duration:             2 * time.Second,
		Token: runtime.ExecutionToken{
			Key: rand.Int63(),
		},
	}
	tm.registerTimer(timer)
	time.Sleep(2 * time.Second)

	// verify that the timer fired exactly once
	assert.NotEmpty(t, tester.processedTimers)
	count := 0
	for _, timerToFire := range tester.processedTimers {
		if timerToFire.EqualTo(timer) {
			count++
		}
	}
	assert.Equal(t, 1, count, "Historic timer should fire exactly once")
}

func TestTimerManagerRemovesWaitingTimerBeforeProcessing(t *testing.T) {
	var (
		mu       sync.Mutex
		attempts int
		handled  int
	)
	timer := runtime.Timer{
		Key:   rand.Int63(),
		DueAt: time.Now().Add(-1 * time.Second),
	}

	tm := newTimerManager(
		func(ctx context.Context, fired runtime.Timer) {
			mu.Lock()
			defer mu.Unlock()
			attempts++
			if attempts == 1 {
				panic("boom")
			}
			handled++
		},
		func(ctx context.Context, end time.Time) ([]runtime.Timer, error) { return nil, nil },
		10*time.Minute,
	)
	tm.start()
	defer tm.stop()

	tm.addWaitingTimer(timer)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return attempts == 1
	}, 2*time.Second, 50*time.Millisecond, "first processing attempt should panic")

	tm.addWaitingTimer(timer)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return attempts == 2 && handled == 1
	}, 2*time.Second, 50*time.Millisecond, "timer should be processed again after panic")
}

func TestTimerManagerRemoveTimerLeavesOthersWaiting(t *testing.T) {
	tm := newTimerManager(
		func(ctx context.Context, fired runtime.Timer) {},
		func(ctx context.Context, end time.Time) ([]runtime.Timer, error) { return nil, nil },
		10*time.Minute,
	)
	tm.start()
	defer tm.stop()

	toRemove := runtime.Timer{Key: rand.Int63(), DueAt: time.Now().Add(1 * time.Hour)}
	toKeep := runtime.Timer{Key: rand.Int63(), DueAt: time.Now().Add(1 * time.Hour)}

	tm.addWaitingTimer(toRemove)
	tm.addWaitingTimer(toKeep)

	tm.removeTimer(toRemove)

	tm.mu.RLock()
	defer tm.mu.RUnlock()
	if assert.Len(t, tm.waitingTimers, 1, "only the targeted timer should be removed") {
		kept := tm.waitingTimers[0]
		assert.Equal(t, toKeep.Key, kept.timer.Key)
		assert.NoError(t, kept.ctx.Err(), "kept timer's context must not be cancelled by unrelated removal")
	}
}

func TestTimerManagerFiringOneDoesNotCancelOthers(t *testing.T) {
	var (
		mu    sync.Mutex
		fired []int64
	)
	tm := newTimerManager(
		func(ctx context.Context, timer runtime.Timer) {
			mu.Lock()
			defer mu.Unlock()
			fired = append(fired, timer.Key)
		},
		func(ctx context.Context, end time.Time) ([]runtime.Timer, error) { return nil, nil },
		10*time.Minute,
	)
	tm.start()
	defer tm.stop()

	first := runtime.Timer{Key: rand.Int63(), DueAt: time.Now().Add(50 * time.Millisecond)}
	second := runtime.Timer{Key: rand.Int63(), DueAt: time.Now().Add(400 * time.Millisecond)}

	tm.addWaitingTimer(first)
	tm.addWaitingTimer(second)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return slices.Contains(fired, first.Key) && slices.Contains(fired, second.Key)
	}, 2*time.Second, 20*time.Millisecond, "both timers should fire — removing the first must not cancel the second")
}

func TestTimerManagerPanicDoesNotStopOtherTimers(t *testing.T) {
	panicKey := rand.Int63()
	normalKeys := []int64{rand.Int63(), rand.Int63(), rand.Int63()}

	var (
		mu         sync.Mutex
		panicCount int
		handled    []int64
	)
	tm := newTimerManager(
		func(ctx context.Context, timer runtime.Timer) {
			mu.Lock()
			defer mu.Unlock()
			if timer.Key == panicKey {
				panicCount++
				panic("boom")
			}
			handled = append(handled, timer.Key)
		},
		func(ctx context.Context, end time.Time) ([]runtime.Timer, error) { return nil, nil },
		10*time.Minute,
	)
	tm.start()
	defer tm.stop()

	now := time.Now()
	// panicking timer fires in the middle, sandwiched between normal ones, to
	// prove the loop keeps processing both before and after the panic.
	tm.addWaitingTimer(runtime.Timer{Key: normalKeys[0], DueAt: now.Add(50 * time.Millisecond)})
	tm.addWaitingTimer(runtime.Timer{Key: panicKey, DueAt: now.Add(150 * time.Millisecond)})
	tm.addWaitingTimer(runtime.Timer{Key: normalKeys[1], DueAt: now.Add(500 * time.Millisecond)})
	tm.addWaitingTimer(runtime.Timer{Key: normalKeys[2], DueAt: now.Add(850 * time.Millisecond)})

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return panicCount == 1 && len(handled) == len(normalKeys)
	}, 3*time.Second, 20*time.Millisecond, "every normal timer must fire exactly once despite the panic")

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, panicCount, "panicking timer should fire exactly once")
	for _, k := range normalKeys {
		assert.Equal(t, 1, countOccurrences(handled, k), "normal timer %d should be processed exactly once", k)
	}
}

func countOccurrences(s []int64, v int64) int {
	n := 0
	for _, x := range s {
		if x == v {
			n++
		}
	}
	return n
}
