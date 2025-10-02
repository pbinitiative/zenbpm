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
