package bpmn

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
)

// processTimerFunc should asynchronously execute the timer and continue processing the process instance
type processTimerFunc func(ctx context.Context, timer runtime.Timer)

// pollTimerFunc must return an array of timers that are in active state and should fire before end
// timerManager does the de-duplication of already running timers and timers returned by this function
type pollTimerFunc func(ctx context.Context, end time.Time) ([]runtime.Timer, error)

type waitingTimer struct {
	ctx    context.Context
	cancel context.CancelFunc
	timer  runtime.Timer
}

type timerManager struct {
	pollTimerDelay   time.Duration
	nextPoll         time.Time
	mu               *sync.RWMutex
	ctx              context.Context
	ctxCancelFunc    context.CancelFunc
	ch               chan runtime.Timer
	logger           hclog.Logger
	processTimerFunc processTimerFunc
	pollTimerFunc    pollTimerFunc
	waitingTimers    []waitingTimer
}

func newTimerManager(processTimerFunc processTimerFunc, pollTimeFunc pollTimerFunc, pollTimerDelay time.Duration) *timerManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &timerManager{
		ctx:              ctx,
		pollTimerDelay:   pollTimerDelay,
		ctxCancelFunc:    cancel,
		mu:               &sync.RWMutex{},
		ch:               make(chan runtime.Timer),
		pollTimerFunc:    pollTimeFunc,
		processTimerFunc: processTimerFunc,
		logger:           hclog.Default().Named("timer-manager"),
	}
}

// registerTimer will register the time if its due date is in the current cycle
func (tm *timerManager) registerTimer(timer runtime.Timer) {
	if timer.DueAt.After(tm.nextPoll) {
		return
	}
	tm.addWaitingTimer(timer)
}

func (tm *timerManager) removeTimer(timer runtime.Timer) {
	// most of the time the timer will be waiting in DB not yet loaded so we just Rlock here to not block other reads
	tm.mu.RLock()
	remove := false
	for _, wt := range tm.waitingTimers {
		if wt.timer.Key == timer.Key {
			remove = true
			break
		}
	}
	tm.mu.RUnlock()
	if !remove {
		return
	}

	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.waitingTimers = slices.DeleteFunc(tm.waitingTimers, func(t waitingTimer) bool {
		t.cancel()
		return t.timer.Key == timer.Key
	})
}

func (tm *timerManager) run() {
	pollTicker := time.NewTicker(tm.pollTimerDelay)
	for {
		select {
		case <-tm.ctx.Done():
			close(tm.ch)
			return
		case timer := <-tm.ch:
			// process timer firing
			if timer.Key == 0 {
				// receive from closed channel
				return
			}
			tm.processTimerFunc(context.Background(), timer)
			tm.mu.Lock()
			tm.waitingTimers = slices.DeleteFunc(tm.waitingTimers, func(item waitingTimer) bool {
				return item.timer.Key == timer.Key
			})
			tm.mu.Unlock()
		case t := <-pollTicker.C:
			nextPoll := t.Add(tm.pollTimerDelay)
			toFireTimers, err := tm.pollTimerFunc(tm.ctx, nextPoll)
			if err != nil {
				tm.logger.Error(fmt.Sprintf("Failed to poll timers for processing: %s", err))
				continue
			}
			for _, tft := range toFireTimers {
				tm.addWaitingTimer(tft)
			}
			tm.nextPoll = t.Add(tm.pollTimerDelay)
		}
	}
}

func (tm *timerManager) addWaitingTimer(tft runtime.Timer) {
	tm.mu.RLock()
	for _, wt := range tm.waitingTimers {
		if wt.timer.Key == tft.Key {
			tm.mu.RUnlock()
			return
		}
	}
	tm.mu.RUnlock()
	tm.mu.Lock()
	defer tm.mu.Unlock()
	timerCtx, timerCancel := context.WithCancel(context.Background())
	tm.waitingTimers = append(tm.waitingTimers, waitingTimer{
		ctx:    timerCtx,
		cancel: timerCancel,
		timer:  tft,
	})
	go func() {
		t := time.NewTimer(time.Until(tft.DueAt))
		select {
		case <-t.C:
			tm.ch <- tft
		case <-timerCtx.Done():
			return
		case <-tm.ctx.Done():
			close(tm.ch)
		}
	}()
}

func (tm *timerManager) start() {
	tm.nextPoll = time.Now().Add(tm.pollTimerDelay)
	go tm.run()
}

func (tm *timerManager) stop() {
	tm.ctxCancelFunc()
}
