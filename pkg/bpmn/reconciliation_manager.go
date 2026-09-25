package bpmn

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pbinitiative/zenbpm/internal/safego"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	reconciliationBusyRetryDelay = 250 * time.Millisecond
	reconciliationMinRetryDelay  = 5 * time.Second
	reconciliationMaxRetryDelay  = 5 * time.Minute
)

type reconciliationRetry struct {
	processInstanceKey int64
	failures           uint8
	next               time.Time
	index              int
}

type reconciliationRetryHeap []*reconciliationRetry

func (retries reconciliationRetryHeap) Len() int { return len(retries) }
func (retries reconciliationRetryHeap) Less(i, j int) bool {
	return retries[i].next.Before(retries[j].next)
}
func (retries reconciliationRetryHeap) Swap(i, j int) {
	retries[i], retries[j] = retries[j], retries[i]
	retries[i].index = i
	retries[j].index = j
}
func (retries *reconciliationRetryHeap) Push(value any) {
	retry := value.(*reconciliationRetry)
	retry.index = len(*retries)
	*retries = append(*retries, retry)
}
func (retries *reconciliationRetryHeap) Pop() any {
	last := len(*retries) - 1
	retry := (*retries)[last]
	(*retries)[last] = nil
	*retries = (*retries)[:last]
	retry.index = -1
	return retry
}

type reconciliationManager struct {
	engine      *Engine
	interval    time.Duration
	gracePeriod time.Duration
	batchSize   int64

	ctx            context.Context
	cancel         context.CancelFunc
	wakeMu         sync.Mutex
	wakeQueue      []int64
	wakeQueued     map[int64]struct{}
	wakeSignal     chan struct{}
	retryByKey     map[int64]*reconciliationRetry
	retryQueue     reconciliationRetryHeap
	retryBaseDelay time.Duration
	retryMaxDelay  time.Duration
	scanFailures   uint8
	nextScan       time.Time
	wg             sync.WaitGroup
	cursor         int64
}

func newReconciliationManager(engine *Engine, interval time.Duration, gracePeriod time.Duration, batchSize int64) *reconciliationManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &reconciliationManager{
		engine:      engine,
		interval:    interval,
		gracePeriod: gracePeriod,
		batchSize:   batchSize,
		ctx:         ctx,
		cancel:      cancel,
		wakeQueued:  make(map[int64]struct{}),
		wakeSignal:  make(chan struct{}, 1),
		retryByKey:  make(map[int64]*reconciliationRetry),
	}
}

func (manager *reconciliationManager) start() {
	manager.wg.Add(1)
	safego.Go("reconciliation-manager", manager.engine.logger, func() {
		defer manager.wg.Done()
		manager.run()
	})
}

func (manager *reconciliationManager) stop() {
	manager.cancel()
	manager.wg.Wait()
	manager.wakeMu.Lock()
	manager.addWakeQueueDepth(-int64(len(manager.wakeQueue)))
	manager.wakeQueue = nil
	clear(manager.wakeQueued)
	manager.wakeMu.Unlock()
	manager.addRetryQueueDepth(-int64(len(manager.retryByKey)))
	clear(manager.retryByKey)
	manager.retryQueue = nil
}

func (manager *reconciliationManager) addWakeQueueDepth(delta int64) {
	if delta != 0 && manager.engine.metrics != nil && manager.engine.metrics.ReconciliationWakeQueueDepth != nil {
		manager.engine.metrics.ReconciliationWakeQueueDepth.Add(context.Background(), delta)
	}
}

func (manager *reconciliationManager) addRetryQueueDepth(delta int64) {
	if delta != 0 && manager.engine.metrics != nil && manager.engine.metrics.ReconciliationRetryQueueDepth != nil {
		manager.engine.metrics.ReconciliationRetryQueueDepth.Add(context.Background(), delta)
	}
}

func (manager *reconciliationManager) wake(processInstanceKey int64) {
	if manager.ctx.Err() != nil {
		return
	}
	manager.wakeMu.Lock()
	defer manager.wakeMu.Unlock()
	if manager.ctx.Err() != nil {
		return
	}
	if _, queued := manager.wakeQueued[processInstanceKey]; queued {
		return
	}
	manager.wakeQueued[processInstanceKey] = struct{}{}
	manager.wakeQueue = append(manager.wakeQueue, processInstanceKey)
	manager.addWakeQueueDepth(1)
	// The signal can be coalesced because the keys remain in wakeQueue.
	select {
	case manager.wakeSignal <- struct{}{}:
	default:
	}
}

func (manager *reconciliationManager) nextWake() (int64, bool) {
	manager.wakeMu.Lock()
	defer manager.wakeMu.Unlock()
	if len(manager.wakeQueue) == 0 {
		return 0, false
	}
	processInstanceKey := manager.wakeQueue[0]
	manager.wakeQueue[0] = 0
	manager.wakeQueue = manager.wakeQueue[1:]
	delete(manager.wakeQueued, processInstanceKey)
	manager.addWakeQueueDepth(-1)
	if len(manager.wakeQueue) == 0 {
		manager.wakeQueue = nil
	} else {
		select {
		case manager.wakeSignal <- struct{}{}:
		default:
		}
	}
	return processInstanceKey, true
}

func reconciliationRetryDelay(base, maximum time.Duration, failures uint8) time.Duration {
	delay := base
	for attempt := uint8(1); attempt < failures && delay < maximum; attempt++ {
		if delay >= maximum/2 {
			return maximum
		}
		delay *= 2
	}
	return delay
}

func (manager *reconciliationManager) retryBounds() (time.Duration, time.Duration) {
	base := manager.retryBaseDelay
	if base <= 0 {
		base = max(reconciliationMinRetryDelay, manager.interval)
	}
	maximum := manager.retryMaxDelay
	if maximum <= 0 {
		maximum = max(reconciliationMaxRetryDelay, base)
	}
	return base, max(maximum, base)
}

// The retry state and heap are owned by the manager loop. A retry is removed
// after a successful attempt or a no-op (for example, an instance completed
// through another path), so resolved instances do not accumulate in memory.
func (manager *reconciliationManager) delayRetry(processInstanceKey int64) (time.Duration, uint8) {
	retry := manager.retryByKey[processInstanceKey]
	if retry == nil {
		retry = &reconciliationRetry{processInstanceKey: processInstanceKey, index: -1}
		manager.retryByKey[processInstanceKey] = retry
		manager.addRetryQueueDepth(1)
	}
	if retry.failures < 255 {
		retry.failures++
	}
	base, maximum := manager.retryBounds()
	delay := reconciliationRetryDelay(base, maximum, retry.failures)
	retry.next = time.Now().Add(delay)
	if retry.index < 0 {
		heap.Push(&manager.retryQueue, retry)
	} else {
		heap.Fix(&manager.retryQueue, retry.index)
	}
	return delay, retry.failures
}

func (manager *reconciliationManager) clearRetry(processInstanceKey int64) {
	retry := manager.retryByKey[processInstanceKey]
	if retry == nil {
		return
	}
	if retry.index >= 0 {
		heap.Remove(&manager.retryQueue, retry.index)
	}
	delete(manager.retryByKey, processInstanceKey)
	manager.addRetryQueueDepth(-1)
}

func (manager *reconciliationManager) delayScan() (time.Duration, uint8) {
	if manager.scanFailures < 255 {
		manager.scanFailures++
	}
	base, maximum := manager.retryBounds()
	delay := reconciliationRetryDelay(base, maximum, manager.scanFailures)
	manager.nextScan = time.Now().Add(delay)
	return delay, manager.scanFailures
}

func (manager *reconciliationManager) run() {
	var ticks <-chan time.Time
	if manager.interval > 0 {
		ticker := time.NewTicker(manager.interval)
		defer ticker.Stop()
		ticks = ticker.C
	}
	var retryTimer *time.Timer
	var retryTicks <-chan time.Time
	var technicalTimer *time.Timer
	var technicalTicks <-chan time.Time
	var busyKeys []int64
	busySet := make(map[int64]struct{})
	defer func() {
		if retryTimer != nil {
			retryTimer.Stop()
		}
		if technicalTimer != nil {
			technicalTimer.Stop()
		}
	}()

	for {
		if manager.ctx.Err() != nil {
			return
		}
		if len(manager.retryQueue) == 0 {
			if technicalTimer != nil {
				technicalTimer.Stop()
			}
			technicalTicks = nil
		} else {
			untilRetry := max(time.Until(manager.retryQueue[0].next), 0)
			if technicalTimer == nil {
				technicalTimer = time.NewTimer(untilRetry)
			} else {
				technicalTimer.Reset(untilRetry)
			}
			technicalTicks = technicalTimer.C
		}
		select {
		case <-manager.ctx.Done():
			return
		case <-manager.wakeSignal:
			if processInstanceKey, ok := manager.nextWake(); ok {
				if manager.resume(processInstanceKey) {
					if _, queued := busySet[processInstanceKey]; !queued {
						busySet[processInstanceKey] = struct{}{}
						busyKeys = append(busyKeys, processInstanceKey)
					}
					if retryTimer == nil {
						retryTimer = time.NewTimer(reconciliationBusyRetryDelay)
						retryTicks = retryTimer.C
					}
				}
			}
		case <-retryTicks:
			retryTimer = nil
			retryTicks = nil
			for _, processInstanceKey := range busyKeys {
				manager.wake(processInstanceKey)
			}
			busyKeys = nil
			clear(busySet)
		case <-technicalTicks:
			now := time.Now()
			for len(manager.retryQueue) > 0 && !manager.retryQueue[0].next.After(now) {
				retry := heap.Pop(&manager.retryQueue).(*reconciliationRetry)
				manager.wake(retry.processInstanceKey)
			}
		case <-ticks:
			manager.reconcileNextBatch()
		}
	}
}

func (manager *reconciliationManager) reconcileNextBatch() {
	if time.Now().Before(manager.nextScan) {
		return
	}
	started := time.Now()
	defer func() { manager.engine.recordReconciliationScanDuration(manager.ctx, time.Since(started)) }()
	runningBefore := time.Now().Add(-manager.gracePeriod)
	tokens, err := manager.engine.persistence.FindRecoverableRunningTokens(manager.ctx, manager.cursor, runningBefore, manager.batchSize)
	if err != nil {
		if manager.ctx.Err() != nil {
			return
		}
		manager.engine.recordReconciliationFailure(manager.ctx, "scan")
		delay, failures := manager.delayScan()
		manager.engine.logger.Error("failed to scan recoverable running tokens", "afterTokenKey", manager.cursor, "runningBefore", runningBefore, "limit", manager.batchSize, "err", err, "consecutiveFailures", failures, "retryAfter", delay)
		return
	}
	if len(tokens) == 0 && manager.cursor != 0 {
		manager.cursor = 0
		tokens, err = manager.engine.persistence.FindRecoverableRunningTokens(manager.ctx, manager.cursor, runningBefore, manager.batchSize)
		if err != nil {
			if manager.ctx.Err() != nil {
				return
			}
			manager.engine.recordReconciliationFailure(manager.ctx, "scan")
			delay, failures := manager.delayScan()
			manager.engine.logger.Error("failed to restart recoverable running token scan", "runningBefore", runningBefore, "limit", manager.batchSize, "err", err, "consecutiveFailures", failures, "retryAfter", delay)
			return
		}
	}
	manager.scanFailures = 0
	manager.nextScan = time.Time{}
	if len(tokens) == 0 {
		return
	}

	manager.cursor = tokens[len(tokens)-1].Key
	if int64(len(tokens)) < manager.batchSize {
		// A partial page proves that this scan reached the current end of the keyspace.
		// Wrap now so tokens below the cursor cannot be starved by new higher keys
		// arriving before the next reconciliation interval.
		manager.cursor = 0
	}
	for _, processInstanceKey := range distinctProcessInstanceKeys(tokens) {
		manager.resume(processInstanceKey)
	}
}

// resume reports a busy instance lock so explicit wakeups can be retried even
// when periodic scanning is disabled. Periodic scans may simply revisit it later.
func (manager *reconciliationManager) resume(processInstanceKey int64) bool {
	if retry := manager.retryByKey[processInstanceKey]; retry != nil && time.Now().Before(retry.next) {
		return false
	}
	outcome, acquired, err := manager.engine.tryResumeProcessInstanceByKey(manager.ctx, processInstanceKey)
	if !acquired {
		return true
	}
	if err != nil {
		if manager.ctx.Err() != nil {
			manager.clearRetry(processInstanceKey)
			return false
		}
		manager.engine.recordReconciliationFailure(manager.ctx, "resume")
		if !outcome.isPersistedIncidentOnly() {
			delay, failures := manager.delayRetry(processInstanceKey)
			manager.engine.logger.Error("failed to recover running process instance", "processInstance", processInstanceKey, "err", err, "consecutiveFailures", failures, "retryAfter", delay)
		} else {
			manager.clearRetry(processInstanceKey)
			manager.engine.logger.Error("failed to recover running process instance", "processInstance", processInstanceKey, "err", err)
		}
		return false
	}
	manager.clearRetry(processInstanceKey)
	if outcome.resumedRunningTokens {
		manager.engine.recordReconciliationRecovery(manager.ctx)
		manager.engine.logger.Info("recovered running process instance", "processInstance", processInstanceKey)
	}
	return false
}

func (engine *Engine) recordReconciliationRecovery(ctx context.Context) {
	if engine.metrics != nil && engine.metrics.ReconciliationRecoveries != nil {
		engine.metrics.ReconciliationRecoveries.Add(ctx, 1)
	}
}

func (engine *Engine) recordReconciliationFailure(ctx context.Context, operation string) {
	if ctx.Err() == nil && engine.metrics != nil && engine.metrics.ReconciliationFailures != nil {
		engine.metrics.ReconciliationFailures.Add(ctx, 1, metric.WithAttributes(attribute.String("operation", operation)))
	}
}

func (engine *Engine) recordReconciliationScanDuration(ctx context.Context, duration time.Duration) {
	if engine.metrics != nil && engine.metrics.ReconciliationScanDuration != nil {
		engine.metrics.ReconciliationScanDuration.Record(ctx, float64(duration)/float64(time.Millisecond))
	}
}

func (engine *Engine) reconciliationSettings() (time.Duration, time.Duration, int64) {
	interval := engine.reconciliationInterval
	if interval <= 0 {
		interval = defaultReconciliationInterval
	}
	gracePeriod := engine.reconciliationGracePeriod
	if gracePeriod <= 0 {
		gracePeriod = defaultReconciliationGracePeriod
	}
	batchSize := engine.reconciliationBatchSize
	if batchSize <= 0 {
		batchSize = defaultReconciliationBatchSize
	}
	return interval, gracePeriod, batchSize
}

func (engine *Engine) reconcileRunningTokensAtStartup(ctx context.Context) error {
	_, _, batchSize := engine.reconciliationSettings()

	var cursor int64
	for {
		tokens, err := engine.persistence.FindRunningTokensAfter(ctx, cursor, batchSize)
		if err != nil {
			engine.recordReconciliationFailure(ctx, "startup_scan")
			return fmt.Errorf("failed to load running tokens after key %d: %w", cursor, err)
		}
		if len(tokens) == 0 {
			return nil
		}

		for _, processInstanceKey := range distinctProcessInstanceKeys(tokens) {
			outcome := &runProcessInstanceOutcome{}
			if err := engine.resumeProcessInstanceByKey(ctx, processInstanceKey, outcome); err != nil {
				engine.recordReconciliationFailure(ctx, "startup_resume")
				engine.logger.Error("failed to recover running process instance at startup", "processInstance", processInstanceKey, "err", err)
			} else if outcome.resumedRunningTokens {
				engine.recordReconciliationRecovery(ctx)
				engine.logger.Info("recovered running process instance at startup", "processInstance", processInstanceKey)
			}
		}
		cursor = tokens[len(tokens)-1].Key
	}
}

func distinctProcessInstanceKeys(tokens []runtime.ExecutionToken) []int64 {
	keys := make([]int64, 0, len(tokens))
	seen := make(map[int64]struct{}, len(tokens))
	for _, token := range tokens {
		if _, exists := seen[token.ProcessInstanceKey]; exists {
			continue
		}
		seen[token.ProcessInstanceKey] = struct{}{}
		keys = append(keys, token.ProcessInstanceKey)
	}
	return keys
}

func (engine *Engine) resumeProcessInstanceByKey(ctx context.Context, processInstanceKey int64, outcome *runProcessInstanceOutcome) error {
	engine.runningInstances.lockInstance(processInstanceKey)
	defer engine.runningInstances.unlockInstance(processInstanceKey)
	return engine.resumeProcessInstanceLockedByKey(ctx, processInstanceKey, outcome)
}

func (engine *Engine) tryResumeProcessInstanceByKey(ctx context.Context, processInstanceKey int64) (outcome *runProcessInstanceOutcome, acquired bool, err error) {
	if !engine.runningInstances.tryLockInstanceOnce(processInstanceKey) {
		return nil, false, nil
	}
	defer engine.runningInstances.unlockInstance(processInstanceKey)
	outcome = &runProcessInstanceOutcome{}
	err = engine.resumeProcessInstanceLockedByKey(ctx, processInstanceKey, outcome)
	return outcome, true, err
}

func (engine *Engine) resumeProcessInstanceLockedByKey(ctx context.Context, processInstanceKey int64, outcome *runProcessInstanceOutcome) error {
	instance, err := engine.persistence.FindProcessInstanceByKey(ctx, processInstanceKey)
	if err != nil {
		outcome.recordTechnicalFailure()
		return fmt.Errorf("failed to load process instance %d for continuation: %w", processInstanceKey, err)
	}
	state := instance.ProcessInstance().State
	if state != runtime.ActivityStateReady && state != runtime.ActivityStateActive {
		return nil
	}

	activeTokens, err := engine.persistence.GetActiveTokensForProcessInstance(ctx, processInstanceKey)
	if err != nil {
		outcome.recordTechnicalFailure()
		return fmt.Errorf("failed to load active tokens for process instance %d: %w", processInstanceKey, err)
	}
	runningTokens := make([]runtime.ExecutionToken, 0, len(activeTokens))
	for _, token := range activeTokens {
		if token.State == runtime.TokenStateRunning {
			runningTokens = append(runningTokens, token)
		}
	}
	if len(runningTokens) == 0 {
		return nil
	}
	if _, complete := engine.persistence.(storage.CompleteProcessInstanceSnapshot); !complete {
		// Some storage implementations keep mutable fields outside the instance
		// snapshot (for example, the in-memory flow-node counter).
		if err := engine.persistence.RefreshProcessInstance(ctx, instance); err != nil {
			outcome.recordTechnicalFailure()
			return fmt.Errorf("failed to refresh process instance %d for continuation: %w", processInstanceKey, err)
		}
	}

	if err := engine.runProcessInstanceLocked(ctx, instance, runningTokens, outcome); err != nil {
		return err
	}
	if outcome != nil {
		outcome.resumedRunningTokens = true
	}
	return nil
}

func (engine *Engine) continuationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	continuationCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stopEngineCancellation := context.AfterFunc(engine.context, cancel)
	return continuationCtx, func() {
		stopEngineCancellation()
		cancel()
	}
}

// continueProcessInstanceAfterCommit resumes the durable state of a process instance instead of
// relying on the caller's pre-commit snapshot. Request cancellation must not strand persisted
// Running tokens, but engine shutdown still cancels the continuation through continuationContext.
func (engine *Engine) continueProcessInstanceAfterCommit(ctx context.Context, processInstanceKey int64) (*runProcessInstanceOutcome, error) {
	continuationCtx, cancelContinuation := engine.continuationContext(ctx)
	defer cancelContinuation()

	outcome := &runProcessInstanceOutcome{}
	err := engine.resumeProcessInstanceByKey(continuationCtx, processInstanceKey, outcome)
	engine.wakeReconciliationAfterContinuationFailure(processInstanceKey, outcome, err)
	return outcome, err
}

func (engine *Engine) wakeReconciliationAfterContinuationFailure(
	processInstanceKey int64,
	outcome *runProcessInstanceOutcome,
	err error,
) {
	if err != nil && !outcome.isPersistedIncidentOnly() {
		engine.wakeReconciliation(processInstanceKey)
	}
}

func (engine *Engine) wakeReconciliation(processInstanceKey int64) {
	engine.lifecycle.reconciliationMu.RLock()
	defer engine.lifecycle.reconciliationMu.RUnlock()

	if engine.lifecycle.reconciliationManager != nil {
		engine.lifecycle.reconciliationManager.wake(processInstanceKey)
	}
}

// swapReconciliationManager publishes replacement before returning the previous manager.
// wakeReconciliation holds the read lock through the non-blocking enqueue, so once this
// method returns no new wake can be sent to the previous manager.
func (engine *Engine) swapReconciliationManager(replacement *reconciliationManager) *reconciliationManager {
	engine.lifecycle.reconciliationMu.Lock()
	defer engine.lifecycle.reconciliationMu.Unlock()

	previous := engine.lifecycle.reconciliationManager
	engine.lifecycle.reconciliationManager = replacement
	return previous
}

func (engine *Engine) currentReconciliationManager() *reconciliationManager {
	engine.lifecycle.reconciliationMu.RLock()
	defer engine.lifecycle.reconciliationMu.RUnlock()
	return engine.lifecycle.reconciliationManager
}

func (engine *Engine) detachReconciliationManager(expected *reconciliationManager) *reconciliationManager {
	engine.lifecycle.reconciliationMu.Lock()
	defer engine.lifecycle.reconciliationMu.Unlock()

	if engine.lifecycle.reconciliationManager != expected {
		return nil
	}
	engine.lifecycle.reconciliationManager = nil
	return expected
}
