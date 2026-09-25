package bpmn

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pbinitiative/zenbpm/internal/safego"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const reconciliationWakeBufferSize = 64

type reconciliationManager struct {
	engine      *Engine
	interval    time.Duration
	gracePeriod time.Duration
	batchSize   int64

	ctx    context.Context
	cancel context.CancelFunc
	wakeCh chan int64
	wg     sync.WaitGroup
	cursor int64
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
		wakeCh:      make(chan int64, reconciliationWakeBufferSize),
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
}

func (manager *reconciliationManager) wake(processInstanceKey int64) {
	select {
	case manager.wakeCh <- processInstanceKey:
	default:
	}
}

func (manager *reconciliationManager) run() {
	var ticks <-chan time.Time
	if manager.interval > 0 {
		ticker := time.NewTicker(manager.interval)
		defer ticker.Stop()
		ticks = ticker.C
	}

	for {
		select {
		case <-manager.ctx.Done():
			return
		case processInstanceKey := <-manager.wakeCh:
			manager.resume(processInstanceKey)
		case <-ticks:
			manager.reconcileNextBatch()
		}
	}
}

func (manager *reconciliationManager) reconcileNextBatch() {
	started := time.Now()
	defer func() { manager.engine.recordReconciliationScanDuration(manager.ctx, time.Since(started)) }()
	runningBefore := time.Now().Add(-manager.gracePeriod)
	tokens, err := manager.engine.persistence.FindRecoverableRunningTokens(manager.ctx, manager.cursor, runningBefore, manager.batchSize)
	if err != nil {
		manager.engine.recordReconciliationFailure(manager.ctx, "scan")
		manager.engine.logger.Error("failed to scan recoverable running tokens", "afterTokenKey", manager.cursor, "runningBefore", runningBefore, "limit", manager.batchSize, "err", err)
		return
	}
	if len(tokens) == 0 && manager.cursor != 0 {
		manager.cursor = 0
		tokens, err = manager.engine.persistence.FindRecoverableRunningTokens(manager.ctx, manager.cursor, runningBefore, manager.batchSize)
		if err != nil {
			manager.engine.recordReconciliationFailure(manager.ctx, "scan")
			manager.engine.logger.Error("failed to restart recoverable running token scan", "runningBefore", runningBefore, "limit", manager.batchSize, "err", err)
			return
		}
	}
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

func (manager *reconciliationManager) resume(processInstanceKey int64) {
	recovered, err := manager.engine.tryResumeProcessInstanceByKey(manager.ctx, processInstanceKey)
	if err != nil {
		manager.engine.recordReconciliationFailure(manager.ctx, "resume")
		manager.engine.logger.Error("failed to recover running process instance", "processInstance", processInstanceKey, "err", err)
	} else if recovered {
		manager.engine.recordReconciliationRecovery(manager.ctx)
		manager.engine.logger.Info("recovered running process instance", "processInstance", processInstanceKey)
	}
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

func (engine *Engine) tryResumeProcessInstanceByKey(ctx context.Context, processInstanceKey int64) (bool, error) {
	if !engine.runningInstances.tryLockInstanceOnce(processInstanceKey) {
		return false, nil
	}
	defer engine.runningInstances.unlockInstance(processInstanceKey)
	outcome := &runProcessInstanceOutcome{}
	err := engine.resumeProcessInstanceLockedByKey(ctx, processInstanceKey, outcome)
	return outcome.resumedRunningTokens, err
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
// wakeReconciliation holds the read lock through the non-blocking delivery, so once this
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
