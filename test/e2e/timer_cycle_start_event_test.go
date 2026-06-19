package e2e

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/pbinitiative/zenbpm/pkg/zenflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const timerCycleSimpleBpmnPath = "../../pkg/bpmn/test-cases/process_definition_start_event/timer-cycle-start-event-simple.bpmn"

// TestTimerCycleStartEvent_ISORepeatingInterval verifies that the engine correctly handles
// an ISO 8601 timeCycle of the form `R2/<startDateTime>/PT1S` on a definition-level timer
// start event. It expects exactly 2 process instances to be created (one per cycle iteration),
// both ending up in the Active state (waiting on the service task), and exactly 2 triggered
// definition-level timers.
func TestTimerCycleStartEvent_ISORepeatingInterval(t *testing.T) {
	// Anchor the start to a near-future moment (today's date) so the engine schedules the first
	// firing at `start` and the second at `start+PT1S` — both within a few seconds.
	// If `start` were in the past at scheduling time the engine's nextDueAt loop would consume
	// repetitions just catching up to `now`, exhausting R2 before any process instance is created.
	startTime := time.Now().Add(1 * time.Second).UTC().Format(time.RFC3339)
	timerCycle := fmt.Sprintf("R2/%s/PT1S", startTime)
	uniqueProcessId := fmt.Sprintf("timer-cycle-iso-%d", time.Now().UnixNano())

	parentInstances, definitionKey := deployTimerCycleSimpleAndWaitFor2Instances(t, timerCycle, uniqueProcessId)

	// All instances must be Active (blocked on the service task job).
	assertAllInstancesActive(t, parentInstances)

	// The R2 cycle must produce exactly 2 triggered definition-level timers.
	assertDefinitionLevelTriggeredTimers(t, parentInstances[0].Key, definitionKey, 2)
}

// TestTimerCycleStartEvent_CronEverySecond verifies the engine handles a cron-style timeCycle
// (`* * * * * *` — every second). The cron expression itself is open-ended, so the test
// deactivates (cancels) the next-armed definition-level timer as soon as the 2nd firing is
// observed, ensuring a 3rd instance is never created. After deactivation it asserts both
// instances are Active and exactly 2 triggered definition-level timers exist.
func TestTimerCycleStartEvent_CronEverySecond(t *testing.T) {
	timerCycle := "* * * * * *" // 6 fields: seconds minute hour day month dow — every second
	uniqueProcessId := fmt.Sprintf("timer-cycle-cron-%d", time.Now().UnixNano())

	parentInstances, definitionKey := deployTimerCycleSimpleAndWaitFor2Instances(t, timerCycle, uniqueProcessId)

	// Cancel any Created definition-level timer for this definition so the cron does not fire again.
	store := mustGetPartitionStore(t, parentInstances[0].Key)
	cancelCreatedDefinitionLevelTimers(t, store, definitionKey)

	// After cancellation, the instance count must remain at 2 (no further firings).
	assertProcessInstanceCountNeverExceeds(t, uniqueProcessId, store, definitionKey, 2, 3*time.Second, 100*time.Millisecond)

	// Re-read instances after the stabilization window above and assert state/count.
	parentInstances = listParentInstances(t, uniqueProcessId)
	require.Equal(t, 2, len(parentInstances), "exactly 2 process instances expected after cron deactivation")
	assertAllInstancesActive(t, parentInstances)

	assertDefinitionLevelTriggeredTimers(t, parentInstances[0].Key, definitionKey, 2)
}

// deployTimerCycleSimpleAndWaitFor2Instances deploys timer-cycle-start-event-simple.bpmn with the
// supplied timeCycle and unique process id, then waits for exactly 2 process instances to be created.
func deployTimerCycleSimpleAndWaitFor2Instances(t *testing.T, timerCycle, uniqueProcessId string) ([]zenclient.ProcessInstancesSimple, int64) {
	t.Helper()

	bpmnData, err := os.ReadFile(timerCycleSimpleBpmnPath)
	require.NoError(t, err)

	modifiedBpmn := []byte(strings.NewReplacer(
		"__TIMER_CYCLE_PLACEHOLDER__", timerCycle,
		"timer-cycle-start-event-simple", uniqueProcessId,
	).Replace(string(bpmnData)))

	deployResp, err := deployDefinitionFromBytes(t, modifiedBpmn, "process_definition_start_event/timer-cycle-start-event-simple.bpmn")
	require.NoError(t, err)

	var definitionKey int64
	if deployResp.JSON201 != nil {
		definitionKey = deployResp.JSON201.ProcessDefinitionKey
	} else {
		require.NotNil(t, deployResp.JSON200, "expected either 200 or 201 response from deploy")
		definitionKey = deployResp.JSON200.ProcessDefinitionKey
	}

	var parentInstances []zenclient.ProcessInstancesSimple
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		page, errGet := app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
			BpmnProcessId: &uniqueProcessId,
		})
		if !assert.NoError(collect, errGet) || !assert.NotNil(collect, page) || !assert.NotNil(collect, page.JSON200) {
			return
		}
		// Accept >=2 here to keep the cron test snappy; callers can still re-list after.
		if !assert.GreaterOrEqual(collect, page.JSON200.TotalCount, 2,
			"expected at least 2 process instances, got %d", page.JSON200.TotalCount) {
			return
		}
		parentInstances = parentInstances[:0]
		for _, p := range page.JSON200.Partitions {
			parentInstances = append(parentInstances, p.Items...)
		}
	}, 20*time.Second, 50*time.Millisecond, "timer cycle %q should create at least 2 process instances", timerCycle)

	return parentInstances, definitionKey
}

func listParentInstances(t *testing.T, bpmnProcessId string) []zenclient.ProcessInstancesSimple {
	t.Helper()
	page, err := app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
		BpmnProcessId: &bpmnProcessId,
	})
	require.NoError(t, err)
	require.NotNil(t, page.JSON200)
	out := make([]zenclient.ProcessInstancesSimple, 0, page.JSON200.TotalCount)
	for _, p := range page.JSON200.Partitions {
		out = append(out, p.Items...)
	}
	return out
}

func assertAllInstancesActive(t *testing.T, instances []zenclient.ProcessInstancesSimple) {
	t.Helper()
	for _, inst := range instances {
		assert.Equal(t, zenclient.ProcessInstanceStateActive, inst.State,
			"process instance %d should be Active (blocked on service task)", inst.Key)
	}
}

func mustGetPartitionStore(t *testing.T, instanceKey int64) storage.Storage {
	t.Helper()
	store, err := app.node.GetPartitionStore(t.Context(), zenflake.GetPartitionId(instanceKey))
	require.NoError(t, err)
	return store
}

// cancelCreatedDefinitionLevelTimers marks every Created definition-level timer (ProcessInstanceKey == nil)
// for the given process definition as Cancelled so the cycle stops firing.
func cancelCreatedDefinitionLevelTimers(t *testing.T, store storage.Storage, definitionKey int64) {
	t.Helper()
	require.NoError(t, cancelCreatedDefinitionLevelTimersWithContext(t.Context(), store, definitionKey))
}

func cancelCreatedDefinitionLevelTimersWithContext(ctx context.Context, store storage.Storage, definitionKey int64) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	created, err := store.FindProcessDefinitionTimers(ctx, definitionKey, bpmnruntime.TimerStateCreated)
	if err != nil {
		return err
	}
	for _, timer := range filterDefinitionLevelTimers(created) {
		timer.TimerState = bpmnruntime.TimerStateCancelled
		if err := store.SaveTimer(ctx, timer); err != nil {
			return err
		}
	}
	return nil
}

func assertProcessInstanceCountNeverExceeds(
	t *testing.T,
	bpmnProcessId string,
	store storage.Storage,
	definitionKey int64,
	maxCount int,
	waitFor time.Duration,
	tick time.Duration,
) {
	t.Helper()

	deadline := time.NewTimer(waitFor)
	defer deadline.Stop()
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		select {
		case <-deadline.C:
			return
		case <-ticker.C:
			page, err := app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
				BpmnProcessId: &bpmnProcessId,
			})
			require.NoError(t, err)
			require.NotNil(t, page)
			require.NotNil(t, page.JSON200)
			require.LessOrEqual(t, page.JSON200.TotalCount, maxCount,
				"cron cycle should be deactivated; no 3rd process instance should appear")

			// In case the engine managed to schedule another timer after our cancel, sweep again.
			require.NoError(t, cancelCreatedDefinitionLevelTimersWithContext(t.Context(), store, definitionKey))
		}
	}
}

// assertDefinitionLevelTriggeredTimers asserts that exactly expectedCount triggered
// definition-level timers exist for the given process definition. The partition is looked up
// via a known process instance key from that partition.
func assertDefinitionLevelTriggeredTimers(t *testing.T, anyInstanceKey, definitionKey int64, expectedCount int) {
	t.Helper()
	store := mustGetPartitionStore(t, anyInstanceKey)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		triggered, err := store.FindProcessDefinitionTimers(t.Context(), definitionKey, bpmnruntime.TimerStateTriggered)
		if !assert.NoError(collect, err) {
			return
		}
		defLevel := filterDefinitionLevelTimers(triggered)
		assert.Equal(collect, expectedCount, len(defLevel),
			"expected %d triggered definition-level timers, got %d (all triggered: %d)", expectedCount, len(defLevel), len(triggered))
	}, 10*time.Second, 100*time.Millisecond,
		"expected exactly %d triggered definition-level timers for definition %d", expectedCount, definitionKey)
}
