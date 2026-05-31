package bpmn

import (
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateInstance_ManualStart_OnEventDrivenStartEvent_WithFailingEventSubprocessSubscription
// covers two related fixes:
//
//  1. A process whose only definition-level start event has an event definition (here: a message
//     start event) MUST still be startable manually via CreateInstanceByKey. The engine falls
//     back to creating an execution token on the first event-driven start event.
//
//  2. Failures while creating subscriptions for nested event subprocess start events (e.g. an
//     unresolvable correlation key expression because the variable is not in scope at instance
//     creation time) MUST NOT abort the parent process instance creation. Instead an incident is
//     recorded against the failing event subprocess start event so the parent can keep running.
func TestCreateInstance_ManualStart_OnEventDrivenStartEvent_WithFailingEventSubprocessSubscription(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	require.NoError(t, engine.Start(t.Context()))
	defer engine.Stop()

	// Keep the service-task pinned in Active so the instance does not race to completion before assertions.
	h := engine.NewTaskHandler().
		Type("input-task-message-event-subprocess-interrupting").
		Handler(func(job ActivatedJob) {
			// intentionally left blank
		})
	defer engine.RemoveHandler(h)

	def, err := engine.LoadFromFile(t.Context(),
		"./test-cases/process_definition_start_event/message-start-event-same-message-for-instance-and-subprocess.bpmn")
	require.NoError(t, err)

	// Manually create an instance — the process has only event-driven start events at the
	// definition level (a message start event) and an event subprocess whose message start
	// event has a correlation key expression `=key` that cannot be evaluated without `key`
	// being provided. Before the fix this errored with
	//   "failed to evaluate message correlation key: result of correlation key evaluation is not a string: %!w(<nil>)"
	instance, err := engine.CreateInstanceByKey(t.Context(), def.Key, nil)
	require.NoError(t, err, "process instance must be creatable even when an event subprocess subscription cannot be created")
	require.NotNil(t, instance)

	piKey := instance.ProcessInstance().Key

	// The instance must be Active (or otherwise alive) — definitely not Failed/Terminated.
	var persisted runtime.ProcessInstance
	require.Eventually(t, func() bool {
		persisted, err = store.FindProcessInstanceByKey(t.Context(), piKey)
		if err != nil {
			return false
		}
		state := persisted.ProcessInstance().GetState()
		return state != runtime.ActivityStateFailed && state != runtime.ActivityStateTerminated
	}, 2*time.Second, 25*time.Millisecond,
		"parent process instance must not be marked Failed/Terminated because of an event subprocess subscription failure")

	// An incident must have been recorded against the failing event subprocess start event
	// (subProcessMessageEvent_12i3m6f) so the failure is visible/resolvable.
	var incidents []runtime.Incident
	require.Eventually(t, func() bool {
		incidents, err = store.FindIncidentsByProcessInstanceKey(t.Context(), piKey)
		return err == nil && len(incidents) == 1
	}, 2*time.Second, 25*time.Millisecond,
		"exactly one incident must be raised for the failing event subprocess subscription")
	require.Len(t, incidents, 1, "exactly one incident must be raised for the failing event subprocess subscription")
	assert.Equal(t, "subProcessMessageEvent_12i3m6f", incidents[0].ElementId,
		"incident must point at the event subprocess start event whose subscription failed")
	assert.Contains(t, incidents[0].Message, "correlation key",
		"incident message must describe the root cause (correlation key evaluation failure)")
	assert.NotContains(t, incidents[0].Message, "%!w(<nil>)",
		"incident message must not contain the broken %%!w(<nil>) formatting verb")
	assert.Nil(t, incidents[0].ResolvedAt, "incident must be unresolved")
	assert.Zero(t, incidents[0].Token.Key, "subscription-creation incidents are not tied to an execution token")

	// Make the previously missing correlation-key variable available and resolve the incident.
	// Resolving this tokenless incident must recreate the event-subprocess subscription without
	// persisting or running a bogus zero-value execution token.
	const correlationKey = "resolved-correlation-key"
	persisted.ProcessInstance().SetVariable("key", correlationKey)
	require.NoError(t, store.SaveProcessInstance(t.Context(), persisted))
	require.NoError(t, engine.ResolveIncident(t.Context(), incidents[0].Key))

	resolvedIncident, err := store.FindIncidentByKey(t.Context(), incidents[0].Key)
	require.NoError(t, err)
	require.NotNil(t, resolvedIncident.ResolvedAt, "incident must be marked resolved after subscription recreation")

	activeSubscriptions, err := store.FindProcessInstanceMessageSubscriptions(t.Context(), piKey, runtime.ActivityStateActive)
	require.NoError(t, err)
	require.Len(t, activeSubscriptions, 1, "resolving the incident must recreate the event subprocess subscription")
	instanceSubscription, ok := activeSubscriptions[0].(*runtime.InstanceMessageSubscription)
	require.True(t, ok, "event subprocess subscription must be tied to the process instance")
	assert.Equal(t, "subProcessMessageEvent_12i3m6f", instanceSubscription.ElementId)
	assert.Equal(t, correlationKey, instanceSubscription.CorrelationKey)

	tokens, err := store.GetAllTokensForProcessInstance(t.Context(), piKey)
	require.NoError(t, err)
	for _, token := range tokens {
		assert.NotZero(t, token.Key, "resolving a tokenless incident must not persist a zero-value execution token")
	}
	_, err = store.GetTokenByKey(t.Context(), 0)
	assert.Error(t, err, "resolving a tokenless incident must not save a token with key 0")

	// Publish the message on the event subprocess message start event using the correct message name and correlation key
	ck := correlationKey
	require.NoError(t,
		engine.PublishMessageByName(t.Context(), "globalMessageRef", &ck, nil),
		"publishing message %q with correlation key %q must succeed now that a matching active subscription exists",
		"globalMessageRef", correlationKey)

	// The interrupting event subprocess (start event → end event, no intermediate tasks) runs to completion and cancels the still-active service task,
	// The parent process instance must therefore reach Completed state.
	require.Eventually(t, func() bool {
		refreshed, fErr := store.FindProcessInstanceByKey(t.Context(), piKey)
		if fErr != nil {
			return false
		}
		return refreshed.ProcessInstance().GetState() == runtime.ActivityStateCompleted
	}, 2*time.Second, 25*time.Millisecond,
		"parent process instance must reach Completed state after the interrupting event subprocess fires")

	// Find the event subprocess child instance (a SubProcessInstance whose parent token
	// belongs to the parent process instance) and verify it is also Completed.
	parentTokens, err := store.GetAllTokensForProcessInstance(t.Context(), piKey)
	require.NoError(t, err)
	var (
		subprocInstance runtime.ProcessInstance
		foundSubproc    bool
	)
	for _, token := range parentTokens {
		children, childErr := store.FindProcessInstancesByParentExecutionTokenKey(t.Context(), token.Key)
		if childErr != nil {
			continue
		}
		for _, child := range children {
			if child.Type() == runtime.ProcessTypeSubProcess {
				subprocInstance = child
				foundSubproc = true
				break
			}
		}
		if foundSubproc {
			break
		}
	}
	require.True(t, foundSubproc, "an event subprocess child instance must have been created when the message was published")
	assert.Equal(t, runtime.ActivityStateCompleted, subprocInstance.ProcessInstance().GetState(),
		"event subprocess child instance must be in Completed state")

	// The event subprocess subscription must have been consumed — no Active subscriptions
	// may remain for this process instance after the interrupting fire.
	activeAfter, err := store.FindProcessInstanceMessageSubscriptions(t.Context(), piKey, runtime.ActivityStateActive)
	require.NoError(t, err)
	assert.Empty(t, activeAfter, "event subprocess subscription must be consumed after the message is published")

	// The event subprocess output mappings must have propagated their variables into the
	// parent process instance scope.
	completedInstance, err := store.FindProcessInstanceByKey(t.Context(), piKey)
	require.NoError(t, err)
	assert.Equal(t, "message-fired", completedInstance.ProcessInstance().GetVariable("subProcessResult"),
		"subProcessResult output variable must be mapped from the event subprocess")
	assert.Equal(t, true, completedInstance.ProcessInstance().GetVariable("messageInterrupted"),
		"messageInterrupted output variable must be mapped from the event subprocess")
	assert.Equal(t, "message-event-subprocess", completedInstance.ProcessInstance().GetVariable("interruptedBy"),
		"interruptedBy output variable must be mapped from the event subprocess")
}
