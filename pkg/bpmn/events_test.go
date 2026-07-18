package bpmn

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
)

func cleanUpMessageSubscriptions() {
	engineStorage.MessageSubscriptions = make(map[int64]runtime.MessageSubscription)
}

func TestCreatingAProcessSetsStateToACTIVE(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/message-intermediate-catch-event.bpmn")
	assert.NoError(t, err)

	// when
	pi, err := bpmnEngine.CreateInstance(t.Context(), process, nil)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateActive, pi.ProcessInstance().GetState(),
		"Since the BPMN contains an intermediate catch event, the process instance must be active and can't complete.")
}

func TestIntermediateCatchEventReceivedMessageCompletesTheInstance(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/message-intermediate-catch-event.bpmn")
	assert.NoError(t, err)
	pi, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	// when
	for _, message := range engineStorage.MessageSubscriptions {
		if message.MessageSubscription().Name == "globalMsgRef" {
			err = bpmnEngine.PublishMessage(t.Context(), message, nil)
			assert.NoError(t, err)
		}
	}

	// then
	instance, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), pi.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState())
}

func TestReceiveTaskOutputMappingUsesInputSnapshotFromSubscriptionCreation(t *testing.T) {
	cleanUpMessageSubscriptions()
	bpmnData, err := os.ReadFile("./test-cases/receive_task/receive-task-boundary-timer-interrupting.bpmn")
	if !assert.NoError(t, err) {
		return
	}

	modifiedBPMN := strings.Replace(string(bpmnData),
		`<bpmn:process id="Process_0anusn1"`,
		fmt.Sprintf(`<bpmn:process id="Process_0anusn1_%d"`, rand.Int63()),
		1,
	)
	modifiedBPMN = strings.Replace(modifiedBPMN,
		`<bpmn:receiveTask id="ReceiveTask_1efx577" name="ReceiveTask" messageRef="Message_3chd4fk">
      <bpmn:incoming>Flow_1xwsg77</bpmn:incoming>`,
		`<bpmn:receiveTask id="ReceiveTask_1efx577" name="ReceiveTask" messageRef="Message_3chd4fk">
      <bpmn:extensionElements>
        <zenbpm:ioMapping>
          <zenbpm:input source="=lateSource" target="localMapped" />
          <zenbpm:output source="=localMapped" target="result" />
        </zenbpm:ioMapping>
      </bpmn:extensionElements>
      <bpmn:incoming>Flow_1xwsg77</bpmn:incoming>`,
		1,
	)

	process, err := bpmnEngine.LoadFromBytes(t.Context(), []byte(modifiedBPMN), rand.Int63())
	if !assert.NoError(t, err) {
		return
	}
	pi, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, map[string]interface{}{})
	if !assert.NoError(t, err) {
		return
	}

	storedInstance, err := engineStorage.FindProcessInstanceByKey(t.Context(), pi.ProcessInstance().Key)
	if !assert.NoError(t, err) {
		return
	}
	storedInstance.ProcessInstance().VariableHolder.SetLocalVariable("lateSource", "changed-after-subscription")
	assert.NoError(t, engineStorage.SaveProcessInstance(t.Context(), storedInstance))

	var subscription runtime.MessageSubscription
	for _, message := range engineStorage.MessageSubscriptions {
		tokenSubscription, ok := message.(*runtime.TokenMessageSubscription)
		if ok && tokenSubscription.Name == "globalMsgRef" && tokenSubscription.ProcessInstanceKey == pi.ProcessInstance().Key {
			subscription = message
			break
		}
	}
	if !assert.NotNil(t, subscription) {
		return
	}

	err = bpmnEngine.PublishMessage(t.Context(), subscription, nil)
	assert.NoError(t, err)

	updatedInstance, err := engineStorage.FindProcessInstanceByKey(t.Context(), pi.ProcessInstance().Key)
	if !assert.NoError(t, err) {
		return
	}
	variables := updatedInstance.ProcessInstance().VariableHolder.LocalVariables()
	assert.Contains(t, variables, "result")
	assert.Nil(t, variables["result"], "receive task input mappings must be evaluated from the variables captured when the subscription was created")
}

func TestIntermediateMessageCatchEventCompletedAt(t *testing.T) {
	cleanUpMessageSubscriptions()
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/message-intermediate-catch-event.bpmn")
	assert.NoError(t, err)

	pi, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	before := time.Now()
	for _, message := range engineStorage.MessageSubscriptions {
		if message.MessageSubscription().Name == "globalMsgRef" {
			err = bpmnEngine.PublishMessage(t.Context(), message, nil)
			assert.NoError(t, err)
		}
	}
	after := time.Now()

	flowElements, err := bpmnEngine.persistence.GetFlowElementInstancesByProcessInstanceKey(t.Context(), pi.ProcessInstance().Key, true)
	assert.NoError(t, err)

	var catchEvent *runtime.FlowElementInstance
	for i := range flowElements {
		if flowElements[i].ElementId == "id-1" {
			catchEvent = &flowElements[i]
			break
		}
	}
	assert.NotNil(t, catchEvent, "intermediate message catch event 'id-1' should be in history")
	if catchEvent == nil {
		return
	}
	assert.NotNil(t, catchEvent.CompletedAt, "intermediate message catch event should have CompletedAt set after the message arrives")
	if catchEvent.CompletedAt != nil {
		assert.False(t, catchEvent.CompletedAt.Before(before), "CompletedAt should be >= publish start time (%v)", before)
		assert.False(t, catchEvent.CompletedAt.After(after), "CompletedAt should be <= publish end time (%v)", after)
	}
}

func TestIntermediateCatchEventACatchEventProducesAnActiveSubscription(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/message-intermediate-catch-event.bpmn")
	assert.NoError(t, err)
	pi, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	subscriptions := engineStorage.MessageSubscriptions
	var subscription runtime.MessageSubscription
	for _, sub := range subscriptions {
		if sub, ok := sub.(*runtime.TokenMessageSubscription); ok && sub.ProcessInstanceKey == pi.ProcessInstance().Key {
			subscription = sub
			break
		}
	}
	assert.NotNil(t, subscription)
	assert.Equal(t, "globalMsgRef", subscription.MessageSubscription().Name)
	assert.Equal(t, "id-1", subscription.MessageSubscription().ElementId)
	assert.Equal(t, runtime.ActivityStateActive, subscription.MessageSubscription().State)
}

func TestIntermediateCatchEventMultipleInstancesWithSameMessageAndKey(t *testing.T) {
	cleanUpMessageSubscriptions()
	engineStorage.Incidents = make(map[int64]runtime.Incident)

	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/message-intermediate-catch-event.bpmn")
	assert.NoError(t, err)
	pi1, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, map[string]interface{}{})
	assert.NoError(t, err)
	pi2, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, map[string]interface{}{})
	assert.Error(t, err)

	for _, message := range engineStorage.MessageSubscriptions {
		if message.MessageSubscription().Name == "globalMsgRef" {
			err := bpmnEngine.PublishMessage(t.Context(), message, nil)
			assert.NoError(t, err)
			err = bpmnEngine.PublishMessage(t.Context(), message, nil)
			assert.Error(t, err)
		}
	}

	pi1, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), pi1.ProcessInstance().Key)
	assert.NoError(t, err)
	pi2, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), pi2.ProcessInstance().Key)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateCompleted, pi1.ProcessInstance().GetState())
	assert.Equal(t, runtime.ActivityStateFailed, pi2.ProcessInstance().GetState())

	incidents, err := bpmnEngine.persistence.FindIncidentsByProcessInstanceKey(t.Context(), pi2.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(incidents))

	err = bpmnEngine.ResolveIncident(t.Context(), incidents[0].Key)
	assert.NoError(t, err)

	for _, message := range engineStorage.MessageSubscriptions {
		if message.MessageSubscription().Name == "globalMsgRef" && message.MessageSubscription().State == runtime.ActivityStateActive {
			err = bpmnEngine.PublishMessage(t.Context(), message, nil)
			assert.NoError(t, err)
		}
	}

	pi2, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), pi2.ProcessInstance().Key)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateCompleted, pi1.ProcessInstance().GetState())
	assert.Equal(t, runtime.ActivityStateCompleted, pi2.ProcessInstance().GetState())
}

func TestHavingIntermediateCatchEventAndServiceTaskInParallelTheProcessStateIsMaintained(t *testing.T) {
	cleanUpMessageSubscriptions()
	cp := CallPath{}

	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/message-intermediate-catch-event-and-parallel-tasks.bpmn")
	assert.NoError(t, err)
	t1H := bpmnEngine.NewTaskHandler().Id("task-1").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(t1H)
	t2H := bpmnEngine.NewTaskHandler().Id("task-2").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(t2H)
	instance, err := bpmnEngine.CreateInstance(t.Context(), process, nil)
	assert.NoError(t, err)

	tokens, err := bpmnEngine.persistence.GetActiveTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	err = bpmnEngine.RunProcessInstance(t.Context(), instance, tokens)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateActive, instance.ProcessInstance().GetState())

	for _, message := range engineStorage.MessageSubscriptions {
		if message.MessageSubscription().Name == "event-1" {
			err = bpmnEngine.PublishMessage(t.Context(), message, nil)
			assert.NoError(t, err)
		}
	}

	assert.Equal(t, "task-2,task-1", cp.CallPath)

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState())
}

func TestMultipleIntermediateCatchEventsPossible(t *testing.T) {
	cleanUpMessageSubscriptions()
	// setup
	cp := CallPath{}

	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/message-multiple-intermediate-catch-events.bpmn")
	assert.NoError(t, err)
	h1 := bpmnEngine.NewTaskHandler().Id("task1").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(h1)
	h2 := bpmnEngine.NewTaskHandler().Id("task2").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(h2)
	h3 := bpmnEngine.NewTaskHandler().Id("task3").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(h3)
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	for _, message := range engineStorage.MessageSubscriptions {
		if message.MessageSubscription().Name == "msg-event-2" {
			err := bpmnEngine.PublishMessage(t.Context(), message, nil)
			assert.NoError(t, err)
		}
	}

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	// then
	assert.Equal(t, "task2", cp.CallPath)
	// then still active, since there's an implicit fork
	assert.Equal(t, runtime.ActivityStateActive, instance.ProcessInstance().GetState())
}

func TestMultipleIntermediateCatchEventsImplicitForkAndMergedCOMPLETED(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/message-multiple-intermediate-catch-events-merged.bpmn")
	assert.NoError(t, err)
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	for _, message := range engineStorage.MessageSubscriptions {
		if message.MessageSubscription().Name == "msg-event-1" {
			err = bpmnEngine.PublishMessage(t.Context(), message, nil)
			assert.NoError(t, err)
		}
	}

	for _, message := range engineStorage.MessageSubscriptions {
		if message.MessageSubscription().Name == "msg-event-2" {
			err = bpmnEngine.PublishMessage(t.Context(), message, nil)
			assert.NoError(t, err)
		}
	}

	for _, message := range engineStorage.MessageSubscriptions {
		if message.MessageSubscription().Name == "msg-event-3" {
			err = bpmnEngine.PublishMessage(t.Context(), message, nil)
			assert.NoError(t, err)
		}
	}

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	// then
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState())
}

func TestMultipleIntermediateCatchEventsImplicitForkAndMergedACTIVE(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/message-multiple-intermediate-catch-events-merged.bpmn")
	assert.NoError(t, err)
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	for _, message := range engineStorage.MessageSubscriptions {
		if message.MessageSubscription().Name == "msg-event-2" {
			err = bpmnEngine.PublishMessage(t.Context(), message, nil)
			assert.NoError(t, err)
		}
	}

	// then
	assert.Equal(t, instance.ProcessInstance().GetState(), runtime.ActivityStateActive)
}

func TestMultipleIntermediateCatchEventsImplicitForkAndParallelGatewayCOMPLETED(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/message-multiple-intermediate-catch-events-parallel.bpmn")
	assert.NoError(t, err)
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	// when
	for _, message := range engineStorage.MessageSubscriptions {
		if message.MessageSubscription().State != runtime.ActivityStateActive {
			continue
		}
		err = bpmnEngine.PublishMessage(t.Context(), message, nil)
		assert.NoError(t, err)
	}

	for _, message := range engineStorage.MessageSubscriptions {
		if message.MessageSubscription().State != runtime.ActivityStateActive {
			continue
		}
		err = bpmnEngine.PublishMessage(t.Context(), message, nil)
		assert.NoError(t, err)
	}

	for _, message := range engineStorage.MessageSubscriptions {
		if message.MessageSubscription().State != runtime.ActivityStateActive {
			continue
		}
		err = bpmnEngine.PublishMessage(t.Context(), message, nil)
		assert.NoError(t, err)
	}

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateCompleted.String(), instance.ProcessInstance().State.String())
}

func TestMultipleIntermediateCatchEventsImplicitForkAndParallelGatewayACTIVE(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/message-multiple-intermediate-catch-events-parallel.bpmn")
	assert.NoError(t, err)
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	// when
	for _, message := range engineStorage.MessageSubscriptions {
		err = bpmnEngine.PublishMessage(t.Context(), message, nil)
		assert.NoError(t, err)
	}

	// then
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState())
}

func TestMultipleIntermediateCatchEventsImplicitForkAndExclusiveGatewayCOMPLETED(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/message-multiple-intermediate-catch-events-exclusive.bpmn")
	assert.NoError(t, err)
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	// when
	for _, message := range engineStorage.MessageSubscriptions {
		if message.MessageSubscription().Name == "msg-event-1" {
			err = bpmnEngine.PublishMessage(t.Context(), message, nil)
			assert.NoError(t, err)
		}
	}
	for _, message := range engineStorage.MessageSubscriptions {
		if message.MessageSubscription().Name == "msg-event-2" {
			err = bpmnEngine.PublishMessage(t.Context(), message, nil)
			assert.NoError(t, err)
		}
	}

	for _, message := range engineStorage.MessageSubscriptions {
		if message.MessageSubscription().Name == "msg-event-3" {
			err = bpmnEngine.PublishMessage(t.Context(), message, nil)
			assert.NoError(t, err)
		}
	}

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState())
}

func TestMultipleIntermediateCatchEventsImplicitForkAndExclusiveGatewayACTIVE(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/message-multiple-intermediate-catch-events-exclusive.bpmn")
	assert.NoError(t, err)
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	// when
	for _, message := range engineStorage.MessageSubscriptions {
		if message.MessageSubscription().Name == "msg-event-2" {
			err := bpmnEngine.PublishMessage(t.Context(), message, nil)
			assert.NoError(t, err)
		}
	}

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateActive, instance.ProcessInstance().GetState())
}

func TestPublishingARandomMessageDoesNoHarm(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/message-intermediate-catch-event.bpmn")
	assert.NoError(t, err)
	instance, err := bpmnEngine.CreateInstance(t.Context(), process, nil)
	assert.NoError(t, err)

	// when
	for _, message := range engineStorage.MessageSubscriptions {
		if message.MessageSubscription().Name == "random-message" {
			err = bpmnEngine.PublishMessage(t.Context(), message, nil)
			assert.NoError(t, err)
		}
	}

	// then
	assert.Equal(t, runtime.ActivityStateActive, instance.ProcessInstance().GetState())
}

func TestEventBasedGatewayCatchFlowAndEventCompletedAt(t *testing.T) {
	cleanUpMessageSubscriptions()
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/message-intermediate-timer-event.bpmn")
	assert.NoError(t, err)

	taskHandler := bpmnEngine.NewTaskHandler().Id("task-for-message").Handler(func(job ActivatedJob) {
		job.Complete()
	})
	defer bpmnEngine.RemoveHandler(taskHandler)

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	before := time.Now()
	for _, message := range engineStorage.MessageSubscriptions {
		if message.MessageSubscription().Name == "message" {
			err = bpmnEngine.PublishMessage(t.Context(), message, nil)
			assert.NoError(t, err)
		}
	}
	after := time.Now()

	flowElements, err := bpmnEngine.persistence.GetFlowElementInstancesByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key, true)
	assert.NoError(t, err)

	byID := make(map[string]runtime.FlowElementInstance, len(flowElements))
	for _, fe := range flowElements {
		byID[fe.ElementId] = fe
	}

	catchFlow, ok := byID["Flow_message"]
	assert.True(t, ok, "selected event-based-gateway catch flow 'Flow_message' should be in history")
	assert.NotNil(t, catchFlow.CompletedAt,
		"selected event-based-gateway catch flow should have CompletedAt set after firing")
	if catchFlow.CompletedAt != nil {
		assert.False(t, catchFlow.CompletedAt.Before(before), "CompletedAt should be >= publish start time (%v)", before)
		assert.False(t, catchFlow.CompletedAt.After(after), "CompletedAt should be <= publish end time (%v)", after)
	}

	catchEvent, ok := byID["message"]
	assert.True(t, ok, "selected event-based-gateway catch event 'message' should be in history")
	assert.NotNil(t, catchEvent.CompletedAt,
		"selected event-based-gateway catch event should have CompletedAt set after firing")
}

func TestEventBasedGatewayJustFiresOneEventAndInstanceCOMPLETED(t *testing.T) {
	cleanUpMessageSubscriptions()
	// setup
	cp := CallPath{}

	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/message-EventBasedGateway.bpmn")
	assert.NoError(t, err)
	instance, err := bpmnEngine.CreateInstance(t.Context(), process, nil)
	assert.NoError(t, err)

	aH := bpmnEngine.NewTaskHandler().Id("task-a").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(aH)
	bH := bpmnEngine.NewTaskHandler().Id("task-b").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(bH)

	// when
	for _, message := range engineStorage.MessageSubscriptions {
		if message.MessageSubscription().Name == "msg-b" {
			err = bpmnEngine.PublishMessage(t.Context(), message, nil)
			assert.NoError(t, err)
		}
	}

	// then
	assert.Equal(t, "task-b", cp.CallPath)

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState())
}

func TestIntermediateMessageCatchEventPublishesVariablesIntoInstance(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple-intermediate-message-catch-event.bpmn")
	assert.NoError(t, err)
	instance, _ := bpmnEngine.CreateInstance(t.Context(), process, nil)

	// when
	for _, message := range engineStorage.MessageSubscriptions {
		if message.MessageSubscription().Name == "msg" {
			vars := map[string]interface{}{"foo": "bar"}
			err = bpmnEngine.PublishMessage(t.Context(), message, vars)
			assert.NoError(t, err)
		}
	}

	// then
	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState())
	assert.Equal(t, "bar", instance.ProcessInstance().GetVariable("mappedFoo"))
}

func TestIntermediateMessageCatchEventOutputMappingReturnsEmpty(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple-intermediate-message-catch-event-broken.bpmn")
	assert.NoError(t, err)
	instance, err := bpmnEngine.CreateInstance(t.Context(), process, nil)
	assert.NoError(t, err)

	// when
	for _, message := range engineStorage.MessageSubscriptions {
		if message.MessageSubscription().Name == "msg" && message.MessageSubscription().State == runtime.ActivityStateActive {
			err = bpmnEngine.PublishMessage(t.Context(), message, nil)
			assert.NoError(t, err)
		}
	}

	// then
	message, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateCompleted)
	assert.NoError(t, err)

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	assert.Equal(t, instance.ProcessInstance().GetState(), runtime.ActivityStateCompleted)
	assert.Nil(t, instance.ProcessInstance().GetVariable("mappedFoo"))
	assert.Equal(t, message[0].MessageSubscription().State, runtime.ActivityStateCompleted)
}

func TestInterruptingBoundaryEventMessageCatchTriggered(t *testing.T) {
	// 1) After process start the message subscription bound to the boundary event should be created
	//    - process should be active
	//    - message subscription should be active
	// 2) After process message is thrown
	//    - the job and
	//    - any other boundary events subscriptions should be cancelled and
	//    - flow outgoing from the boundary should be taken

	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/message-boundary-event-interrupting.bpmn")
	assert.NoError(t, err)
	variableContext := make(map[string]interface{}, 1)
	randomCorellationKey := "message-boundary-event-interruptingCorrelationKey"
	// when
	instance, err := bpmnEngine.CreateInstance(t.Context(), process, variableContext)
	assert.NoError(t, err)

	// then
	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subscriptions))

	jobs := findActiveJobsForProcessInstance(instance.ProcessInstance().Key, "simple-job")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(jobs))

	// when
	variables := map[string]interface{}{"payload": "message payload"}
	err = bpmnEngine.PublishMessageByName(t.Context(), "simple-boundary", &randomCorellationKey, variables)
	assert.NoError(t, err)

	// then
	subscriptions, err = bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState())

	jobs = findActiveJobsForProcessInstance(instance.ProcessInstance().Key, "simple-job")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(jobs))

}

func TestInterruptingBoundaryMessageEventCompletedAt(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/message-boundary-event-interrupting.bpmn")
	assert.NoError(t, err)
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	before := time.Now()
	ck := "message-boundary-event-interruptingCorrelationKey"
	err = bpmnEngine.PublishMessageByName(t.Context(), "simple-boundary", &ck,
		map[string]interface{}{"payload": "message payload"})
	assert.NoError(t, err)
	after := time.Now()

	flowElements, err := bpmnEngine.persistence.GetFlowElementInstancesByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key, true)
	assert.NoError(t, err)

	var boundary *runtime.FlowElementInstance
	for i := range flowElements {
		if flowElements[i].ElementId == "Event_1n9fcqj" {
			boundary = &flowElements[i]
			break
		}
	}
	assert.NotNil(t, boundary, "boundary message event 'Event_1n9fcqj' should be in history")
	if boundary == nil {
		return
	}
	assert.NotNil(t, boundary.CompletedAt,
		"boundary message event should have CompletedAt set after the boundary fires")
	if boundary.CompletedAt != nil {
		assert.False(t, boundary.CompletedAt.Before(before), "CompletedAt should be >= publish start time (%v)", before)
		assert.False(t, boundary.CompletedAt.After(after), "CompletedAt should be <= publish end time (%v)", after)
	}
	assert.Equal(t, map[string]any{"payload": "message payload"}, boundary.OutputVariables,
		"boundary message event should carry its mapped output variables in history")
}

func TestNoninterruptingBoundaryEventMessageCatchTriggered(t *testing.T) {
	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/message-boundary-event-noninterrupting.bpmn")
	assert.NoError(t, err)
	variableContext := make(map[string]interface{}, 1)
	randomCorellationKey := fmt.Sprint(rand.Int63())
	variableContext["correlationKey"] = randomCorellationKey
	// when
	instance, err := bpmnEngine.CreateInstance(t.Context(), process, variableContext)
	assert.NoError(t, err)

	// then
	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subscriptions))

	jobs := findActiveJobsForProcessInstance(instance.ProcessInstance().Key, "simple-job")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(jobs))

	// when
	variables := map[string]interface{}{"payload": "message payload"}
	err = bpmnEngine.PublishMessageByName(t.Context(), "simple-boundary", &randomCorellationKey, variables)
	assert.NoError(t, err)

	variables = map[string]interface{}{"payload": "message payload"}
	err = bpmnEngine.PublishMessageByName(t.Context(), "simple-boundary", &randomCorellationKey, variables)
	assert.NoError(t, err)

	variables = map[string]interface{}{"payload": "message payload"}
	err = bpmnEngine.PublishMessageByName(t.Context(), "simple-boundary", &randomCorellationKey, variables)
	assert.NoError(t, err)

	// then
	subscriptions, err = bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subscriptions))

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateActive, instance.ProcessInstance().GetState())

	jobs = findActiveJobsForProcessInstance(instance.ProcessInstance().Key, "simple-job")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(jobs))

	countCompletedTokens := 0
	for _, token := range engineStorage.ExecutionTokens {
		if token.ProcessInstanceKey == instance.ProcessInstance().Key && token.State == runtime.TokenStateCompleted {
			countCompletedTokens++
		}
	}
	assert.Equal(t, 3, countCompletedTokens)

}

func TestBoundaryEventActivityCompleteCancelsSubscriptions(t *testing.T) {
	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/message-boundary-event-noninterrupting.bpmn")
	assert.NoError(t, err)
	variableContext := make(map[string]interface{}, 1)
	randomCorellationKey := rand.Int63()
	variableContext["correlationKey"] = fmt.Sprint(randomCorellationKey)
	// when
	instance, err := bpmnEngine.CreateInstance(t.Context(), process, variableContext)
	assert.NoError(t, err)

	// then
	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subscriptions))

	jobs := findActiveJobsForProcessInstance(instance.ProcessInstance().Key, "simple-job")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(jobs))

	// when
	err = bpmnEngine.JobCompleteByKey(t.Context(), jobs[0].Key, jobs[0].InputVariables)
	assert.NoError(t, err)

	// then
	subscriptions, err = bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	jobs = findActiveJobsForProcessInstance(instance.ProcessInstance().Key, "simple-job")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(jobs))

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState())

}

func TestMessageEventMultiInstanceBusinessRule(t *testing.T) {
	t.Skip("Local business rules dont support boundary events yet")

	cleanUpMessageSubscriptions()
	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/multi_instance_business_rule.bpmn")
	assert.NoError(t, err)
	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{"test1", "test2", "test3"}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)

	// when
	count := 0
	for _, message := range engineStorage.MessageSubscriptions {
		if message.MessageSubscription().Name == "boundary message" {
			err = bpmnEngine.PublishMessage(t.Context(), message, nil)
			assert.NoError(t, err)
			count++
		}
	}
	assert.Equal(t, 1, count)

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState())

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	tokens, err := bpmnEngine.persistence.GetAllTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))

	subProcesses, err := bpmnEngine.persistence.FindProcessInstancesByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subProcesses))
	assert.Equal(t, runtime.ActivityStateTerminated, subProcesses[0].ProcessInstance().GetState())
}

func TestMessageEventMultiInstanceParallelBusinessRule(t *testing.T) {
	t.Skip("Local Business Rules dont support boundary events yet")

	cleanUpMessageSubscriptions()
	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/multi_instance_parallel_business_rule.bpmn")
	assert.NoError(t, err)
	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{"test1", "test2", "test3"}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)

	// when
	count := 0
	for _, message := range engineStorage.MessageSubscriptions {
		if message.MessageSubscription().Name == "boundary message" {
			err = bpmnEngine.PublishMessage(t.Context(), message, nil)
			assert.NoError(t, err)
			count++
		}
	}
	assert.Equal(t, 1, count)

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState())

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	tokens, err := bpmnEngine.persistence.GetAllTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))

	subProcesses, err := bpmnEngine.persistence.FindProcessInstancesByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subProcesses))
	assert.Equal(t, runtime.ActivityStateTerminated, subProcesses[0].ProcessInstance().GetState())
}

func TestMessageEventMultiInstance(t *testing.T) {
	bpmnFiles := map[string]string{
		"TestMessageEventMultiInstanceParallelServiceTask": "./test-cases/multi_instance_parallel_service_task.bpmn",
		"TestMessageEventMultiInstanceParallelSubProcess":  "./test-cases/multi_instance_parallel_sub_process_task.bpmn",
		"TestMessageEventMultiInstanceSubProcess":          "./test-cases/multi_instance_sub_process_task.bpmn",
		"TestMessageEventMultiInstanceServiceTask":         "./test-cases/multi_instance_service_task.bpmn",
		"TestMessageEventMultiInstanceReceiveTask":         "./test-cases/receive_task/multi_instance_receive_task.bpmn",
		"TestMessageEventMultiInstanceParallelReceiveTask": "./test-cases/receive_task/multi_instance_parallel_receive_task.bpmn",
	}
	for testName, filePath := range bpmnFiles {
		process, err := bpmnEngine.LoadFromFile(t.Context(), filePath)
		assert.NoError(t, err)
		t.Run(testName, func(t *testing.T) {
			cleanUpMessageSubscriptions()
			// given

			variableContext := make(map[string]interface{}, 1)
			variableContext["testInputCollection"] = []string{"test1", "test2", "test3"}
			instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
			assert.NoError(t, err)

			// wait until the boundary message subscription has been created for the multi instance activity
			assert.Eventually(t, func() bool {
				count := 0
				for _, message := range engineStorage.MessageSubscriptions {
					if message.MessageSubscription().Name == "boundary message" && message.MessageSubscription().State == runtime.ActivityStateActive {
						count++
					}
				}
				return count == 1
			}, 2*time.Second, 50*time.Millisecond)

			// when
			count := 0
			for _, message := range engineStorage.MessageSubscriptions {
				if message.MessageSubscription().Name == "boundary message" && message.MessageSubscription().State == runtime.ActivityStateActive {
					err = bpmnEngine.PublishMessage(t.Context(), message, nil)
					assert.NoError(t, err)
					count++
				}
			}
			assert.Equal(t, 1, count)

			instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
			assert.NoError(t, err)

			// then
			assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState())

			subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
			assert.NoError(t, err)
			assert.Equal(t, 0, len(subscriptions))

			tokens, err := bpmnEngine.persistence.GetAllTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
			assert.NoError(t, err)
			assert.Equal(t, 1, len(tokens))

			subProcesses, err := bpmnEngine.persistence.FindProcessInstancesByParentExecutionTokenKey(t.Context(), tokens[0].Key)
			assert.NoError(t, err)
			assert.Equal(t, 1, len(subProcesses))
			assert.Equal(t, runtime.ActivityStateTerminated, subProcesses[0].ProcessInstance().GetState())
		})
	}
}

func TestMessageEventMultiInstanceCallActivity(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	_, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/multi_instance_call_activity_process.bpmn")
	assert.NoError(t, err)
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/multi_instance_call_activity_task.bpmn")
	assert.NoError(t, err)

	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{"test1", "test2", "test3"}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)

	count := 0
	for _, message := range engineStorage.MessageSubscriptions {
		if message.MessageSubscription().Name == "boundary message" {
			err = bpmnEngine.PublishMessage(t.Context(), message, nil)
			assert.NoError(t, err)
			count++
		}
	}
	assert.Equal(t, 1, count)

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState())

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	tokens, err := bpmnEngine.persistence.GetAllTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))

	subProcesses, err := bpmnEngine.persistence.FindProcessInstancesByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subProcesses))
	assert.Equal(t, runtime.ActivityStateTerminated, subProcesses[0].ProcessInstance().GetState())
}

func TestMessageEventMultiInstanceParallelCallActivity(t *testing.T) {
	cleanUpMessageSubscriptions()
	// given
	_, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/multi_instance_call_activity_process.bpmn")
	assert.NoError(t, err)
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/multi_instance_parallel_call_activity_task.bpmn")
	assert.NoError(t, err)

	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{"test1", "test2", "test3"}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)

	// when
	count := 0
	for _, message := range engineStorage.MessageSubscriptions {
		if message.MessageSubscription().Name == "boundary message" {
			err = bpmnEngine.PublishMessage(t.Context(), message, nil)
			assert.NoError(t, err)
			count++
		}
	}
	assert.Equal(t, 1, count)

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState())

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	tokens, err := bpmnEngine.persistence.GetAllTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))

	subProcesses, err := bpmnEngine.persistence.FindProcessInstancesByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subProcesses))
	assert.Equal(t, runtime.ActivityStateTerminated, subProcesses[0].ProcessInstance().GetState())
}

func TestNoneIntermediateThrowEventReturnsErrorAndFailsInstance(t *testing.T) {
	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/none-intermediate-throw-event.bpmn")
	assert.NoError(t, err)

	// when
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)

	// then
	assert.Error(t, err)
	assert.ErrorContains(t, err, "none intermediate throw event is not supported")
	assert.Equal(t, runtime.ActivityStateFailed, instance.ProcessInstance().State)

	instanceDb, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateFailed, instanceDb.ProcessInstance().State)
}
