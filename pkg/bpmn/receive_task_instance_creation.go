package bpmn

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
)

func findReceiveTaskById(process *bpmn20.TProcess, id string) *bpmn20.TReceiveTask {
	for i := range process.ReceiveTask {
		if process.ReceiveTask[i].GetId() == id {
			return &process.ReceiveTask[i]
		}
	}
	return nil
}

func findInstantiatingReceiveTasks(process *bpmn20.TProcess) []*bpmn20.TReceiveTask {
	result := make([]*bpmn20.TReceiveTask, 0)
	for i := range process.ReceiveTask {
		if process.ReceiveTask[i].Instantiate {
			result = append(result, &process.ReceiveTask[i])
		}
	}
	return result
}

func hasPlainStartEvent(process *bpmn20.TProcess) bool {
	for i := range process.StartEvents {
		if len(process.StartEvents[i].EventDefinitions) == 0 {
			return true
		}
	}
	return false
}

func shouldRearmInstantiatingReceiveTaskSubscriptions(state runtime.ActivityState) bool {
	return state == runtime.ActivityStateCompleted ||
		state == runtime.ActivityStateTerminated ||
		state == runtime.ActivityStateFailed
}

// newReceiveTaskDefinitionSubscription builds the process-definition-level message subscription that backs an instantiating receive task.
func (engine *Engine) newReceiveTaskDefinitionSubscription(
	processDefinition runtime.ProcessDefinition,
	receiveTask *bpmn20.TReceiveTask,
) (*runtime.DefinitionMessageSubscription, error) {
	messageName, err := engine.getMessageName(processDefinition, bpmn20.TMessageEventDefinition{MessageRef: receiveTask.MessageRef})
	if err != nil {
		return nil, fmt.Errorf("failed to resolve message name for instantiating receive task %s: %w", receiveTask.GetId(), err)
	}
	return &runtime.DefinitionMessageSubscription{
		MessageSubscriptionData: runtime.MessageSubscriptionData{
			Key:                  engine.generateKey(),
			ElementId:            receiveTask.GetId(),
			Name:                 messageName,
			State:                runtime.ActivityStateActive,
			ProcessDefinitionKey: processDefinition.Key,
			CreatedAt:            time.Now(),
		},
	}, nil
}

// createInstantiatingReceiveTaskSubscriptions registers a process-definition-level message subscription for
// every instantiating receive task (instantiate="true") of the given process definition.
func (engine *Engine) createInstantiatingReceiveTaskSubscriptions(
	ctx context.Context,
	batch storage.Batch,
	processDefinition runtime.ProcessDefinition,
) error {
	for _, receiveTask := range findInstantiatingReceiveTasks(&processDefinition.Definitions.Process) {
		subscription, err := engine.newReceiveTaskDefinitionSubscription(processDefinition, receiveTask)
		if err != nil {
			return err
		}
		if err := batch.SaveMessageSubscription(ctx, subscription); err != nil {
			return fmt.Errorf("failed to save definition message subscription for instantiating receive task %s of definition %d: %w", receiveTask.GetId(), processDefinition.Key, err)
		}
	}
	return nil
}

// publishMessageOnReceiveTaskInstanceCreation activates a (definition-level) message subscription that belongs
// to an instantiating receive task. It consumes the subscription and creates a new process instance that starts at the receive task.
func (engine *Engine) publishMessageOnReceiveTaskInstanceCreation(ctx context.Context, message *runtime.DefinitionMessageSubscription, variables map[string]any) error {
	engine.runningInstances.lockInstance(message.Key)
	defer engine.runningInstances.unlockInstance(message.Key)

	// Re-fetch under the trigger lock to confirm we are the winner. If another publisher already consumed
	// this subscription it will be Completed / not found, in which case we silently no-op.
	refreshed, err := engine.persistence.FindMessageSubscriptionByKey(ctx, message.Key, runtime.ActivityStateActive)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil
		}
		return errors.Join(newEngineErrorf("failed to find active definition message subscription: %d", message.Key), err)
	}
	defSub, ok := refreshed.(*runtime.DefinitionMessageSubscription)
	if !ok {
		return fmt.Errorf("expected DefinitionMessageSubscription for key %d, got %T", message.Key, refreshed)
	}

	batch := engine.persistence.NewBatch()
	defSub.State = runtime.ActivityStateCompleted
	if err := batch.SaveMessageSubscription(ctx, defSub); err != nil {
		return fmt.Errorf("failed to save definition message subscription %d: %w", defSub.Key, err)
	}
	if err := batch.Flush(ctx); err != nil {
		return fmt.Errorf("failed to flush instantiating receive task consumption batch (subscription=%d, definition=%d): %w", defSub.Key, defSub.ProcessDefinitionKey, err)
	}

	processDefinition, pdErr := engine.persistence.FindProcessDefinitionByKey(ctx, defSub.ProcessDefinitionKey)
	instance, err := engine.CreateInstanceWithStartingElements(ctx, defSub.ProcessDefinitionKey, []string{defSub.ElementId}, variables, nil)
	if err != nil {
		if pdErr == nil {
			if rearmErr := engine.rearmInstantiatingReceiveTaskSubscriptions(ctx, &processDefinition); rearmErr != nil {
				return fmt.Errorf("failed to create process instance for instantiating receive task %s of definition %d: %w; additionally failed to re-arm subscription: %w", defSub.ElementId, defSub.ProcessDefinitionKey, err, rearmErr)
			}
		}
		return fmt.Errorf("failed to create process instance for instantiating receive task %s of definition %d: %w", defSub.ElementId, defSub.ProcessDefinitionKey, err)
	}

	if err := engine.completeInstantiatingReceiveTask(ctx, instance.ProcessInstance().Key, defSub.ElementId, variables); err != nil {
		return fmt.Errorf("failed to complete instantiating receive task %s for process instance %d of definition %d: %w", defSub.ElementId, instance.ProcessInstance().Key, defSub.ProcessDefinitionKey, err)
	}
	return nil
}

// completeInstantiatingReceiveTask correlates the triggering message onto the receive task's own message
// subscription of a freshly instantiated process instance, driving the token past the receive task.
func (engine *Engine) completeInstantiatingReceiveTask(ctx context.Context, processInstanceKey int64, receiveTaskId string, variables map[string]any) error {
	instance, err := engine.persistence.FindProcessInstanceByKey(ctx, processInstanceKey)
	if err != nil {
		return fmt.Errorf("failed to find process instance %d: %w", processInstanceKey, err)
	}
	receiveTask := findReceiveTaskById(&instance.ProcessInstance().Definition.Definitions.Process, receiveTaskId)
	if receiveTask == nil {
		return fmt.Errorf("failed to find instantiating receive task %s in process definition %d", receiveTaskId, instance.ProcessInstance().Definition.Key)
	}

	subscriptions, err := engine.persistence.FindProcessInstanceMessageSubscriptions(ctx, processInstanceKey, runtime.ActivityStateActive)
	if err != nil {
		return fmt.Errorf("failed to find active message subscriptions for process instance %d: %w", processInstanceKey, err)
	}
	for _, subscription := range subscriptions {
		tokenSub, ok := subscription.(*runtime.TokenMessageSubscription)
		if !ok || tokenSub.MessageSubscription().ElementId != receiveTaskId {
			continue
		}
		owns, err := engine.receiveTaskOwnsSubscription(ctx, instance, receiveTask, tokenSub)
		if err != nil {
			return fmt.Errorf("failed to resolve receive task subscription for instantiating receive task %s in process instance %d: %w", receiveTaskId, processInstanceKey, err)
		}
		if owns {
			return engine.PublishMessageOnToken(ctx, tokenSub, variables)
		}
	}
	return fmt.Errorf("no active message subscription found for instantiating receive task %s in process instance %d", receiveTaskId, processInstanceKey)
}

// rearmInstantiatingReceiveTaskSubscriptions re-creates the process-definition-level message subscriptions for
// the instantiating receive tasks of the given definition once a process instance is no longer active.
func (engine *Engine) rearmInstantiatingReceiveTaskSubscriptions(ctx context.Context, processDefinition *runtime.ProcessDefinition) error {
	if processDefinition == nil {
		return nil
	}
	receiveTasks := findInstantiatingReceiveTasks(&processDefinition.Definitions.Process)
	if len(receiveTasks) == 0 {
		return nil
	}

	batch := engine.persistence.NewBatch()
	rearmed := false
	for _, receiveTask := range receiveTasks {
		subscription, err := engine.newReceiveTaskDefinitionSubscription(*processDefinition, receiveTask)
		if err != nil {
			return err
		}
		existing, err := engine.persistence.FindDefinitionMessageSubscription(ctx, processDefinition.Key, receiveTask.GetId(), subscription.Name, runtime.ActivityStateActive)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return errors.Join(newEngineErrorf("failed to look up definition message subscription for receive task %s (%s)", receiveTask.GetId(), subscription.Name), err)
		}
		if existing != nil {
			// A definition subscription is already active (e.g. re-armed by another instance); nothing to do.
			continue
		}
		if err := batch.SaveMessageSubscription(ctx, subscription); err != nil {
			return fmt.Errorf("failed to re-arm definition message subscription for instantiating receive task %s of definition %d: %w", receiveTask.GetId(), processDefinition.Key, err)
		}
		rearmed = true
	}
	if !rearmed {
		return nil
	}
	return batch.Flush(ctx)
}
