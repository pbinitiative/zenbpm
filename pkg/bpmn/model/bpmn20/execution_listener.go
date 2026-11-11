package bpmn20

import (
	"slices"
	"strings"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/extensions"
)

// Every flow node reads its execution listeners (TFlowNode.ExecutionListeners).
// Which of them the engine runs is decided here, by ListenerEventTypesRunOn
// alone: a deployment refuses every listener outside that table
// (CheckListeners), and the engine runs none outside it.

// ListenerEventTypesRunOn answers the event types of the listeners the engine
// runs on the flow node, and none where it runs no listener:
//
//   - "start" and "end" on an element whose own work is one job: a service,
//     send or user task, a business rule task with a task definition, and an
//     intermediate event which throws a message; none of them where the
//     activity is multi-instance;
//   - "end" on a start event directly under the process. A start event takes
//     no "start" listener, as in Camunda 8.
//
// An element type is added here when the engine runs it through its element
// lifecycle.
func (p *TProcess) ListenerEventTypesRunOn(node FlowNode) []string {
	if activity, ok := node.(Activity); ok && activity.GetMultiInstance() != nil {
		return nil
	}
	switch element := node.(type) {
	case *TStartEvent:
		if p.IsTopLevelStartEvent(element) {
			return []string{extensions.ExecutionListenerEventTypeEnd}
		}
	case *TServiceTask, *TSendTask, *TUserTask:
		return listenerEventTypesOfAJobElement
	case *TBusinessRuleTask:
		if _, external := element.Implementation.(*TBusinessRuleTaskExternal); external {
			return listenerEventTypesOfAJobElement
		}
	case *TIntermediateThrowEvent:
		if _, throwsMessage := element.EventDefinition.(TMessageEventDefinition); throwsMessage {
			return listenerEventTypesOfAJobElement
		}
	}
	return nil
}

// listenerEventTypesOfAJobElement are the event types run on an element whose
// own work is one job.
var listenerEventTypesOfAJobElement = []string{extensions.ExecutionListenerEventTypeStart, extensions.ExecutionListenerEventTypeEnd}

// RunnableListenersOf answers the node's listeners of the given event type
// which the engine runs, in the order of the model.
func (p *TProcess) RunnableListenersOf(node FlowNode, eventType string) []extensions.TExecutionListener {
	if !slices.Contains(p.ListenerEventTypesRunOn(node), eventType) {
		return nil
	}
	var listeners []extensions.TExecutionListener
	for _, listener := range node.GetExecutionListeners() {
		if listener.EventType == eventType {
			listeners = append(listeners, listener)
		}
	}
	return listeners
}

// RunsListenersOn reports whether the node carries a listener the engine runs.
func (p *TProcess) RunsListenersOn(node FlowNode) bool {
	runnableEventTypes := p.ListenerEventTypesRunOn(node)
	for _, listener := range node.GetExecutionListeners() {
		if slices.Contains(runnableEventTypes, listener.EventType) {
			return true
		}
	}
	return false
}

// IsTopLevelStartEvent reports whether the start event is one of the
// process's own start events, not one of a sub-process.
func (p *TProcess) IsTopLevelStartEvent(startEvent *TStartEvent) bool {
	for i := range p.StartEvents {
		if p.StartEvents[i].GetId() == startEvent.GetId() {
			return true
		}
	}
	return false
}

// ListenerTask is the job-carrying side of an execution listener. It reads as
// the element it belongs to (the embedded FlowNode), so that the job of the
// listener is created, handed out and resolved as the job of a task on that
// element. It has no input or output mappings: a listener job carries its
// instance's variables as every job does, and its completion changes no
// variable. The element stays what it is, so a start event's own output
// mappings are never evaluated again when the listener's job completes.
type ListenerTask struct {
	FlowNode
	listener extensions.TExecutionListener
}

// NewListenerTask answers the task of one listener of the element.
func NewListenerTask(element FlowNode, listener extensions.TExecutionListener) *ListenerTask {
	return &ListenerTask{FlowNode: element, listener: listener}
}

// EventType answers the listener's event type.
func (task *ListenerTask) EventType() string { return task.listener.EventType }

// Element answers the element the listener belongs to.
func (task *ListenerTask) Element() FlowNode { return task.FlowNode }

// GetTaskType answers the listener's job type, without surrounding
// whitespace, which a worker could not tell apart.
func (task *ListenerTask) GetTaskType() string { return strings.TrimSpace(task.listener.Type) }

// GetInputMapping answers none: a listener job carries its instance's variables.
func (task *ListenerTask) GetInputMapping() []extensions.TIoMapping {
	return nil
}

// GetOutputMapping answers none: a listener's completion changes no variable.
func (task *ListenerTask) GetOutputMapping() []extensions.TIoMapping {
	return nil
}

// GetTaskDefinition answers the listener's job type, retries and backoff in
// the shape of a task definition, so that a listener's job is created, retried
// and backed off as every other job.
func (task *ListenerTask) GetTaskDefinition() extensions.TTaskDefinition {
	return extensions.TTaskDefinition{
		TypeName:     task.GetTaskType(),
		Retries:      task.listener.Retries,
		RetryBackoff: task.listener.RetryBackoff,
	}
}

// ListenerTaskOf answers the task of the element's listener of the given
// event type and job type, which a job of that listener names, or nil when
// the element has no such listener. A deployment refuses two listeners of an
// element which share both.
func (p *TProcess) ListenerTaskOf(elementID string, eventType string, jobType string) InternalTask {
	element := p.GetFlowNodeById(elementID)
	if element == nil {
		return nil
	}
	for _, listener := range element.GetExecutionListeners() {
		if listener.EventType == eventType && strings.TrimSpace(listener.Type) == jobType {
			return NewListenerTask(element, listener)
		}
	}
	return nil
}
