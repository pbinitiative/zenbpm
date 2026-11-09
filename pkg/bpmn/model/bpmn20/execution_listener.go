package bpmn20

import (
	"strings"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/extensions"
)

// ExecutionListenerOwner is a flow node whose model reads the execution
// listeners of its extensionElements. The start event is the only owner so
// far. Another flow node becomes one by reading its listeners, implementing
// this interface and being named in modelReadsListenersOf and
// listenerOwnersOf; RunnableListenerEventTypes then decides whether the
// engine runs them.
type ExecutionListenerOwner interface {
	FlowNode
	GetExecutionListeners() []extensions.TExecutionListener
}

// RunnableListenerEventTypes answers the event types of the listeners the
// engine runs on the owner: "end" on a start event directly under the
// process, and none anywhere else. A deployment refuses a listener of any
// other event type, and every listener of an owner which has none.
func (p *TProcess) RunnableListenerEventTypes(owner ExecutionListenerOwner) []string {
	if startEvent, ok := owner.(*TStartEvent); ok && p.IsTopLevelStartEvent(startEvent) {
		return []string{extensions.ExecutionListenerEventTypeEnd}
	}
	return nil
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

// ListenerTaskOf answers the task of the owner's listener of the given event
// type, or nil when the owner has none. Only one listener per event type is
// supported; a deployment refuses a second one.
func ListenerTaskOf(owner ExecutionListenerOwner, eventType string) *ListenerTask {
	listener := extensions.FirstExecutionListenerOf(owner.GetExecutionListeners(), eventType)
	if listener == nil {
		return nil
	}
	return &ListenerTask{FlowNode: owner, listener: *listener}
}

// EventType answers the listener's event type.
func (task *ListenerTask) EventType() string { return task.listener.EventType }

// Element answers the element the listener belongs to.
func (task *ListenerTask) Element() FlowNode { return task.FlowNode }

// ValidateLiterals refuses the listener when its attributes are not literals
// (see ValidateListenerLiterals).
func (task *ListenerTask) ValidateLiterals() error { return ValidateListenerLiterals(task.listener) }

// GetTaskType answers the listener's job type, without surrounding
// whitespace, which a worker could not tell apart.
func (task *ListenerTask) GetTaskType() string { return strings.TrimSpace(task.listener.Type) }
func (task *ListenerTask) GetInputMapping() []extensions.TIoMapping {
	return nil
}
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

// TaskOfJob answers the task a job belongs to: the listener of the given
// event type on the element, or the element's own internal task where the
// event type is "" (see ListenerEventTypeOfJob). The answer is nil when the
// element no longer has what created the job.
func (p *TProcess) TaskOfJob(elementID string, listenerEventType string) InternalTask {
	if listenerEventType == "" {
		return p.GetInternalTaskById(elementID)
	}
	owner, ok := p.GetFlowNodeById(elementID).(ExecutionListenerOwner)
	if !ok {
		return nil
	}
	if task := ListenerTaskOf(owner, listenerEventType); task != nil {
		return task
	}
	return nil
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

// ListenerEventTypeOfJob answers the event type of the execution listener a job
// belongs to, from the job's element type, or "" for the job of a task. It is
// the one place which tells a listener's job from a task's job: only a
// listener creates a job on a start event, so no column of the job is needed
// yet.
//
// TODO: before a listener runs on an element which also creates task jobs, or
// one element runs listeners of two event types, store the event type in a
// nullable listener_event_type column of job (its migration fills 'end' where
// element_type = 'START_EVENT'), set it in createInternalTask, and let this
// function read the job instead of its element type.
func ListenerEventTypeOfJob(elementType string) string {
	if ElementType(elementType) == ElementTypeStartEvent {
		return extensions.ExecutionListenerEventTypeEnd
	}
	return ""
}
