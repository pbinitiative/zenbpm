// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package bpmn

import (
	"slices"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
)

type taskMatcher func(element bpmn20.InternalTask) bool

type taskHandlerType string

const (
	taskHandlerForId              = "TASK_HANDLER_ID"
	taskHandlerForType            = "TASK_HANDLER_TYPE"
	taskHandlerForAssignee        = "TASK_HANDLER_ASSIGNEE"
	taskHandlerForCandidateGroups = "TASK_HANDLER_CANDIDATE_GROUPS"
)

type taskHandler struct {
	handlerType taskHandlerType
	matches     taskMatcher
	handler     func(job ActivatedJob)
}

type newTaskHandlerCommand struct {
	handlerType taskHandlerType
	matcher     taskMatcher
	append      func(handler *taskHandler)
}

type NewTaskHandlerCommand2 interface {
	// Handler is the actual handler to be executed
	Handler(func(job ActivatedJob)) *taskHandler
}

type NewTaskHandlerCommand1 interface {
	// Id defines a handler for a given element ID (as defined in the task element in the BPMN file)
	// This is 1:1 relation between a handler and a task definition (since IDs are supposed to be unique).
	Id(id string) NewTaskHandlerCommand2

	// Type defines a handler for a Service Task with a given 'type';
	// Hereby 'type' is defined as 'taskDefinition' extension element in the BPMN file.
	// This allows a single handler to be used for multiple task definitions.
	Type(taskType string) NewTaskHandlerCommand2

	// Assignee defines a handler for a User Task with a given 'assignee';
	// Hereby 'assignee' is defined as 'assignmentDefinition' extension element in the BPMN file.
	Assignee(assignee string) NewTaskHandlerCommand2

	// CandidateGroups defines a handler for a User Task with given 'candidate groups';
	// For the handler you can specify one or more groups.
	// If at least one matches a given user task, the handler will be called.
	CandidateGroups(groups ...string) NewTaskHandlerCommand2
}

// NewTaskHandler registers a handler function to be called for service tasks with a given taskId
func (engine *Engine) NewTaskHandler() NewTaskHandlerCommand1 {
	cmd := newTaskHandlerCommand{
		append: func(handler *taskHandler) {
			engine.taskhandlersMu.Lock()
			defer engine.taskhandlersMu.Unlock()
			engine.taskHandlers = append(engine.taskHandlers, handler)
		},
	}
	return cmd
}

// Id implements NewTaskHandlerCommand1
func (thc newTaskHandlerCommand) Id(id string) NewTaskHandlerCommand2 {
	thc.matcher = func(element bpmn20.InternalTask) bool {
		return element.GetId() == id
	}
	thc.handlerType = taskHandlerForId
	return thc
}

// Type implements NewTaskHandlerCommand1
func (thc newTaskHandlerCommand) Type(taskType string) NewTaskHandlerCommand2 {
	thc.matcher = func(element bpmn20.InternalTask) bool {
		return element.GetTaskType() == taskType
	}
	thc.handlerType = taskHandlerForType
	return thc
}

// Handler implements NewTaskHandlerCommand2
func (thc newTaskHandlerCommand) Handler(f func(job ActivatedJob)) *taskHandler {
	th := taskHandler{
		handlerType: thc.handlerType,
		matches:     thc.matcher,
		handler:     f,
	}
	thc.append(&th)
	return &th
}

// RemoveHandler removes the handler created by Handler method
func (engine *Engine) RemoveHandler(handler *taskHandler) {
	engine.taskhandlersMu.Lock()
	defer engine.taskhandlersMu.Unlock()
	for i, hand := range engine.taskHandlers {
		if hand == handler {
			engine.taskHandlers = slices.Delete(engine.taskHandlers, i, i+1)
			return
		}
	}
}

// Assignee implements NewTaskHandlerCommand2
// TODO: it is an unlikely scenario for getting a predefined handler for User Task. To be redesigned
func (thc newTaskHandlerCommand) Assignee(assignee string) NewTaskHandlerCommand2 {
	thc.matcher = func(element bpmn20.InternalTask) bool {
		utl, isUserTask := element.(bpmn20.UserTaskElement)
		if !isUserTask {
			return false
		}
		return utl.GetAssignmentAssignee() == assignee
	}
	thc.handlerType = taskHandlerForAssignee
	return thc
}

// CandidateGroups implements NewTaskHandlerCommand2
// TODO: it is an unlikely scenario for getting a predefined handler for User Task. To be redesigned
func (thc newTaskHandlerCommand) CandidateGroups(groups ...string) NewTaskHandlerCommand2 {
	thc.matcher = func(element bpmn20.InternalTask) bool {
		utl, isUserTask := element.(bpmn20.UserTaskElement)
		if !isUserTask {
			return false
		}
		for _, group := range groups {
			if slices.Contains(utl.GetAssignmentCandidateGroups(), group) {
				return true
			}
		}
		return false
	}
	thc.handlerType = taskHandlerForCandidateGroups
	return thc
}

func (engine *Engine) findTaskHandler(element bpmn20.InternalTask) func(job ActivatedJob) {
	engine.taskhandlersMu.RLock()
	defer engine.taskhandlersMu.RUnlock()
	searchOrder := []taskHandlerType{taskHandlerForId}
	if element.GetType() == bpmn20.ElementTypeServiceTask {
		searchOrder = append(searchOrder, taskHandlerForType)
	}
	if element.GetType() == bpmn20.ElementBusinessRuleTask {
		searchOrder = append(searchOrder, taskHandlerForType)
	}
	if element.GetType() == bpmn20.ElementTypeUserTask {
		searchOrder = append(searchOrder, taskHandlerForAssignee, taskHandlerForCandidateGroups)
	}
	for _, handlerType := range searchOrder {
		for _, handler := range engine.taskHandlers {
			if handler.handlerType == handlerType {
				if handler.matches(element) {
					return handler.handler
				}
			}
		}
	}
	return nil
}
