---
sidebar_position: 30
---

# User task

A User Task is a BPMN flow element that represents work performed by a human. It suspends process execution until a user completes the task and provides input or makes a decision.

## Key characteristics
- Requires human intervention:
	a User Task pauses the process instance and waits for a human to complete the work and submit a response.

- Can have incoming and outgoing sequence flows:
	User Tasks connect to other flow elements via sequence flows, allowing complex workflows with conditional routing based on task outcomes.

- Input and output parameters:
	User Tasks can receive data from the process and return data back to continue execution with user-provided information.

- Assignment rules:
	tasks can be assigned to specific users, groups, or dynamically based on process variables and expressions.

- Types of user task handling:
	- **Simple assignment:**
		task is assigned to a single user or group. The assigned user must claim and complete the task.

	- **Pool-based assignment:**
		task is assigned to a pool of candidates (e.g., all users in a department). Any member can claim and complete it.

	- **Dynamic assignment:**
		task assignment is determined at runtime based on process variables, expressions, or business rules.

	- **Escalation:**
		if a User Task is not completed within a specified time, it can be escalated to a manager or supervisor.

	- **Delegation:**
		the assigned user can delegate the task to another user while maintaining audit trail and original assignment context.

	- **Priority-based:**
		user asks can have priority levels (low, medium, high, critical) to help users prioritize their work queue.

## Graphical notation
![User task usage example](./../../assets/bpmn/user_task.svg)

A rectangle with a person icon in the top-left corner.

## XML Definition
```xml
<bpmn:userTask id="UserTask_1" name="Approve Request">
  <bpmn:incoming>Flow1</bpmn:incoming>
  <bpmn:outgoing>Flow2</bpmn:outgoing>
</bpmn:userTask>
```

## Current Implementation
User task is fully supported.