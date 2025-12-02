---
sidebar_position: 10
---

# End event

An End Event is a BPMN flow element that terminates a process instance or a token. It indicates where and how a process flow ends.

## Key characteristics
- Terminates the process or token
	When reached, the End Event finishes the active token and may end the process instance.

- No outgoing sequence flows
	By definition, it cannot have outgoing connections because it stops the flow.

- Multiple end events allowed
	A process can contain multiple End Events to indicate different termination points or results.

- Types of end events
	- None (normal termination)
	- Error
	- Terminate (stops the entire process)
	- Message
	- Signal
	- Escalation
	- Compensate
	- Conditional

## Graphical notation
![End event usage example](./../../assets/bpmn/end_event.svg)

A bold single-line circle (solid thick outline).

## XML Definition 
```xml
<bpmn:endEvent id="EndEvent_1" name="End" />
```

## Current Implementation
End event is fully supported.
