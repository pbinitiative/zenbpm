---
sidebar_position: 10
---

# End event

An End Event is a BPMN flow element that terminates a process instance or a token. It indicates where and how a process flow ends.

## Key characteristics
- Terminates the process or token:
	when reached, the End Event finishes the active token and may end the process instance.

- No outgoing sequence flows:
	by definition, it cannot have outgoing connections because it stops the flow.

- Multiple end events allowed:
	a process can contain multiple End Events to indicate different termination points or results.

- Types of end events:
	- **None:** (normal termination)
		terminates the process or token without any special behavior. The simplest form of End Event used for regular process completion.

	- **Error:**
		throws an error that can be caught by an Error Boundary Event on an enclosing scope (subprocess, process). Used to signal abnormal termination within error handling flows.

	- **Terminate:** (stops the entire process)
		immediately terminates the entire process instance, regardless of active tokens or subprocesses. All other tokens and activities are forcefully ended.

	- **Message:**
		sends a message to another process or external system when the End Event is reached. Enables process-to-process communication and integration scenarios.

	- **Signal:**
		broadcasts a signal that can be caught by Signal Boundary Events, Signal Intermediate Events, or Signal Start Events in other processes or the same process. Useful for cross-process signaling and event broadcasting.

	- **Escalation:**
		escalates an escalation that can be caught by an Escalation Boundary Event on an enclosing scope. Typically used to bubble up issues for higher-level handling within nested scopes.

	- **Compensation:**
		triggers compensation of activities in the current scope. Used in compensation workflows to undo or reverse previous actions.

	- **Cancel:**
		used only within a Transaction Subprocess. Cancels the transaction and triggers any defined compensation handlers.

	- **Multiple:**
		represents multiple triggers in one End Event (logical OR). Any one of the triggers can complete the event.

	- **Parallel Multiple:**
		represents multiple triggers in one End Event (logical AND). All triggers must occur to complete the event.


	

## Graphical notation
![End event usage example](./../../assets/bpmn/end_event.svg)

A bold single-line circle (solid thick outline).

## XML Definition 
```xml
<bpmn:endEvent id="EndEvent_1" name="End" />
```

## Current Implementation
End event is fully supported.
