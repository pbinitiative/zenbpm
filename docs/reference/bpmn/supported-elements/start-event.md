---
sidebar_position: 1
---
# Start event

A Start Event is a BPMN flow element that marks the point where a process or subprocess begins execution. It indicates when and under what conditions a process instance is created.

## Key characteristics
- Creates a new process instance
    A Start Event signals the initiation of a process. Each trigger results in one new instance.

- No incoming sequence flows
  By definition, it cannot have incoming connections, because nothing precedes the start.

- Exactly one required in a top-level process
  A process must have at least one Start Event.2
  Subprocesses may have multiple start events (for event subprocesses).

- **Type of trigger determines activation**
  Depending on the type, Start Events react to specific triggers and create new process instances when those triggers occur:

  - **None** (manual start):
    The simplest start event — a process instance is created manually (via user action or API) without an external event.

  - **Message:**
    Starts a process when a message is received. Useful for integrating with external systems and event-driven architectures.

  - **Timer:**
    Starts instances based on time: at a specific date/time, after a duration, or on a repeating schedule (cron-based schedule). 

  - **Error** (event subprocess):    Used within event subprocesses to start a subprocess in response to an error condition thrown inside the same scope.

  - **Signal:**
    Starts instances in response to a broadcast signal. Signals are global (within the engine/cluster) and can start multiple process instances that listen for the same signal.

  - **Escalation:**
    Starts an event subprocess when an escalation is raised. Typically used to escalate issues to higher-level part of the process.

  - **Conditional:**
    Starts a process when a specified condition evaluates to true (based on process variables or external state). Useful for reactive or rule-based instantiation.

  - **Compensation:**
    Starts compensation handlers for previously completed activities when compensation is triggered for the scope.

  - **Multiple triggers:**
    A Start Event can be modeled with multiple event definitions (or a process can have multiple Start Events). Each defined trigger can independently create instances when its condition is met.

  **Allowed contexts**
  - **Allowed in main process:**
    - None
    - Message
    - Timer
    - Conditional
    - Signal

  - **Allowed only in Event Subprocess:**
    - Error
    - Escalation
    - Compensation

## Graphical notation
![Start event usage example](./../../assets/bpmn/start_event.png)

A thin single-line circle.

## XML Definition
```xml
<bpmn:startEvent id="startEvent" name="Start">
  <bpmn:outgoing>MyFlow</bpmn:outgoing>
</bpmn:startEvent>
```

## Current Implementation
Start event is fully supported.
