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

- Type of trigger determines activation
  Depending on the type, Start Events react to:
  - None (manual start)
  - Message
  - Timer
  - Error (event subprocess)
  - Signal
  - Escalation
  - Conditional
  - Compensation (event subprocess)
- Multiple triggers

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
