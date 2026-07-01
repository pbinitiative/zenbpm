---
sidebar_position: 4
---
# Message Start Event

A Message Start Event starts a process instance when a specific named message is received by the engine.

## Key characteristics

- The process instance is created when the engine receives a message matching the defined message name.
- The message can carry payload variables that are passed into the new process instance.
- Different processes can listen for different messages, or multiple processes can listen for the same message.

## Graphical notation

A thin single-line circle with an unfilled envelope icon inside.

<img src="/img/bpmn/events/message-start-event.svg" alt="Message start event" width="120" height="120" />

## Configuration

| Field        | Format | Example          |
|--------------|--------|------------------|
| Message name | String | `OrderReceived`  |

## XML Definition

```xml
<bpmn:message id="Message_1" name="OrderReceived" />

<bpmn:startEvent id="StartEvent_1" name="Order received">
  <bpmn:outgoing>Flow_1</bpmn:outgoing>
  <bpmn:messageEventDefinition id="MessageEventDefinition_1" messageRef="Message_1" />
</bpmn:startEvent>
```

## Current Implementation

Supported.

