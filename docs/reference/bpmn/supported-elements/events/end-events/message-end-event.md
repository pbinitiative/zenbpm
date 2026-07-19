---
sidebar_position: 2
---
# Message End Event

A Message End Event sends a defined message before ending the path of execution.

## Key characteristics

- Has no outgoing sequence flows.
- Sends the defined message and then ends the path.
- Useful for notifying external systems or triggering other processes upon completion.

## Graphical notation

A thick single-line circle with a filled envelope icon.

<img src="/img/bpmn/UNI_endEventMessage.svg" width="130" />

## XML Definition

```xml
<bpmn:endEvent id="sendAndEnd" name="Send and end">
  <bpmn:incoming>Flow_1</bpmn:incoming>
  <bpmn:messageEventDefinition messageRef="Message_1" />
</bpmn:endEvent>

<bpmn:message id="Message_1" name="OrderCompleted" />
```

## Current Implementation

Supported.
