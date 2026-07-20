---
sidebar_position: 1
---
# Message Intermediate Catch Event

An intermediate event that pauses the flow and waits until a defined message is received, then continues.

## Key characteristics

- Has one incoming and one outgoing sequence flow.
- Blocks the token until the matching message arrives.
- Correlated to an incoming message by its message definition.

## Graphical notation

A double-line circle with an unfilled envelope icon.

<img src={require('!url-loader!../../../../assets/bpmn/events/message-catch-event.svg').default} alt="Message catch event" width="120" height="120" />

## XML Definition

```xml
<bpmn:intermediateCatchEvent id="receiveMsg" name="Wait for message">
  <bpmn:incoming>Flow_1</bpmn:incoming>
  <bpmn:outgoing>Flow_2</bpmn:outgoing>
  <bpmn:messageEventDefinition messageRef="Message_1" />
</bpmn:intermediateCatchEvent>
```

## Current Implementation

Supported.

