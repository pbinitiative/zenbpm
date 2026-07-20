---
sidebar_position: 2
---
# Message Intermediate Throw Event

An intermediate event that sends a defined message and then continues execution without waiting.

## Key characteristics

- Has one incoming and one outgoing sequence flow.
- Sends the message and immediately continues — does not block.

## Graphical notation

A double-line circle with a filled envelope icon.

<img src={require('!url-loader!../../../../assets/bpmn/events/message-throw-event.svg').default} alt="Message throw event" width="120" height="120" />

## XML Definition

```xml
<bpmn:intermediateThrowEvent id="sendMsg" name="Send message">
  <bpmn:incoming>Flow_1</bpmn:incoming>
  <bpmn:outgoing>Flow_2</bpmn:outgoing>
  <bpmn:messageEventDefinition messageRef="Message_1" />
</bpmn:intermediateThrowEvent>
```

## Current Implementation

Supported.

