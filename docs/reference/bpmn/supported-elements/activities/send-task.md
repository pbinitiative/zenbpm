---
sidebar_position: 41
---
# Send task

A Send Task sends a defined message and then immediately continues execution without waiting for a response.

## Key characteristics

- One incoming and one outgoing sequence flow.
- Sends the message and moves on — does not block the token.
- Functionally equivalent to a Message Intermediate Throw Event, but modelled as a task.

## Graphical notation

A rounded rectangle with a filled envelope icon in the top-left.

<img src="/img/bpmn/cs_sendTask.svg" width="160" />

## XML Definition

```xml
<bpmn:sendTask id="sendConfirmation" name="Send confirmation" messageRef="Message_1">
  <bpmn:incoming>Flow_1</bpmn:incoming>
  <bpmn:outgoing>Flow_2</bpmn:outgoing>
</bpmn:sendTask>

<bpmn:message id="Message_1" name="OrderConfirmation" />
```

## Current Implementation

*To confirm with the team.*
