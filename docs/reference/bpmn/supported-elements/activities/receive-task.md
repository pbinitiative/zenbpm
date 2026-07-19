---
sidebar_position: 42
---
# Receive task

A Receive Task pauses the process and waits until a specific named message is received, then continues execution.

## Key characteristics

- One incoming and one outgoing sequence flow.
- Blocks the token until the matching message arrives.
- Functionally equivalent to a Message Intermediate Catch Event, but modelled as a task.

## Graphical notation

A rounded rectangle with an envelope icon in the top-left.

<img src="/img/bpmn/cs_receiveTask.svg" width="160" />

## XML Definition

```xml
<bpmn:receiveTask id="waitForPayment" name="Wait for payment" messageRef="Message_1">
  <bpmn:incoming>Flow_1</bpmn:incoming>
  <bpmn:outgoing>Flow_2</bpmn:outgoing>
</bpmn:receiveTask>

<bpmn:message id="Message_1" name="PaymentReceived" />
```

## Current Implementation

*To confirm with the team.*
