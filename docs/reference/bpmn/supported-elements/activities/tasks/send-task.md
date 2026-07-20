---
sidebar_position: 40
---

# Send task

A Send Task is a BPMN flow element that sends a defined message and immediately continues execution without waiting for a response. It is functionally equivalent to a Message Intermediate Throw Event but modelled as a task shape.

## Key characteristics
- Fire-and-forget messaging:
	A Send Task sends the message and moves on immediately — it does not block the token waiting for a reply.

- Can have incoming and outgoing sequence flows:
	Send Tasks connect to other flow elements via sequence flows, allowing complex workflows with conditional routing.

- Message correlation:
	The message sent can trigger a Message Start Event or be correlated to a waiting Message Catch Event or Receive Task in another process.

## Graphical notation

A rounded rectangle with a filled envelope icon in the top-left corner.

<img src="/img/bpmn/activities/send-task.svg" alt="Send Task" width="110" height="90" />

## XML Definition
```xml
<bpmn:sendTask id="SendTask_1" name="Send order confirmation" messageRef="Message_OrderConfirmation">
  <bpmn:incoming>Flow1</bpmn:incoming>
  <bpmn:outgoing>Flow2</bpmn:outgoing>
</bpmn:sendTask>

<bpmn:message id="Message_OrderConfirmation" name="OrderConfirmation" />
```

## Current Implementation
Supported.
