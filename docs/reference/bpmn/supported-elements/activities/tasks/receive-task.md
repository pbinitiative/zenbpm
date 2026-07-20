---
sidebar_position: 50
---

# Receive task

A Receive Task is a BPMN flow element that pauses the process and waits until a specific named message is received, then continues execution. It is functionally equivalent to a Message Intermediate Catch Event but modelled as a task shape.

## Key characteristics
- Blocking wait for a message:
	A Receive Task blocks the token until the matching message arrives.

- Can have incoming and outgoing sequence flows:
	Receive Tasks connect to other flow elements via sequence flows, allowing complex workflows with conditional routing.

- Message correlation:
	The expected message is correlated by name and an optional correlation key, so the correct waiting instance receives it.

## Graphical notation

A rounded rectangle with an outline envelope icon in the top-left corner.

<img src={require('!url-loader!../../../../assets/bpmn/activities/receive-task.svg').default} alt="Receive Task" width="110" height="90" />

## XML Definition
```xml
<bpmn:receiveTask id="ReceiveTask_1" name="Wait for payment confirmation" messageRef="Message_PaymentConfirmed">
  <bpmn:incoming>Flow1</bpmn:incoming>
  <bpmn:outgoing>Flow2</bpmn:outgoing>
</bpmn:receiveTask>

<bpmn:message id="Message_PaymentConfirmed" name="PaymentConfirmed" />
```

## Current Implementation
Supported.
