---
sidebar_position: 90
---
# Event-Based Gateway

An Event-Based gateway routes the flow based on which event occurs first. Instead of evaluating data conditions, it waits for competing events and takes the path of the winner.

## Key characteristics

- **Diverging only** — each outgoing path leads to a catching event (Message, Timer, etc.).
- The first event to occur determines the route; all other waiting branches are cancelled.
- Commonly used to model "wait for reply or timeout" patterns.
- **Limitation:** currently only for Message and Timer events.

## Graphical notation

A diamond with a pentagon-in-circle icon.

<img src="/img/bpmn/gateways/event-based-gateway.svg" alt="Event-based gateway usage example" width="120" height="120" />

## XML Definition

```xml
<bpmn:eventBasedGateway id="waitForReplyOrTimeout" name="Reply or timeout?">
  <bpmn:incoming>Flow_1</bpmn:incoming>
  <bpmn:outgoing>Flow_reply</bpmn:outgoing>
  <bpmn:outgoing>Flow_timeout</bpmn:outgoing>
</bpmn:eventBasedGateway>

<bpmn:intermediateCatchEvent id="waitReply" name="Reply received">
  <bpmn:incoming>Flow_reply</bpmn:incoming>
  <bpmn:outgoing>Flow_processReply</bpmn:outgoing>
  <bpmn:messageEventDefinition messageRef="Message_Reply" />
</bpmn:intermediateCatchEvent>

<bpmn:intermediateCatchEvent id="waitTimeout" name="48h timeout">
  <bpmn:incoming>Flow_timeout</bpmn:incoming>
  <bpmn:outgoing>Flow_escalate</bpmn:outgoing>
  <bpmn:timerEventDefinition>
    <bpmn:timeDuration>PT48H</bpmn:timeDuration>
  </bpmn:timerEventDefinition>
</bpmn:intermediateCatchEvent>
```

## Practical example

After sending a quote to a customer, the process waits at an Event-Based Gateway. If the customer replies within 48 hours, the flow continues to process the reply. If the timer fires first, the process escalates to a follow-up task.

## Current Implementation

Supported for **Message** and **Timer** events. Signal, Conditional, and Receive Task variants are not yet implemented.
