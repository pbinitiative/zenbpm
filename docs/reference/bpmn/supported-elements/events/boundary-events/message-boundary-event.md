---
sidebar_position: 1
---

# Message Boundary Event

A Message Boundary Event is attached to the boundary of an activity and is triggered when a specific named message is received while the activity is active.

## Key characteristics

- **Message-driven interruption:** When the defined message arrives, the boundary event fires and the flow continues along the boundary event's outgoing sequence flow.
- **Interrupting vs. non-interrupting:**
  - **Interrupting:** The attached activity is cancelled and the token moves to the boundary event's outgoing flow.
  - **Non-interrupting:** The attached activity continues executing and a new token is created on the boundary event's outgoing flow.
- **Message correlation:** The message is matched by name and can carry payload variables into the process.

## Graphical notation

A double-ring circle with an envelope icon, attached to the border of an activity.

<img src="/img/bpmn/UNI_intermediateEventMessageCatch.svg" width="130" /> <img src="/img/bpmn/UNI_intermediateEventMessageBoundary.svg" width="130" />

## XML Definition

```xml
<bpmn:boundaryEvent id="BoundaryEvent_1" attachedToRef="Activity_1">
  <bpmn:outgoing>Flow_1</bpmn:outgoing>
  <bpmn:messageEventDefinition id="MessageEventDefinition_1" messageRef="Message_1" />
</bpmn:boundaryEvent>

<bpmn:message id="Message_1" name="PaymentReceived" />
```

## Current Implementation

Supported.
