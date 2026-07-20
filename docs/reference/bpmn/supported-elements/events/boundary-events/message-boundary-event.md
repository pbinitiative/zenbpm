---
sidebar_position: 1
---
# Message Boundary Event

A boundary event attached to an activity that triggers when a specific named message is received while the activity is active.

## Key characteristics

- Attached to an activity; has no incoming flow and one outgoing flow.
- The message is matched by name and can carry payload variables into the process.
- **Interrupting:** The attached activity is cancelled and the token moves to the boundary event's outgoing flow.
- **Non-interrupting:** The attached activity continues and a new token is created on the boundary event's outgoing flow.

## Graphical notation

A circle on the activity border with an envelope icon. Solid border = interrupting; dashed border = non-interrupting.

<div style={{"display": "flex", "gap": "24px", "alignItems": "flex-start"}}>

<img src="/img/bpmn/events/message-boundary-event.svg" alt="Interrupting" width="120" height="120" />
<img src="/img/bpmn/events/message-boundary-event-non-interrupting.svg" alt="Non-interrupting" width="120" height="120" />

</div>

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
