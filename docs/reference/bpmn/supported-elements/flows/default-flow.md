---
sidebar_position: 2
---
# Default flow

A Default Flow is a fallback sequence flow on an Exclusive or Inclusive gateway. It is taken when no other outgoing condition evaluates to true.

## Key characteristics

- Defined on an Exclusive or Inclusive gateway as the `default` attribute.
- Taken only when all other outgoing conditions are false.
- Has no condition expression of its own.

## Graphical notation

A sequence flow arrow with a short diagonal slash near the source.

![Default flow](../../../assets/bpmn/flows/default-flow.svg)

## XML Definition

```xml
<bpmn:exclusiveGateway id="Gateway_1" name="Check amount" default="Flow_default">
  <bpmn:outgoing>Flow_high</bpmn:outgoing>
  <bpmn:outgoing>Flow_default</bpmn:outgoing>
</bpmn:exclusiveGateway>

<bpmn:sequenceFlow id="Flow_default" sourceRef="Gateway_1" targetRef="Task_fallback" />
```

## Current Implementation

Supported.
