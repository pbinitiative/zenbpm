---
sidebar_position: 3
---
# Conditional flow

A Conditional Flow is a sequence flow with an attached condition expression. It is evaluated by an Exclusive or Inclusive gateway to determine whether the flow is taken.

## Key characteristics

- Carries a condition expression that evaluates to `true` or `false`.
- Used on outgoing flows of Exclusive and Inclusive gateways.
- If the condition is true, the token follows this flow.

## Graphical notation

A sequence flow arrow with a small diamond marker near the source.

![Conditional flow](/img/bpmn/flows/conditional-flow.png)

## XML Definition

```xml
<bpmn:sequenceFlow id="Flow_high" sourceRef="Gateway_1" targetRef="Task_highValue">
  <bpmn:conditionExpression>=amount > 1000</bpmn:conditionExpression>
</bpmn:sequenceFlow>
```

## Current Implementation

Supported.
