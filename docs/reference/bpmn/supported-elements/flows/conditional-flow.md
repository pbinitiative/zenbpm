---
sidebar_position: 3
---
# Conditional flow

A Conditional Flow is a sequence flow with an attached condition expression, leaving an activity. The condition determines whether the token takes the flow.

## Key characteristics

- Carries a condition expression that evaluates to `true` or `false`.
- Used on outgoing flows of Exclusive and Inclusive gateways.
- If the condition is true, the token follows this flow.

## Graphical notation

A sequence flow arrow with a small diamond marker near the source.

![Conditional flow](../../../assets/bpmn/flows/conditional-flow.svg)

## XML Definition

```xml
<bpmn:sequenceFlow id="Flow_high" sourceRef="Gateway_1" targetRef="Task_highValue">
  <bpmn:conditionExpression>=amount > 1000</bpmn:conditionExpression>
</bpmn:sequenceFlow>
```

## Current Implementation

Not supported. A condition expression on a flow leaving an activity is not evaluated. Condition expressions are only evaluated on the outgoing flows of an [Exclusive gateway](../gateways/exclusive-gateway.md) or [Inclusive gateway](../gateways/inclusive-gateway.md) — use a gateway to route on conditions.
