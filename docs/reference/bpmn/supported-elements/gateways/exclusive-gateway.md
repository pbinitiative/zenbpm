---
sidebar_position: 80
---
# Exclusive Gateway

An Exclusive (XOR) gateway routes the flow down exactly one outgoing path, chosen by evaluating conditions on each outgoing sequence flow.

## Key characteristics

- **Diverging:** evaluates outgoing conditions and takes exactly one matching path.
- If multiple conditions are true, the first in model order wins.
- A **default flow** can be defined as a fallback when no condition matches.
- **Converging:** forwards the first arriving token without waiting for others.

## Graphical notation

A diamond with an "X" icon.

<img src="/img/bpmn/gateways/exclusive-gateway.svg" alt="Exclusive gateway usage example" width="120" height="120" />

## XML Definition

```xml
<bpmn:exclusiveGateway id="checkAmount" name="Amount?" default="Flow_default">
  <bpmn:incoming>Flow_1</bpmn:incoming>
  <bpmn:outgoing>Flow_high</bpmn:outgoing>
  <bpmn:outgoing>Flow_default</bpmn:outgoing>
</bpmn:exclusiveGateway>

<bpmn:sequenceFlow id="Flow_high" sourceRef="checkAmount" targetRef="Task_ManualReview">
  <bpmn:conditionExpression>= amount > 10000</bpmn:conditionExpression>
</bpmn:sequenceFlow>
<bpmn:sequenceFlow id="Flow_default" sourceRef="checkAmount" targetRef="Task_AutoApprove" />
```

## Practical example

An order approval process routes orders over €10,000 to a manual review task, while smaller orders are auto-approved. The Exclusive Gateway evaluates the `amount` variable and picks exactly one outgoing path.

## Current Implementation

Fully supported, including default flows and FEEL condition expressions.
