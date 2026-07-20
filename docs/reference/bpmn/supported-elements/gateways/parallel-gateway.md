---
sidebar_position: 60
---
# Parallel Gateway

A Parallel (AND) gateway splits the flow into all outgoing paths simultaneously and, when used as a merge, waits for all incoming branches to complete before continuing.

## Key characteristics

- **Diverging:** activates all outgoing paths unconditionally — no conditions needed.
- **Converging:** waits for tokens on all incoming paths before releasing the token forward.
- **Limitation:** works for a single overlapping parallel flow; nested/recursive parallel flows have undefined behaviour.

## Graphical notation

A diamond with a "+" icon.

<img src="/img/bpmn/gateways/parallel-gateway.svg" alt="Parallel gateway usage example" width="120" height="120" />

## XML Definition

```xml
<bpmn:parallelGateway id="forkChecks" name="Run checks in parallel">
  <bpmn:incoming>Flow_1</bpmn:incoming>
  <bpmn:outgoing>Flow_creditCheck</bpmn:outgoing>
  <bpmn:outgoing>Flow_idCheck</bpmn:outgoing>
</bpmn:parallelGateway>

<bpmn:parallelGateway id="joinChecks" name="All checks done">
  <bpmn:incoming>Flow_creditResult</bpmn:incoming>
  <bpmn:incoming>Flow_idResult</bpmn:incoming>
  <bpmn:outgoing>Flow_decision</bpmn:outgoing>
</bpmn:parallelGateway>
```

## Practical example

A loan application process runs a credit check and an identity verification simultaneously. Both must complete before the loan decision gateway is reached. The Parallel Gateway fork starts both, and the join waits for both results.

## Current Implementation

Supported. Handles parallel flows correctly when one parallel section is active at a time. Multiple nested or deeply recursive parallel flows may have undefined behaviour.
