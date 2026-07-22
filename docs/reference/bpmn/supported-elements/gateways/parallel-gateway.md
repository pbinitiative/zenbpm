---
sidebar_position: 30
---

# Parallel gateway

A Parallel (AND) gateway splits the flow into **all** outgoing paths at once — no conditions involved — and, as a join, waits until a token has arrived on **every** incoming flow before continuing. Fork and join are usually used as a pair enclosing the parallel section.

<img src={require('!url-loader!../../../assets/bpmn/gateways/parallel-gateway.svg').default} alt="Parallel gateway" width="110" height="110" />

Rendered as a diamond with a plus marker.

## Use cases

- **Independent work in parallel** — run a credit check and an identity verification at the same time instead of one after the other.
- **Synchronize before a decision** — wait until all parallel checks have completed before the process continues to the loan decision.
- **Fan out notifications** — trigger several unconditional side activities at once after a milestone.

## Usage in BPMN

A Parallel gateway needs no configuration — no conditions, no default flow. Condition expressions on its outgoing flows are ignored.

Execution flow when diverging:

1. A token arrives at the gateway.
2. A separate token is created for every outgoing sequence flow, unconditionally, and all branches run in parallel.

Execution flow when converging (multiple incoming flows):

1. Each arriving token waits at the gateway.
2. When tokens have arrived on **all** incoming flows, the gateway fires: the waiting tokens are consumed and one token is created per outgoing flow.

For running the *same* activity many times in parallel — once per element of a collection — use a [multi-instance activity](../activities/activity-multi-instance.md) instead of modelling parallel branches explicitly.

:::note[Limitation]
The join synchronizes correctly for a single parallel section at a time. Nested or overlapping parallel sections flowing through the same joining gateway concurrently have undefined behaviour.
:::

## Related documentation

- [Inclusive gateway](./inclusive-gateway.md) — parallel branches activated by conditions instead of unconditionally.
- [Multi-instance activity](../activities/activity-multi-instance.md) — data-driven parallelism over a collection.

## XML example

A loan application forks into a credit check and an identity check; the joining gateway waits for both results before the decision:

```xml
<bpmn:parallelGateway id="Gateway_ForkChecks" name="Run checks">
  <bpmn:incoming>Flow_In</bpmn:incoming>
  <bpmn:outgoing>Flow_CreditCheck</bpmn:outgoing>
  <bpmn:outgoing>Flow_IdCheck</bpmn:outgoing>
</bpmn:parallelGateway>

<bpmn:parallelGateway id="Gateway_JoinChecks" name="All checks done">
  <bpmn:incoming>Flow_CreditResult</bpmn:incoming>
  <bpmn:incoming>Flow_IdResult</bpmn:incoming>
  <bpmn:outgoing>Flow_Decision</bpmn:outgoing>
</bpmn:parallelGateway>
```
