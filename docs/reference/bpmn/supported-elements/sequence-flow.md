---
sidebar_position: 4
---

# Sequence flow

A Sequence Flow connects two BPMN elements and defines the order of execution: when the source element completes, the token moves along the flow to the target. Sequence flows are what turn individual activities, events, and gateways into a process.

<img src={require('!url-loader!../../assets/bpmn/flows/sequence-flow.svg').default} alt="Sequence flow" width="150" height="50" />

Rendered as a solid arrow from the source element to the target element.

## Usage in BPMN

| Markup              | Attribute   | Required | Description                                   |
| ------------------- | ----------- | -------- | ----------------------------------------------- |
| `bpmn:sequenceFlow` | `id`        | yes      | Unique identifier of the flow.                |
| `bpmn:sequenceFlow` | `sourceRef` | yes      | The element the token leaves.                 |
| `bpmn:sequenceFlow` | `targetRef` | yes      | The element the token moves to.               |

Rules:

- Every flow element needs at least one incoming and one outgoing sequence flow — except start events (no incoming) and end events (no outgoing).
- When an element other than a gateway has **several outgoing flows, all of them are followed**: a token is created per flow, forming an implicit parallel fork. Use [gateways](./gateways/index.md) to route deliberately instead.
- Condition expressions are evaluated **only on flows leaving an [Exclusive](./gateways/exclusive-gateway.md) or [Inclusive gateway](./gateways/inclusive-gateway.md)** — a condition on any other sequence flow is ignored.

## Default flow

<img src={require('!url-loader!../../assets/bpmn/flows/default-flow.svg').default} alt="Default flow" width="150" height="50" />

Rendered with a short diagonal slash across the flow near its source.

The default flow is the fallback path of an [Exclusive](./gateways/exclusive-gateway.md) or [Inclusive gateway](./gateways/inclusive-gateway.md). It is referenced by the gateway's `default` attribute, carries no condition expression of its own, and is taken only when none of the gateway's other outgoing conditions evaluates to true.

:::note[Not yet supported]

- **Conditional flow** — a condition expression on a flow leaving an activity is not evaluated; route on conditions with an Exclusive or Inclusive gateway instead.
- **Message flow** — the collaboration-diagram connector between pools; message exchange is modelled with [message events](./events/message-events.md) and [Send](./activities/tasks/send-task.md)/[Receive tasks](./activities/tasks/receive-task.md) instead.
- **Data association** — the dotted connector between data objects and activities; data is passed through [variables](../variables.md) instead.

:::

## Related documentation

- [Gateways](./gateways/index.md) — deliberate routing: conditions, default flows, forks, and joins.
- [Link events](./events/link-events.md) — connecting distant points of a diagram without drawing a sequence flow.

## XML example

A task connected to a gateway, which routes onward through a conditional flow or its default flow:

```xml
<bpmn:sequenceFlow id="Flow_ToCheck" sourceRef="Task_PrepareOrder" targetRef="Gateway_CheckAmount" />

<bpmn:exclusiveGateway id="Gateway_CheckAmount" name="Amount?" default="Flow_Default">
  <bpmn:incoming>Flow_ToCheck</bpmn:incoming>
  <bpmn:outgoing>Flow_High</bpmn:outgoing>
  <bpmn:outgoing>Flow_Default</bpmn:outgoing>
</bpmn:exclusiveGateway>

<bpmn:sequenceFlow id="Flow_High" sourceRef="Gateway_CheckAmount" targetRef="Task_ManualReview">
  <bpmn:conditionExpression>= amount > 10000</bpmn:conditionExpression>
</bpmn:sequenceFlow>
<bpmn:sequenceFlow id="Flow_Default" sourceRef="Gateway_CheckAmount" targetRef="Task_AutoApprove" />
```
