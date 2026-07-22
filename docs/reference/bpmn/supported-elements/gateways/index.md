---
sidebar_position: 1
---

# Gateways

A gateway controls how the process flow diverges and converges: it models decisions, forks, and joins by routing tokens along one or more sequence flows. A gateway performs no work of its own — a token passes through it immediately, and only the converging (joining) gateways ever make a token wait.

Gateways are rendered as a diamond; the marker inside indicates the type. Each type answers the routing question differently:

- **Diverging** (one incoming, several outgoing flows) — which outgoing paths receive a token: exactly one ([Exclusive](./exclusive-gateway.md)), every path whose condition is true ([Inclusive](./inclusive-gateway.md)), all of them ([Parallel](./parallel-gateway.md)), or the path of the first event to occur ([Event-based](./event-based-gateway.md)).
- **Converging** (several incoming, one outgoing flow) — whether to synchronize: pass each token through ([Exclusive](./exclusive-gateway.md)), wait for all incoming branches ([Parallel](./parallel-gateway.md)), or wait for exactly the branches that are still active ([Inclusive](./inclusive-gateway.md)).

## Types

Green icons are supported and link to their documentation.

<table className="bpmn-types-table">
  <thead>
    <tr>
      <th>Gateway</th>
      <th style={{width: '90px'}}>Icon</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><a href="./exclusive-gateway">Exclusive</a></td>
      <td><a href="./exclusive-gateway"><img className="bpmn-supported" src={require('!url-loader!../../../assets/bpmn/gateways/exclusive-gateway.svg').default} alt="Exclusive Gateway" height="70" /></a></td>
      <td>Routes the token down exactly one outgoing path — the first whose condition is true.</td>
    </tr>
    <tr>
      <td><a href="./inclusive-gateway">Inclusive</a></td>
      <td><a href="./inclusive-gateway"><img className="bpmn-supported" src={require('!url-loader!../../../assets/bpmn/gateways/inclusive-gateway.svg').default} alt="Inclusive Gateway" height="70" /></a></td>
      <td>Activates every outgoing path whose condition is true; as a join, waits for all branches that are still active.</td>
    </tr>
    <tr>
      <td><a href="./parallel-gateway">Parallel</a></td>
      <td><a href="./parallel-gateway"><img className="bpmn-supported" src={require('!url-loader!../../../assets/bpmn/gateways/parallel-gateway.svg').default} alt="Parallel Gateway" height="70" /></a></td>
      <td>Activates all outgoing paths unconditionally; as a join, waits for a token on every incoming flow.</td>
    </tr>
    <tr>
      <td><a href="./event-based-gateway">Event-based</a></td>
      <td><a href="./event-based-gateway"><img className="bpmn-supported" src={require('!url-loader!../../../assets/bpmn/gateways/event-based-gateway.svg').default} alt="Event-Based Gateway" height="70" /></a></td>
      <td>Routes the token along the path of whichever event occurs first, instead of evaluating data conditions.</td>
    </tr>
  </tbody>
</table>

:::note[Not yet supported]
Complex gateway
:::

## Usage in BPMN

The routing decision is not configured on the gateway itself but on its **outgoing sequence flows**:

| Markup                     | Where                        | Description                                                                                                                                                                                  |
| -------------------------- | ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bpmn:conditionExpression` | outgoing `bpmn:sequenceFlow` | A FEEL expression prefixed with `=` that must evaluate to a boolean, e.g. `= amount > 10000`. Evaluated against the process instance variables. See [Conditional flow](../flows/conditional-flow.md). |
| `default` attribute        | the gateway element          | References the fallback flow taken when no condition evaluates to true. The default flow itself carries no condition. See [Default flow](../flows/default-flow.md).                          |

Rules shared by the condition-evaluating gateways (Exclusive and Inclusive):

1. When a token arrives, the conditions of the outgoing flows are evaluated against the current process variables. Conditions must be written as `=`-prefixed FEEL expressions returning `true` or `false`.
2. A flow **without** a condition expression is only followed when it is the gateway's single outgoing flow — with several outgoing flows, leave only the default flow unconditioned.
3. If no condition matches and a `default` flow is defined, the default flow is taken.
4. If no condition matches and there is no default flow, the token fails and an **incident** is recorded.

The [Parallel gateway](./parallel-gateway.md) ignores conditions entirely, and the [Event-based gateway](./event-based-gateway.md) replaces conditions with competing catch events.

## Related documentation

- [Conditional flow](../flows/conditional-flow.md) — the condition expression markup on sequence flows.
- [Default flow](../flows/default-flow.md) — the fallback flow of Exclusive and Inclusive gateways.
- [Events overview](../events/index.mdx) — the catch events an Event-based gateway races.

## XML example

The shared markup on a condition-evaluating gateway: two conditional flows evaluated in order and a default flow referenced by the gateway's `default` attribute:

```xml
<bpmn:exclusiveGateway id="Gateway_Amount" name="Amount?" default="Flow_Default">
  <bpmn:incoming>Flow_In</bpmn:incoming>
  <bpmn:outgoing>Flow_High</bpmn:outgoing>
  <bpmn:outgoing>Flow_Medium</bpmn:outgoing>
  <bpmn:outgoing>Flow_Default</bpmn:outgoing>
</bpmn:exclusiveGateway>

<bpmn:sequenceFlow id="Flow_High" sourceRef="Gateway_Amount" targetRef="Task_ManualReview">
  <bpmn:conditionExpression>= amount > 10000</bpmn:conditionExpression>
</bpmn:sequenceFlow>
<bpmn:sequenceFlow id="Flow_Medium" sourceRef="Gateway_Amount" targetRef="Task_StandardReview">
  <bpmn:conditionExpression>= amount > 1000</bpmn:conditionExpression>
</bpmn:sequenceFlow>
<bpmn:sequenceFlow id="Flow_Default" sourceRef="Gateway_Amount" targetRef="Task_AutoApprove" />
```
