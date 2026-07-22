---
sidebar_position: 20
---

# Inclusive gateway

An Inclusive (OR) gateway activates **every** outgoing path whose condition is true — from one to all of them — letting the matching branches run in parallel. As a join, it waits for exactly the branches that are still active before continuing.

<img src={require('!url-loader!../../../assets/bpmn/gateways/inclusive-gateway.svg').default} alt="Inclusive gateway" width="110" height="110" />

Rendered as a diamond with a circle marker.

## Use cases

- **Any combination of actions** — send an e-mail, an SMS, or both, depending on which notification channels the customer opted into.
- **Conditional side branches** — run an extra compliance check in parallel with the main flow only for high-value orders, and join both paths afterwards.
- **Synchronize what actually ran** — merge the branches of an earlier inclusive fork without knowing in advance how many of them were activated.

## Usage in BPMN

The markup is the same as for the [Exclusive gateway](./exclusive-gateway.md) — conditions on the outgoing flows plus an optional default flow — but every condition is evaluated:

| Markup                     | Where                        | Required | Description                                                                                                     |
| -------------------------- | ---------------------------- | -------- | ---------------------------------------------------------------------------------------------------------------- |
| `bpmn:conditionExpression` | outgoing `bpmn:sequenceFlow` | yes, on every non-default flow | A FEEL expression prefixed with `=` that must evaluate to a boolean.                                            |
| `default`                  | the gateway element          | no       | References the fallback flow, taken only when **no** condition evaluates to true.                               |

Execution flow when diverging:

1. A token arrives and **all** outgoing flows' conditions are evaluated against the current process variables — a true condition does not stop the evaluation of the others.
2. A separate token is created for every flow whose condition is true, and the activated branches run in parallel.
3. If no condition matches, the `default` flow is taken; without a default flow, the token fails and an incident is recorded.

Execution flow when converging (multiple incoming flows):

1. An arriving token waits at the gateway while any other active token in the process instance **can still reach** the gateway through the process graph.
2. When no more tokens can arrive, the gateway fires: the waiting tokens are merged and the outgoing flows are activated according to their conditions, as above.

Unlike the [Parallel gateway](./parallel-gateway.md), the join does not need tokens on *all* incoming flows — it synchronizes exactly the branches the earlier fork actually activated.

## Related documentation

- [Conditional flow](../flows/conditional-flow.md) and [Default flow](../flows/default-flow.md) — the sequence flow markup in detail.
- [Exclusive gateway](./exclusive-gateway.md) — when exactly one path should be taken.
- [Parallel gateway](./parallel-gateway.md) — when all paths should always run.

## XML example

A notification step that e-mails, texts, or both, depending on the customer's preferences, with a push notification as the fallback; a second inclusive gateway joins whichever branches ran:

```xml
<bpmn:inclusiveGateway id="Gateway_Notify" name="Which channels?" default="Flow_Default">
  <bpmn:incoming>Flow_In</bpmn:incoming>
  <bpmn:outgoing>Flow_Email</bpmn:outgoing>
  <bpmn:outgoing>Flow_Sms</bpmn:outgoing>
  <bpmn:outgoing>Flow_Default</bpmn:outgoing>
</bpmn:inclusiveGateway>

<bpmn:sequenceFlow id="Flow_Email" sourceRef="Gateway_Notify" targetRef="Task_SendEmail">
  <bpmn:conditionExpression>= customer.emailOptIn</bpmn:conditionExpression>
</bpmn:sequenceFlow>
<bpmn:sequenceFlow id="Flow_Sms" sourceRef="Gateway_Notify" targetRef="Task_SendSms">
  <bpmn:conditionExpression>= customer.smsOptIn</bpmn:conditionExpression>
</bpmn:sequenceFlow>
<bpmn:sequenceFlow id="Flow_Default" sourceRef="Gateway_Notify" targetRef="Task_SendPush" />

<bpmn:inclusiveGateway id="Gateway_NotifyDone" name="Notifications sent">
  <bpmn:incoming>Flow_EmailDone</bpmn:incoming>
  <bpmn:incoming>Flow_SmsDone</bpmn:incoming>
  <bpmn:incoming>Flow_PushDone</bpmn:incoming>
  <bpmn:outgoing>Flow_Out</bpmn:outgoing>
</bpmn:inclusiveGateway>
```
