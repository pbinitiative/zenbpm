---
sidebar_position: 10
---

# Exclusive gateway

An Exclusive (XOR) gateway routes the token down **exactly one** outgoing path. The conditions on the outgoing sequence flows are evaluated in the order the flows are defined, and the first one that evaluates to true wins; a default flow serves as the fallback when none matches.

<img src={require('!url-loader!../../../assets/bpmn/gateways/exclusive-gateway.svg').default} alt="Exclusive gateway" width="110" height="110" />

Rendered as a diamond with an X marker.

## Use cases

- **Business decisions** — route orders above €10,000 to manual review and everything else to auto-approval, based on a process variable.
- **Branch on an earlier result** — after a Business rule task, continue along the path matching the decision outcome.
- **Merge alternative paths** — reunite the branches of an earlier decision back into a single flow before the next common step.

## Usage in BPMN

The decision is configured on the outgoing sequence flows, not on the gateway:

| Markup                     | Where                        | Required | Description                                                                                                                                    |
| -------------------------- | ---------------------------- | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `bpmn:conditionExpression` | outgoing `bpmn:sequenceFlow` | yes, on every non-default flow | A FEEL expression prefixed with `=` that must evaluate to a boolean, e.g. `= amount > 10000`.                                                  |
| `default`                  | the gateway element          | no       | References the fallback flow, taken when no condition evaluates to true. The default flow carries no condition expression.                     |

Execution flow when diverging:

1. A token arrives and the outgoing flows' conditions are evaluated against the current process variables, **in the order the flows are defined in the XML**.
2. The first condition that evaluates to `true` wins — remaining conditions are not evaluated, and the token continues along that single flow.
3. If no condition matches, the `default` flow is taken.
4. If no condition matches and no default flow is defined — or a condition does not evaluate to a boolean — the token fails and an incident is recorded.

A flow without a condition expression is only followed when it is the gateway's single outgoing flow; with several outgoing flows, every non-default flow needs a condition.

When converging, an Exclusive gateway performs **no synchronization**: each arriving token is passed straight to the outgoing flow, independently of the others.

## Related documentation

- [Conditional flow](../flows/conditional-flow.md) and [Default flow](../flows/default-flow.md) — the sequence flow markup in detail.
- [Inclusive gateway](./inclusive-gateway.md) — when more than one matching path should be taken.
- [Business rule task](../activities/tasks/business-rule-task.md) — evaluating a DMN decision whose result the gateway branches on.

## XML example

An order approval decision: orders above 10 000 go to manual review, everything else takes the default flow to auto-approval:

```xml
<bpmn:exclusiveGateway id="Gateway_CheckAmount" name="Amount?" default="Flow_Default">
  <bpmn:incoming>Flow_In</bpmn:incoming>
  <bpmn:outgoing>Flow_High</bpmn:outgoing>
  <bpmn:outgoing>Flow_Default</bpmn:outgoing>
</bpmn:exclusiveGateway>

<bpmn:sequenceFlow id="Flow_High" sourceRef="Gateway_CheckAmount" targetRef="Task_ManualReview">
  <bpmn:conditionExpression>= amount > 10000</bpmn:conditionExpression>
</bpmn:sequenceFlow>
<bpmn:sequenceFlow id="Flow_Default" sourceRef="Gateway_CheckAmount" targetRef="Task_AutoApprove" />
```
