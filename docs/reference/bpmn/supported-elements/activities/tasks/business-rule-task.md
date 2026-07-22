---
sidebar_position: 30
---

# Business rule task

A Business Rule Task evaluates a business decision and makes the result available to the process. It delegates decision logic — eligibility checks, scoring, pricing — to a rule engine instead of hard-coding it into the process flow.

<img src={require('!url-loader!../../../../assets/bpmn/activities/business-rule-task.svg').default} alt="Business rule task" width="110" height="90" />

Rendered as a rounded rectangle with a table icon in the top-left corner.

## Use cases

- **Scoring** — calculate a credit or risk score before approving an order, claim, or loan.
- **Eligibility checks** — decide whether a customer qualifies for a discount or a claim can be settled automatically.
- **Dynamic calculations** — derive prices, fees, or routing priorities from decision tables instead of hard-coded process logic.

## Usage in BPMN

The implementation type is selected by the extension element present on the task: `zenbpm:calledDecision` evaluates a deployed DMN decision with the internal [DMN engine](../../../../dmn/dmn-engine.md), while `zenbpm:taskDefinition` delegates the decision to an external [job worker](#job-based).

### Called decision

To evaluate a decision internally, reference a deployed DMN decision in a `zenbpm:calledDecision` extension element. Optionally, control the data that flows into and out of the task with a `zenbpm:ioMapping`.

| Extension element                    | Attribute          | Required | Description                                                                                                                          |
| ------------------------------------ | ------------------ | -------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `zenbpm:calledDecision`              | `decisionId`       | yes      | Id of the deployed DMN decision to evaluate. Can be prefixed with the DMN resource id (`myDrd.myDecision`) to disambiguate decisions with the same id. |
| `zenbpm:calledDecision`              | `resultVariable`   | yes      | Name of the local variable that receives the decision output.                                                                          |
| `zenbpm:ioMapping` → `zenbpm:input`  | `source`, `target` | no       | Maps process variables into the task's local scope, used as the decision input context. See [Variables](../../../variable-mapping.md).        |
| `zenbpm:ioMapping` → `zenbpm:output` | `source`, `target` | no       | Maps the decision result back to the process scope. See [Variables](../../../variable-mapping.md).                                            |

Execution flow:

1. A token arrives at the Business Rule Task and input mappings are evaluated into the task's local scope.
2. The internal DMN engine looks up the latest deployed version of the decision with the configured `decisionId` and evaluates it with the local variables as input.
3. The decision output is stored in the local variable named by `resultVariable`.
4. Output mappings are applied and the token moves on. **Without output mappings, the decision result is not propagated to the process scope** — map the result variable to keep it.
5. If the decision cannot be found or evaluation fails, the task fails.

### Job-based

With a `zenbpm:taskDefinition` extension element the decision is evaluated by an external job worker instead of the internal DMN engine. The task is configured and executed exactly like a [Service task](./service-task.md) — see the [Service task usage](./service-task.md#usage-in-bpmn) for the configuration and XML example.

## Related documentation

- [DMN engine](../../../../dmn/dmn-engine.md) — how decisions are deployed and evaluated by the internal engine.
- [DMN supported elements](../../../../dmn/supported-elements/index.md) — decision tables, literal expressions, and other DMN elements a called decision can use.
- [Variables](../../../variable-mapping.md) — variable scoping and output mapping propagation rules for activities.
- [Jobs](../../../../jobs.md) — how jobs for the job-based implementation are created, distributed, and completed.
- [Implement a job worker](../../../../../how-to/implement-job-worker.md) — build the application that evaluates job-based decisions.
- [Error boundary event](../../events/error-events.md#error-boundary-event) — routing the process when the decision evaluation fails.
- [Multi-instance activity](../activity-multi-instance.md) — evaluating the decision once per element of a collection.

## XML example

A Business Rule Task that evaluates the decision `can_auto_liquidate` with the internal DMN engine. The input mapping provides the claim amount as decision input, the result is stored in `autoLiquidation`, and the output mapping propagates it to the process variable `claimDecision`.

```xml
<bpmn:businessRuleTask id="Activity_CheckClaim" name="Check auto-liquidation">
  <bpmn:extensionElements>
    <zenbpm:calledDecision decisionId="can_auto_liquidate" resultVariable="autoLiquidation" />
    <zenbpm:ioMapping>
      <zenbpm:input source="=claim.amount" target="amountOfDamage" />
      <zenbpm:output source="=autoLiquidation" target="claimDecision" />
    </zenbpm:ioMapping>
  </bpmn:extensionElements>
  <bpmn:incoming>Flow_In</bpmn:incoming>
  <bpmn:outgoing>Flow_Out</bpmn:outgoing>
</bpmn:businessRuleTask>
```
