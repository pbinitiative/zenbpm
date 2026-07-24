---
sidebar_position: 1
---

# Decision requirements graph

A Decision Requirements Graph (DRG) is the set of decisions in a DMN file together with the requirements between them. A decision can require another decision — consuming its result as input — or require input data, declaring a value the caller has to supply. When ZenBPM evaluates a decision it evaluates the whole requirement subgraph beneath it, not just the decision itself.

<img src={require('!url-loader!../../assets/dmn/decision-requirements-graph.svg').default} alt="Decision requirements graph" width="480" height="130" />

Input data is rendered as an oval, decisions as rectangles, and information requirements as arrows pointing from the required element to the element that requires it.

## Use cases

- **Layer a complex decision** — derive a risk level from the raw data first, then decide the approval outcome from that risk level, instead of one wide table with every combination.
- **Reuse a sub-decision** — several decisions in the same file can require the same intermediate decision, which is evaluated once per requiring decision.
- **Declare required data** — input data states which variables a decision needs, so evaluation fails immediately with a clear error when a caller forgets one.

## Usage in DMN

Every `<decision>` in a deployed file becomes a decision definition that can be evaluated on its own — there is no separate "root" decision. Requirements are declared with `<informationRequirement>` elements on the decision that consumes them.

| Element                                              | Attribute | Required | Description                                                                                                                                                     |
| ---------------------------------------------------- | --------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `decision`                                           | `id`      | yes      | Identifies the decision. Referenced by `zenbpm:calledDecision` on a business rule task and by `requiredDecision` inside the same file.                            |
| `decision`                                           | `name`    | no       | Display name. Also the key under which this decision's result is exposed to decisions that require it; falls back to `id` when empty.                             |
| `decision` → `extensionElements` → `zenbpm:versionTag` | `value` | no       | Marks this version of the decision so it can be selected with the `versionTag` binding type. See [DMN engine](../dmn-engine.md#binding-types).                    |
| `informationRequirement` → `requiredDecision`        | `href`   | –        | `#` followed by the `id` of another decision **in the same DMN file**.                                                                                            |
| `informationRequirement` → `requiredInput`           | `href`   | –        | `#` followed by the `id` of an `inputData` element.                                                                                                               |
| `inputData`                                          | `id`      | yes      | Identifies the input data node, referenced by `requiredInput`.                                                                                                    |
| `inputData`                                          | `name`    | yes      | The variable name that must be present in the evaluation input. Evaluation fails when it is missing.                                                              |

Evaluation flow:

1. Evaluation starts at the decision selected by the caller — the `decisionId` of a [business rule task](../../bpmn/supported-elements/activities/tasks/business-rule-task.md) or of the REST evaluate request.
2. The decision's information requirements are resolved first, in document order, before its own logic runs.
3. For a `requiredInput`, the engine checks that a variable with the input data's `name` is present in the input variables. If it is missing, evaluation fails.
4. For a `requiredDecision`, the required decision is evaluated recursively with the **original input variables** — it does not see values produced by its siblings. Its result is then merged into the requiring decision's variable context.
5. The requiring decision's own logic runs against the input variables plus the results of its required decisions.
6. The result of the starting decision is returned, together with an ordered list of every decision that was evaluated. That list is persisted as the decision instance's audit trail. See [DMN engine](../dmn-engine.md#decision-instances).

### Referencing a required decision's result

A required decision's result is merged under its **`name`** attribute, falling back to `id` when `name` is empty. For a decision table the merged value is the hit policy result, so a named output is reached as `<decisionName>.<outputName>`:

```xml
<inputExpression id="approval_input_riskLevel" typeRef="string">
  <text>determineRiskLevel.risk_level</text>
</inputExpression>
```

:::caution
The `<variable>` element of a decision that holds a decision table is **not** used as the result key — the decision's `name` is. Keep the `name` of any decision that other decisions require a valid FEEL identifier: names containing spaces cannot be referenced from an input expression.
:::

:::note[Limitations]
`requiredDecision` can only reference decisions inside the same DMN file; requirements across deployed files are not supported.

Cyclic requirements are not rejected at deployment time. A cycle makes evaluation recurse indefinitely, so validate the graph in your modeller before deploying.
:::

## Related documentation

- [DMN engine](../dmn-engine.md) — deployment, versioning, binding types, and how a DRG is evaluated.
- [Business rule task](../../bpmn/supported-elements/activities/tasks/business-rule-task.md) — invoking a decision from a BPMN process.
- [Decision table](./decision-table.md) and [Literal expression](./literal-expression.md) — the decision logic a decision node can hold.

## XML example

A two-decision graph. `determineRiskLevel` requires the input data `amount` and `customer_type` and produces a `risk_level`; `determineApprovalResult` requires `determineRiskLevel` and reads its output as `determineRiskLevel.risk_level`. Evaluating `determineApprovalResult` with `{"amount": 1500, "customer_type": "VIP"}` evaluates both decisions and returns `{"approval_result": "APPROVED"}`.

```xml
<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/"
             id="definitions_drd_approval" name="DRD Approval Decisions"
             namespace="https://zenbpm.io/dmn/drd-approval">
  <inputData id="input_amount" name="amount">
    <variable id="input_amount_variable" name="amount" typeRef="number" />
  </inputData>
  <inputData id="input_customer_type" name="customer_type">
    <variable id="input_customer_type_variable" name="customer_type" typeRef="string" />
  </inputData>

  <decision id="determineRiskLevel" name="determineRiskLevel">
    <informationRequirement>
      <requiredInput href="#input_amount" />
    </informationRequirement>
    <informationRequirement>
      <requiredInput href="#input_customer_type" />
    </informationRequirement>
    <decisionTable id="decisionTable_riskLevel" hitPolicy="FIRST">
      <input id="risk_input_amount" label="Amount">
        <inputExpression id="risk_input_amount_expression" typeRef="number">
          <text>amount</text>
        </inputExpression>
      </input>
      <input id="risk_input_customer_type" label="Customer type">
        <inputExpression id="risk_input_customer_type_expression" typeRef="string">
          <text>customer_type</text>
        </inputExpression>
      </input>
      <output id="risk_output" label="Risk level" name="risk_level" typeRef="string" />
      <rule id="risk_rule_1">
        <inputEntry id="risk_rule_1_amount"><text>&gt;= 10000</text></inputEntry>
        <inputEntry id="risk_rule_1_customer_type"><text>-</text></inputEntry>
        <outputEntry id="risk_rule_1_output"><text>"HIGH"</text></outputEntry>
      </rule>
      <rule id="risk_rule_3">
        <inputEntry id="risk_rule_3_amount"><text>&lt; 10000</text></inputEntry>
        <inputEntry id="risk_rule_3_customer_type"><text>"VIP"</text></inputEntry>
        <outputEntry id="risk_rule_3_output"><text>"LOW"</text></outputEntry>
      </rule>
    </decisionTable>
  </decision>

  <decision id="determineApprovalResult" name="determineApprovalResult">
    <informationRequirement>
      <requiredDecision href="#determineRiskLevel" />
    </informationRequirement>
    <decisionTable id="decisionTable_approvalResult" hitPolicy="UNIQUE">
      <input id="approval_input_riskLevel" label="Risk level">
        <inputExpression id="approval_input_riskLevel_expression" typeRef="string">
          <text>determineRiskLevel.risk_level</text>
        </inputExpression>
      </input>
      <output id="approval_output" label="Approval result" name="approval_result" typeRef="string" />
      <rule id="approval_rule_1">
        <inputEntry id="approval_rule_1_riskLevel"><text>"LOW"</text></inputEntry>
        <outputEntry id="approval_rule_1_output"><text>"APPROVED"</text></outputEntry>
      </rule>
      <rule id="approval_rule_3">
        <inputEntry id="approval_rule_3_riskLevel"><text>"HIGH"</text></inputEntry>
        <outputEntry id="approval_rule_3_output"><text>"REJECTED"</text></outputEntry>
      </rule>
    </decisionTable>
  </decision>
</definitions>
```
