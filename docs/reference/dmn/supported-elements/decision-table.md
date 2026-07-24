---
sidebar_position: 2
---

# Decision table

A Decision Table is the most common way to express decision logic in DMN. It maps combinations of input conditions to output results: input columns extract values from the decision's variables, each rule is a row that tests those values, and the hit policy decides what to return when more than one row matches.

<img src={require('!url-loader!../../assets/dmn/decision-table.svg').default} alt="Decision table" width="250" height="113" />

Rendered as a rectangle with a table icon in the top-left corner, labelled with the decision name.

## Use cases

- **Tiered rules** — map a claim amount and customer type to a discount, a risk level, or an approval outcome.
- **Eligibility matrices** — express a policy that a business analyst maintains as rows, without changing the process model.
- **Classification** — turn a numeric score into a category that later decisions or gateways branch on.

## Usage in DMN

A decision table is placed inside a `<decision>` element. Rules are evaluated top to bottom; within a rule, input entries are evaluated left to right and matching stops at the first cell that does not match.

| Element                     | Attribute     | Required | Description                                                                                                                                                           |
| --------------------------- | ------------- | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `decisionTable`             | `hitPolicy`   | no       | How matching rules are combined. Defaults to `UNIQUE` when absent. See [Hit policy](#hit-policy).                                                                        |
| `decisionTable`             | `aggregation` | no       | Aggregator applied to the collected values, only with `hitPolicy="COLLECT"`. One of `SUM`, `MIN`, `MAX`, `COUNT`.                                                        |
| `input` → `inputExpression` | `text`        | yes      | FEEL expression evaluated against the decision's variables to produce the value this column tests.                                                                       |
| `input`                     | `label`       | no       | Column heading. Reported as the input name in the decision instance audit trail.                                                                                        |
| `output`                    | `name`        | no       | Key the output value is returned under. When a table has a single output and no `name`, the raw value is returned instead of a map. See [Result shape](#result-shape).   |
| `output`                    | `typeRef`     | no       | Documented type of the column. Not enforced by the engine for decision tables.                                                                                          |
| `rule` → `inputEntry`       | `text`        | no       | FEEL unary test for the corresponding input column. An empty entry is treated as `-` and always matches.                                                                 |
| `rule` → `outputEntry`      | `text`        | yes      | FEEL expression evaluated when the rule matches, producing the value of the corresponding output column.                                                                 |

Evaluation flow:

1. Each `inputExpression` is evaluated once against the decision's variables. The evaluated values are recorded in the decision instance audit trail.
2. For every rule, each `inputEntry` is evaluated as a FEEL unary test with `?` bound to the corresponding input value — so `> 100`, `"VIP"`, `[1..10]` and `? != null` all work. A rule matches when all of its input entries match.
3. For every matching rule, each `outputEntry` is evaluated as a FEEL expression against the decision's variables. `?` is **not** available in output entries.
4. The hit policy combines the matching rules into the decision result.
5. When no rule matches, the result is `null` for the single-hit policies and an empty list for `COLLECT` and `RULE ORDER`.

:::caution
When a table has more than one output column, every `output` must have a unique `name`. A table with duplicate or missing output names fails at evaluation, not at deployment.
:::

## Hit policy

The hit policy defines how the decision table handles multiple matching rules. An unsupported hit policy fails at evaluation time — from a [business rule task](../../bpmn/supported-elements/activities/tasks/business-rule-task.md) this raises an incident.

| Hit policy       | Status          | Description                                                                     |
| ---------------- | --------------- | --------------------------------------------------------------------------------- |
| Unique (default) | ✅ Supported     | At most one rule may match. Evaluation fails when several rules match.            |
| First            | ✅ Supported     | Returns the output of the first matching rule. Rule scanning stops at that rule.  |
| Any              | ✅ Supported     | All matching rules must produce equal outputs. Evaluation fails when they differ. |
| Collect          | ✅ Supported     | Returns the outputs of all matching rules as a list, in rule order.               |
| Collect (SUM)    | ✅ Supported     | Sums the collected values. Requires exactly one output column with numeric values.|
| Collect (MIN)    | ✅ Supported     | Smallest collected value. Requires exactly one numeric output column.             |
| Collect (MAX)    | ✅ Supported     | Largest collected value. Requires exactly one numeric output column.              |
| Collect (COUNT)  | ✅ Supported     | Number of matching rules. Requires exactly one output column.                     |
| Rule order       | ✅ Supported     | Returns all matching outputs as a list in rule order — behaves the same as Collect.|
| Priority         | ❌ Not supported | Would return the matching rule with the highest-priority output value.            |
| Output order     | ❌ Not supported | Would return all matching outputs ordered by output priority.                     |

A `COLLECT` aggregator over zero matching rules returns `null`. Applying `SUM`, `MIN` or `MAX` to a non-numeric output, or any aggregator to a table with more than one output column, fails at evaluation.

## Result shape

The shape of the decision result depends on the output columns and the hit policy:

| Table                     | Hit policy              | Result                                                          |
| ------------------------- | ----------------------- | ----------------------------------------------------------------- |
| One output, no `name`     | Unique / First / Any    | The raw value, e.g. `20`                                          |
| One or more named outputs | Unique / First / Any    | A map of output name to value, e.g. `{"discount": 20}`            |
| One output, no `name`     | Collect / Rule order    | A list of raw values, e.g. `[10, 2.5]`                            |
| One or more named outputs | Collect / Rule order    | A list of maps, e.g. `[{"discount": 10}, {"discount": 2.5}]`      |
| One output                | Collect with aggregator | The aggregated value, wrapped in a map when the output is named   |

## Related documentation

- [DMN engine](../dmn-engine.md) — deployment, evaluation, and how a decision result reaches the caller.
- [Decision requirements graph](./decision-requirements-graph.md) — using the result of one decision as input to another.
- [Business rule task](../../bpmn/supported-elements/activities/tasks/business-rule-task.md) — invoking a decision from a BPMN process.

## XML example

A discount table with two input columns and one named output. With `hitPolicy="FIRST"` the first matching row wins, so a VIP order of `1500` returns `{"discount": 20}`.

```xml
<decision id="determineDiscount" name="determineDiscount">
  <decisionTable id="decisionTable_discount" hitPolicy="FIRST">
    <input id="input_amount" label="Amount">
      <inputExpression id="input_amount_expression" typeRef="number">
        <text>amount</text>
      </inputExpression>
    </input>
    <input id="input_customer_type" label="Customer type">
      <inputExpression id="input_customer_type_expression" typeRef="string">
        <text>customer_type</text>
      </inputExpression>
    </input>
    <output id="output_discount" label="Discount" name="discount" typeRef="number" />
    <rule id="rule_vip_customer">
      <inputEntry id="rule_vip_customer_amount"><text>-</text></inputEntry>
      <inputEntry id="rule_vip_customer_type"><text>"VIP"</text></inputEntry>
      <outputEntry id="rule_vip_customer_output"><text>20</text></outputEntry>
    </rule>
    <rule id="rule_large_amount">
      <inputEntry id="rule_large_amount_amount"><text>&gt;= 1000</text></inputEntry>
      <inputEntry id="rule_large_amount_type"><text>-</text></inputEntry>
      <outputEntry id="rule_large_amount_output"><text>10</text></outputEntry>
    </rule>
    <rule id="rule_low_amount">
      <inputEntry id="rule_low_amount_amount"><text>&lt; 1000</text></inputEntry>
      <inputEntry id="rule_low_amount_type"><text>-</text></inputEntry>
      <outputEntry id="rule_low_amount_output"><text>0</text></outputEntry>
    </rule>
  </decisionTable>
</decision>
```
