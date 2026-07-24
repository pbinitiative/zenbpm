---
sidebar_position: 3
---

# Literal expression

A Literal Expression is a decision whose logic is a single FEEL expression. The expression is evaluated against the decision's variables and its value becomes the decision result, named after the decision's `<variable>` element. Use it where a table would be overkill — a calculation, a lookup, or a reshaping of data produced by a required decision.

<img src={require('!url-loader!../../assets/dmn/literal-expression.svg').default} alt="Literal expression" width="250" height="113" />

Rendered as a rectangle with a curly-braces icon in the top-left corner, labelled with the decision name.

## Use cases

- **Calculations** — derive a total, a fee, or a proration from values the process already holds.
- **Reshaping a result** — turn the output of a required decision into the structure the process expects.
- **Simple lookups** — pick a value with an `if … then … else` chain that does not warrant a decision table.

## Usage in DMN

A literal expression is placed inside a `<decision>` element next to a `<variable>` element that names and types the result.

| Element             | Attribute            | Required | Description                                                                                                                             |
| ------------------- | -------------------- | -------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `variable`          | `name`               | yes      | Key the result is returned under, and the key under which requiring decisions see it. See [Result shape](#result-shape).                   |
| `variable`          | `typeRef`            | no       | Expected type of the result. Unlike in a decision table, this **is** checked — a result that does not match fails the evaluation.          |
| `literalExpression` | `expressionLanguage` | no       | Must be `feel` or absent. Any other value fails at evaluation.                                                                            |
| `literalExpression` → `text` | –           | yes      | The FEEL expression. May span multiple lines; string literals containing newlines are preserved as `\n`.                                   |

Evaluation flow:

1. The expression text is evaluated as FEEL against the decision's variables — the evaluation input plus the results of any required decisions.
2. The result is checked against `variable/@typeRef` and returned under `variable/@name`.
3. Any FEEL error — an unknown variable, a type mismatch, an unsupported expression language — fails the evaluation. From a [business rule task](../../bpmn/supported-elements/activities/tasks/business-rule-task.md) this raises an incident.

:::caution
The `<variable>` element is mandatory for a literal expression. A decision that has a `<literalExpression>` but no `<variable>` has no name to return its result under and cannot be evaluated.
:::

### Type reference

`typeRef` is enforced against the Go value the FEEL runtime produces:

| `typeRef`                     | Accepted result                                            |
| ----------------------------- | ------------------------------------------------------------ |
| absent, `any`, or any other value | Returned unchanged, no check                             |
| `string`                      | A string                                                     |
| `boolean`                     | A boolean, or `null`                                         |
| `number`                      | A floating point or integer number, or `null`                |
| `integer`, `long`, `double`   | Exactly that numeric representation                          |
| `date`                        | A date/time value                                            |

Prefer `number` for numeric decisions: FEEL arithmetic does not guarantee which numeric representation it yields, so the narrower `integer`, `long` and `double` types can reject results that are numerically correct.

## Result shape

The result is a single value under the variable name. A decision `totalPrice` with `<variable name="total" />` returns `{"total": 4500}` to a requiring decision, which reads it as `totalPrice.total`; the process-facing result of the evaluation is the value `4500` itself. See [DMN engine](../dmn-engine.md#result-of-an-evaluation).

## Related documentation

- [DMN engine](../dmn-engine.md) — deployment, evaluation, and FEEL evaluation rules.
- [Decision requirements graph](./decision-requirements-graph.md) — using the result of one decision as input to another.
- [Business rule task](../../bpmn/supported-elements/activities/tasks/business-rule-task.md) — invoking a decision from a BPMN process.

## XML example

A literal expression that computes an order total from three process variables and returns it as `total`.

```xml
<decision id="totalPrice" name="totalPrice">
  <variable id="totalPriceVariable" name="total" typeRef="number" />
  <literalExpression id="totalPriceExpression">
    <text>basePrice * quantity * (1 - discount)</text>
  </literalExpression>
</decision>
```

A multi-line expression. Newlines inside string literals are preserved, so the result is `Hello,\nWorld!`.

```xml
<decision id="greeting" name="greeting">
  <variable id="greetingVariable" name="message" typeRef="string" />
  <literalExpression id="greetingExpression">
    <text>"Hello,
World!"</text>
  </literalExpression>
</decision>
```
