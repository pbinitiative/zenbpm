---
sidebar_position: 4
---
# DMN engine

## Referencing required decisions

Use a decision's `id` in downstream FEEL expressions. For example, a decision
declared as `<decision id="riskDecision" name="Risk decision">` is referenced as
`riskDecision.risk`.

The decision `name` is a display label and is not exposed as an alias. Models
that reference a required decision by its name must update the expression to
use its ID.

The outer key stored in a decision table's evaluated-decision history is always
the decision `id`, regardless of whether a display `name` is present.
