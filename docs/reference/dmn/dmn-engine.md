---
sidebar_position: 1
---

# DMN engine

The DMN engine evaluates decisions modelled with the [DMN 1.3 specification](https://www.omg.org/spec/DMN/1.3/PDF) (namespace `https://www.omg.org/spec/DMN/20191111/MODEL/`) and aims to stay compatible with [Camunda 8](https://docs.camunda.io/). It can be used through the ZenBPM platform or as a [standalone library](https://pkg.go.dev/github.com/pbinitiative/zenbpm/pkg/dmn).

Decision logic is expressed in FEEL. The engine evaluates FEEL through the same runtime the BPMN engine uses for expressions, so a decision sees process variables exactly as a sequence flow condition would.

See [Supported elements](./supported-elements/index.md) for the DMN elements the engine understands.

## Deployment

A DMN file is deployed as a whole and is split into two levels of definition:

- A **DMN resource definition** — the deployed file itself: its `<definitions>` id and name, the raw XML, and a checksum.
- A **decision definition** per `<decision>` element in the file. Every decision is independently addressable and can be evaluated on its own.

Both levels are versioned independently and get their own engine-generated key.

Deploy over REST with `POST /dmn-resource-definitions`, which returns the `dmnResourceDefinitionKey` of the deployed resource. See the [OpenAPI specification](pathname:///redocusaurus/api.yaml).

### Versioning

Versions are derived on deployment, not declared in the file:

1. The engine computes an MD5 checksum of the XML and looks for existing resource definitions with the same `<definitions>` id.
2. **If the checksum matches the latest existing version, nothing is stored** and the existing definition is returned. Redeploying an unchanged file is a no-op.
3. Otherwise the resource definition is stored with `latest version + 1`, and every decision it contains is stored as `latest version of that decision id + 1`.

Because decision versions are counted per decision id, a decision's version can differ from the version of the file that carries it.

### Version tags

A decision can carry a version tag, which lets callers pin a named version instead of always taking the latest:

```xml
<decision id="dmn_business_rule_version_tag" name="DMN Business Rule Version Tag">
  <extensionElements>
    <zenbpm:versionTag value="stable-tag" />
  </extensionElements>
  ...
</decision>
```

The `zenbpm` namespace is declared as `xmlns:zenbpm="http://zenbpm.pbinitiative.org/1.0"`.

### Validation

A file is validated **before anything is stored** — a rejected file deploys nothing and the API answers `400 Bad Request`. Validation is deliberately shallow and variable-free; for every decision table (including tables nested in contexts) it checks:

- **Rule shape** — every rule must carry exactly one input entry per input and one output entry per output.
- **Input entry syntax** — every cell must parse as a FEEL unary test.
- **Output entry syntax** — every output entry must parse as a FEEL expression.

The syntax checks parse only. A cell or output may reference variables that do not exist yet (`customer_type`, `low`), because whether a name resolves is a runtime question — see [FEEL evaluation](#feel-evaluation). Structural checks that need the whole graph — cyclic requirements, duplicate ids, hit-policy conformance, `typeRef` matching — are still deferred to evaluation.

## Evaluating a decision

A decision can be evaluated in three ways:

- From a process, with a [business rule task](../bpmn/supported-elements/activities/tasks/business-rule-task.md) carrying a `zenbpm:calledDecision` extension element. The task's local variables are the evaluation input.
- Over REST, with `POST /decision-definitions/{decisionId}/evaluate` and a body carrying `bindingType` and the input `variables`.
- Directly in Go, with `ZenDmnEngine.FindAndEvaluateDRD`, when the engine is embedded as a library.

### Binding types

The binding type selects which deployed version of the decision is evaluated.

| Binding type | Status           | Behaviour                                                                                             |
| ------------ | ---------------- | ----------------------------------------------------------------------------------------------------- |
| `latest`     | ✅ Supported     | Evaluates the highest version of the decision. Also used when no binding type is given.               |
| `versionTag` | ✅ Supported     | Evaluates the highest version of the decision whose `zenbpm:versionTag` matches the requested tag.    |
| `deployment` | ❌ Not supported | Would bind to the version deployed together with the calling process definition. Requesting it fails. |

### Resolving the decision id

With the `latest` binding type, the decision id may be qualified with the id of the DMN resource that contains it:

| Form                    | Resolution                                                                                                                                     |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `myDecision`            | Latest version of the decision with this id, across all deployed DMN resources.                                                                |
| `myResource.myDecision` | Latest version of `myDecision` within the DMN resource `myResource`. Use this to disambiguate a decision id that exists in more than one file. |

Anything with more than one dot is rejected.

Over REST the two parts are sent separately: put the decision id in the path and the resource id in the request body's `dmnResourceDefinitionId` field, which is only honoured with `bindingType: latest`.

### Result of an evaluation

An evaluation returns three things:

- **`decisionOutput`** — the result of the decision that was asked for, unwrapped from its name. A decision table with a single unnamed output returns the raw value; named outputs return a map; a `COLLECT` table returns a list. See [Result shape](./supported-elements/decision-table.md#result-shape).
- **`evaluatedDecisions`** — the requested decision followed by every decision evaluated to satisfy its requirements, each with its evaluated inputs, matched rules and output. This is the audit trail.
- **`decisionInstanceKey`** — the key of the persisted [decision instance](#decision-instances).

For a business rule task, `decisionOutput` is what lands in the task's `resultVariable`.

## Evaluating a decision requirements graph

Evaluating a decision evaluates the whole requirement subgraph beneath it. Requirements are resolved depth-first, in document order, before the decision's own logic runs:

- A `requiredInput` is checked against the evaluation input. If no variable matches the input data's `name`, the evaluation fails.
- A `requiredDecision` is evaluated recursively **with the original evaluation input**, so a required decision never sees the results of its siblings. A decision table or context result is merged into the requiring decision's variable context under the required decision's `id`; a literal-expression result is merged under its `variable/@name`. The decision's `name` attribute is display-only.

Requirements can only point at decisions inside the same DMN file. Cyclic requirements are not detected at deployment time — validation of cycles, duplicate ids and similar structural problems is not implemented yet.

See [Decision requirements graph](./supported-elements/decision-requirements-graph.md) for the modelling side and a worked example.

## FEEL evaluation

Every expression in a DMN file is evaluated by the FEEL runtime against the decision's variable context — the evaluation input plus the results of any required decisions.

| Location                          | Evaluated as    | Context                                                                     |
| --------------------------------- | --------------- | --------------------------------------------------------------------------- |
| Decision table input expression   | FEEL expression | The decision's variables.                                                   |
| Decision table input entry (cell) | FEEL unary test | The decision's variables, plus `?` bound to the value of that input column. |
| Decision table output entry       | FEEL expression | The decision's variables. `?` is **not** available.                         |
| Literal expression                | FEEL expression | The decision's variables.                                                   |

A few details are worth knowing:

- **An empty input entry means "any"**. A cell with no text — or only whitespace — is treated as `-` and always matches, which is how modellers leave a column unconstrained for a rule.
- **A name in an input entry cell must resolve**. Cells are evaluated in _strict_ mode: a name that is neither a variable in the decision's context nor a FEEL built-in fails the evaluation with `unknown variable(s) in unary test: <name>`. This catches an unquoted string — `VIP` written where the literal `"VIP"` was meant. Left lenient, such a cell tests the input against the unbound name's `null`, i.e. `? = null`; since `null = null` is `true` in FEEL, it **matches whenever that input column is itself `null`** (a missing input) and misses otherwise — so it quietly fires the _wrong_ rule rather than erroring. Expression positions are not strict: a missing variable in an input expression, output entry or literal expression is `null`, as in FEEL generally.
- **Newlines inside string literals are normalised**. XML formatting can introduce line breaks into an expression; the engine rewrites newlines that occur inside FEEL string literals to `\n` before evaluating, so a multi-line literal keeps its intended value instead of becoming a syntax error.

The expression language of a literal expression must be `feel` or absent — any other `expressionLanguage` fails the evaluation.

**Compatibility with Camunda.** Camunda 7 and Camunda 8 (feel-scala) resolve a missing variable to `null` and never raise: an input entry referencing an unknown name becomes a `null` comparison, which _matches_ when that input column is `null` and misses otherwise — so a forgotten pair of quotes can silently fire the wrong rule. ZenBPM is stricter **for input entry cells only** — an unknown name there fails the evaluation, turning that quiet mis-match into a visible incident. Because every other position keeps FEEL's lenient `null`, a decision table ported from Camunda evaluates identically as long as the names its cells reference actually resolve at runtime.

## Decision instances

Every successful evaluation is persisted as a **decision instance**: the decision id, the output variables, the full list of evaluated decisions, the keys of the DMN resource and decision definitions, and a timestamp. When the evaluation was triggered by a business rule task, the process instance key and flow element instance key are recorded too, linking the decision back to the process that made it.

Decision instances are the audit trail for "why did the engine decide this" — they carry the evaluated input values and the rules that matched, per decision in the graph.

They are read over REST with `GET /decision-instances` (filterable by DMN resource, process instance and evaluation time) and `GET /decision-instances/{decisionInstanceKey}`.

A failed evaluation does not produce a decision instance.

## Error handling

Structural and syntactic problems are caught at [deployment](#validation) and reject the file. Everything semantic surfaces at evaluation time: a file that deploys can still fail to evaluate, because deployment does not check that the names an expression references actually exist.

Evaluation fails when:

- The decision id cannot be resolved, or the requested binding type or version tag matches nothing.
- A required input variable is missing, or a required decision is missing or fails.
- A decision has no supported logic — an empty decision node.
- A FEEL expression cannot be evaluated, or a literal expression's result does not match its `typeRef`.
- An input entry cell references a name that is neither a variable in the context nor a FEEL built-in — reported as `unknown variable(s) in unary test: <name>`. See [FEEL evaluation](#feel-evaluation).
- A decision table uses an unsupported hit policy, has duplicate output names, or violates its hit policy at runtime — several rules matching under `UNIQUE`, or differing outputs under `ANY`.

Called from a [business rule task](../bpmn/supported-elements/activities/tasks/business-rule-task.md), any of these fails the task and raises an incident. The failure can be handled in the process with an error boundary event.

## Storage

The engine talks to storage through the `DecisionStorage` interface, which covers DMN resource definitions, decision definitions and decision instances. The ZenBPM platform backs it with [RqLite](../storage.md); an [in-memory implementation](https://pkg.go.dev/github.com/pbinitiative/zenbpm/pkg/storage/inmemory) is the default when the engine is embedded as a library and is what the unit tests run against.
