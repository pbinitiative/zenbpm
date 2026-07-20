---
sidebar_position: 1
---
# Decision table

A Decision Table is the most common way to express decision logic in DMN. It maps combinations of input conditions to output results in a structured table format.

## Key characteristics

- Consists of input columns (conditions) and output columns (results).
- Each row defines a rule: when all input conditions match, the corresponding output is returned.
- The hit policy determines how matching rules are combined when multiple rows match.

## Graphical notation

A rectangle with a table icon, labelled with the decision name.

<img src={require('!url-loader!../../assets/dmn/decision-table.svg').default} alt="Decision table" width="250" height="113" />

## XML Definition

```xml
<decision id="discountDecision" name="Discount">
  <decisionTable id="decisionTable_1" hitPolicy="UNIQUE">
    <input id="input_1" label="Customer type">
      <inputExpression typeRef="string">
        <text>customerType</text>
      </inputExpression>
    </input>
    <output id="output_1" label="Discount" typeRef="integer" />
    <rule id="rule_1">
      <inputEntry><text>"VIP"</text></inputEntry>
      <outputEntry><text>20</text></outputEntry>
    </rule>
  </decisionTable>
</decision>
```

## Current Implementation

Support varies by hit policy — see the [Hit policy](#hit-policy) table below.

## Input

An Input column defines one condition evaluated against incoming data. Each input references a FEEL expression that extracts a value from the available context.

### Key characteristics

- Each input column has a label and an input expression.
- The input expression is a FEEL expression referencing a variable (e.g. `customerType`).
- An optional type can be specified to constrain allowed values.

### XML Definition

```xml
<input id="input_1" label="Customer type">
  <inputExpression typeRef="string">
    <text>customerType</text>
  </inputExpression>
</input>
```

## Output

An Output column defines the value returned when a rule matches. A decision table can have one or more output columns.

### Key characteristics

- Each output column has a label and an optional type.
- When a rule matches, the output values of that rule are returned.
- With multiple output columns, the result is a context (key-value map).

### XML Definition

```xml
<output id="output_1" label="Discount" typeRef="integer" />
```

## Rule

A Rule is a single row in the decision table. It specifies an input entry (condition) for each input column and an output entry (result) for each output column.

### Key characteristics

- A rule matches when all its input entries evaluate to true for the given input values.
- Input entries use FEEL expressions or shorthand (e.g. `"VIP"`, `> 100`, `[1..10]`).
- Output entries define the value returned when the rule matches.

### XML Definition

```xml
<rule id="rule_1">
  <inputEntry><text>"VIP"</text></inputEntry>
  <outputEntry><text>20</text></outputEntry>
</rule>
```

## Hit policy

The hit policy defines how the decision table handles multiple matching rules.

| Hit policy | Status | Description |
|------------|--------|-------------|
| Unique | ✅ Supported | Only one rule may match. Returns a single output. |
| First | ✅ Supported | Returns the output of the first matching rule (in order). |
| Collect | ✅ Supported | Returns all matching outputs as a list. |
| Any | 🔜 Coming soon | All matching rules must return the same output; returns that output. |
| Collect aggregators | 🔜 Coming soon | Aggregates collected outputs using sum, min, max, or count. |
| Priority | ❌ Not supported | Returns the matching rule with the highest priority output value. |
| Rule order | ❌ Not supported | Returns all matching outputs ordered by rule sequence. |
| Output order | ❌ Not supported | Returns all matching outputs ordered by output priority. |
