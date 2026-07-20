---
sidebar_position: 2
---
# Literal expression

A Literal Expression is a decision expressed as a single FEEL (Friendly Enough Expression Language) expression that evaluates to a value.

## Key characteristics

- Contains a single expression instead of a table.
- Useful for simple calculations or transformations.
- The result type can be specified explicitly.

## Graphical notation

A rectangle with a curly-braces icon, labelled with the decision name.

<img src={require('!url-loader!../../assets/dmn/literal-expression.svg').default} alt="Literal expression" width="250" height="113" />

## XML Definition

```xml
<decision id="totalPrice" name="Total price">
  <literalExpression>
    <text>basePrice * quantity * (1 - discount)</text>
  </literalExpression>
</decision>
```
