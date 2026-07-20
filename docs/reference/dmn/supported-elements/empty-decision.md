---
sidebar_position: 3
---
# Empty decision

An Empty Decision is a decision node with no logic defined. It serves as a placeholder for a decision that has not yet been implemented.

## Key characteristics

- Contains no decision logic.
- Used during modelling as a placeholder.
- Should be replaced with a Decision Table or Literal Expression before deployment.

## Graphical notation

A plain rectangle with no icon, labelled with the decision name.

<img src={require('!url-loader!../../assets/dmn/empty-decision.svg').default} alt="Empty decision" width="250" height="113" />

## XML Definition

```xml
<decision id="pendingDecision" name="Pending decision" />
```
