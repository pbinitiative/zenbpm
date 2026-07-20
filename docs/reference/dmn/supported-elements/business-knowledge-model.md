---
sidebar_position: 6
---
# Business knowledge model

A Business Knowledge Model (BKM) is a reusable encapsulation of decision logic — typically a function — that can be invoked by decisions or other BKMs within the DMN model.

## Key characteristics

- Encapsulates a FEEL function or decision table as a reusable component.
- Invoked via a knowledge requirement from a decision.
- Promotes reuse across multiple decisions in the same model.

## Graphical notation

A rectangle with clipped corners, labelled with the BKM name.

<img src={require('!url-loader!../../assets/dmn/business-knowledge-model.svg').default} alt="Business knowledge model" width="250" height="95" />

## XML Definition

```xml
<businessKnowledgeModel id="discountFunction" name="Discount function">
  <encapsulatedLogic>
    <formalParameter name="customerType" typeRef="string" />
    <literalExpression>
      <text>if customerType = "VIP" then 20 else 5</text>
    </literalExpression>
  </encapsulatedLogic>
</businessKnowledgeModel>
```
