---
sidebar_position: 80
---

# Exclusive gateway
An Exclusive Gateway controls branching and merging of sequence flows based on conditions. When used as a decision point, it evaluates outgoing sequence flow conditions and allows only one to be taken. When used as a merge, it simply passes through the first arriving token without synchronization.

## Key characteristics
- Condition-based routing:
	Only one outgoing flow is selected. If multiple conditions evaluate to true, the BPMN specification does not define the selection priority; engines commonly choose the first in model order.

- Single outgoing token:
	Exactly one branch is activated during divergence.

- Default flow support:
	A default flow may be defined to handle cases where all conditions evaluate to false.

- Convergence behavior:
	The gateway does not synchronize multiple tokens; it forwards the first token that arrives. 
## Usage patterns
- **Decision (Diverging Exclusive Gateway):**
	Used to model conditional branching (if/else). Each outgoing flow has a condition; exactly one path is selected at runtime.

- **Merge (Converging Exclusive Gateway):**
	Used to merge alternative branches back into a single flow. The gateway forwards the first token arriving from any branch.

- **Conditional routing with default:**
	Define a default outgoing sequence flow to provide a fallback path when no conditions apply.

## Graphical notation
![Exclusive gateway usage example](./../../assets/bpmn/exclusive_gateway.svg)

A diamond shape with an X (cross) inside.

## XML Definition
```xml
<bpmn:exclusiveGateway id="ExclusiveGateway_1" name="Decide">
  <bpmn:incoming>Flow1</bpmn:incoming>
  <bpmn:outgoing>Flow2</bpmn:outgoing>
  <bpmn:outgoing>Flow3</bpmn:outgoing>
</bpmn:exclusiveGateway>
```

## Current Implementation
- Exclusive Gateway is fully supported.
