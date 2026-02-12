---
sidebar_position: 70
---

# Inclusive Gateway

An Inclusive Gateway (OR) is a BPMN flow element that allows one or more outgoing paths to be activated based on conditions, and synchronizes incoming paths from active branches. It enables complex branching where one or more paths can be taken, and ensures all active branches complete before proceeding.

## Key characteristics
- Multiple path activation:
	Inclusive Gateways evaluate conditions on outgoing sequence flows and can activate multiple paths at the same time if their conditions evaluate to true.

- Default flow support:
	A default outgoing sequence flow can be explicitly defined on the gateway and is taken only when none of the defined condition expressions evaluate to true, ensuring that at least one path is followed.

- Synchronization at convergence:
	When used as a convergence point, the gateway waits for tokens from all previously activated branches before producing a single outgoing token.

- Condition evaluation:
	Each outgoing sequence flow may have a condition expression. A sequence flow without a condition is not implicitly a default flow unless it is explicitly marked as such.

- Token-based behavior:
	Multiple tokens can be created on outgoing flows during divergence, and all incoming tokens from active paths must arrive before one outgoing token is produced during convergence.

## Usage patterns
- **Multi-choice branching (Diverging Inclusive Gateway)**
	Used when one or more paths can be taken based on different conditions. Unlike Exclusive Gateways, multiple branches may execute concurrently.

- **Synchronization (Converging Inclusive Gateway)**
	Used to merge multiple branches back into a single flow, waiting for all active paths to complete before continuing.

- **Conditional routing with defaults**
	Define conditions on one or more outgoing sequence flows and explicitly designate a default flow on the Inclusive Gateway. The default flow is taken only if none of the defined condition expressions evaluate to true.

## Graphical notation
![Inclusive gateway usage example](./../../assets/bpmn/inclusive_gateway.svg)

A diamond shape with a circle inside.

## XML Definition
```xml
<bpmn:inclusiveGateway id="InclusiveGateway_1"
                       name="Evaluate Options"
                       default="Flow3">
  <bpmn:incoming>Flow1</bpmn:incoming>
  <bpmn:outgoing>Flow2</bpmn:outgoing>
  <bpmn:outgoing>Flow3</bpmn:outgoing>
</bpmn:inclusiveGateway>
```

## Current Implementation
- Inclusive Gateway is fully supported.
