---
sidebar_position: 60
---

# Parallel gateway

A Parallel Gateway is a BPMN flow element that synchronizes parallel flows within a process. It allows a process to split into multiple concurrent paths and synchronize when those paths complete, enabling parallel execution and synchronization of activities.

## Key characteristics
- Synchronization control:
	Parallel Gateways split and join concurrent flows, ensuring all branches complete before proceeding downstream.

- No condition evaluation:
	Unlike Exclusive Gateways, Parallel Gateways do not evaluate conditions. All outgoing flows are activated when splitting.

- Token-based execution:
	Each incoming token creates tokens on all outgoing flows (split). All incoming tokens must arrive before one outgoing token is produced (join).

- All paths must complete:
	In join mode, the gateway waits for tokens from all incoming paths. It does not proceed until all branches have sent their tokens.

- Types of parallel gateway usage:
	- **Split (Diverging Parallel Gateway):**
		A single incoming token creates multiple outgoing tokens, one on each outgoing flow. All branches execute concurrently.

	- **Join (Converging Parallel Gateway):**
		Multiple incoming tokens are collected. When all paths have sent a token, a single outgoing token is produced.

	- **Combined Split and Join (in Loop or Complex Structures):**
		A Parallel Gateway can function as both a split and join in the same process flow, creating loops with parallel branches.

## Graphical notation
![Parallel gateway usage example](./../../assets/bpmn/parallel_gateway.svg)

A diamond shape with a thick plus sign (+) inside.

## XML Definition
```xml
<bpmn:parallelGateway id="ParallelGateway_1" name="Split">
  <bpmn:incoming>Flow1</bpmn:incoming>
  <bpmn:outgoing>Flow2</bpmn:outgoing>
  <bpmn:outgoing>Flow3</bpmn:outgoing>
</bpmn:parallelGateway>
```

## Current Implementation
Current implementation handles parallel flows correctly if there is only one overlapping flow in the process instance. **Multiple recursive parallel flows have currently undefined behaviour.**