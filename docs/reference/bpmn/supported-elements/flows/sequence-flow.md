---
sidebar_position: 1
---
# Sequence flow

A Sequence Flow is the basic connector between BPMN elements. It defines the order in which activities and events are executed.

## Key characteristics

- Connects exactly two elements: a source and a target.
- The token follows the sequence flow after the source element completes.
- Every flow element (except start and end events) requires at least one incoming and one outgoing sequence flow.

## Graphical notation

A solid arrow line between two elements.

![Sequence flow](/img/bpmn/flows/sequence-flow.png)

## XML Definition

```xml
<bpmn:sequenceFlow id="Flow_1" sourceRef="Task_1" targetRef="Task_2" />
```

## Current Implementation

Supported.
