---
sidebar_position: 3
---
# Data Store Annotation

A Data Store Annotation is a BPMN artifact used to attach a descriptive note to a data store or other diagram element, providing context or explanation without affecting the process flow.

## Key characteristics

- Has no effect on process execution.
- Connected to elements via an Association line.
- Used purely for documentation and diagram readability.

## Graphical notation

An open rectangle (bracket shape) connected to the annotated element by a dashed line.

<img src="/img/bpmn/cs_textAnnotation.svg" width="160" />

## XML Definition

```xml
<bpmn:textAnnotation id="Annotation_1">
  <bpmn:text>This store holds archived orders.</bpmn:text>
</bpmn:textAnnotation>
<bpmn:association id="Association_1" sourceRef="DataStore_1" targetRef="Annotation_1" />
```

## Current Implementation

*To confirm with the team.*
