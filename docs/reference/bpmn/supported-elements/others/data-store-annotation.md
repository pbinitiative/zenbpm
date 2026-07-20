---
sidebar_position: 3
---
# Text Annotation

A Text Annotation (sometimes called a Data Store Annotation) is a BPMN artifact used to attach a descriptive note to any diagram element. It has no effect on process execution and exists purely for documentation purposes.

## Key characteristics

- No effect on process execution.
- Connected to elements via a dashed **Association** line.
- Used to clarify intent, document decisions, or add context for diagram readers.

## Graphical notation

An open bracket shape connected to the annotated element by a dashed line.

<img src={require('!url-loader!../../../assets/bpmn/others/data-store-annotation.svg').default} alt="Text Annotation" height="90" />

## XML Definition

```xml
<bpmn:textAnnotation id="Annotation_1">
  <bpmn:text>This service calls the external payment gateway. SLA: 2s response time.</bpmn:text>
</bpmn:textAnnotation>

<bpmn:association id="Association_1"
                  sourceRef="Task_ChargeCard"
                  targetRef="Annotation_1" />
```

## Current Implementation

Supported as a **visual/documentation element**. Annotations are parsed and stored but have no runtime effect on the process engine.
