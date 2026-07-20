---
sidebar_position: 2
---
# Data Object Reference

A Data Object Reference is a BPMN artifact that represents data produced or consumed within a process — such as a document or a record — scoped to the lifetime of the process instance.

## Key characteristics

- Not part of the process flow; has no incoming or outgoing sequence flows.
- Connected to activities via **Data Associations** (dotted arrows).
- Unlike a Data Store Reference, the data exists only for the duration of the process instance.

## Graphical notation

A document icon with a folded corner.

<img src={require('!url-loader!../../../assets/bpmn/others/data-object-reference.svg').default} alt="Data Object Reference" height="90" />

## XML Definition

```xml
<bpmn:dataObjectReference id="DataObjectRef_Invoice"
                          name="Invoice"
                          dataObjectRef="DataObject_Invoice" />
<bpmn:dataObject id="DataObject_Invoice" />
```

## Current Implementation

Supported as a **visual/documentation element** only. The BPMN engine does not enforce data object lifecycle automatically — data management must be handled through process variables in the Job Worker or Script Task.
