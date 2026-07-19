---
sidebar_position: 2
---
# Data Object Reference

A Data Object Reference is a BPMN artifact that represents data produced or consumed within a process — such as a document or a record — scoped to the process instance.

## Key characteristics

- Not part of the process flow; has no incoming or outgoing sequence flows.
- Connected to activities via Data Associations (read/write).
- Scoped to the process instance — unlike a Data Store Reference, the data does not persist beyond the instance.

## Graphical notation

A document icon with a folded corner.

<img src="/img/bpmn/UNI_dataObject.svg" width="130" />

## XML Definition

```xml
<bpmn:dataObjectReference id="DataObject_1" name="Order Document" dataObjectRef="DataObject_ref" />
<bpmn:dataObject id="DataObject_ref" />
```

## Current Implementation

*To confirm with the team.*
