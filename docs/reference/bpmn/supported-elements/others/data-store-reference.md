---
sidebar_position: 1
---
# Data Store Reference

A Data Store Reference is a BPMN artifact that represents a persistent external data store — such as a database or file system — that activities can read from or write to.

## Key characteristics

- Not part of the process flow; has no incoming or outgoing sequence flows.
- Connected to activities via **Data Associations** (dotted arrows indicating read or write).
- Represents data that persists beyond the lifetime of a single process instance.

## Graphical notation

A cylinder icon, similar to a database symbol.

<img src={require('!url-loader!../../../assets/bpmn/others/data-store-reference.svg').default} alt="Data Store Reference" height="90" />

## XML Definition

```xml
<bpmn:dataStoreReference id="DataStore_CustomerDB" name="Customer Database" />

<bpmn:dataInputAssociation id="DataInputAssociation_1">
  <bpmn:sourceRef>DataStore_CustomerDB</bpmn:sourceRef>
  <bpmn:targetRef>Task_LoadCustomer</bpmn:targetRef>
</bpmn:dataInputAssociation>
```

## Current Implementation

Supported as a **visual/documentation element** only. The BPMN engine does not automatically read from or write to external data stores based on Data Associations — this must be implemented in the Job Worker or Script Task logic.
