---
sidebar_position: 1
---
# Data Store Reference

A Data Store Reference is a BPMN artifact that represents a persistent data store — such as a database — that exists outside the process and can be read from or written to by activities.

## Key characteristics

- Not part of the process flow; has no incoming or outgoing sequence flows.
- Connected to activities via Data Associations (read/write).
- Represents data that persists beyond the lifetime of a single process instance.

## Graphical notation

A cylinder icon (similar to a database symbol).

<img src="/img/bpmn/UNI_dataStore.svg" width="130" />

## XML Definition

```xml
<bpmn:dataStoreReference id="DataStore_1" name="Customer Database" />
```

## Current Implementation

*To confirm with the team.*
