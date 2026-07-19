---
sidebar_position: 5
---
# Empty Pool

An Empty Pool (Black Box Pool) is a BPMN participant whose internal process is hidden or unknown. It represents an external participant in a collaboration without exposing its implementation.

## Key characteristics

- Contains no visible internal flow.
- Used to represent external systems or third parties whose internals are not modelled.
- Communicates with other pools via Message Flows.

## Graphical notation

A plain rectangle labelled with the participant name, with no internal elements.

## XML Definition

```xml
<bpmn:collaboration id="Collaboration_1">
  <bpmn:participant id="Participant_external" name="Payment Provider" />
</bpmn:collaboration>
```

## Current Implementation

*To confirm with the team.*
