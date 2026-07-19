---
sidebar_position: 4
---
# Expanded Pool

An Expanded Pool is a BPMN participant that contains a visible internal process flow with lanes, activities, and events. It represents one participant in a collaboration diagram.

## Key characteristics

- Contains a fully visible process flow.
- Can be divided into Lanes to represent different roles or departments within the participant.
- Communicates with other pools via Message Flows.

## Graphical notation

A large rectangle labelled with the participant name, containing the process flow. Lanes appear as horizontal or vertical subdivisions.

<img src="/img/bpmn/UNI_poolLane.svg" width="250" />

## XML Definition

```xml
<bpmn:collaboration id="Collaboration_1">
  <bpmn:participant id="Participant_1" name="Order Service" processRef="Process_1" />
</bpmn:collaboration>
```

## Current Implementation

*To confirm with the team.*
