---
sidebar_position: 110
---
# Link Intermediate Throw Event

A Link Intermediate Throw Event is a BPMN flow element used to transfer the process flow to another point within the same process. It represents a source point that links to a corresponding Link Intermediate Catch Event and allows the process to continue without using long or crossing sequence flows.

Link events are used purely for modeling convenience and readability. They do not represent asynchronous behavior, waiting, or external interaction.

## Key characteristics

- **Process-internal navigation:**  
  A Link Intermediate Throw Event transfers the process flow to a corresponding Link Intermediate Catch Event within the same process.

- **Always paired with a catch event:**  
  A Link Intermediate Throw Event must have a matching Link Intermediate Catch Event with the same link name.

- **No outgoing sequence flow:**  
  A Link Intermediate Throw Event does not have an outgoing sequence flow. Control flow is transferred implicitly to the corresponding Link Intermediate Catch Event.

- **Single incoming sequence flow:**  
  A Link Intermediate Throw Event has exactly one incoming sequence flow.

- **Immediate flow transfer:**  
  When reached, the process flow is immediately transferred to the corresponding Link Intermediate Catch Event without pausing execution.

- **Multiple link pairs allowed:**  
  A process can contain multiple link event pairs to structure complex and readable process models.

## Types of link events

- **Link Intermediate Throw Event:**  
  Transfers control to a corresponding Link Intermediate Catch Event.

- **Link Intermediate Catch Event:**  
  Receives control from a corresponding Link Intermediate Throw Event with the same link name and continues the process flow.

## Graphical notation

A double-lined circle with a link arrow icon pointing out of the event.

## XML Definition

```xml
<bpmn:intermediateThrowEvent id="LinkThrowEvent_1" name="Jump Flow">
  <bpmn:linkEventDefinition name="Link_1" />
</bpmn:intermediateThrowEvent>
```

## Current Implementation
Link intermediate throw event is fully supported.