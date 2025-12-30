---
sidebar_position: 120
---
# Link Intermediate Catch Event

A Link Intermediate Catch Event is a BPMN flow element used to connect different parts of the same process. It represents a target point for a corresponding Link Intermediate Throw Event and allows the process flow to continue without using long or crossing sequence flows.

Link events are used purely for modeling convenience and readability. They do not represent asynchronous behavior or external interaction.

## Key characteristics

- **Process-internal navigation:**  
  A Link Intermediate Catch Event receives the process flow from a corresponding Link Intermediate Throw Event within the same process.

- **Always paired with a throw event:**  
  A Link Intermediate Catch Event must have a matching Link Intermediate Throw Event with the same link name.

- **No incoming sequence flow:**  
  A Link Intermediate Catch Event does not have an incoming sequence flow. Control flow is transferred implicitly from the corresponding Link Intermediate Throw Event.

- **Outgoing sequence flows allowed:**  
  The process continues immediately along its outgoing sequence flow when the event is reached.

- **Multiple link pairs allowed:**  
  A process can contain multiple link event pairs to structure complex process models.

## Types of link events

- **Link Intermediate Throw Event:**  
  Transfers control to a corresponding Link Intermediate Catch Event.

- **Link Intermediate Catch Event:**  
  Receives control from a corresponding Link Intermediate Throw Event with the same link name and continues the process flow.

## Graphical notation

A double-lined circle with a link arrow icon pointing into the event.

## XML Definition

```xml
<bpmn:intermediateCatchEvent id="LinkCatchEvent_1" name="Continue Flow">
  <bpmn:linkEventDefinition name="Link_1" />
</bpmn:intermediateCatchEvent>
```


##  Current Implementation
Link intermediate catch event is fully supported.