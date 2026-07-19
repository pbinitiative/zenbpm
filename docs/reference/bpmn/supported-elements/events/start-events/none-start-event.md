---
sidebar_position: 2
---
# None Start Event

A None Start Event is the simplest type of Start Event. It has no specific trigger — a process instance is created manually, typically via an API call or user action.

## Key characteristics

- **Manual instantiation:** The process must be explicitly started by a caller (e.g., via REST API or internal trigger).
- **No event definition:** No event definition element is attached to the start event.
- **Default start event:** When no trigger type is specified, a Start Event is treated as a None Start Event.

## Graphical notation

A thin single-line circle with no icon inside.

![None start event](../../../../assets/bpmn/start_event.png)

## XML Definition

```xml
<bpmn:startEvent id="StartEvent_1" name="Start">
  <bpmn:outgoing>Flow_1</bpmn:outgoing>
</bpmn:startEvent>
```

## Current Implementation

Supported.
