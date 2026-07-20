---
sidebar_position: 2
---

# None Start Event

The simplest start event — it has no specific trigger. A process instance is created manually, typically via an API call or internal trigger.

## Key characteristics

- No event definition element is attached.
- The process must be explicitly started by a caller (e.g. via REST API).
- When no trigger type is specified, a Start Event is treated as a None Start Event.

## Graphical notation

A thin single-line circle with no icon inside.

<img src={require('!url-loader!../../../../assets/bpmn/events/none-start-event.svg').default} alt="None start event" width="120" height="120" />

## XML Definition

```xml
<bpmn:startEvent id="StartEvent_1" name="Start">
  <bpmn:outgoing>Flow_1</bpmn:outgoing>
</bpmn:startEvent>
```

## Current Implementation

Supported.
