---
sidebar_position: 1
---
# None End Event

The simplest end event — it ends the current path without throwing any result. The process instance completes once no active tokens remain.

## Key characteristics

- Has no outgoing sequence flows.
- A process may have several end events.
- No event definition element is attached — the path simply ends.

## Graphical notation

A thick single-line circle with no icon inside.

<img src="/img/bpmn/events/none-end-event.svg" alt="None end event" width="120" height="120" />

## XML Definition

```xml
<bpmn:endEvent id="endEvent" name="End">
  <bpmn:incoming>Flow_1</bpmn:incoming>
</bpmn:endEvent>
```

## Current Implementation

Supported.
