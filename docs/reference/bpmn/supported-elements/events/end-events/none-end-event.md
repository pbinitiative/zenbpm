---
sidebar_position: 1
---
# None End Event

A None End Event marks where a path of execution finishes — the process instance completes once no active tokens remain. It ends the path without throwing any result.

## Key characteristics

- Has no outgoing sequence flows.
- A process may have several end events.
- No event definition element is attached — the path simply ends.

## Graphical notation

A thick single-line circle with no icon inside.

<img src="/img/bpmn/end_event.svg" width="130" />

## XML Definition

```xml
<bpmn:endEvent id="endEvent" name="End">
  <bpmn:incoming>Flow_1</bpmn:incoming>
</bpmn:endEvent>
```

## Current Implementation

Supported.
