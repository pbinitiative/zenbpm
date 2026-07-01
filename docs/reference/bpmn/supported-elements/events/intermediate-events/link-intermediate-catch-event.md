---
sidebar_position: 4
---
# Link Intermediate Catch Event

The receiving side of a link pair — matched to its throw counterpart by name. Used to connect two points in a diagram without drawing a sequence flow between them.

## Key characteristics

- Has one outgoing sequence flow; no incoming flow.
- Resumes the flow where the matching Link Throw Event left off.
- Matched by link name within the same process.
- Commonly used as an "off-page connector" to keep large diagrams readable.

## Graphical notation

A double-line circle with an unfilled arrow icon.

<img src="/img/bpmn/events/link-intermediate-catch-event.svg" alt="Link intermediate catch event" width="120" height="120" />

## XML Definition

```xml
<bpmn:intermediateCatchEvent id="linkIn" name="ContinueA">
  <bpmn:outgoing>Flow_2</bpmn:outgoing>
  <bpmn:linkEventDefinition name="A" />
</bpmn:intermediateCatchEvent>
```

## Current Implementation

Supported.

