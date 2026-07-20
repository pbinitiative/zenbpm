---
sidebar_position: 5
---
# Link Intermediate Throw Event

The sending side of a link pair. It connects two points in the same process without drawing a sequence flow between them.

## Key characteristics

- Has one incoming sequence flow; no outgoing flow.
- Paired by name with a Link Intermediate Catch Event in the same process.
- Commonly used as an "off-page connector" to keep large diagrams readable.

## Graphical notation

A double-line circle with a filled arrow icon.

<img src={require('!url-loader!../../../../assets/bpmn/events/link-intermediate-throw-event.svg').default} alt="Link intermediate throw event" width="120" height="120" />

## XML Definition

```xml
<bpmn:intermediateThrowEvent id="linkOut" name="ContinueA">
  <bpmn:incoming>Flow_1</bpmn:incoming>
  <bpmn:linkEventDefinition name="A" />
</bpmn:intermediateThrowEvent>
```

## Current Implementation

Supported.

