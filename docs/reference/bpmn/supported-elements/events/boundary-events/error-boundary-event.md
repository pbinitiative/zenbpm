---
sidebar_position: 3
---
# Error Boundary Event

A boundary event that triggers when a matching BPMN error is raised inside the activity it is attached to.

## Key characteristics

- Attached to an activity; has no incoming flow and one outgoing flow.
- Matches a thrown error by `errorCode`.
- Matching walks up the scope hierarchy; an unhandled error becomes an incident.
- Always interrupting — cancels the host activity when triggered.

## Supported activity types

Service task, User task, Business Rule task, Script task, Call activity.

## Graphical notation

A solid double-line circle on the activity border with a lightning-bolt icon.

<img src={require('!url-loader!../../../../assets/bpmn/events/error-boundary-event.svg').default} alt="Error boundary event" width="120" height="120" />

## XML Definition

```xml
<bpmn:boundaryEvent id="onError" name="On error" attachedToRef="Task_1">
  <bpmn:outgoing>Flow_2</bpmn:outgoing>
  <bpmn:errorEventDefinition errorRef="Error_1" />
</bpmn:boundaryEvent>
```

## Current Implementation

Supported. Always interrupting.
