---
sidebar_position: 2
---
# Timer Boundary Event

A boundary event attached to an activity that fires when a specified time condition is met while the activity is active.

## Key characteristics

- Attached to an activity; has no incoming flow and one outgoing flow.
- **Interrupting:** The attached activity is cancelled and the token moves to the boundary event's outgoing flow.
- **Non-interrupting:** The attached activity continues and a parallel token is created on the boundary event's outgoing flow (e.g. for sending reminders).
- Supports three timer types: duration, date, and cycle.

## Graphical notation

A circle on the activity border with a clock icon. Solid border = interrupting; dashed border = non-interrupting.

<div style={{"display": "flex", "gap": "24px", "alignItems": "flex-start"}}>

<img src={require('!url-loader!../../../../assets/bpmn/events/timer-boundary-event.svg').default} alt="Interrupting" width="120" height="120" />
<img src={require('!url-loader!../../../../assets/bpmn/events/timer-boundary-event-non-interrupting.svg').default} alt="Non-interrupting" width="120" height="120" />

</div>

## Configuration

| Field    | Format                  | Example                |
|----------|-------------------------|------------------------|
| Date     | ISO 8601 datetime       | `2026-10-01T12:00:00Z` |
| Duration | ISO 8601 duration       | `PT1H`, `P14D`         |
| Cycle    | ISO 8601 repeat or cron | `R5/PT10S`             |

## XML Definition

```xml
<bpmn:boundaryEvent id="BoundaryEvent_1" attachedToRef="Activity_1">
  <bpmn:outgoing>Flow_1</bpmn:outgoing>
  <bpmn:timerEventDefinition id="TimerEventDefinition_1">
    <bpmn:timeDuration xsi:type="bpmn:tFormalExpression">PT1H</bpmn:timeDuration>
  </bpmn:timerEventDefinition>
</bpmn:boundaryEvent>
```

## Current Implementation

Supported.
