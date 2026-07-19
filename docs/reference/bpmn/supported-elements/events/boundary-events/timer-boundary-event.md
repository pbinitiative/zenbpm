---
sidebar_position: 2
---

# Timer Boundary Event

A Timer Boundary Event is attached to the boundary of an activity and fires when a specified time condition is met while the activity is active.

## Key characteristics

- **Time-based trigger:** The event fires after a duration, at a specific date/time, or on a repeating cycle while the activity is waiting.
- **Interrupting vs. non-interrupting:**
  - **Interrupting:** The attached activity is cancelled and the token moves to the boundary event's outgoing flow.
  - **Non-interrupting:** The attached activity continues and a parallel token is created on the boundary event's outgoing flow (e.g. for sending reminders).
- **Timer types supported:**
  - **Duration:** Fires after the activity has been active for the specified duration.
  - **Date:** Fires at a specific point in time.
  - **Cycle:** Fires repeatedly at a defined interval.

## Graphical notation

A double-ring circle with a clock icon, attached to the border of an activity.

<img src="/img/bpmn/UNI_intermediateEventTimer.svg" width="130" /> <img src="/img/bpmn/UNI_intermediateEventTimerBoundary.svg" width="130" />

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
