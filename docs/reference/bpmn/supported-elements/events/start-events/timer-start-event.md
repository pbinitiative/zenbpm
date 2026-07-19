---
sidebar_position: 3
---
# Timer Start Event

A Timer Start Event starts a process instance at a specific time, after a duration, or on a recurring schedule.

## Key characteristics

- **Time-based trigger:** The process instance is created automatically when the specified time condition is met.
- **Supports three timer types:**
  - **Date:** Starts once at a specific date and time (ISO 8601).
  - **Duration:** Starts after a specified duration has elapsed.
  - **Cycle:** Starts repeatedly on a cron-based or repeating interval schedule.

## Graphical notation

A thin single-line circle with a clock icon inside.

<img src="/img/bpmn/UNI_startEventTimer.svg" width="130" />

## Configuration

| Field | Format | Example |
|-------|--------|---------|
| Date | ISO 8601 datetime | `2026-10-01T12:00:00Z` |
| Duration | ISO 8601 duration | `PT15S`, `P14D` |
| Cycle | ISO 8601 repeat or cron | `R5/PT10S` |

## XML Definition

```xml
<bpmn:startEvent id="StartEvent_1" name="Start on schedule">
  <bpmn:outgoing>Flow_1</bpmn:outgoing>
  <bpmn:timerEventDefinition id="TimerEventDefinition_1">
    <bpmn:timeCycle xsi:type="bpmn:tFormalExpression">R/PT1H</bpmn:timeCycle>
  </bpmn:timerEventDefinition>
</bpmn:startEvent>
```

## Current Implementation

Supported.
