---
sidebar_position: 130
---
# Timer Intermediate Catch Event

A Timer Intermediate Catch Event is a BPMN flow element that pauses the process flow until a specified time condition is met. It is used to model delays, deadlines, timeouts, or periodic waiting within a process.

The process execution waits at the event until the timer condition is fulfilled, after which the flow continues.

## Key characteristics

- **Time-based waiting:**  
  A Timer Intermediate Catch Event waits for a defined time condition, such as a specific date, a duration, or a repeating interval.

- **Pauses process execution:**  
  When reached, the process execution is suspended until the timer is triggered.

- **Incoming and outgoing sequence flows:**  
  A Timer Intermediate Catch Event must have at least one incoming sequence flow and typically has one outgoing sequence flow.

- **No external interaction:**  
  The event is triggered purely by time conditions and does not depend on messages, signals, or external actions.

- **Single trigger condition:**  
  Each Timer Intermediate Catch Event is defined by exactly one timer configuration.

## Timer definitions

A Timer Intermediate Catch Event can be configured using one of the following timer types:

- **Time Date:**  
  Triggers at a specific point in time (for example, `2026-01-01T10:00:00`).

- **Time Duration:**  
  Triggers after a specified duration has elapsed (for example, `PT5M` for ten minutes).

- **Time Cycle:**  
  Triggers repeatedly based on a defined cycle (for example, every day at midnight).

## Graphical notation
![End event usage example](./../../assets/bpmn/timer_intermediate_catch_event.svg)

A double-lined circle with a clock icon inside the event.

## XML Definition

```xml
<bpmn:intermediateCatchEvent id="TimerCatchEvent_1" name="Wait 5 Minutes">
  <bpmn:timerEventDefinition>
    <bpmn:timeDuration>PT5M</bpmn:timeDuration>
  </bpmn:timerEventDefinition>
</bpmn:intermediateCatchEvent>
```
## Current Implementation
Timer intermediate catch event is fully supported.