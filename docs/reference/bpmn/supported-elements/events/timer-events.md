---
sidebar_position: 20
---

# Timer events

Timer events act on the passage of time: they start processes on a schedule, delay a flow, or bound how long an activity may run. The engine schedules the timers itself — they are persisted with the process state and fire automatically, without any external trigger. Timer events are always catching.

<table className="bpmn-types-table">
  <thead>
    <tr>
      <th>Position</th>
      <th style={{width: '110px'}}>Icon</th>
      <th>Behavior</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><a href="#timer-start-event">Timer start event</a></td>
      <td><a href="#timer-start-event"><img className="bpmn-supported" src={require('!url-loader!../../../assets/bpmn/events/timer-start.svg').default} alt="Timer start event" height="70" /></a></td>
      <td>Creates a process instance at a fixed time or on a recurring schedule.</td>
    </tr>
    <tr>
      <td><a href="#timer-intermediate-catch-event">Timer intermediate catch event</a></td>
      <td><a href="#timer-intermediate-catch-event"><img className="bpmn-supported" src={require('!url-loader!../../../assets/bpmn/events/timer-intermediate-catch.svg').default} alt="Timer intermediate catch event" height="70" /></a></td>
      <td>Pauses the token until the timer fires.</td>
    </tr>
    <tr>
      <td><a href="#timer-boundary-event">Timer boundary event</a></td>
      <td><a href="#timer-boundary-event"><img className="bpmn-supported" src={require('!url-loader!../../../assets/bpmn/events/timer-boundary-interrupting.svg').default} alt="Timer boundary event (interrupting)" height="70" /> <img className="bpmn-supported" src={require('!url-loader!../../../assets/bpmn/events/timer-boundary-non-interrupting.svg').default} alt="Timer boundary event (non-interrupting)" height="70" /></a></td>
      <td>Fires while an activity is active — interrupting or non-interrupting.</td>
    </tr>
  </tbody>
</table>

## Use cases

- **Scheduled processes** — run a reporting or cleanup process every night at 2:00 with a cycle timer start event.
- **Delays** — wait 24 hours after registration before sending a follow-up.
- **Timeouts** — cancel a user task that has not been completed within its SLA and route to an escalation path (interrupting boundary).
- **Reminders** — send a reminder every two days while an approval is pending, without disturbing the task itself (non-interrupting boundary with a cycle).

## Usage in BPMN

A timer event contains a `bpmn:timerEventDefinition` with exactly one of three time specifications:

| Element             | Meaning                                       | Format                                                                                                          | Example                          |
| ------------------- | --------------------------------------------- | --------------------------------------------------------------------------------------------------------------- | -------------------------------- |
| `bpmn:timeDate`     | Fire once, at a fixed moment                  | [ISO 8601 date-time](https://en.wikipedia.org/wiki/ISO_8601#Combined_date_and_time_representations)             | `2026-10-01T12:00:00Z`           |
| `bpmn:timeDuration` | Fire once, a period after the event activates | [ISO 8601 duration](https://en.wikipedia.org/wiki/ISO_8601#Durations)                                           | `PT15M`, `P14D`                  |
| `bpmn:timeCycle`    | Fire repeatedly                               | [ISO 8601 repeating interval](https://en.wikipedia.org/wiki/ISO_8601#Recurring_intervals), or a cron expression | `R5/PT10S`, `R/P1D`, `0 2 * * *` |

Supported cycle forms:

- `R[n]/<duration>` — repeat every duration, starting one period after activation; `R` without a number repeats indefinitely, e.g. `R3/P2D` or `R/PT1H`.
- `R[n]/<start>/<duration>`, `R[n]/<duration>/<end>`, `R[n]/<start>/<end>` — repeating intervals with an explicit first occurrence or end date.
- Cron expressions with 5, 6, or 7 fields (optional seconds and year, `?` treated as `*`), e.g. `0 2 * * *` for every night at 2:00.

Where each specification applies:

- **Timer start event** — `timeDate` for a one-shot start, `timeCycle` for a schedule.
- **Intermediate catch and boundary events** — typically `timeDuration` (relative to when the token arrived or the activity started) or `timeDate`; a non-interrupting boundary event additionally accepts `timeCycle` to fire repeatedly.

### Timer start event

Schedules process instance creation without any external trigger. A `timeDate` timer creates one instance at the given moment; a `timeCycle` timer creates an instance at each occurrence and re-arms itself until the cycle's repetitions or end date are exhausted. The schedule is registered when the process definition is deployed.

A timer start event inside an [Event sub process](../activities/event-sub-process.md) additionally supports `timeDuration`, measured from when its containing scope becomes active.

### Timer intermediate catch event

Pauses the flow: the token waits at the event until the timer fires — after a `timeDuration` measured from the token's arrival, or at an absolute `timeDate` — and then continues along the outgoing flow. Together with an [Event-based gateway](../gateways/event-based-gateway.md), a timer catch event commonly models a timeout branch racing against a message.

### Timer boundary event

Arms a timer when the attached activity starts and cancels it when the activity completes. If the timer fires first:

- **Interrupting** (solid border) — the activity is cancelled and the token continues along the boundary event's outgoing flow. Use for timeouts and SLAs.
- **Non-interrupting** (dashed border, `cancelActivity="false"`) — the activity keeps running and a parallel token is created on the outgoing flow. With a `timeCycle`, the timer re-arms after each occurrence for as long as the activity is active — use for repeated reminders.

## Related documentation

- [Events overview](./index.mdx#boundary-events) — shared boundary event behavior: subscription lifecycle, interrupting vs non-interrupting.
- [Event-based gateway](../gateways/event-based-gateway.md) — racing a timer against a message to model timeouts.
- [User task](../activities/tasks/user-task.md) — the activity most commonly guarded by timer boundary events.

## XML example

A nightly schedule, a 24-hour delay, and a repeated reminder on a user task:

```xml
<bpmn:startEvent id="Start_Nightly" name="Every night at 2:00">
  <bpmn:outgoing>Flow_1</bpmn:outgoing>
  <bpmn:timerEventDefinition>
    <bpmn:timeCycle>0 2 * * *</bpmn:timeCycle>
  </bpmn:timerEventDefinition>
</bpmn:startEvent>

<bpmn:intermediateCatchEvent id="Wait_OneDay" name="Wait 24 hours">
  <bpmn:incoming>Flow_1</bpmn:incoming>
  <bpmn:outgoing>Flow_2</bpmn:outgoing>
  <bpmn:timerEventDefinition>
    <bpmn:timeDuration>P1D</bpmn:timeDuration>
  </bpmn:timerEventDefinition>
</bpmn:intermediateCatchEvent>

<bpmn:boundaryEvent id="Boundary_Reminder" name="Every 2 days"
                    attachedToRef="Activity_ApproveRequest" cancelActivity="false">
  <bpmn:outgoing>Flow_SendReminder</bpmn:outgoing>
  <bpmn:timerEventDefinition>
    <bpmn:timeCycle>R/P2D</bpmn:timeCycle>
  </bpmn:timerEventDefinition>
</bpmn:boundaryEvent>
```
