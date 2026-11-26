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
- Cron expressions with 5, 6, or 7 fields (optional seconds and year, `?` treated as `*`), e.g. `0 2 * * *` for every night at 2:00. A cron expression is read in the time zone of the engine.

Where each specification applies:

- **Timer start event** — `timeDate` for a one-shot start, `timeCycle` for a schedule.
- **Intermediate catch and boundary events** — typically `timeDuration` (relative to when the token arrived or the activity started) or `timeDate`; a non-interrupting boundary event additionally accepts `timeCycle` to fire repeatedly.

### FEEL expressions

Each of the three elements takes either a literal, as above, or a FEEL expression. A value starting with `=` is an expression, evaluated against the variables of the process instance when the timer is created; any other value is a literal. This lets a timer wait for a moment computed at runtime:

| Element             | The expression evaluates to                                                              | Example                                                        |
| ------------------- | ---------------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| `bpmn:timeDate`     | an ISO 8601 date-time string, or a FEEL date and time                                    | `=dueAt`, `=date and time(sentAt) + duration("P3D")`           |
| `bpmn:timeDuration` | an ISO 8601 duration string, or a FEEL duration                                          | `=reminderAfter`, `=duration("PT" + string(hours) + "H")`       |
| `bpmn:timeCycle`    | a string holding an ISO 8601 repeating interval or a cron expression                     | `=reminderCycle`, `="R" + string(reminders) + "/P2D"`          |

- **When it is evaluated** — when the timer is created: as the token reaches an intermediate catch event, as the activity of a boundary event starts, or as the scope of an event sub process becomes active. A `timeCycle` is evaluated once, when its first timer is created; every later occurrence repeats the cycle it evaluated to then, whatever the variables hold by that time.
- **Which variables it reads** — those of the process instance the timer belongs to. Inside an embedded sub process that is the instance of the sub process, which holds its own copy of the variables, taken when the sub process started: a change of the root instance's variables after that does not reach it. A boundary timer does not read the input mappings of its activity, which only the activity's job sees; neither does the correlation key of a message boundary event.
- **When it does not evaluate** — an expression which evaluates to `null`, as a reference to a variable the instance does not have does, or to a value its element cannot take, creates an incident: on the catch event, on the activity a boundary event is attached to, or on the start event of an event sub process, whose scope keeps running. Correct the variables of the instance the incident is listed on (inside an embedded sub process, the instance of the sub process) and resolve the incident, and the expression is evaluated again. The message of the incident names the expression and, when it evaluated to a string, a number or a boolean, that result.
- **A moment in the past** — a `timeDate` in the past and a `timeDuration` which is zero or negative, as `=date and time(deadline) - now()` is once the deadline has passed, make the timer due at once: the flow goes on without waiting. A `timeCycle` which has no occurrence left, as `R1/2020-01-01T00:00:00Z/PT1H` has none, is another matter. On an intermediate catch event, where nothing else would let the token go on, it creates an incident; on a boundary event or in an event sub process no timer is created, as for a literal.
- **Limits** — a timer is due in the years 1 to 9999, told both in UTC and in the time zone of the engine. A date, a duration or the first occurrence of a cycle which puts it elsewhere creates an incident; a cycle whose later occurrence would lie beyond them ends there. The hours, minutes and seconds of a duration, or of the period of a cycle, add up to at most 292 years: write a longer one in days, months or years. An expression is at most 4000 characters long.
- **Timer start event of a process** — its timer is created when the process definition is deployed, where there is no instance and so no variables. The expression is evaluated without variables; it may use functions such as `now()`, but one which refers to a variable evaluates to `null` and the deployment is refused. So is one which takes longer than two seconds to evaluate, and a `timeDuration` expression, which has nothing to be measured from there. An expression stopped after two seconds is interrupted, but a regular expression which backtracks (`matches()`, `replace()`, `split()`) cannot be: its match keeps a processor busy until it ends, which can take hours. Until then the deployment of every further timer expression of a process start event is refused, with a message which says so; a restart of the node ends it.
- **Deployment** — an expression which is not valid FEEL is refused when the definition is deployed, and so is one which starts with a second `=`, which the FEEL parser would take and which evaluates to `null`. What it evaluates to is known only at runtime. The check has no variables to go by, so a variable whose name contains a FEEL keyword (`valid for`, `return date`) is refused although it would evaluate in an instance: give the variable a name without one. The check takes longer the deeper an expression is nested: one whose check takes longer than two seconds, as one nested about a thousand levels deep does, is refused as one which could not be checked.
- **Time zones** — a date-time without a zone or offset, whether a string or a FEEL date and time, is read in the time zone of the engine, as a literal is. Name the offset where it matters: `2026-12-03T09:00:00+01:00`. A zone id (`@Europe/Prague`) is not supported and evaluates to `null`.
- **Dates without a time** — avoid them for a timer, they do not all mean the same midnight. A date-only string (`"2026-12-03"`) and `today()` are midnight in the time zone of the engine, while `date("2026-12-03")` is midnight in UTC; `date and time(date("2026-12-03"), time("09:00:00"))` joins the two and lands a day early when the engine runs west of UTC, and `date("2026-12-03") - today()` is off by the offset of the engine's zone. Write the moment as one value with its time and offset instead. A FEEL time (`time("09:00:00")`) names no day and is refused, and so is a date and time on 1900-01-01, the day the FEEL runtime holds a time on: the runtime itself takes every such value for a time. Write a moment of that day as an ISO 8601 string.
- **Durations** — the years, months, weeks and days of a duration move the timer by the calendar, so they have to be whole numbers. The difference of two durations is what it amounts to: `=duration("P1D") - duration("PT2H")` waits 22 hours. A duration string may be negative and name fractions of a second: `"-PT1H"`, or what `string()` makes of a FEEL duration, `"PT-2H-0.25S"`. The FEEL runtime adds and subtracts durations but does not multiply, divide or negate them, nor take their `abs`, `sum` or `max`: each of these evaluates to `null`. Compute the number first and build the duration from it: `=duration("PT" + string(hours * 2) + "H")`. Do not build a timer on `years and months duration(from, to)`: the FEEL runtime counts the days left over as a fraction of a month, so that from 15 January to 20 March is not `P2M` but 2.16 months, `P2M4DT20H7M44.516S` as a string. As a `timeDuration` that creates an incident, as a fraction of a month is no whole number; added to a date it moves the timer by those days and hours.
- **FEEL values kept in variables** — a variable which an output mapping filled with a FEEL date and time or a FEEL duration can be the whole expression (`=dueAt`), but nothing more can be computed from it: `=dueAt + duration("P1D")` evaluates to `null`. Keep such a value as a string, with `string(...)`, and read it with `date and time(dueAt)` or `duration(waitFor)`.

In XML an expression is the content of the element, with `<` and `&` escaped as in any XML text:

```xml
<bpmn:timeDate xsi:type="bpmn:tFormalExpression">=date and time(sentAt) + duration("P3D")</bpmn:timeDate>
```

### Timer start event

Schedules process instance creation without any external trigger. A `timeDate` timer creates one instance at the given moment; a `timeCycle` timer creates an instance at each occurrence and re-arms itself until the cycle's repetitions or end date are exhausted. The schedule is registered when the process definition is deployed, so a [FEEL expression](#feel-expressions) here is evaluated without variables.

A top-level timer start carries no payload and creates an instance with an empty root variable scope. It supports `zenbpm:output` mappings that initialize root variables from literals or expressions before the outgoing flow runs. A mapping evaluation failure creates an incident on the start event and leaves the root variables unchanged.

A timer start event inside an [Event sub process](../activities/event-sub-process.md) additionally supports `timeDuration`, measured from when its containing scope becomes active.

### Timer intermediate catch event

Pauses the flow: the token waits at the event until the timer fires — after a `timeDuration` measured from the token's arrival, or at an absolute `timeDate`, either of which may be a [FEEL expression](#feel-expressions) over the instance's variables — and then continues along the outgoing flow. Together with an [Event-based gateway](../gateways/event-based-gateway.md), a timer catch event commonly models a timeout branch racing against a message.

Timer intermediate catch events carry no payload and do not apply output mappings. Process variables remain unchanged, including when the timer follows an event-based gateway.

### Timer boundary event

Arms a timer when the attached activity starts and cancels it when the activity completes. If the timer fires first:

- **Interrupting** (solid border) — the activity is cancelled and the token continues along the boundary event's outgoing flow. Use for timeouts and SLAs.
- **Non-interrupting** (dashed border, `cancelActivity="false"`) — the activity keeps running and a parallel token is created on the outgoing flow. With a `timeCycle`, the timer re-arms after each occurrence for as long as the activity is active — use for repeated reminders.

Timer boundary events support `zenbpm:output` mappings. When the timer fires, expressions evaluate against the containing process scope and mapped values are propagated to that scope before the outgoing flow. Without mappings, the timer adds no variables.

## Related documentation

- [Events overview](./index.mdx#boundary-events) — shared boundary event behavior: subscription lifecycle, interrupting vs non-interrupting.
- [Event-based gateway](../gateways/event-based-gateway.md) — racing a timer against a message to model timeouts.
- [User task](../activities/tasks/user-task.md) — the activity most commonly guarded by timer boundary events.

## XML example

A nightly schedule, a 24-hour delay, a wait until a date computed from a variable, and a repeated reminder on a user task:

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

<bpmn:intermediateCatchEvent id="Wait_PayoutDeadline" name="3 days after sending">
  <bpmn:incoming>Flow_2</bpmn:incoming>
  <bpmn:outgoing>Flow_3</bpmn:outgoing>
  <bpmn:timerEventDefinition>
    <bpmn:timeDate>=date and time(sentAt) + duration("P3D")</bpmn:timeDate>
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
