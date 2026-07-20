---
sidebar_position: 1
---

# Start Events

A Start Event defines the beginning of a process instance. It has no incoming sequence flows and exactly one outgoing sequence flow.

## Key characteristics

- Has no incoming sequence flows and one outgoing sequence flow.
- Creates a new process instance when triggered.
- A process can have multiple start events, each with a different trigger type.
- Graphically represented as a thin single-line circle.

## Types

Green icons are supported and link to their documentation.

<table className="bpmn-types-table">
  <thead>
    <tr>
      <th>Event</th>
      <th style={{width: '90px'}}>Icon</th>
      <th>Trigger</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><a href="./none-start-event">None</a></td>
      <td><a href="./none-start-event"><img className="bpmn-supported" src={require('!url-loader!../../../../assets/bpmn/events/none-start.svg').default} alt="None Start Event" height="70" /></a></td>
      <td>Explicit API call or internal trigger — no event definition</td>
    </tr>
    <tr>
      <td><a href="./message-start-event">Message</a></td>
      <td><a href="./message-start-event"><img className="bpmn-supported" src={require('!url-loader!../../../../assets/bpmn/events/message-start.svg').default} alt="Message Start Event" height="70" /></a></td>
      <td>A named message is received by the engine</td>
    </tr>
    <tr>
      <td><a href="./timer-start-event">Timer</a></td>
      <td><a href="./timer-start-event"><img className="bpmn-supported" src={require('!url-loader!../../../../assets/bpmn/events/timer-start.svg').default} alt="Timer Start Event" height="70" /></a></td>
      <td>A time condition is met (date, duration, or cycle)</td>
    </tr>
  </tbody>
</table>

:::note[Not yet supported]
Conditional, Signal
:::
