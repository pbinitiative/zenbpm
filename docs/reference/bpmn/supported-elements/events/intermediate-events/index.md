---
sidebar_position: 1
---

# Intermediate Events

An Intermediate Event occurs between the start and end of a process. Intermediate events are either **catching** (they wait for a trigger) or **throwing** (they produce a result and immediately continue).

## Key characteristics

- Graphically represented as a double-line circle.
- **Catch events** have one incoming and one outgoing sequence flow.
- **Throw events** have one incoming and one outgoing sequence flow.
- Link events are an exception: the Catch end has no incoming flow, the Throw end has no outgoing flow.

## Types

Green icons are supported and link to their documentation.

<table className="bpmn-types-table">
  <thead>
    <tr>
      <th>Event</th>
      <th style={{width: '90px'}}>Icon</th>
      <th>Direction</th>
      <th>Behavior</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><a href="./message-catch-event">Message Catch</a></td>
      <td><a href="./message-catch-event"><img className="bpmn-supported" src={require('!url-loader!../../../../assets/bpmn/events/message-intermediate-catch.svg').default} alt="Message Intermediate Catch Event" height="70" /></a></td>
      <td>Catch</td>
      <td>Pauses the flow and waits until a named message is received</td>
    </tr>
    <tr>
      <td><a href="./message-throw-event">Message Throw</a></td>
      <td><a href="./message-throw-event"><img className="bpmn-supported" src={require('!url-loader!../../../../assets/bpmn/events/message-intermediate-throw.svg').default} alt="Message Intermediate Throw Event" height="70" /></a></td>
      <td>Throw</td>
      <td>Sends a defined message and immediately continues</td>
    </tr>
    <tr>
      <td><a href="./timer-intermediate-catch-event">Timer Catch</a></td>
      <td><a href="./timer-intermediate-catch-event"><img className="bpmn-supported" src={require('!url-loader!../../../../assets/bpmn/events/timer-intermediate-catch.svg').default} alt="Timer Intermediate Catch Event" height="70" /></a></td>
      <td>Catch</td>
      <td>Pauses the flow until a time condition is met</td>
    </tr>
    <tr>
      <td><a href="./link-intermediate-catch-event">Link Catch</a></td>
      <td><a href="./link-intermediate-catch-event"><img className="bpmn-supported" src={require('!url-loader!../../../../assets/bpmn/events/link-intermediate-catch.svg').default} alt="Link Intermediate Catch Event" height="70" /></a></td>
      <td>Catch</td>
      <td>Receiving side of a link pair — resumes flow from its throw counterpart</td>
    </tr>
    <tr>
      <td><a href="./link-intermediate-throw-event">Link Throw</a></td>
      <td><a href="./link-intermediate-throw-event"><img className="bpmn-supported" src={require('!url-loader!../../../../assets/bpmn/events/link-intermediate-throw.svg').default} alt="Link Intermediate Throw Event" height="70" /></a></td>
      <td>Throw</td>
      <td>Sending side of a link pair — transfers flow to the matching catch event</td>
    </tr>
  </tbody>
</table>

:::note[Not yet supported]
- Catch triggers: Conditional, Signal
- Throw triggers: None, Escalation, Signal, Compensation
:::
