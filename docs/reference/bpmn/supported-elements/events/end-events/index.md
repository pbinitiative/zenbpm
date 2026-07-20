---
sidebar_position: 1
---

# End Events

An End Event marks the completion of a process path. It has no outgoing sequence flows.

## Key characteristics

- Has no outgoing sequence flows.
- A process may have multiple end events.
- The process instance completes once all active tokens have reached an end event.
- Graphically represented as a thick single-line circle.

## Types

Green icons are supported and link to their documentation.

<table className="bpmn-types-table">
  <thead>
    <tr>
      <th>Event</th>
      <th style={{width: '90px'}}>Icon</th>
      <th>Behavior</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><a href="./none-end-event">None</a></td>
      <td><a href="./none-end-event"><img className="bpmn-supported" src={require('!url-loader!../../../../assets/bpmn/events/none-end.svg').default} alt="None End Event" height="70" /></a></td>
      <td>The path ends without any result</td>
    </tr>
    <tr>
      <td><a href="./message-end-event">Message</a></td>
      <td><a href="./message-end-event"><img className="bpmn-supported" src={require('!url-loader!../../../../assets/bpmn/events/message-end.svg').default} alt="Message End Event" height="70" /></a></td>
      <td>Sends a defined message before ending the path</td>
    </tr>
    <tr>
      <td><a href="./error-end-event">Error</a></td>
      <td><a href="./error-end-event"><img className="bpmn-supported" src={require('!url-loader!../../../../assets/bpmn/events/error-end.svg').default} alt="Error End Event" height="70" /></a></td>
      <td>Throws a BPMN error that can be caught by an Error Boundary Event</td>
    </tr>
    <tr>
      <td><a href="./terminate-end-event">Terminate</a></td>
      <td><a href="./terminate-end-event"><img className="bpmn-supported" src={require('!url-loader!../../../../assets/bpmn/events/terminate-end-event.svg').default} alt="Terminate End Event" height="70" /></a></td>
      <td>Immediately cancels the entire process instance including all parallel tokens</td>
    </tr>
  </tbody>
</table>

:::note[Not yet supported]
Escalation, Signal, Compensation, Cancel
:::
