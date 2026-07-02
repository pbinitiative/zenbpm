---
sidebar_position: 1
---

# Boundary Events

A Boundary Event is attached to the border of an activity. It triggers when a specific condition is met while the activity is active, creating an alternative path out of that activity.

## Key characteristics

- Attached to an activity; has no incoming flow and one outgoing flow.
- Can be **interrupting** or **non-interrupting**:
  - **Interrupting:** The attached activity is cancelled and the token moves along the boundary event's outgoing flow.
  - **Non-interrupting:** The attached activity continues and a new parallel token is created on the boundary event's outgoing flow.
- Graphically represented as a circle on the activity border. Solid border = interrupting; dashed border = non-interrupting.

## Types

Green icons are supported and link to their documentation.

<table className="bpmn-types-table">
  <thead>
    <tr>
      <th>Event</th>
      <th style={{width: '90px'}}>Icon</th>
      <th>Trigger</th>
      <th>Interrupting</th>
      <th>Non-interrupting</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><a href="./message-boundary-event">Message</a></td>
      <td><a href="./message-boundary-event"><img className="bpmn-supported" src="/img/bpmn/events/message-boundary-interrupting.svg" alt="Message Boundary Event" height="70" /></a></td>
      <td>A named message is received while the activity is active</td>
      <td>✅ Supported</td>
      <td>✅ Supported</td>
    </tr>
    <tr>
      <td><a href="./timer-boundary-event">Timer</a></td>
      <td><a href="./timer-boundary-event"><img className="bpmn-supported" src="/img/bpmn/events/timer-boundary-interrupting.svg" alt="Timer Boundary Event" height="70" /></a></td>
      <td>A time condition is met while the activity is active</td>
      <td>✅ Supported</td>
      <td>✅ Supported</td>
    </tr>
    <tr>
      <td><a href="./error-boundary-event">Error</a></td>
      <td><a href="./error-boundary-event"><img className="bpmn-supported" src="/img/bpmn/events/error-boundary-interrupting.svg" alt="Error Boundary Event" height="70" /></a></td>
      <td>A BPMN error is thrown inside the activity</td>
      <td>✅ Supported</td>
      <td>❌ Not supported</td>
    </tr>
  </tbody>
</table>

:::note[Not yet supported]
Escalation, Conditional, Signal, Compensation, Cancel
:::
