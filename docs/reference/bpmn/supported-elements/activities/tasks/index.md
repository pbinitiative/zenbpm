---
sidebar_position: 1
---

# Tasks

Tasks are the basic elements of BPMN processes — atomic units of work composed to create a meaningful result. When a token arrives at a task, execution pauses until the work is completed; the token then continues along the outgoing sequence flow. The granularity of a task depends on the process design — a step can be modeled as a single task or split into several smaller tasks.

## Key characteristics

- One incoming and one outgoing sequence flow.
- Represents a single, atomic unit of work.
- Can define input and output parameter mappings between the process and the task.
- Errors during execution can be handled using boundary events.

## Types

Green icons are supported and link to their documentation.

<table className="bpmn-types-table">
  <thead>
    <tr>
      <th>Task</th>
      <th style={{width: '90px'}}>Icon</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><a href="./service-task">Service task</a></td>
      <td><a href="./service-task"><img className="bpmn-supported" src={require('!url-loader!../../../../assets/bpmn/activities/service-task.svg').default} alt="Service Task" height="70" /></a></td>
      <td>Work performed automatically by a system, without human involvement. It automates process steps by invoking external services, APIs, or backend systems.</td>
    </tr>
    <tr>
      <td><a href="./user-task">User task</a></td>
      <td><a href="./user-task"><img className="bpmn-supported" src={require('!url-loader!../../../../assets/bpmn/activities/user-task.svg').default} alt="User Task" height="70" /></a></td>
      <td>Work performed by a human. It suspends process execution until a user completes the task and provides input or makes a decision.</td>
    </tr>
    <tr>
      <td><a href="./business-rule-task">Business rule task</a></td>
      <td><a href="./business-rule-task"><img className="bpmn-supported" src={require('!url-loader!../../../../assets/bpmn/activities/business-rule-task.svg').default} alt="Business Rule Task" height="70" /></a></td>
      <td>Provides a mechanism for invoking business rules, delegating complex decision logic to rule engines.</td>
    </tr>
    <tr>
      <td><a href="./send-task">Send task</a></td>
      <td><a href="./send-task"><img className="bpmn-supported" src={require('!url-loader!../../../../assets/bpmn/activities/send-task.svg').default} alt="Send Task" height="70" /></a></td>
      <td>Sends a defined message and immediately continues execution without waiting for a response.</td>
    </tr>
    <tr>
      <td><a href="./receive-task">Receive task</a></td>
      <td><a href="./receive-task"><img className="bpmn-supported" src={require('!url-loader!../../../../assets/bpmn/activities/receive-task.svg').default} alt="Receive Task" height="70" /></a></td>
      <td>Pauses the process and waits until a specific named message is received, then continues execution.</td>
    </tr>
    <tr>
      <td><a href="./script-task">Script task</a></td>
      <td><a href="./script-task"><img className="bpmn-supported" src={require('!url-loader!../../../../assets/bpmn/activities/script-task.svg').default} alt="Script Task" height="70" /></a></td>
      <td>Executes a script within the process flow, allowing custom logic, calculations, data transformations, and integrations.</td>
    </tr>
  </tbody>
</table>

:::note[Not yet supported]
Manual task
:::
