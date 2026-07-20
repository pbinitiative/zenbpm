---
sidebar_position: 1
---

# Subprocesses

Subprocesses are element containers that group a set of activities, events, and gateways into a single unit, typically to improve diagram readability or to decompose a complex process into smaller, reusable parts.

## Key characteristics

- Runs within its own scope, logically connected to the parent process instance.
- Can have boundary events attached — when triggered, the subprocess is interrupted, regardless of which of its elements is currently active.
- By default, variables are not inherited from the parent process instance; explicit input and output mappings are required to transfer data.
- Can be configured to run as multiple instances of the same subprocess.

## Types

Green icons are supported and link to their documentation.

<table className="bpmn-types-table">
  <thead>
    <tr>
      <th>Subprocess</th>
      <th style={{width: '90px'}}>Icon</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><a href="./call-activity">Call activity</a></td>
      <td><a href="./call-activity"><img className="bpmn-supported" src="/img/bpmn/activities/call-activity.svg" alt="Call Activity" height="70" /></a></td>
      <td>Invokes a global process or a global task. It allows processes to reuse externally defined process logic. <strong>Limitation:</strong> only direct process ID references, only the "latest" version tag, no error handling or compensation.</td>
    </tr>
    <tr>
      <td><a href="./sub-process">Sub process</a></td>
      <td><a href="./sub-process"><img className="bpmn-supported" src="/img/bpmn/activities/sub-process.svg" alt="Sub-Process" height="70" /></a></td>
      <td>Defines a new process scope running within a parent process, used to break down a complex process into smaller, more manageable parts.</td>
    </tr>
    <tr>
      <td><a href="./activity-multi-instance">Multi-instance activity</a></td>
      <td><a href="./activity-multi-instance"><img className="bpmn-supported" src="/img/bpmn/activities/activity-multi-instance.svg" alt="Multi-Instance Activity" height="70" /></a></td>
      <td>Allows an activity to be executed multiple times for a collection of items, either sequentially or in parallel. <strong>Limitation:</strong> completion condition (early termination) is not yet supported.</td>
    </tr>
  </tbody>
</table>
