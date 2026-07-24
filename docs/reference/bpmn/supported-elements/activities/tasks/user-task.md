---
sidebar_position: 20
---

# User task

A User Task represents work performed by a human. The process pauses at the task until a person completes the work and submits a result — typically through a task list or form application built on top of the ZenBPM API. Internally the task creates a **job** that the application completes on the user's behalf.

<img src={require('!url-loader!../../../../assets/bpmn/activities/user-task.svg').default} alt="User task" width="110" height="90" />

Rendered as a rounded rectangle with a person icon in the top-left corner.

## Use cases

- **Approvals** — a manager approves an order, invoice, or leave request before the process continues.
- **Manual data entry** — a user fills in a form to supply data the process cannot obtain automatically.
- **Exception review** — a case flagged by automated checks is reviewed by a human before processing resumes.

## Usage in BPMN

A User Task always creates a job of the fixed type `user-task-type` — unlike a [Service task](./service-task.md), no `zenbpm:taskDefinition` is needed. Variable mappings with `zenbpm:ioMapping` work exactly as for a [Service task](./service-task.md#usage-in-bpmn).

The task can be routed to its performer with a `zenbpm:assignmentDefinition` extension element:

| Attribute         | Required | Description                                                     |
| ----------------- | -------- | --------------------------------------------------------------- |
| `assignee`        | no       | The user the task is assigned to.                               |
| `candidateGroups` | no       | Comma-separated list of groups whose members can claim the task. |

## Related documentation

- [Service task](./service-task.md) — full configuration and execution details for job-based tasks: `zenbpm:taskDefinition`, variable mappings, and job handling.

## XML example

A User Task assigned to `jane.doe`, claimable by the `sales` and `support` groups. The input mapping exposes the order total to the form; the output mapping stores the user's decision in the process variable `orderApproved`.

```xml
<bpmn:userTask id="Activity_ApproveOrder" name="Approve order">
  <bpmn:extensionElements>
    <zenbpm:assignmentDefinition assignee="jane.doe" candidateGroups="sales, support" />
    <zenbpm:ioMapping>
      <zenbpm:input source="=order.total" target="orderTotal" />
      <zenbpm:output source="=approved" target="orderApproved" />
    </zenbpm:ioMapping>
  </bpmn:extensionElements>
  <bpmn:incoming>Flow_In</bpmn:incoming>
  <bpmn:outgoing>Flow_Out</bpmn:outgoing>
</bpmn:userTask>
```
