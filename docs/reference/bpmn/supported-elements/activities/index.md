---
sidebar_position: 0
sidebar_label: Overview
---

# Activities

An activity is work performed within a business process. When a token arrives at an activity, the process waits until the work is completed; the token then continues along the outgoing sequence flow.

<img src={require('!url-loader!../../../assets/bpmn/activities/activity.svg').default} alt="Activity" width="110" height="90" />

Activities come in two kinds:

- [Tasks](./tasks/index.md) — atomic units of work, such as executing a job, evaluating a decision, or waiting for a human or a message.
- Subprocesses — compound activities that contain their own flow: a [Sub process](./sub-process.md) embedded in the diagram, or a [Call activity](./call-activity.md) invoking another process.

However different the work they perform is, all activities behave the same way towards the surrounding process — this page describes that shared behavior.

## Markers

Markers are small icons at the bottom center of the activity shape that modify how the activity executes, without changing its type. ZenBPM supports the two multi-instance markers:

<table className="bpmn-types-table">
  <thead>
    <tr>
      <th>Marker</th>
      <th style={{width: '90px'}}>Icon</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><a href="./activity-multi-instance">Parallel multi-instance</a></td>
      <td><a href="./activity-multi-instance"><img className="bpmn-supported" src={require('!url-loader!../../../assets/bpmn/activities/activity-multi-instance-parallel.svg').default} alt="Parallel multi-instance marker" height="70" /></a></td>
      <td>Runs the activity once per element of a collection, with all instances created at the same time.</td>
    </tr>
    <tr>
      <td><a href="./activity-multi-instance">Sequential multi-instance</a></td>
      <td><a href="./activity-multi-instance"><img className="bpmn-supported" src={require('!url-loader!../../../assets/bpmn/activities/activity-multi-instance-serial.svg').default} alt="Sequential multi-instance marker" height="70" /></a></td>
      <td>Runs the activity once per element of a collection, one instance after another.</td>
    </tr>
  </tbody>
</table>

## Usage in BPMN

Every activity, regardless of its type, supports the following markup:

| Markup                                                | Description                                                                                                                                                                                |
| ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `zenbpm:ioMapping` → `zenbpm:input` / `zenbpm:output` | Maps variables between the process scope and the activity's local scope. See [Variables](../../variables.md).                                                                              |
| `bpmn:multiInstanceLoopCharacteristics`               | A marker (not a separate activity type) that runs the activity once per element of a collection, sequentially or in parallel. See [Multi-instance activity](./activity-multi-instance.md). |
| `bpmn:boundaryEvent` attached via `attachedToRef`     | Reacts to errors, messages, or timers while the activity is active, and routes the process accordingly. See [Boundary events](../events/boundary-events/index.md).                         |

The shared lifecycle:

1. A token arrives at the activity and input mappings are evaluated into the activity's local variable scope.
2. The activity performs its specific work — creates a job, evaluates a decision, waits for a message, or runs a child process.
3. On completion, output mappings propagate selected variables back to the parent scope. **Without output mappings, no variables are propagated.**
4. The token continues along the outgoing sequence flow. While the activity is active, attached boundary events can interrupt it or trigger parallel flows.

## Related documentation

- [Variables](../../variables.md) — variable scoping and mapping rules shared by all activities.
- [Boundary events](../events/boundary-events/index.md) — handling errors, timeouts, and messages on a running activity.
- [Multi-instance activity](./activity-multi-instance.md) — the multi-instance marker in detail.

## XML example

The common markup applied to one concrete activity (a Service task): variable mappings, a multi-instance marker running the task once per customer, and an error boundary event:

```xml
<bpmn:serviceTask id="Activity_NotifyCustomers" name="Notify customers">
  <bpmn:extensionElements>
    <zenbpm:taskDefinition type="send-notification" />
    <zenbpm:ioMapping>
      <zenbpm:input source="=campaign.id" target="campaignId" />
      <zenbpm:output source="=deliveryStatus" target="lastDeliveryStatus" />
    </zenbpm:ioMapping>
  </bpmn:extensionElements>
  <bpmn:multiInstanceLoopCharacteristics>
    <bpmn:extensionElements>
      <zenbpm:loopCharacteristics inputCollection="=customers" inputElement="customer" />
    </bpmn:extensionElements>
  </bpmn:multiInstanceLoopCharacteristics>
  <bpmn:incoming>Flow_In</bpmn:incoming>
  <bpmn:outgoing>Flow_Out</bpmn:outgoing>
</bpmn:serviceTask>

<bpmn:boundaryEvent id="Boundary_NotifyFailed" attachedToRef="Activity_NotifyCustomers">
  <bpmn:errorEventDefinition errorRef="Error_NotifyFailed" />
</bpmn:boundaryEvent>
```
