---
sidebar_position: 30
---

# Multi-instance activity

A multi-instance activity runs one activity once per element of a collection — sequentially or in parallel. It is a marker placed on an activity, not a separate element type, and can be applied to any supported Task, Sub process, or Call activity. In ZenBPM the iterations execute inside a dedicated child process instance linked to the parent; the parent token waits at the activity until all iterations have completed.

<div style={{"display": "flex", "gap": "24px", "alignItems": "flex-start"}}>

<img src={require('!url-loader!../../../assets/bpmn/activities/activity-multi-instance-parallel.svg').default} alt="Parallel multi-instance" width="110" height="90" />
<img src={require('!url-loader!../../../assets/bpmn/activities/activity-multi-instance-serial.svg').default} alt="Sequential multi-instance" width="110" height="90" />

</div>

Rendered as the activity shape with a marker at the bottom center: three vertical bars for parallel, three horizontal bars for sequential execution.

## Use cases

- **Collect approvals** — route the same user task to each approver in a list, one after another (sequential) or all at once (parallel).
- **Process items of an order** — create one job per order line to reserve stock or calculate prices concurrently.
- **Fan out child processes** — start a Call activity per element, for example one delivery process per shipment.

## Usage in BPMN

Add a `bpmn:multiInstanceLoopCharacteristics` element to the activity and configure it with a `zenbpm:loopCharacteristics` extension element:

| Element                                 | Attribute          | Required | Description                                                                                                                                                       |
| --------------------------------------- | ------------------ | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bpmn:multiInstanceLoopCharacteristics` | `isSequential`     | no       | `true` runs the iterations one after another; `false` or absent runs them all in parallel.                                                                        |
| `zenbpm:loopCharacteristics`            | `inputCollection`  | yes      | FEEL expression (prefixed with `=`) evaluated against the process scope. Must produce a list — otherwise the activity fails.                                      |
| `zenbpm:loopCharacteristics`            | `inputElement`     | yes      | Name of the iteration-local variable that holds the current collection element.                                                                                   |
| `zenbpm:loopCharacteristics`            | `outputCollection` | no       | Name of the process variable that receives the list of iteration results. Define it together with `outputElement` when the iterations produce results to keep.    |
| `zenbpm:loopCharacteristics`            | `outputElement`    | no       | FEEL expression evaluated after each iteration, producing that iteration's entry in the output collection. See [Variables](../../variable-mapping.md) for mapping rules. |

Execution flow:

1. A token arrives at the activity and `inputCollection` is evaluated against the process scope. A non-list value fails the activity; an empty list completes it immediately with an empty output collection.
2. The engine creates a dedicated child process instance for the iterations, linked to the parent and running on the same [partition](../../../cluster.md). The parent token waits at the activity, and boundary events attached to the activity cover the entire multi-instance execution.
3. Iterations run inside the child instance — all at once in parallel mode, one at a time in collection order in sequential mode. Each iteration executes the activity with its own local scope holding the current element under `inputElement`; the process variables remain readable by expressions and input mappings.
4. After an iteration completes, `outputElement` is evaluated against the iteration's input variables plus the variables created by the activity's **output mappings**. **Raw variables returned by a job are not visible to `outputElement`** — map every value it needs with an output mapping on the activity first.
5. When all iterations have completed, the collected results are written to the parent process scope as `outputCollection` — one entry per iteration, in collection order for sequential execution; the order is not guaranteed for parallel execution. The parent token then continues along the outgoing sequence flow.

## Related documentation

- [Variables](../../variable-mapping.md) — variable scoping and the output mapping rules the iterations rely on.
- [Boundary events](../events/index.mdx#boundary-events) — interrupting or reacting to the multi-instance activity as a whole.

## XML example

A sequential multi-instance user task that gathers one approval per element of `approvers`. The task's output mappings expose the values entered by the user, and `outputElement` combines them into one object per iteration, collected in `approvalResults`:

```xml
<bpmn:userTask id="Activity_ReviewRequest" name="Review request">
  <bpmn:extensionElements>
    <zenbpm:ioMapping>
      <zenbpm:output source="=approver" target="reviewer" />
      <zenbpm:output source="=approved" target="approved" />
    </zenbpm:ioMapping>
  </bpmn:extensionElements>
  <bpmn:multiInstanceLoopCharacteristics isSequential="true">
    <bpmn:extensionElements>
      <zenbpm:loopCharacteristics
        inputCollection="=approvers"
        inputElement="approver"
        outputCollection="approvalResults"
        outputElement="={ reviewer: reviewer, approved: approved }" />
    </bpmn:extensionElements>
  </bpmn:multiInstanceLoopCharacteristics>
  <bpmn:incoming>Flow_In</bpmn:incoming>
  <bpmn:outgoing>Flow_Out</bpmn:outgoing>
</bpmn:userTask>
```

With `approvers = ["alice", "bob"]` and the two task completions returning `approved = true` and `approved = false`, the parent process receives:

```json
{
  "approvalResults": [
    { "reviewer": "alice", "approved": true },
    { "reviewer": "bob", "approved": false }
  ]
}
```
