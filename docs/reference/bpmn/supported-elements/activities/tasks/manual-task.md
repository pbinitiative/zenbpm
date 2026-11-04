---
sidebar_position: 60
---

# Manual task

A Manual Task represents work a person performs outside of the process engine — without a task list, a form, or a job worker. ZenBPM treats it as a **pass-through activity**: when a token arrives, the task is activated and completed at once, and the process continues along the outgoing sequence flow. The task documents the manual step in the model and records it in the process instance history.

<img src={require('!url-loader!../../../../assets/bpmn/activities/manual-task.svg').default} alt="Manual task" width="110" height="90" />

Rendered as a rounded rectangle with a hand icon in the top-left corner.

## Use cases

- **Physical work** — a warehouse worker packs the shipment or a technician installs a device on site.
- **Offline steps** — a customer signs a paper contract or a document is handed over in person.
- **Documenting a process** — keep a step that happens outside the IT systems visible in the model and in the history, without blocking the process.

A Manual Task never waits for anyone. If the process must not continue until a person confirms the work, use a [User task](./user-task.md) instead.

## Usage in BPMN

A Manual Task needs no configuration. Optionally, it can map variables with `zenbpm:ioMapping` and run once per element of a collection with a [multi-instance marker](../activity-multi-instance.md).

| Extension element                    | Attribute          | Required | Description                                                                                                       |
| ------------------------------------ | ------------------ | -------- | ----------------------------------------------------------------------------------------------------------------- |
| `zenbpm:ioMapping` → `zenbpm:input`  | `source`, `target` | no       | Maps process variables into the task's local scope. See [Variables](../../../variable-mapping.md).                |
| `zenbpm:ioMapping` → `zenbpm:output` | `source`, `target` | no       | Maps values from the task's local scope back to the process scope. See [Variables](../../../variable-mapping.md). |

Execution flow:

1. A token arrives at the Manual Task and input mappings are evaluated into the task's local scope.
2. Output mappings are applied. **Without output mappings, no variables are propagated to the process scope.**
3. The task completes immediately — no job and no user task is created — and the token moves on along the outgoing sequence flow.
4. The task is recorded in the history of the process instance with the element type `MANUAL_TASK`, its creation and completion time, and its input and output variables, available via the `/process-instances/{key}/history` endpoint.
5. If a mapping expression cannot be evaluated, the task fails.

With a multi-instance marker, every iteration completes immediately as well, and `outputCollection` collects the result of each iteration as for any other activity. Since the task has no wait state, boundary events attached to it have no chance to trigger.

## Related documentation

- [User task](./user-task.md) — for work done by a person that the process has to wait for.
- [Multi-instance activity](../activity-multi-instance.md) — running a Manual Task once per element of a collection.
- [Variables](../../../variable-mapping.md) — how input and output mappings propagate variables.

## XML example

A Manual Task in which the customer signs the contract in person. The input mapping provides the name of the customer and the output mapping records who signed the contract in the process variable `signedBy`.

```xml
<bpmn:manualTask id="Activity_SignContract" name="Sign the contract">
  <bpmn:extensionElements>
    <zenbpm:ioMapping>
      <zenbpm:input source="=customer.name" target="signatory" />
      <zenbpm:output source="=signatory" target="signedBy" />
    </zenbpm:ioMapping>
  </bpmn:extensionElements>
  <bpmn:incoming>Flow_In</bpmn:incoming>
  <bpmn:outgoing>Flow_Out</bpmn:outgoing>
</bpmn:manualTask>
```
