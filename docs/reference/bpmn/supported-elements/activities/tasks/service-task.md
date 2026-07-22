---
sidebar_position: 10
---

# Service task

A Service Task represents work performed automatically by a system, without human involvement. In ZenBPM a Service Task does not call the external system itself — when a token reaches the task, the engine creates a **job** and waits. A job worker (your application) picks the job up, executes the business logic, and completes it, after which the token continues along the outgoing sequence flow.

<img src={require('!url-loader!../../../../assets/bpmn/activities/service-task.svg').default} alt="Service task" width="110" height="90" />

Rendered as a rounded rectangle with a gear icon in the top-left corner.

## Use cases

- **Call an external system** — charge a credit card through a payment gateway, send a request to a shipping provider, or query a customer record from a CRM.
- **Run background processing** — generate a PDF invoice, resize an image, or transform a dataset as part of the process.
- **Update another system of record** — write the process result to an ERP, a database, or a legacy application so downstream systems stay in sync.

## Usage in BPMN

To make a Service Task executable, define the job type in a `zenbpm:taskDefinition` extension element. Optionally, control the data that flows into and out of the task with a `zenbpm:ioMapping`.

| Extension element                    | Attribute          | Required | Description                                                                                                                                                                               |
| ------------------------------------ | ------------------ | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zenbpm:taskDefinition`              | `type`             | yes      | The job type. The engine creates a job of this type when the task is activated; workers subscribe to this type to receive the job.                                                        |
| `zenbpm:ioMapping` → `zenbpm:input`  | `source`, `target` | no       | Maps process variables into the task's local scope, visible to the job worker. See [Variables](../../../variables.md). |
| `zenbpm:ioMapping` → `zenbpm:output` | `source`, `target` | no       | Maps the job's result variables back to the process scope. See [Variables](../../../variables.md).                     |

Execution flow:

1. A token arrives at the Service Task and the engine creates a job with the configured `type`, carrying the task's variables.
2. A worker activates the job via the REST or gRPC API (or an internal task handler registered on the engine), performs the work, and completes or fails it.
3. On completion, output mappings are applied and the token moves on. **Without output mappings, no variables returned by the worker are propagated to the process scope** — define an output mapping for every value you want to keep.
4. On failure, an incident is created; the failure can be handled with an error boundary event.

:::note[Not yet supported]
The `retries` attribute of `zenbpm:taskDefinition` and task headers are parsed but not yet applied by the engine.
:::

## Related documentation

- [Jobs](../../../../jobs.md) — how jobs are created, distributed, and completed over the REST and gRPC APIs.
- [Implement a job worker](../../../../../how-to/implement-job-worker.md) — build the application that executes Service Task jobs.
- [Variables](../../../variables.md) — variable scoping and output mapping propagation rules for activities.
- [Handle errors](../../../../../how-to/handle-errors.md) — failing jobs, incidents, and recovering from errors.
- [Error boundary event](../../events/boundary-events/error-boundary-event.md) — routing the process when a job throws a BPMN error.
- [Multi-instance activity](../activity-multi-instance.md) — running a Service Task once per element of a collection.

## XML example

A Service Task that creates a job of type `charge-card`. The input mapping passes the order total to the worker as `amount`; the output mapping stores the worker's `transactionId` result in the process variable `paymentTransactionId`.

```xml
<bpmn:serviceTask id="Activity_ChargeCard" name="Charge credit card">
  <bpmn:extensionElements>
    <zenbpm:taskDefinition type="charge-card" />
    <zenbpm:ioMapping>
      <zenbpm:input source="=order.totalAmount" target="amount" />
      <zenbpm:output source="=transactionId" target="paymentTransactionId" />
    </zenbpm:ioMapping>
  </bpmn:extensionElements>
  <bpmn:incoming>Flow_In</bpmn:incoming>
  <bpmn:outgoing>Flow_Out</bpmn:outgoing>
</bpmn:serviceTask>
```
