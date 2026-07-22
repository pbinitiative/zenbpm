---
sidebar_position: 40
---

# Send task

A Send Task sends a message to an external recipient and immediately continues execution without waiting for a response. In ZenBPM the actual sending is performed by a job worker — the task creates a **job** and the worker delivers the message (e-mail, message queue, API call, or a message correlated to another process).

<img src={require('!url-loader!../../../../assets/bpmn/activities/send-task.svg').default} alt="Send task" width="110" height="90" />

Rendered as a rounded rectangle with a filled envelope icon in the top-left corner.

## Use cases

- **Notify a customer** — send an order confirmation e-mail or SMS and continue the process without waiting for a reply.
- **Publish an event** — push a message to a message broker so downstream systems learn about a process milestone.
- **Message another process** — send a message that is correlated to a message start or catch event in a different process.

## Usage in BPMN

A Send Task is configured and executed exactly like a [Service task](./service-task.md): define the job type in `zenbpm:taskDefinition` and optionally map variables with `zenbpm:ioMapping`. See the [Service task usage](./service-task.md#usage-in-bpmn) for details.

## Related documentation

- [Service task](./service-task.md) — full configuration and execution details for job-based tasks: `zenbpm:taskDefinition`, variable mappings, and job handling.

## XML example

A Send Task that creates a job of type `send-email`. The input mappings give the worker the recipient address and the order it should reference in the message.

```xml
<bpmn:sendTask id="Activity_SendConfirmation" name="Send order confirmation">
  <bpmn:extensionElements>
    <zenbpm:taskDefinition type="send-email" />
    <zenbpm:ioMapping>
      <zenbpm:input source="=customer.email" target="recipient" />
      <zenbpm:input source="=order.id" target="orderId" />
    </zenbpm:ioMapping>
  </bpmn:extensionElements>
  <bpmn:incoming>Flow_In</bpmn:incoming>
  <bpmn:outgoing>Flow_Out</bpmn:outgoing>
</bpmn:sendTask>
```
