---
sidebar_position: 50
---

# Receive task

A Receive Task pauses the process and waits until a specific message is received, then continues execution. It behaves like a [Message intermediate catch event](../../events/message-events.md#message-intermediate-catch-event) modelled as a task shape, which allows attaching boundary events and multi-instance markers.

<img src={require('!url-loader!../../../../assets/bpmn/activities/receive-task.svg').default} alt="Receive task" width="110" height="90" />

Rendered as a rounded rectangle with an unfilled envelope icon in the top-left corner.

## Use cases

- **Wait for confirmation** — pause the order process until a payment confirmation arrives from the payment provider.
- **Request–response between processes** — wait for the reply message of a request previously sent by a [Send task](./send-task.md).
- **External callbacks** — hold the process until an external system reports back, such as a shipment-delivered notification.

## Usage in BPMN

A Receive Task references a message definition via its `messageRef` attribute; the message is correlated to a waiting instance exactly like a [Message intermediate catch event](../../events/message-events.md#catching-message-events) — see that page for message definitions, subscriptions, and correlation keys. Additionally, with `instantiate="true"` the Receive Task can start a new process instance when the message arrives, like a message start event.

## Related documentation

- [Message events](../../events/message-events.md#catching-message-events) — a Receive Task receives messages in nearly the same way as a message intermediate catch event, so everything about defining messages, subscribing to them, and correlating them to a waiting process instance is documented there and applies here as well.

## XML example

A Receive Task that waits for the `OrderPaid` message, correlated to the process instance by the order id:

```xml
<bpmn:receiveTask id="Activity_WaitForPayment" name="Wait for payment" messageRef="Message_OrderPaid">
  <bpmn:incoming>Flow_In</bpmn:incoming>
  <bpmn:outgoing>Flow_Out</bpmn:outgoing>
</bpmn:receiveTask>

<bpmn:message id="Message_OrderPaid" name="OrderPaid">
  <bpmn:extensionElements>
    <zenbpm:subscription correlationKey="=order.id" />
  </bpmn:extensionElements>
</bpmn:message>
```
