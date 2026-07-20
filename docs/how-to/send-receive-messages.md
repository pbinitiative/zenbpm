# Send and Receive Messages

Messages allow external systems to communicate with running process instances. They are used to trigger **Message Start Events**, **Message Intermediate Catch Events**, and **Message Boundary Events**.

## Message Correlation

When you send a message, ZenBPM correlates it to the correct process instance using:
1. The **message name** — must match the `name` attribute on the BPMN message element.
2. A **correlation key** (optional) — a process variable value used to identify the specific instance.

## Trigger a Message Start Event

To start a new process instance via a message:

```http
POST /messages/publish
Content-Type: application/json

{
  "messageName": "OrderReceived",
  "variables": {
    "orderId": "ORD-001",
    "customerId": "C-42"
  }
}
```

A new process instance is created for each published message.

## Correlate a Message to a Running Instance

To resume a waiting **Message Intermediate Catch Event** or **Message Boundary Event**:

```http
POST /messages/publish
Content-Type: application/json

{
  "messageName": "PaymentConfirmed",
  "correlationKey": "ORD-001",
  "variables": {
    "paymentReference": "PAY-XYZ",
    "paidAmount": 1500
  }
}
```

### How correlation works

The `correlationKey` is matched against a process variable that was designated as the correlation variable when the Catch Event was defined in the BPMN. Make sure the running instance has a variable with the matching value.

## BPMN Setup

For a Message Intermediate Catch Event:

```xml
<bpmn:message id="Message_PaymentConfirmed" name="PaymentConfirmed" />

<bpmn:intermediateCatchEvent id="WaitForPayment" name="Wait for payment">
  <bpmn:messageEventDefinition messageRef="Message_PaymentConfirmed" />
</bpmn:intermediateCatchEvent>
```

The process instance must have a variable matching the correlation key you send.

## Troubleshooting

**Message not delivered:** Check that the message name matches exactly (case-sensitive) and the correlation key variable exists in the target instance.

**Multiple instances matched:** Each message is delivered to exactly one instance. If multiple instances are waiting for the same message with the same correlation key, the result is undefined — design your process to avoid this.

## Next Steps

- [Handle errors](./handle-errors) using Error Boundary Events
- [Use process variables](./use-process-variables) to store and retrieve correlation data
