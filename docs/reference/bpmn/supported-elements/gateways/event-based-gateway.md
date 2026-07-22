---
sidebar_position: 40
---

# Event-based gateway

An Event-based gateway routes the flow based on **which event occurs first**. Instead of evaluating data conditions, it subscribes to all the catch events on its outgoing paths and takes the path of the winner — the other branches are cancelled.

<img src={require('!url-loader!../../../assets/bpmn/gateways/event-based-gateway.svg').default} alt="Event-based gateway" width="110" height="110" />

Rendered as a diamond with a pentagon-in-double-circle marker.

## Use cases

- **Reply or timeout** — after sending a quote, wait for the customer's reply message, but escalate if a 48-hour timer fires first.
- **Race competing messages** — continue differently depending on whether a payment confirmation or a cancellation arrives first.
- **Periodic re-check while waiting** — combine a message catch with a timer to poll or remind until the awaited message arrives.

## Usage in BPMN

The gateway itself carries no configuration; its outgoing sequence flows must each lead **directly to an intermediate catch event** — in ZenBPM a [message catch event](../events/message-events.md#message-intermediate-catch-event) or a [timer catch event](../events/timer-events.md#timer-intermediate-catch-event). No conditions, and no other element types (receive tasks or other events after the gateway are not supported).

Execution flow:

1. A token arrives at the gateway and the engine subscribes to **all** target events at once — message subscriptions are created and timers armed, while the token waits at the gateway.
2. The first event to occur wins: for a message, its payload is propagated according to the catch event's output mappings (see [Variables](../../variable-mapping.md)).
3. All other subscriptions are terminated and timers cancelled — the losing branches can no longer trigger.
4. The token continues along the outgoing flow of the **winning catch event**, not of the gateway.

## Related documentation

- [Message events](../events/message-events.md#message-intermediate-catch-event) — message definitions, subscriptions, and correlation keys for the message branches.
- [Timer events](../events/timer-events.md#timer-intermediate-catch-event) — timer formats for the timeout branches.
- [Exclusive gateway](./exclusive-gateway.md) — branching on data instead of on events.

## XML example

After sending a quote, the process waits for the customer's reply; if none arrives within 48 hours, the timer branch escalates:

```xml
<bpmn:eventBasedGateway id="Gateway_ReplyOrTimeout" name="Reply or timeout?">
  <bpmn:incoming>Flow_In</bpmn:incoming>
  <bpmn:outgoing>Flow_Reply</bpmn:outgoing>
  <bpmn:outgoing>Flow_Timeout</bpmn:outgoing>
</bpmn:eventBasedGateway>

<bpmn:intermediateCatchEvent id="Catch_Reply" name="Reply received">
  <bpmn:incoming>Flow_Reply</bpmn:incoming>
  <bpmn:outgoing>Flow_ProcessReply</bpmn:outgoing>
  <bpmn:messageEventDefinition messageRef="Message_QuoteReply" />
</bpmn:intermediateCatchEvent>

<bpmn:intermediateCatchEvent id="Catch_Timeout" name="48h passed">
  <bpmn:incoming>Flow_Timeout</bpmn:incoming>
  <bpmn:outgoing>Flow_Escalate</bpmn:outgoing>
  <bpmn:timerEventDefinition>
    <bpmn:timeDuration>PT48H</bpmn:timeDuration>
  </bpmn:timerEventDefinition>
</bpmn:intermediateCatchEvent>

<bpmn:message id="Message_QuoteReply" name="QuoteReply">
  <bpmn:extensionElements>
    <zenbpm:subscription correlationKey="=quote.id" />
  </bpmn:extensionElements>
</bpmn:message>
```
