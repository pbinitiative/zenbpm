---
sidebar_position: 10
---

# Message events

Message events let a process react to or produce **messages** — named payloads published to the engine by external systems or by other processes. Catching message events pause the process until a matching message is published; throwing message events do not send anything themselves — like a [Send task](../activities/tasks/send-task.md), they create a **job** and a worker performs the actual delivery.

<table className="bpmn-types-table">
  <thead>
    <tr>
      <th>Position</th>
      <th style={{width: '110px'}}>Icon</th>
      <th>Behavior</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><a href="#message-start-event">Message start event</a></td>
      <td><a href="#message-start-event"><img className="bpmn-supported" src={require('!url-loader!../../../assets/bpmn/events/message-start.svg').default} alt="Message start event" height="70" /></a></td>
      <td>Creates a new process instance each time a matching message is published.</td>
    </tr>
    <tr>
      <td><a href="#message-intermediate-catch-event">Message intermediate catch event</a></td>
      <td><a href="#message-intermediate-catch-event"><img className="bpmn-supported" src={require('!url-loader!../../../assets/bpmn/events/message-intermediate-catch.svg').default} alt="Message intermediate catch event" height="70" /></a></td>
      <td>Pauses the token until a message is correlated to the waiting instance.</td>
    </tr>
    <tr>
      <td><a href="#message-boundary-event">Message boundary event</a></td>
      <td><a href="#message-boundary-event"><img className="bpmn-supported" src={require('!url-loader!../../../assets/bpmn/events/message-boundary-interrupting.svg').default} alt="Message boundary event (interrupting)" height="70" /> <img className="bpmn-supported" src={require('!url-loader!../../../assets/bpmn/events/message-boundary-non-interrupting.svg').default} alt="Message boundary event (non-interrupting)" height="70" /></a></td>
      <td>Reacts to a message while an activity is active — interrupting or non-interrupting.</td>
    </tr>
    <tr>
      <td><a href="#message-intermediate-throw-event">Message intermediate throw event</a></td>
      <td><a href="#message-intermediate-throw-event"><img className="bpmn-supported" src={require('!url-loader!../../../assets/bpmn/events/message-intermediate-throw.svg').default} alt="Message intermediate throw event" height="70" /></a></td>
      <td>Creates a job; a worker sends the message, then the token continues.</td>
    </tr>
    <tr>
      <td><a href="#message-end-event">Message end event</a></td>
      <td><a href="#message-end-event"><img className="bpmn-supported" src={require('!url-loader!../../../assets/bpmn/events/message-end.svg').default} alt="Message end event" height="70" /></a></td>
      <td>Creates a job; a worker sends the message, then the path ends.</td>
    </tr>
  </tbody>
</table>

## Use cases

- **Start a process from the outside** — create an order process instance whenever an `OrderReceived` message is published by the shop frontend.
- **Wait for an external system** — pause the process until the payment provider publishes a `PaymentConfirmed` message correlated by the order id.
- **React while an activity runs** — cancel a running approval task when the customer withdraws the request (interrupting boundary), or log intermediate updates without disturbing it (non-interrupting boundary).
- **Notify at milestones** — send a message to another system or process when a path completes, via a message throw or message end event.

## Usage in BPMN

Every message event references a `bpmn:message` definition through the `messageRef` attribute of its `bpmn:messageEventDefinition`. Everything else depends on the direction: **catching** events subscribe to messages published to the engine, while **throwing** events are executed as jobs.

### Catching message events

Message start, message intermediate catch, and message boundary events receive messages. The message's `name` is what publishers address; for events in a running instance, a `zenbpm:subscription` extension on the message defines the **correlation key** that selects which instance a published message belongs to.

| Markup                                    | Attribute        | Required | Description                                                                                                                                                                                        |
| ----------------------------------------- | ---------------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bpmn:message`                            | `name`           | yes      | The message name. Publishers address messages by this name.                                                                                                                                        |
| `bpmn:message` → `zenbpm:subscription`    | `correlationKey` | recommended | A literal value or a FEEL expression (prefixed with `=`) evaluated against the instance variables when the subscription is created; a published message must carry the same key to be correlated. Without it, messages are matched by name alone, so any waiting instance can consume the publish. Not used by message start events. |
| `bpmn:messageEventDefinition`             | `messageRef`     | yes      | References the `bpmn:message` this event waits for.                                                                                                                                                |
| `zenbpm:ioMapping` → `zenbpm:output`      | `source`, `target` | no     | Filters which payload variables are propagated to the instance. Catching events have no input side. Not applied on message start events. See [Variables](../../variable-mapping.md).                      |

How a message reaches a catching event:

1. When a token arrives at the event, the engine evaluates the correlation key against the instance variables and creates a **message subscription**. For message start events, the subscription is created at deployment time and has no correlation key.
2. An external system publishes a message by name — and, when targeting a running instance, with a correlation key — via the REST or gRPC API. See [Send and receive messages](../../../../how-to/send-receive-messages.md).
3. The engine matches the message to the waiting subscription and applies the payload: **without output mappings, all payload variables are propagated to the instance**; with `zenbpm:output` mappings, only the mapped ones. See [Variables](../../variable-mapping.md).
4. The token continues along the event's outgoing flow — or, for a start event, a new instance begins.

#### Message start event

Starts a new process instance whenever a matching message is published. The subscription exists at the process definition level, so no correlation key is involved — every published message with the matching name creates a fresh instance, and the full message payload becomes the instance's initial variables (output mappings are not applied on start events). Note that each published message is consumed by exactly one subscription: if several deployed processes subscribe to the same message name, one publish creates a single instance, not one per process.

A message start event inside an [Event sub process](../activities/event-sub-process.md) works differently: its subscription is created per instance of the containing scope and **does** use a correlation key.

#### Message intermediate catch event

Pauses the flow until a message with the matching name *and* correlation key is published. The correlation key expression is evaluated when the token arrives at the event, so the identifying variable (for example the order id) must already exist in the instance at that point. A [Receive task](../activities/tasks/receive-task.md) behaves the same way but is modelled as an activity, which allows attaching boundary events and multi-instance markers.

#### Message boundary event

Subscribes to a message while the attached activity is active. When the message is correlated:

- **Interrupting** (solid border) — the activity is cancelled together with its jobs and child instances, and the token continues along the boundary event's outgoing flow.
- **Non-interrupting** (dashed border, `cancelActivity="false"`) — the activity keeps running, a parallel token is created on the outgoing flow, and the subscription is re-created so the event can trigger again while the activity remains active.

### Throwing message events

To the engine, a throwing message event **is simply a job** — its execution is identical to a [Send task](../activities/tasks/send-task.md). The engine does not read the message definition and does not deliver anything itself: it creates a job of the configured type, and what is sent, and to whom, is entirely up to the job worker — publishing a message back to the engine to trigger another process, or calling an external system.

| Markup                                                | Attribute          | Required | Description                                                                                                                                        |
| ----------------------------------------------------- | ------------------ | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bpmn:messageEventDefinition`                         | `messageRef`       | yes      | Marks the event as a message event and documents which message it represents. The engine does not evaluate the referenced message — it only matters to the modeler and the worker. |
| `zenbpm:taskDefinition`                               | `type`             | yes      | The job type. The engine creates a job of this type when the token arrives; workers subscribe to this type to receive the job.                     |
| `zenbpm:ioMapping` → `zenbpm:input` / `zenbpm:output` | `source`, `target` | no       | Maps variables into the job and the job's result back to the process, following the same rules as activities. See [Variables](../../variable-mapping.md). |

Execution flow:

1. A token arrives at the event and the engine creates a job with the configured `type`; input mappings determine the variables the worker sees.
2. A worker activates the job, sends the message, and completes the job.
3. Output mappings propagate the worker's result following the activity rules — **without output mappings, nothing is propagated**.
4. The token continues along the outgoing flow (intermediate throw event) or the path ends (message end event).

#### Message intermediate throw event

Sends the message and continues: once the worker completes the job, the token moves along the outgoing sequence flow.

#### Message end event

Identical to the message intermediate throw event, except that the path ends once the job completes: a worker sends the message, then the token is consumed.

## Related documentation

- [Send and receive messages](../../../../how-to/send-receive-messages.md) — publishing messages and correlating them over the REST API.
- [Variables](../../variable-mapping.md) — payload propagation rules for catching events.
- [Receive task](../activities/tasks/receive-task.md) — the activity-shaped counterpart of the message catch event.
- [Send task](../activities/tasks/send-task.md) — the activity-shaped counterpart of the message throw event.
- [Jobs](../../../jobs.md) — how the jobs created by throwing message events are distributed and completed.
- [Event-based gateway](../gateways/event-based-gateway.md) — racing a message catch event against other triggers.

## XML example

An order process started by a message, waiting for a payment confirmation correlated by order id, and notifying the shipping system through a message throw event executed by a worker:

```xml
<bpmn:message id="Message_OrderReceived" name="OrderReceived" />

<bpmn:message id="Message_PaymentConfirmed" name="PaymentConfirmed">
  <bpmn:extensionElements>
    <zenbpm:subscription correlationKey="=order.id" />
  </bpmn:extensionElements>
</bpmn:message>

<bpmn:message id="Message_ShipOrder" name="ShipOrder" />

<bpmn:startEvent id="Start_OrderReceived" name="Order received">
  <bpmn:outgoing>Flow_1</bpmn:outgoing>
  <bpmn:messageEventDefinition messageRef="Message_OrderReceived" />
</bpmn:startEvent>

<bpmn:intermediateCatchEvent id="Catch_PaymentConfirmed" name="Payment confirmed">
  <bpmn:extensionElements>
    <zenbpm:ioMapping>
      <zenbpm:output source="=paymentReference" target="paymentReference" />
    </zenbpm:ioMapping>
  </bpmn:extensionElements>
  <bpmn:incoming>Flow_1</bpmn:incoming>
  <bpmn:outgoing>Flow_2</bpmn:outgoing>
  <bpmn:messageEventDefinition messageRef="Message_PaymentConfirmed" />
</bpmn:intermediateCatchEvent>

<bpmn:intermediateThrowEvent id="Throw_NotifyShipping" name="Notify shipping">
  <bpmn:extensionElements>
    <zenbpm:taskDefinition type="publish-message" />
    <zenbpm:ioMapping>
      <zenbpm:input source="=order.id" target="orderId" />
    </zenbpm:ioMapping>
  </bpmn:extensionElements>
  <bpmn:incoming>Flow_2</bpmn:incoming>
  <bpmn:outgoing>Flow_3</bpmn:outgoing>
  <bpmn:messageEventDefinition messageRef="Message_ShipOrder" />
</bpmn:intermediateThrowEvent>
```
