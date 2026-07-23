---
sidebar_position: 25
---

# Event sub process

An Event sub process is a [Sub process](./sub-process.md) that is not part of the normal flow: it has no incoming or outgoing sequence flows and is started by its own **start event** instead. While its containing scope is active, the engine keeps the trigger armed — when the event occurs, the embedded flow runs as a child instance of that scope, either interrupting the scope or running alongside it.

<img src={require('!url-loader!../../../assets/bpmn/activities/event-sub-process.svg').default} alt="Event sub process" width="110" height="90" />

Rendered as a rounded rectangle with a dotted border; expanded, the embedded flow is drawn inside, beginning with the start event that triggers it.

## Use cases

- **Cancellation handling** — an interrupting message event sub process cancels the whole order flow when a `CancelOrder` message arrives, no matter which step the process is currently at.
- **Reminders and housekeeping while the process runs** — a non-interrupting timer event sub process with a cycle fires repeatedly alongside the normal flow.
- **Scope-level error handling** — an error event sub process catches errors thrown anywhere in its scope, replacing error boundary events on each individual activity.

## Usage in BPMN

| Element / attribute                             | Required | Description                                                                                                                                                    |
| ----------------------------------------------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bpmn:subProcess` → `triggeredByEvent="true"`   | yes      | Marks the Sub process as an event sub process.                                                                                                                 |
| exactly one `bpmn:startEvent` inside            | yes      | The trigger — a [message](../events/message-events.md), [timer](../events/timer-events.md), or [error](../events/error-events.md) event definition.            |
| start event `isInterrupting` attribute          | no       | Defaults to `true`. `false` makes the event sub process non-interrupting; error start events are always interrupting.                                          |
| `zenbpm:ioMapping` → `zenbpm:output` on the start event | no | Filters which trigger payload variables are propagated; without it the entire payload is propagated, like on any catching event. See [Variables](../../variable-mapping.md). |

Execution flow:

1. When an instance of the containing scope is created, the engine arms the trigger: a message subscription — with the correlation key evaluated against the scope's variables, unlike a top-level message start event — or a timer (`timeDate`, `timeDuration`, or `timeCycle`). An error start event needs no subscription; it is matched when an error propagates through the scope. If arming fails (for example the correlation key cannot be evaluated), an incident is recorded.
2. When the trigger fires, the behavior depends on the start event:
   - **Interrupting** (default) — all active tokens of the containing scope are cancelled, then the event sub process runs.
   - **Non-interrupting** (`isInterrupting="false"`) — the scope keeps running and the trigger is re-armed (the message subscription is re-created, a `timeCycle` timer is re-scheduled), so the event sub process can fire any number of times while the scope is active.
3. The trigger payload is propagated to the scope following the catching-event rules and is also visible inside the event sub process, which then executes as its own child instance. Event sub processes nested deeper are armed when their own scope activates.

For **error** start events, the event sub process takes part in regular error propagation: at each scope, an [error boundary event](../events/error-events.md#error-boundary-event) attached to the throwing activity wins over the scope's error event sub process, and a start event referencing a specific error code wins over a catch-all one without `errorRef`.

## Related documentation

- [Sub process](./sub-process.md) — the embedded flow, scoping, and variable propagation shared with regular Sub processes.
- [Message events](../events/message-events.md#catching-message-events) — message definitions, subscriptions, and correlation keys for message triggers.
- [Timer events](../events/timer-events.md) — the timer formats available for timer triggers.
- [Error events](../events/error-events.md) — how thrown errors are matched and propagated.
- [Variables](../../variable-mapping.md) — payload propagation rules for catching events.

## XML example

An interrupting event sub process inside an order process: whenever a `CancelOrder` message correlated by order id arrives, the running flow is cancelled, the stock is released, and the instance ends:

```xml
<bpmn:subProcess id="EventSub_Cancel" name="Handle cancellation" triggeredByEvent="true">
  <bpmn:startEvent id="Start_CancelRequested" name="Cancel requested">
    <bpmn:outgoing>Flow_E1</bpmn:outgoing>
    <bpmn:messageEventDefinition messageRef="Message_CancelOrder" />
  </bpmn:startEvent>
  <bpmn:serviceTask id="Task_ReleaseStock" name="Release stock">
    <bpmn:extensionElements>
      <zenbpm:taskDefinition type="release-stock" />
    </bpmn:extensionElements>
    <bpmn:incoming>Flow_E1</bpmn:incoming>
    <bpmn:outgoing>Flow_E2</bpmn:outgoing>
  </bpmn:serviceTask>
  <bpmn:endEvent id="End_Cancelled" name="Order cancelled">
    <bpmn:incoming>Flow_E2</bpmn:incoming>
  </bpmn:endEvent>
  <bpmn:sequenceFlow id="Flow_E1" sourceRef="Start_CancelRequested" targetRef="Task_ReleaseStock" />
  <bpmn:sequenceFlow id="Flow_E2" sourceRef="Task_ReleaseStock" targetRef="End_Cancelled" />
</bpmn:subProcess>

<bpmn:message id="Message_CancelOrder" name="CancelOrder">
  <bpmn:extensionElements>
    <zenbpm:subscription correlationKey="=order.id" />
  </bpmn:extensionElements>
</bpmn:message>
```
