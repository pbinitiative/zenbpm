---
sidebar_position: 20
---

# Sub process

A Sub process embeds a complete flow — with its own start and end events — inside a parent process. It groups related steps into a single unit with its own variable scope and error handling boundary. In ZenBPM the embedded flow runs as its own child process instance linked to the parent on the same [partition](../../../cluster.md); the parent token waits at the Sub process until the embedded flow completes.

<img src={require('!url-loader!../../../assets/bpmn/activities/sub-process.svg').default} alt="Sub process" width="110" height="90" />

Rendered collapsed as a rounded rectangle with a plus marker at the bottom center; expanded, the embedded flow is drawn inside the rounded rectangle.

## Use cases

- **Handle a section as a unit** — attach a single timer or error [boundary event](../events/boundary-events/index.md) to the Sub process to time-box or cancel a whole group of steps at once.
- **Repeat a section per item** — put the [multi-instance marker](./activity-multi-instance.md) on the Sub process to run the embedded flow once per element of a collection.
- **React to events while the process runs** — model an event sub process that is triggered by a message or timer whenever its scope is active.

## Usage in BPMN

A Sub process needs no ZenBPM-specific configuration: it is a `bpmn:subProcess` element containing its own flow elements. Data flow in and out is controlled with a `zenbpm:ioMapping`, like on any activity.

| Element / attribute                  | Required | Description                                                                                                                       |
| ------------------------------------ | -------- | ---------------------------------------------------------------------------------------------------------------------------------|
| `zenbpm:ioMapping` → `zenbpm:input`  | no       | Initializes variables of the child instance. See [Variables](../../variables.md).                                                 |
| `zenbpm:ioMapping` → `zenbpm:output` | no       | Maps variables of the completed child instance back to the parent scope. See [Variables](../../variables.md).                     |
| `triggeredByEvent` attribute         | no       | `true` turns the Sub process into an event sub process — see below.                                                               |

Execution flow:

1. A token arrives at the Sub process, input mappings are evaluated, and a child process instance is created — linked to the parent, on the same partition, starting with a snapshot copy of the parent's variables plus the input-mapped values. The parent token waits, and boundary events attached to the Sub process are armed; when one triggers, the entire embedded flow is interrupted regardless of which inner element is active.
2. A token starts at each start event of the embedded flow — multiple start events of any supported type are allowed.
3. The embedded flow executes as its own instance in its own scope; changes to its variables do not write back to the parent.
4. On completion, **only output-mapped variables are propagated to the parent scope**, and the parent token continues.
5. An error end event inside the Sub process bubbles up and can be caught by an [error boundary event](../events/boundary-events/error-boundary-event.md) on the Sub process or a surrounding scope. An uncaught error creates an incident.

### Event sub process

A Sub process with `triggeredByEvent="true"` is not part of the normal flow — it has no incoming or outgoing sequence flows. It must contain exactly one start event, of message or timer type, and it is armed whenever its containing scope is active:

- With an **interrupting** start event, triggering it cancels the remaining flow of the containing scope.
- With a **non-interrupting** start event, the event sub process runs alongside the normal flow and can trigger repeatedly (for example on a timer cycle).

Event sub processes can be nested inside other Sub processes.

## Related documentation

- [Variables](../../variables.md) — variable scoping and output mapping propagation rules.
- [Boundary events](../events/boundary-events/index.md) — interrupting the whole Sub process on errors, timeouts, or messages.
- [Multi-instance activity](./activity-multi-instance.md) — running the embedded flow once per element of a collection.

## XML example

A Sub process that reserves stock for an order. The input mapping passes the order items into the child instance; inside, a service task performs the reservation and output-maps its result, which the Sub process output mapping propagates back to the parent as `reservationId`:

```xml
<bpmn:subProcess id="Activity_ReserveItems" name="Reserve items">
  <bpmn:extensionElements>
    <zenbpm:ioMapping>
      <zenbpm:input source="=order.items" target="items" />
      <zenbpm:output source="=reservationId" target="reservationId" />
    </zenbpm:ioMapping>
  </bpmn:extensionElements>
  <bpmn:incoming>Flow_In</bpmn:incoming>
  <bpmn:outgoing>Flow_Out</bpmn:outgoing>
  <bpmn:startEvent id="SubStart">
    <bpmn:outgoing>Flow_S1</bpmn:outgoing>
  </bpmn:startEvent>
  <bpmn:serviceTask id="Task_ReserveStock" name="Reserve stock">
    <bpmn:extensionElements>
      <zenbpm:taskDefinition type="reserve-stock" />
      <zenbpm:ioMapping>
        <zenbpm:output source="=reservationId" target="reservationId" />
      </zenbpm:ioMapping>
    </bpmn:extensionElements>
    <bpmn:incoming>Flow_S1</bpmn:incoming>
    <bpmn:outgoing>Flow_S2</bpmn:outgoing>
  </bpmn:serviceTask>
  <bpmn:endEvent id="SubEnd">
    <bpmn:incoming>Flow_S2</bpmn:incoming>
  </bpmn:endEvent>
  <bpmn:sequenceFlow id="Flow_S1" sourceRef="SubStart" targetRef="Task_ReserveStock" />
  <bpmn:sequenceFlow id="Flow_S2" sourceRef="Task_ReserveStock" targetRef="SubEnd" />
</bpmn:subProcess>
```
