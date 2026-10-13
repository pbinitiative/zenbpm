---
sidebar_position: 10
---

# Call activity

A Call Activity invokes another, independently deployed process. It lets several processes reuse the same process logic and keeps large models decomposed into separately deployed and versioned pieces. In ZenBPM the called process runs as its own process instance linked to the parent on the same [partition](../../../cluster.md); the parent token waits at the Call Activity until the called instance completes.

<img src={require('!url-loader!../../../assets/bpmn/activities/call-activity.svg').default} alt="Call activity" width="110" height="90" />

Rendered as a rounded rectangle with a thick border and a plus marker at the bottom center.

## Use cases

- **Reuse shared logic** — call one deployed process, such as customer verification, from several different parent processes.
- **Decompose large processes** — split a complex model into separately deployed processes with independent lifecycles.
- **Process collections** — combine with the [multi-instance marker](./activity-multi-instance.md) to start one called instance per element, for example one delivery process per shipment.

## Usage in BPMN

Reference the process to call in a `zenbpm:calledElement` extension element. Optionally, control the data that flows into and out of the called instance with a `zenbpm:ioMapping`.

| Extension element                    | Attribute          | Required | Description |
| ------------------------------------ | ------------------ | -------- | ----------- |
| `zenbpm:calledElement`               | `processId`        | yes      | Id of the deployed process to start. Only a direct, static id is supported; expressions are not evaluated. |
| `zenbpm:calledElement`               | `bindingType`      | no       | Set `bindingType="versionTag"` together with `versionTag="..."` for version-tag selection, or set `bindingType="latest"` to select the highest deployed numeric version. When omitted, `latest` is the default unless the direct `version` attribute is configured. |
| `zenbpm:calledElement`               | `versionTag`       | with `bindingType="versionTag"` | The engine first looks for an exact match against the deployed process's stored `zenbpm:versionTag`. If no exact tag matches and the value has the form `v<positive-number>`, such as `v2`, the engine selects that exact numeric process-definition version. If neither definition exists, the Call Activity creates an incident; it does not fall back to the latest version. |
| `zenbpm:calledElement`               | `version`          | no       | Selects an exact positive numeric process-definition version directly. Do not combine it with `bindingType`. |
| `zenbpm:calledElement`               | `propagateAllChildVariables` | no | **Not supported** and ignored when present. Unlike Zeebe, where it defaults to `true`, ZenBPM never propagates all variables of the called instance; only output-mapped variables reach the parent. |
| `zenbpm:ioMapping` → `zenbpm:input`  | `source`, `target` | no       | Initializes variables of the called instance. See [Variables](../../variable-mapping.md). |
| `zenbpm:ioMapping` → `zenbpm:output` | `source`, `target` | no       | Maps variables of the completed called instance back to the process scope. See [Variables](../../variable-mapping.md). |

:::tip[Version tag precedence]

An exact stored version tag always takes precedence over the numeric interpretation. For example, if a deployed definition has the stored version tag `v2`, `versionTag="v2"` selects that tagged definition even when it is not numeric version 2. The numeric-version lookup is attempted only when no exact stored tag matches.

To avoid this ambiguity, do not use the `v<positive-number>` form for stored version tags unless this precedence is intentional.

:::

Execution flow:

1. A token arrives at the Call Activity and input mappings are evaluated. A direct numeric `version` selects that exact deployed process version. With `bindingType="versionTag"`, the engine first resolves an exact stored tag and then, only for an unmatched `versionTag="v<positive-number>"`, tries the corresponding numeric version. Without an explicit selection, the engine resolves the available version with the highest number. If all lookups applicable to an explicit selection fail, the activity creates an incident instead of falling back to the latest version.
2. A new instance of the called process is created, linked to the parent and running on the same partition. It starts with a snapshot copy of the parent's variables plus the input-mapped values; changes inside the called instance do not write back to the parent. The parent token waits, and boundary events attached to the Call Activity are armed.
3. The called instance executes like any other process instance.
4. On completion, **only output-mapped variables are propagated to the parent scope**, and the parent token continues. An output mapping's source expression is evaluated against the called instance's variables. Without output mappings no variable of the called instance reaches the parent — not even one whose name already exists in the parent. Boundary event subscriptions that did not trigger are closed.

## Error handling

An error raised inside the called instance propagates up to the parent and can be caught there. Both error sources are supported:

- an [error end event](../events/error-events.md#error-end-event) reached in the called process, and
- a job worker reporting a business error, that is, failing a job in the called process with an error code.

The engine looks for a handler in the called instance first. If the called instance has none, it looks at the Call Activity, where an [error boundary event](../events/error-events.md#error-boundary-event) referencing the error code takes precedence over a catch-all one, and then at the parent's error [event sub process](./event-sub-process.md). The search continues across nested Call Activities until a handler matches.

When an error boundary event on the Call Activity catches the error:

- The error boundary event is interrupting. The called instance, and every instance the error travelled through, is terminated, and the parent token leaves through the boundary event's outgoing flow.
- The Call Activity's own output mappings are **not** evaluated. Data reaches the parent only as the error payload.
- A business error from a failed job carries the job's variables as its payload. They are all propagated to the parent unless the boundary event defines `zenbpm:output` mappings, in which case only the mapped variables are (see [Variables](../../variable-mapping.md)). An error end event carries no payload.

When nothing catches the error, an incident is recorded instead:

- For a business error from a failed job, the incident belongs to that job in the called instance. The parent and the called instance stay active, and the parent token keeps waiting at the Call Activity.
- For an error end event, the incident is recorded in the root process instance, on the element through which the error arrived there: the Call Activity, or an enclosing sub process or multi-instance activity. Unlike the job case, that element's token is completed and its boundary event subscriptions are closed, so the parent does not continue on its own. No instance is terminated or failed: the instances along the way stay active.

## Related documentation

- [Variables](../../variable-mapping.md) — variable scoping and output mapping propagation rules.
- [Error boundary event](../events/error-events.md#error-boundary-event) — catching errors thrown by the called process.
- [Multi-instance activity](./activity-multi-instance.md) — calling the process once per element of a collection.

## XML example

A Call Activity that starts the latest deployed version of the `customer-verification` process. The input mapping passes the customer id into the called instance; the output mapping stores the verification result in the parent process variable `customerVerified`:

```xml
<bpmn:callActivity id="Activity_VerifyCustomer" name="Verify customer">
  <bpmn:extensionElements>
    <zenbpm:calledElement processId="customer-verification" />
    <zenbpm:ioMapping>
      <zenbpm:input source="=order.customerId" target="customerId" />
      <zenbpm:output source="=verificationResult" target="customerVerified" />
    </zenbpm:ioMapping>
  </bpmn:extensionElements>
  <bpmn:incoming>Flow_In</bpmn:incoming>
  <bpmn:outgoing>Flow_Out</bpmn:outgoing>
</bpmn:callActivity>
```

To select exact numeric version 2:

```xml
<zenbpm:calledElement processId="customer-verification" version="2" />
```

To select by stored process version tag instead:

```xml
<zenbpm:calledElement
  processId="customer-verification"
  bindingType="versionTag"
  versionTag="stable-release" />
```

To use the `versionTag` binding while selecting numeric version 2 when no definition has the exact stored tag `v2`:

```xml
<zenbpm:calledElement
  processId="customer-verification"
  bindingType="versionTag"
  versionTag="v2" />
```
