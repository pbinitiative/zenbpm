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
4. On completion, **only output-mapped variables are propagated to the parent scope**, and the parent token continues.
5. An error end event in the called process bubbles up to the parent and can be caught by an [error boundary event](../events/error-events.md#error-boundary-event) on the Call Activity — including across nested Call Activities. An uncaught error creates an incident.

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
