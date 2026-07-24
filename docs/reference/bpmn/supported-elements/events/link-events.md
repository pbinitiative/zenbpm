---
sidebar_position: 40
---

# Link events

Link events connect two points of the same process without drawing a sequence flow between them. They always come as a pair matched by link name: the **throw** event is the exit point, the **catch** event is where the flow resumes. Links are purely a diagramming device — the token moves from throw to catch immediately, nothing is executed and nothing is waited for.

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
      <td><a href="#link-intermediate-throw-event">Link intermediate throw event</a></td>
      <td><a href="#link-intermediate-throw-event"><img className="bpmn-supported" src={require('!url-loader!../../../assets/bpmn/events/link-intermediate-throw.svg').default} alt="Link intermediate throw event" height="70" /></a></td>
      <td>Exit point — hands the token over to the catch event with the same link name.</td>
    </tr>
    <tr>
      <td><a href="#link-intermediate-catch-event">Link intermediate catch event</a></td>
      <td><a href="#link-intermediate-catch-event"><img className="bpmn-supported" src={require('!url-loader!../../../assets/bpmn/events/link-intermediate-catch.svg').default} alt="Link intermediate catch event" height="70" /></a></td>
      <td>Re-entry point — the flow resumes here.</td>
    </tr>
  </tbody>
</table>

## Use cases

- **Off-page connectors** — split a large diagram into readable sections and connect them by name instead of dragging sequence flows across the whole canvas.
- **Avoid crossing flows** — replace long sequence flows that would cross many other elements with a link pair.
- **Shared continuation** — let several branches jump to the same continuation point: multiple throw events with the same link name can target one catch event.

## Usage in BPMN

Both sides carry a `bpmn:linkEventDefinition` whose `name` pairs them up.

| Markup                     | Attribute | Required | Description                                                                                      |
| -------------------------- | --------- | -------- | ------------------------------------------------------------------------------------------------ |
| `bpmn:linkEventDefinition` | `name`    | yes      | The link name. Throw and catch events with the same name in the same process form a pair.        |

Rules:

- The throw event has one incoming sequence flow and **no outgoing flow**; the catch event has one outgoing sequence flow and **no incoming flow**.
- Throw and catch must be in the same process — a link cannot cross a process or sub process boundary.
- When a token reaches the throw event, it continues immediately at the matching catch event's outgoing flow. There is no wait state, and the process variables are unchanged.
- A throw event whose link name has no matching catch event fails the token at runtime.

### Link intermediate throw event

The sending side, rendered as a double-line circle with a filled arrow. The token enters, the engine finds the catch event with the same link name, and execution continues there.

### Link intermediate catch event

The receiving side, rendered as a double-line circle with an unfilled arrow. It is never activated by an incoming flow — only through its throw counterpart.

## Related documentation

- [Events overview](./index.mdx) — positions, catch and throw semantics shared by all events.
- [Sequence flow](../sequence-flow.md) — the connection a link pair substitutes for.

## XML example

A link pair named `Fulfillment` connecting the end of the ordering section to the fulfillment section of the same process:

```xml
<bpmn:intermediateThrowEvent id="Link_ToFulfillment" name="To fulfillment">
  <bpmn:incoming>Flow_OrderAccepted</bpmn:incoming>
  <bpmn:linkEventDefinition id="LinkDef_Throw" name="Fulfillment" />
</bpmn:intermediateThrowEvent>

<bpmn:intermediateCatchEvent id="Link_FromOrdering" name="From ordering">
  <bpmn:outgoing>Flow_StartFulfillment</bpmn:outgoing>
  <bpmn:linkEventDefinition id="LinkDef_Catch" name="Fulfillment" />
</bpmn:intermediateCatchEvent>
```
