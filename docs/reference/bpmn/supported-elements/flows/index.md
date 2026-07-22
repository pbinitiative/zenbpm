---
sidebar_position: 1
---

# Flows

A Flow connects BPMN elements and defines the order in which they are executed. The token follows the flow from a source element to a target element once the source element completes.

## Key characteristics

- Connects exactly two elements: a source and a target.
- The token follows the flow after the source element completes.
- Every flow element (except start and end events) requires at least one incoming and one outgoing sequence flow.
- Some variants apply only to flows leaving an Exclusive or Inclusive gateway.

## Types

Green icons are supported and link to their documentation.

<table className="bpmn-types-table">
  <thead>
    <tr>
      <th>Flow</th>
      <th style={{width: '150px'}}>Icon</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><a href="./sequence-flow">Sequence flow</a></td>
      <td><a href="./sequence-flow"><img className="bpmn-supported" src={require('!url-loader!../../../assets/bpmn/flows/sequence-flow.svg').default} alt="Sequence Flow" height="50" /></a></td>
      <td>The basic connector between BPMN elements. Defines the order in which activities and events are executed.</td>
    </tr>
    <tr>
      <td><a href="./default-flow">Default flow</a></td>
      <td><a href="./default-flow"><img className="bpmn-supported" src={require('!url-loader!../../../assets/bpmn/flows/default-flow.svg').default} alt="Default Flow" height="50" /></a></td>
      <td>A fallback sequence flow on an Exclusive or Inclusive gateway. Taken when no other outgoing condition evaluates to true.</td>
    </tr>
    <tr>
      <td>Conditional flow</td>
      <td><img className="bpmn-unsupported" src={require('!url-loader!../../../assets/bpmn/flows/conditional-flow.svg').default} alt="Conditional Flow" height="50" /></td>
      <td>A sequence flow with an attached condition expression on a flow leaving an activity. Condition expressions are only evaluated on outgoing flows of an <a href="../gateways/exclusive-gateway">Exclusive</a> or <a href="../gateways/inclusive-gateway">Inclusive gateway</a> — use a gateway to route on conditions.</td>
    </tr>
    <tr>
      <td>Message flow</td>
      <td><img className="bpmn-unsupported" src={require('!url-loader!../../../assets/bpmn/flows/message-flow.svg').default} alt="Message Flow" height="50" /></td>
      <td>Shows the flow of messages between two separate participants (pools) in a Collaboration diagram. Modelled explicitly via Message Events or Send/Receive tasks rather than as part of the process's Sequence Flow.</td>
    </tr>
    <tr>
      <td>Data association</td>
      <td><img className="bpmn-unsupported" src={require('!url-loader!../../../assets/bpmn/flows/data-association.svg').default} alt="Data Association" height="50" /></td>
      <td>Connects a Data Object or Data Store to an activity, showing that data is read or written by that activity. Drawn as a dotted line with an open arrowhead.</td>
    </tr>
  </tbody>
</table>

:::note[Not yet supported]
Conditional flow, Message flow, Data association
:::
