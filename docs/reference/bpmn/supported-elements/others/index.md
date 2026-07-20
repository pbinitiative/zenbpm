---
sidebar_position: 1
---

# Others

Others are supporting BPMN artifacts that are not part of the control flow itself. They add data, documentation, or collaboration context to a diagram without being connected via sequence flows.

## Key characteristics

- Not part of the process flow; has no incoming or outgoing sequence flows.
- Connected to flow elements via Associations, Data Associations, or Message Flows rather than Sequence Flows.
- Mostly supported as visual/documentation elements — the BPMN engine does not automatically act on them at runtime.

## Types

Green icons are supported and link to their documentation.

<table className="bpmn-types-table">
  <thead>
    <tr>
      <th>Element</th>
      <th style={{width: '150px'}}>Icon</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><a href="./data-store-reference">Data Store Reference</a></td>
      <td><a href="./data-store-reference"><img src={require('!url-loader!../../../assets/bpmn/others/data-store-reference.svg').default} alt="Data Store Reference" height="70" /></a></td>
      <td>Represents a persistent external data store — such as a database or file system — that activities can read from or write to. Supported as a visual/documentation element only — not enforced by the engine at runtime.</td>
    </tr>
    <tr>
      <td><a href="./data-object-reference">Data Object Reference</a></td>
      <td><a href="./data-object-reference"><img src={require('!url-loader!../../../assets/bpmn/others/data-object-reference.svg').default} alt="Data Object Reference" height="70" /></a></td>
      <td>Represents data produced or consumed within a process — such as a document or a record — scoped to the lifetime of the process instance. Supported as a visual/documentation element only — not enforced by the engine at runtime.</td>
    </tr>
    <tr>
      <td><a href="./data-store-annotation">Text Annotation</a></td>
      <td><a href="./data-store-annotation"><img className="bpmn-supported" src={require('!url-loader!../../../assets/bpmn/others/data-store-annotation.svg').default} alt="Text Annotation" height="70" /></a></td>
      <td>Attaches a descriptive note to any diagram element. Has no effect on process execution and exists purely for documentation purposes.</td>
    </tr>
    <tr>
      <td><a href="./expanded-pool">Expanded Pool</a></td>
      <td><a href="./expanded-pool"><img src={require('!url-loader!../../../assets/bpmn/others/expanded-pool.svg').default} alt="Expanded Pool" height="70" /></a></td>
      <td>A participant whose internal process flow is fully visible, used in Collaboration diagrams to show how it interacts with others through Message Flows. Supported as a visual/documentation element — Message Flows between pools are modelled explicitly via Message Events or Send/Receive Tasks.</td>
    </tr>
    <tr>
      <td><a href="./empty-pool">Empty Pool</a></td>
      <td><a href="./empty-pool"><img src={require('!url-loader!../../../assets/bpmn/others/empty-pool.svg').default} alt="Empty Pool" height="70" /></a></td>
      <td>A participant whose internal process is hidden or unknown, representing an external system or third party without exposing its implementation. Supported as a visual/documentation element — integration is handled through Job Workers and Message correlation, not automatic messaging to the pool.</td>
    </tr>
  </tbody>
</table>

:::note[Visual/documentation only]
Not enforced by the engine at runtime: Data Store Reference, Data Object Reference, Expanded Pool, Empty Pool
:::
