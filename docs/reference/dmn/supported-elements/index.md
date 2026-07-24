---
sidebar_position: 0
---

# Supported elements

DMN elements supported by the ZenBPM engine.

A DMN file deployed to ZenBPM describes a **Decision Requirements Graph** — one or more decisions, the data they need, and the requirements between them. Each **Decision** node carries exactly one piece of decision logic. DMN defines several logic variants; ZenBPM documents the two that are covered by tests and used in practice: the **Decision table** and the **Literal expression**.

## Types

Green icons are supported and link to their documentation.

<table className="bpmn-types-table">
  <thead>
    <tr>
      <th>Element</th>
      <th style={{width: '240px'}}>Icon</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><a href="./decision-requirements-graph">Decision requirements graph</a></td>
      <td><a href="./decision-requirements-graph"><img className="bpmn-supported" src={require('!url-loader!../../assets/dmn/decision-requirements-graph.svg').default} alt="Decision requirements graph" height="60" /></a></td>
      <td>The decisions in a DMN file and the information requirements that wire them together, including the input data each decision needs.</td>
    </tr>
    <tr>
      <td><a href="./decision-table">Decision table</a></td>
      <td><a href="./decision-table"><img className="bpmn-supported" src={require('!url-loader!../../assets/dmn/decision-table.svg').default} alt="Decision table" height="70" /></a></td>
      <td>Maps combinations of input conditions to output results in a structured table — the most common way to express decision logic in DMN.</td>
    </tr>
    <tr>
      <td><a href="./literal-expression">Literal expression</a></td>
      <td><a href="./literal-expression"><img className="bpmn-supported" src={require('!url-loader!../../assets/dmn/literal-expression.svg').default} alt="Literal expression" height="70" /></a></td>
      <td>A decision expressed as a single FEEL expression that evaluates directly to a value.</td>
    </tr>
  </tbody>
</table>

:::note[Not supported]
**Business knowledge model** and **Knowledge source** are ignored by the XML parser. A file containing them deploys successfully, but they have no effect on evaluation and cannot be invoked.

**Decision service** is not supported.

A **decision with no logic** — an empty decision node used as a modelling placeholder — deploys successfully but fails when it is evaluated. Give every decision you intend to evaluate a decision table or a literal expression.
:::
