---
sidebar_position: 0
---

# Supported elements

DMN elements supported by the ZenBPM engine.

## Types

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
      <td><a href="./decision-table">Decision table</a></td>
      <td><a href="./decision-table"><img className="bpmn-supported" src="/img/dmn/decision-table.svg" alt="Decision table" height="70" /></a></td>
      <td>Maps combinations of input conditions to output results in a structured table format — the most common way to express decision logic in DMN.</td>
    </tr>
    <tr>
      <td><a href="./literal-expression">Literal expression</a></td>
      <td><a href="./literal-expression"><img className="bpmn-supported" src="/img/dmn/literal-expression.svg" alt="Literal expression" height="70" /></a></td>
      <td>A decision expressed as a single FEEL expression that evaluates directly to a value.</td>
    </tr>
    <tr>
      <td><a href="./empty-decision">Empty decision</a></td>
      <td><a href="./empty-decision"><img className="bpmn-supported" src="/img/dmn/empty-decision.svg" alt="Empty decision" height="70" /></a></td>
      <td>A decision node with no logic defined, used as a placeholder for a decision that has not yet been implemented.</td>
    </tr>
    <tr>
      <td><a href="./input-data">Input data</a></td>
      <td><a href="./input-data"><img className="bpmn-supported" src="/img/dmn/input-data.svg" alt="Input data" height="60" /></a></td>
      <td>An external data source or variable provided as input to one or more decisions.</td>
    </tr>
    <tr>
      <td><a href="./knowledge-source">Knowledge source</a></td>
      <td><a href="./knowledge-source"><img className="bpmn-supported" src="/img/dmn/knowledge-source.svg" alt="Knowledge source" height="70" /></a></td>
      <td>A reference to the authority, regulation, or document that justifies a decision. Documentation only, with no effect on execution.</td>
    </tr>
    <tr>
      <td><a href="./business-knowledge-model">Business knowledge model</a></td>
      <td><a href="./business-knowledge-model"><img className="bpmn-supported" src="/img/dmn/business-knowledge-model.svg" alt="Business knowledge model" height="60" /></a></td>
      <td>A reusable encapsulation of decision logic — typically a function — that can be invoked by decisions or other business knowledge models.</td>
    </tr>
  </tbody>
</table>
