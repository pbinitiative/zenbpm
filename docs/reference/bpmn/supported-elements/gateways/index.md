---
sidebar_position: 1
---

# Gateways

A Gateway controls how the process flow diverges and converges. It is used to model decisions, forks, and joins in a process, routing the token along one or more outgoing sequence flows based on conditions or events.

## Key characteristics

- Graphically represented as a diamond shape, with a marker icon indicating the gateway type.
- Can be **diverging** — splitting one incoming flow into multiple outgoing paths — or **converging** — merging multiple incoming flows back into one.
- The specific splitting and merging behavior (conditions, parallel execution, or competing events) depends on the gateway type.

## Types

Green icons are supported and link to their documentation.

<table className="bpmn-types-table">
  <thead>
    <tr>
      <th>Gateway</th>
      <th style={{width: '90px'}}>Icon</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><a href="./exclusive-gateway">Exclusive</a></td>
      <td><a href="./exclusive-gateway"><img className="bpmn-supported" src="/img/bpmn/gateways/exclusive-gateway.svg" alt="Exclusive Gateway" height="70" /></a></td>
      <td>Routes the flow down exactly one outgoing path, chosen by evaluating conditions on each outgoing sequence flow.</td>
    </tr>
    <tr>
      <td><a href="./inclusive-gateway">Inclusive</a></td>
      <td><a href="./inclusive-gateway"><img className="bpmn-supported" src="/img/bpmn/gateways/inclusive-gateway.svg" alt="Inclusive Gateway" height="70" /></a></td>
      <td>Activates one or more outgoing paths whose conditions are true, allowing multiple branches to run simultaneously.</td>
    </tr>
    <tr>
      <td><a href="./parallel-gateway">Parallel</a></td>
      <td><a href="./parallel-gateway"><img className="bpmn-supported" src="/img/bpmn/gateways/parallel-gateway.svg" alt="Parallel Gateway" height="70" /></a></td>
      <td>Splits the flow into all outgoing paths simultaneously and, when used as a merge, waits for all incoming branches to complete before continuing. <strong>Limitation:</strong> works for a single overlapping parallel flow; nested/recursive parallel flows have undefined behaviour.</td>
    </tr>
    <tr>
      <td><a href="./event-based-gateway">Event-Based</a></td>
      <td><a href="./event-based-gateway"><img className="bpmn-supported" src="/img/bpmn/gateways/event-based-gateway.svg" alt="Event-Based Gateway" height="70" /></a></td>
      <td>Routes the flow based on which event occurs first, instead of evaluating data conditions. <strong>Limitation:</strong> currently only for Message and Timer events.</td>
    </tr>
  </tbody>
</table>

:::note[Not yet supported]
Complex gateway
:::
