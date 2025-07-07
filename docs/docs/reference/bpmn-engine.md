---
sidebar_position: 2
---
# BPMN engine

BPMN engine tries to adhere to the [bpmn 2 specification](https://www.omg.org/spec/BPMN/2.0.2/PDF) and be compatible with (Camunda 8)[https://docs.camunda.io/].

## Interacting with the BPMN engine

## Workers

## Supported elements
TODO: we should split these into separate pages with examples.

### Start event
Start event is fully supported.
### End event
End event is fully supported.
### Service task
Fully supported through external workers.
### User task
Fully supported through external workers.
### Business rule task
:::note[Future plans]
Supported through internal [dmn engine](/reference/dmn-engine).
:::
### Call activity
The subprocess for call activity is started on the same partition as the process that invoked it.
### Parallel gateway
Current implementation handles parallel flows correctly if there is only one overlapping flow in the process instance. **Multiple recursive parallel flows have currently undefined behaviour.**
### Inclusive gateway
Inclusive gateway is fully supported.
### Exclusive gateway
Exclusive gateway is fully supported.
### Event based gateway
Event based gateway currently supports message and timer events.
### Message catch event
Message catch event is fully supported.
### Link intermediate throw event
Link intermediate throw event is fully supported.
### Link intermediate catch event
Link intermediate catch event is fully supported.
### Timer intermediate catch event
Tiemr intermediate catch event is fully supported.
:::note[Future plans]
### Message throw event
### Error event
### Boundary event
### Activity multi instance
### Script task
:::


## XML parser
TODO: add information about how we are parsing xml definitions of processes
