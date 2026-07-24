---
sidebar_position: 2
sidebar_label: Variable mapping
---

# Variables

ZenBPM stores process data as variables. BPMN elements can read variables from their current scope, create local variables while they execute, and propagate selected values back to the parent or catching scope.

## Output mappings

Output mappings control which variables are written from an element's local scope back to the parent process scope. ZenBPM uses different default propagation rules for activities and catching events.

| Element type                                                                                           | With output mappings                             | Without output mappings                                                                                                                 |
| ------------------------------------------------------------------------------------------------------ | ------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------- |
| Activities, such as Service Tasks, User Tasks, Business Rule Tasks, Sub Processes, and Call Activities | Only explicitly mapped variables are propagated. | No output variables are propagated. Output variables returned by a job or produced by the activity are not written to the parent scope. |
| Catching events that receive a payload, such as Message Catch Events and Error Boundary Events         | Only explicitly mapped variables are propagated. | All payload variables are propagated to the catching scope as-is.                                                                       |

An output mapping evaluates its source expression against the element's local variable context plus the payload variables. The evaluated result is written to the mapping target in the parent or catching scope.

For activities, define an output mapping whenever data should leave the activity:

```xml
<zenbpm:ioMapping>
  <zenbpm:output source="=result" target="processResult" />
</zenbpm:ioMapping>
```

For catching events, omit output mappings when the complete message or error payload should be copied into the catching scope. Add output mappings when only selected payload variables should be exposed.

## Where output variables are stored

When a job completes the full output variables returned by the job worker are stored on the `Job` record in the `outputVariables` field. These are available via the `/jobs` and `/process-instances/{key}/jobs` endpoints.

When an output mapping is defined on the activity, only the explicitly mapped variables are propagated to the parent process scope. The mapped output variables are stored as historical data on the `FlowElementInstance` record, visible via the `/process-instances/{key}/history` endpoint.

When no output mapping variable is defined, _no variables_ are propagated to the parent process scope and `FlowElementInstance` have no output variables.
