---
sidebar_position: 141
---

# Error boundary event

Error boundary event allows a process to react when something goes wrong during task execution. It is attached to an activity (such as a service task or call activity) and activates when a matching BPMN error is raised. When triggered, the activity is interrupted and execution continues along the boundary event's outgoing sequence flow.

## When to use

Use an error boundary event when a job worker detects a problem and the process should handle it explicitly — for example, switch to an alternative flow, notify a user, or skip a failed step.

In ZenBPM, errors are raised by failing a job with an `errorCode` via the job API. The engine then looks for a matching error boundary event attached to the activity (or its ancestor scopes) and routes execution accordingly.

:::note
ZenBPM currently does not support error end events as a way to throw errors. Errors can only be raised through the job failure API.
:::

## Behavior

- **Always interrupting** — when the error is caught, the attached activity stops and does not continue on its normal outgoing flow.
- **Active only while the activity runs** — the boundary event can only catch errors while the attached activity is in an active state.
- **Specific match takes priority** — if multiple error boundary events exist on the same activity, a boundary event with a matching `errorCode` is selected first. A catch-all boundary event (one without `errorRef`) is used only as a fallback.
- **Unhandled error creates an incident** — if no matching boundary event is found anywhere in the scope hierarchy, the job is marked as failed and an incident is created.

## Error propagation

When a job is failed with an `errorCode`, ZenBPM walks up the scope hierarchy looking for a boundary event that can handle it:

1. First, the engine checks boundary events attached to the activity where the error originated.
2. If no match is found and the activity is inside a subprocess, call activity, or multi-instance body, the engine moves to the parent scope.
3. This continues up the hierarchy until a match is found or the top-level process is reached.

At each level, exact `errorCode` matches are checked first. If none match, a catch-all boundary event at that level is used. If a parent scope catches the error, all child instances under the failing token are terminated before execution continues from the catching boundary event.

**Example:** A service task inside a called process fails with `errorCode="VALIDATION_ERROR"`. The called process has no matching boundary event, but the parent call activity does. The called process instance is terminated and the parent process continues along the boundary event's outgoing flow.

## Error code matching

ZenBPM matches errors by `errorCode`, not by the boundary event ID or the error name.

The boundary event references a BPMN error definition via `errorRef`. That definition contains an `errorCode`. When a job fails with an `errorCode`, the engine compares the two values using exact string equality.

```xml
<bpmn:error id="Error_validation" name="Validation error" errorCode="VALIDATION_ERROR" />
```

A job failed with `errorCode="VALIDATION_ERROR"` matches this error. A job failed with `errorCode="TIMEOUT"` does not.

If a job is failed without an `errorCode`, the engine does not attempt to match any error boundary event and creates an incident directly.

#### XML reference
```xml
<bpmn:boundaryEvent id="boundary-error" attachedToRef="my-service-task">
  <bpmn:outgoing>Flow_error_handled</bpmn:outgoing>
  <bpmn:errorEventDefinition id="ErrorEventDef_1" errorRef="Error_validation" />
</bpmn:boundaryEvent>

<bpmn:error id="Error_validation" name="Validation error" errorCode="VALIDATION_ERROR" />
```

## Catch-all boundary event

A catch-all error boundary event has an `errorEventDefinition` without `errorRef`. It catches any error regardless of its `errorCode`.

- No `<bpmn:error />` definition is needed in the process.
- If the same activity has both a specific error boundary event and a catch-all, the specific match takes priority.
- Even for catch-all events, the job must be failed with an `errorCode` — failing without one always results in an incident.

#### XML reference
```xml
<bpmn:boundaryEvent id="boundary-error-catchall" attachedToRef="my-service-task">
  <bpmn:outgoing>Flow_any_error</bpmn:outgoing>
  <bpmn:errorEventDefinition id="ErrorEventDef_catchall" />
</bpmn:boundaryEvent>
```

## Variable propagation

When a job is failed with an `errorCode`, the fail request can also include variables. These variables are available to the catching boundary event and can be mapped using output mappings.

- **With output mappings** — only explicitly mapped variables are propagated to the parent scope. Each mapping evaluates its source expression against the error variables and writes the result to the target variable.
- **Without output mappings** — all variables from the error are propagated to the parent scope as-is.
