---
sidebar_position: 4
---
# Terminate End Event

A Terminate End Event immediately ends the entire process instance, cancelling all remaining tokens, jobs, and subscriptions — regardless of other active paths.

## Key characteristics

- Has no outgoing sequence flows.
- Cancels the whole instance instantly, not just the current path.
- Differs from None End Event: even if other parallel branches are still active, they are all cancelled.

## Graphical notation

A thick single-line circle with a filled circle inside.

<img src="/img/bpmn/UNI_endEventTerminate.svg" width="130" />

## XML Definition

```xml
<bpmn:endEvent id="terminate" name="Terminate">
  <bpmn:incoming>Flow_1</bpmn:incoming>
  <bpmn:terminateEventDefinition />
</bpmn:endEvent>
```

## Current Implementation

Supported.
