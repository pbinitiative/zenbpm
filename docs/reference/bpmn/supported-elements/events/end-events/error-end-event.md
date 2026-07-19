---
sidebar_position: 3
---
# Error End Event

An Error End Event throws a BPMN error that an Error Boundary Event on an enclosing scope can catch.

## Key characteristics

- Has no outgoing sequence flows.
- Throws a BPMN error identified by an `errorCode`.
- The error propagates up the scope hierarchy; if no boundary event catches it, it becomes an incident.

## Graphical notation

A thick single-line circle with a lightning-bolt icon.

<img src="/img/bpmn/UNI_endEventError.svg" width="130" />

## XML Definition

```xml
<bpmn:endEvent id="throwError" name="Payment failed">
  <bpmn:incoming>Flow_1</bpmn:incoming>
  <bpmn:errorEventDefinition errorRef="Error_1" />
</bpmn:endEvent>

<bpmn:error id="Error_1" name="PaymentFailed" errorCode="PAYMENT_FAILED" />
```

## Current Implementation

Supported.
