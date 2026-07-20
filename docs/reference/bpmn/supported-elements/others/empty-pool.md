---
sidebar_position: 5
---
# Empty Pool

An Empty Pool (also called a Black Box Pool) is a BPMN participant whose internal process is hidden or unknown. It represents an external system or third party in a Collaboration diagram without exposing its implementation.

## Key characteristics

- Contains no visible internal flow.
- Used to represent external participants (e.g. a third-party payment provider) whose internals are not modelled.
- Communicates with other pools via **Message Flows**.

## Graphical notation

A plain labelled rectangle with no internal elements visible.

<img src="/img/bpmn/others/empty-pool.svg" alt="Empty Pool" width="400" height="180" />

## XML Definition

```xml
<bpmn:collaboration id="Collaboration_1">
  <bpmn:participant id="Participant_PaymentGateway"
                    name="Payment Gateway" />
  <bpmn:messageFlow id="MessageFlow_Request"
                    sourceRef="Task_SendPaymentRequest"
                    targetRef="Participant_PaymentGateway" />
  <bpmn:messageFlow id="MessageFlow_Response"
                    sourceRef="Participant_PaymentGateway"
                    targetRef="Event_PaymentConfirmed" />
</bpmn:collaboration>
```

## Current Implementation

Supported as a **visual/documentation element**. The engine does not send messages to or receive messages from external pools automatically — integration with external systems is handled through Job Workers and Message correlation.
