---
sidebar_position: 4
---
# Expanded Pool

An Expanded Pool is a BPMN participant whose internal process flow is fully visible. It is used in **Collaboration diagrams** to show how one participant's process interacts with others through Message Flows.

## Key characteristics

- Contains a fully visible internal process with activities, events, and gateways.
- Can be subdivided into **Lanes** representing different roles or departments.
- Communicates with other pools via **Message Flows** (not Sequence Flows).

## Graphical notation

A large labelled rectangle containing the internal process flow. Lanes appear as horizontal or vertical subdivisions.

<img src="/img/bpmn/others/expanded-pool.svg" alt="Expanded Pool" width="400" height="180" />

## XML Definition

```xml
<bpmn:collaboration id="Collaboration_1">
  <bpmn:participant id="Participant_OrderService"
                    name="Order Service"
                    processRef="Process_OrderService" />
  <bpmn:participant id="Participant_PaymentProvider"
                    name="Payment Provider" />
  <bpmn:messageFlow id="MessageFlow_1"
                    sourceRef="Task_RequestPayment"
                    targetRef="Participant_PaymentProvider" />
</bpmn:collaboration>
```

## Current Implementation

Supported as a **visual/documentation element**. The ZenBPM engine executes the process contained within the pool. Message Flows between pools are modelled explicitly using Message Events or Send/Receive Tasks in the process flow.
