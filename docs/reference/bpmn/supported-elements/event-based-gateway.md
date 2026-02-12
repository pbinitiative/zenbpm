---
sidebar_position: 90
---
# Event-Based Gateway

An Event-Based Gateway is a BPMN element that decides the path of a process based on which event happens first.

Unlike data-based (exclusive) gateways, the decision is **not** made by evaluating conditions, but by **waiting for events**.

## Key characteristics
- **Event-driven decision:**
  The gateway waits for one of several alternative events to occur. The first event that occurs determines the outgoing path.

- **Exactly one path is taken:**
  Once an event is triggered, all other alternative events are canceled and their paths are disabled.

- **No conditions on sequence flows:**
  Outgoing sequence flows must not have conditions. The logic is defined by the events themselves.

- **Must be followed only by event-catching elements:**
  Each outgoing path must lead directly to an **Intermediate Catching Event**, or a **Receive Task**

- **Used for reactive behavior:**
  Commonly applied in scenarios where a process must react to external signals, messages, timers, or user responses.

- **Consumes no token by itself:**
  The gateway passes control to the event that occurs; the token continues only along the triggered path.

## Allowed event types after the gateway
Outgoing sequence flows from an Event-Based Gateway may lead to the following catching events or tasks:

- **Message Intermediate Catch Event:**
  Waits for a specific message from another process or external system.

- **Timer Intermediate Catch Event:**
  Waits until a specified time condition is met (date, duration, or cycle).

- **Signal Intermediate Catch Event:**
  Reacts to a broadcast signal.

- **Conditional Intermediate Catch Event:**
  Proceeds when a defined condition becomes true.

- **Receive Task:**
  Represents waiting for a message, modeled as an activity rather than an event.

> **Note:** Throwing events are not allowed after an Event-Based Gateway.

## Graphical notation

A diamond shape with a **pentagon event marker** inside.

## XML Definition
```xml
<bpmn:eventBasedGateway id="EventBasedGateway_1" name="Wait for Event">
  <bpmn:outgoing>Flow_Message</bpmn:outgoing>
  <bpmn:outgoing>Flow_Timer</bpmn:outgoing>
</bpmn:eventBasedGateway>
```

## Current Implementation
Event based gateway currently supports message and timer events.

