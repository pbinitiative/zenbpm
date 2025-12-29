---
sidebar_position: 150
---
# Message Throw Event

A **Message Throw Event** is a BPMN event that sends a message to another process, participant, or external system during the execution of a process. It represents an explicit point in the process where communication is initiated.

Message Throw Events are used to model **process-to-process communication**, **system integration**, and **event-driven interactions** where the current process actively sends information outward.

## Key characteristics

- **Actively sends a message:**  
  When the event is reached, a message is sent to a defined recipient. The process does not wait for a response unless explicitly modeled elsewhere.

- **Does not terminate the process (except End Event variant):**  
  In its Intermediate form, the process continues after the message is sent.  
  In its End Event form, the message is sent and the process (or token) is terminated.

- **Requires a defined message:** 
Must reference a BPMN **Message** (`messageRef`) that defines the payload and semantic meaning of the communication.

- **Asynchronous by nature:** 
Sending a message does not imply synchronization or guaranteed immediate reception. Delivery semantics depend on the execution engine and integration setup.

- **A logical receiver expected:** 
The message should be intended for a known receiver, such as another process, a participant in another pool, or an external system.

## Variants of Message Throw Events

- **Intermediate Message Throw Event:**  
  Sends a message during the process flow and then continues execution.

- **Message End Event:**  
  Sends a message and immediately ends the process or token.

## Graphical notation

- **Intermediate Message Throw Event:**  
  A double-line circle with a **filled (black) envelope icon** inside.

- **Message End Event:**  
  A bold single-line circle with a **filled envelope icon** inside.

![Message throw event usage example](./../../assets/bpmn/message_throw_event.svg)

## XML Definition

### Message definition
```xml
<bpmn:message id="Message_OrderSent" name="Order Sent" />

<bpmn:intermediateThrowEvent id="ThrowMessage_1" name="Send Order">
  <bpmn:messageEventDefinition messageRef="Message_OrderSent" />
</bpmn:intermediateThrowEvent>

<bpmn:endEvent id="EndEvent_Message_1" name="Send Completion Message">
  <bpmn:messageEventDefinition messageRef="Message_OrderSent" />
</bpmn:endEvent>

