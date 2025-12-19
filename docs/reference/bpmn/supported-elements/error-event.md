---
sidebar_position: 160
---

# Error End Event

An **Error End Event** is a BPMN flow element that terminates a process instance or a token by throwing an error. The error can be caught by an Error Boundary Event on a parent process or subprocess. 

## Key characteristics
- **Terminates the process or token:**  
  When reached, the Error End Event finishes the active token and signals an error for handling by an enclosing scope.

- **No outgoing sequence flows:**  
  By definition, it cannot have outgoing connections because it stops the flow.

- **Associated with an error:**  
  Must reference a defined BPMN **Error** (`errorRef`) that identifies the type of error thrown.

## Graphical notation
A bold single-line circle with a small **lightning bolt or “X”** inside, indicating an error.

## XML Definition
<bpmn:error id="Error_1" name="Payment Failed" errorCode="ERR001" />

<bpmn:endEvent id="EndEvent_Error_1" name="Error End Event">
  <bpmn:errorEventDefinition errorRef="Error_1" />
</bpmn:endEvent>

