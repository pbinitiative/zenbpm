---
sidebar_position: 50
---

# Sub Process

A Sub Process is a BPMN flow element that defines a new process scope running within a parent process. It can be used to break down a complex process into smaller, more manageable parts.

## Starting Sub Process
Currently, there is no limitation to Start Event elements. There can be any supported types and amount.

## Execution behavior
A Sub Process behaves similarly to an independent process, but it is logically connected to the parent process instance.

When a Sub Process is triggered:
- A new process instance is created.
- The new instance is linked to its parent process instance and uses sub process part of parent's process definition.
- The child process runs in its own isolated scope.

The child process for Sub Process  is started on the same [partition](/reference/cluster) as the parent process.

#### Variable handling
By default, no variables are inherited from the parent process instance.
The child process operates within its own variable scope.
Upon completion, result variables are not automatically propagated back to the parent process instance.
Explicit input and output mappings must be defined if variable transfer is required.

## Input/Output
Input and Output parameters define how variables are transferred between the parent instance and Sub Process instance.

#### Input parameters
Used to initialize variables in the child process when the Sub Process starts.
#### Output parameters
Used to map variables from the child process back to the parent process when the Sub Process completes.

These mappings control the variable scope at the start and end of the Sub Process instance.

## XML Definition
```xml
<bpmn:subProcess id="SubProcess" name="SubProcess">
  <bpmn:incoming>Flow1</bpmn:incoming>
  <bpmn:outgoing>Flow2</bpmn:outgoing>
</bpmn:subProcess>
```

