---
sidebar_position: 50
---

# Call activity

A Call Activity is a BPMN flow element that invokes a global process or a global task. It allows processes to reuse externally defined process logic, enabling modular and reusable process design.

## Starting a Call Activity

**Binding:** Specifies how the process definition is resolved.
Currently, only `latest` tag is supported. This means the most recent version of the referenced process definition will be executed.

**ProcessId:** Identifies the process definition to be executed. Currently, only a direct ID reference is supported.

## Engine Behavior
A Call Activity behaves similarly to an independent process, but it is logically connected to the parent process instance.

When a Call Activity is triggered:
- A new process instance is created.
- The new instance is linked to its parent process instance.
- The child process runs in its own isolated scope.

The called process for Call Activity is started on the same [partition](/reference/cluster) as the parent process that invoked it.

#### Variable Handling
By default, no variables are inherited from the parent process instance.
The called process operates within its own variable scope.
Upon completion, result variables are not automatically propagated back to the parent process instance.
Explicit input and output mappings must be defined if variable transfer is required.

## Input/Output
Input and Output parameters define how variables are transferred between the parent instance and Call Activity instance.

#### Input parameters
Used to initialize variables in the called process when the Call Activity starts.
#### Output parameters
Used to map variables from the called process back to the parent process when the Call Activity completes.

These mappings control the variable scope at the start and end of the Call Activity instance.

## Boundary Events
Boundary Events can be attached to a Call Activity element to handle exceptional situations or alternative flows during its execution.
Interrupting Boundary Events cause Call Activity to terminate and the parent process continues execution along the Boundary Event’s outgoing flow. 

## Key characteristics
- Reusable subprocess invocation:
	Call Activities reference global processes or subprocesses that can be called from multiple places, promoting reusability.

- Input and output parameters:
	Supports passing data into and out of the called process through input/output parameter mappings.

- Process modularity:
	Enables breaking down complex processes into smaller, manageable subprocesses that can be called as needed.

- Multiple instance support:
	Can be configured for multiple instances, allowing parallel execution of the same subprocess.

- Error handling and compensation:
	Supports boundary events for handling errors or escalations from the called process and allows compensation, if compensation behavior is explicitly defined in the called process.

## Usage patterns
- **Process decomposition:**
	Use to break down large processes into smaller, reusable subprocesses.

- **Reusable logic:**
	Call common business logic or workflows from multiple parent processes.

- **Dynamic subprocess selection:**
	Conditionally call different subprocesses based on process data.

- **Parallel processing:**
	Configure multiple instances to process collections or perform parallel operations.

## Graphical notation
![Call activity usage example](./../../assets/bpmn/call_activity.svg)

A rectangle with a thick border and a subprocess marker (small rectangle with a plus sign) in the bottom-center.

## XML Definition
```xml
<bpmn:callActivity id="CallActivity_1" name="Process Order" calledElement="ProcessOrderSubprocess">
  <bpmn:incoming>Flow1</bpmn:incoming>
  <bpmn:outgoing>Flow2</bpmn:outgoing>
</bpmn:callActivity>
```

