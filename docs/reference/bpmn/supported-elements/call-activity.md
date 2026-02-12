---
sidebar_position: 50
---

# Call activity

A Call Activity is a BPMN flow element that invokes a global process or a global task. It allows processes to reuse externally defined process logic, enabling modular and reusable process design.

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

## Current Implementation
The subprocess for call activity is started on the same partition as the process that invoked it.
