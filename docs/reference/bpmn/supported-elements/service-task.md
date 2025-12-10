---
sidebar_position: 20
---

# Service task

A Service Task is a BPMN flow element that represents work performed automatically by a system, without human involvement. It automates process steps by invoking external services, APIs, or backend systems.

## Key characteristics
- Automated execution:
	A Service Task executes automatically without requiring human interaction. It invokes an external service and waits for the result.

- Can have incoming and outgoing sequence flows:
	Service Tasks connect to other flow elements via sequence flows, allowing complex workflows with conditional routing based on service outcomes.

- Input and output parameters:
	Service Tasks can receive data from the process and return data back to continue execution with service-provided information.

- Implementation types:
	Service Tasks can invoke different types of implementations (HTTP calls, web services, scripts, etc.) based on configuration.

- Error handling:
	Service Tasks support error handling through boundary events and fault responses when service calls fail.

- Types of service task handling:
	- **HTTP/REST invocation:**
		Calls external REST APIs or HTTP endpoints. Sends request data and processes the response to continue execution.

	- **SOAP web service:**
		Invokes SOAP-based web services with XML payloads. Suitable for enterprise system integration.

	- **Message-based:**
		Sends a message to an external system and waits for a correlated response or acknowledgment.

	- **Script execution:**
		Executes server-side scripts or expressions to perform calculations, data transformations, or business logic.

	- **Database operations:**
		Executes database queries, updates, or stored procedures to interact with data systems.

	- **External worker pattern:**
		Delegates work to external worker applications that subscribe to task types and handle execution asynchronously.

	- **System integration:**
		Integrates with third-party systems, microservices, or legacy systems through adapters or connectors.

## Graphical notation
![Service task usage example](./../../assets/bpmn/service_task.svg)

A rectangle with a gear icon in the top-left corner.

## XML Definition
```xml
<bpmn:serviceTask id="ServiceTask_1" name="Process Payment">
  <bpmn:incoming>Flow1</bpmn:incoming>
  <bpmn:outgoing>Flow2</bpmn:outgoing>
</bpmn:serviceTask>
```

## Current Implementation
Service task is fully supported.