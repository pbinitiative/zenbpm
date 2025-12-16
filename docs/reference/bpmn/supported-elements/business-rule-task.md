---
sidebar_position: 40
---

# Business Rule Task

A Business Rule Task is a BPMN flow element that provides a mechanism for invoking business rules. It allows processes to delegate complex decision logic to rule engines, enabling dynamic and maintainable business logic execution.

## Key characteristics
- Rule engine integration:
	Business Rule Tasks invoke a business rule decision, which may be implemented using external or embedded rule engines or decision services to evaluate business rules against provided data.

- Input and output parameters:
	Tasks can receive input data from the process and return decision results back to the process instance.

- Decision automation:
	Enables automated decision-making based on predefined business rules, reducing manual intervention and ensuring consistency.

- Rule versioning and management:
	Supports integration with rule management systems for versioning, testing, and deployment of business rules.

- Error handling:
	Errors during rule evaluation can be handled using BPMN error or escalation boundary events when the evaluation fails or returns unexpected results.

## Usage patterns
- **Decision automation**
	Use when complex business logic needs to be evaluated automatically, such as credit scoring, eligibility checks, or policy enforcement.

- **Rule-based routing**
	Combine with gateways to route process flows based on rule evaluation results.

- **Dynamic calculations**
	Apply business rules for pricing, risk assessment, or other computational decisions.

## Graphical notation
![Business rule task usage example](./../../assets/bpmn/business_rule_task.svg)

A rounded rectangle with the business rule marker (grid icon) in the top-left corner.

## XML Definition
```xml
<bpmn:businessRuleTask id="BusinessRuleTask_1" name="Evaluate Credit Score">
  <bpmn:incoming>Flow1</bpmn:incoming>
  <bpmn:outgoing>Flow2</bpmn:outgoing>
</bpmn:businessRuleTask>
```

## Current Implementation
Future plans: Supported through internal [dmn engine](/reference/dmn-engine).
