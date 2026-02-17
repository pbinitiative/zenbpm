---
sidebar_position: 180
---

# Script task

A Script Task is a BPMN flow element that executes a script within the process flow. It allows for custom logic, calculations, data transformations, and integrations to be performed using scripting languages.

## Key characteristics
- Script execution:
	Script Tasks execute scripts written in supported scripting languages (JavaScript, Groovy, etc.) within the process engine.

- Input and output parameters:
	Can access process variables as input and modify or create new variables as output.

- Synchronous execution:
	Script Tasks are typically executed synchronously by process engines.

- Error handling:
    Errors must be mapped to BPMN Error Events.

- Expression evaluation:
	Can be used for simple expressions or complex multi-line scripts.

## Usage patterns
- **Data transformation:**
	Use to manipulate process data, perform calculations, or format information.

- **Business logic execution:**
	Implement custom business rules or decision logic that doesn't fit standard BPMN elements.

- **Variable manipulation:**
	Set, modify, or compute process variables based on complex logic.

- **Validation and formatting:**
	Validate data or format output for downstream processing.

## Graphical notation


A rectangle with a script icon (scroll/roll) in the top-left corner.

## XML Definition
```xml
<bpmn:scriptTask id="ScriptTask_1" name="Calculate Total" scriptFormat="javascript">
  <bpmn:incoming>Flow1</bpmn:incoming>
  <bpmn:outgoing>Flow2</bpmn:outgoing>
  <bpmn:script>
    <![CDATA[
      var total = execution.getVariable('price') * execution.getVariable('quantity');
      execution.setVariable('total', total);
    ]]>
  </bpmn:script>
</bpmn:scriptTask>
```

## Current Implementation
