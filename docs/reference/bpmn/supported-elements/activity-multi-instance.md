---
sidebar_position: 170
---

# Activity Multi-Instance

An Activity Multi-Instance is a BPMN activity configuration that allows an activity to be executed multiple times for a collection of items. It is used to model repeated execution of the same activity, either sequentially or in parallel, based on defined loop characteristics.

Multi-instance behavior can be applied to tasks and subprocesses.

## Key characteristics

- **Multiple executions of the same activity:**  
  The activity is executed once for each element in a defined collection.

- **Sequential or parallel execution:**  
  A Multi-Instance activity can be configured to execute:
  - **Sequentially:** instances are executed one after another.
  - **In parallel:** all instances are executed at the same time.

- **Collection-based:**  
  The number of instances is determined by a collection or an expression evaluated at runtime.

- **Shared activity definition:**  
  All instances share the same activity definition but have their own execution context.

- **Completion condition (optional):**  
  The activity may define a completion condition that allows the multi-instance execution to finish before all instances complete.

- **Applicable to multiple activity types:**  
  Multi-instance behavior can be applied to Tasks and Subprocesses.

## Execution behavior

- For **parallel multi-instance**, multiple tokens are created, one for each instance.
- For **sequential multi-instance**, a single token is reused to execute instances one after another.
- The activity completes when:
  - all instances finish, or
  - the defined completion condition evaluates to true.

## Supported activity types

Multi-instance behavior can be applied to the following activity types:

- Task
- UserTask
- ServiceTask
- SendTask
- ReceiveTask
- BusinessRuleTask
- ScriptTask
- ManualTask
- SubProcess
- CallActivity

## Graphical notation

A standard activity shape with a **multi-instance marker** at the bottom center:

- **Three vertical lines:** parallel multi-instance

![Parallel Multi-instance usage example](./../../assets/bpmn/parallel.svg)

- **Three horizontal lines:** sequential multi-instance

![Horizontal Multi-instance usage example](./../../assets/bpmn/sequential.svg)


## XML Definition

### Parallel multi-instance example

```xml
<bpmn:userTask id="ReviewTask" name="Review Document">
  <bpmn:multiInstanceLoopCharacteristics isSequential="false">
    <bpmn:loopCardinality>5</bpmn:loopCardinality>
  </bpmn:multiInstanceLoopCharacteristics>
</bpmn:userTask>
