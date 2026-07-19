---
sidebar_position: 170
---

# Activity Multi-Instance

An Activity Multi-Instance is a BPMN activity configuration that allows an activity to be executed multiple times for a
collection of items. It is used to model repeated execution of the same activity, either sequentially or in parallel,
based on defined loop characteristics.

Multi-instance behavior can be applied to activities.

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
  The activity may define a completion condition that allows the multi-instance execution to finish before all instances
  complete.

## Starting a Multi Instance

A Multi-Instance configuration acts as a wrapper around the activity and controls how it is repeated and completed. Configuration has the following parameters:

| Parameter Name       | Description                                                                                                                                                                                                                   |
|----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Input Collection     | Specifies the collection that the Multi-Instance activity iterates over. A new instance of the wrapped activity is created for each element in this collection. Each iteration processes a single element from the collection |
| Output Collection    | Specifies the collection that aggregates results from all iterations. The collection becomes available after the entire Multi-Instance execution completes                                                                    |
| Output Element       | Defines the result produced by a single iteration of the Multi-Instance activity. This value is derived from variables within the iteration scope                                                                             |
| Completion Condition | Not supported yet.                                                                                                                                                                                                            |

## Execution behavior

A Multi Instance behaves similarly to an independent process, but it is logically connected to the parent process
instance.


### Sequential Multi Instance
When a Sequential Multi Instance is triggered:
- A new process instance is created.
- The new instance is linked to its parent process instance.
- The child process runs in its own isolated scope.
- Only one activity instance is created and active at once.
- Child process executes only its given activity one by one in a loop.

### Parallel Multi Instance
When a Parallel Multi Instance is triggered:
- A new process instance is created.
- The new instance is linked to its parent process instance.
- The child process runs in its own isolated scope.
- All the required activity instances are created.
- Activity instance are executed in parallel.

The child process for Multi Instance is started on the same [partition](/reference/cluster) as the parent process that
invoked it.

In case of **Parallel** Multi Instance the behavior is the same expect the instances are all started at the start of the
Multi Instance process.

#### Variable Handling

By default, no variables are inherited from the parent process instance.
The child process operates within its own variable scope using variables from **Input Collection**.
Upon completion **Output Elements** are collected and aggregated to **Output Collection** which is then propagated into the parent instance.

## Input/Output

Input and Output parameters still belong to its task, subprocess or call activity. These mappings control the variable
scope at the start and end of the task, subprocess or call activity that is run in Multi Instance.

### Output element with output mappings

The Multi-Instance `outputElement` is evaluated after each iteration completes. Its expression can read:

- the iteration input variables, such as the `inputElement`
- the variables produced by the wrapped activity's output mappings

Raw variables returned by a job or created inside the wrapped activity are not automatically available to `outputElement`. If `outputElement` should use a value returned by the activity, map that value with an output mapping first.

```xml
<bpmn:userTask id="review_task">
  <bpmn:extensionElements>
    <zenbpm:ioMapping>
      <zenbpm:output source="=approver" target="reviewer" />
      <zenbpm:output source="=approved" target="approved" />
    </zenbpm:ioMapping>
  </bpmn:extensionElements>

  <bpmn:multiInstanceLoopCharacteristics isSequential="true">
    <bpmn:extensionElements>
      <zenbpm:loopCharacteristics
        inputCollection="=approvers"
        inputElement="approver"
        outputCollection="approvalResults"
        outputElement="={ reviewer: reviewer, approved: approved }" />
    </bpmn:extensionElements>
  </bpmn:multiInstanceLoopCharacteristics>
</bpmn:userTask>
```

In this example, each iteration receives one `approver`. When the user task completes, the task output mappings create `reviewer` and `approved` in the iteration output. The Multi-Instance `outputElement` then uses those mapped variables to append one object to `approvalResults`.

For example, with `approvers = ["alice", "bob"]` and task completions that return `approved = true` and `approved = false`, the parent process receives:

```json
{
  "approvalResults": [
    { "reviewer": "alice", "approved": true },
    { "reviewer": "bob", "approved": false }
  ]
}
```

For parallel Multi-Instance activities, `approvalResults` still contains one item per completed iteration, but the item order is not guaranteed.

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

![Parallel Multi-instance usage example](../../../assets/bpmn/parallel.svg)

- **Three horizontal lines:** sequential multi-instance

![Horizontal Multi-instance usage example](../../../assets/bpmn/sequential.svg)

## XML Definition

### Parallel multi-instance example

```xml
<bpmn:userTask id="ReviewTask" name="Review Document">
  <bpmn:multiInstanceLoopCharacteristics isSequential="false">
    <bpmn:loopCardinality>5</bpmn:loopCardinality>
  </bpmn:multiInstanceLoopCharacteristics>
</bpmn:userTask>
