---
sidebar_position: 3
sidebar_label: Execution listeners
---

# Execution listeners

An execution listener is a job the engine creates at a fixed point of an element's life, for an
external worker to take part in it. ZenBPM runs listeners in the XML shape of Camunda 8, on these
elements:

| Element | Event types |
|---|---|
| a service task, a send task, a user task | `start`, `end` |
| a business rule task with a `zenbpm:taskDefinition` | `start`, `end` |
| an intermediate event which throws a message | `start`, `end` |
| a start event directly under `<process>` | `end` |

They run wherever such an element sits: in the process, in an embedded sub-process and in a
called process. They do not run on a multi-instance activity. A listener
anywhere else is refused when the process is deployed (see
[What a deployment refuses](#what-a-deployment-refuses)), except one on the `<process>` element
itself, which ZenBPM neither runs nor refuses.

## The life of an element with listeners

```xml
<bpmn:serviceTask id="ship">
  <bpmn:extensionElements>
    <zenbpm:taskDefinition type="ship-order" />
    <zenbpm:executionListeners>
      <zenbpm:executionListener eventType="start" type="reserve-stock" />
      <zenbpm:executionListener eventType="end" type="notify-customer" />
    </zenbpm:executionListeners>
  </bpmn:extensionElements>
  <bpmn:incoming>Flow_1</bpmn:incoming>
  <bpmn:outgoing>Flow_2</bpmn:outgoing>
</bpmn:serviceTask>
```

A token which enters the element stays in it across several jobs, one at a time:

1. The element's input mappings are evaluated.
2. Its `start` listeners run, in the order of the model: the job of the first is created, and
   when it is completed the job of the next.
3. The element's own work runs: the job of the task. The boundary events of the task watch this
   step only, neither the listeners before it nor the ones after it.
4. When the job of the task is completed, the output mappings are applied.
5. Its `end` listeners run, in the order of the model.
6. The token leaves the element by its outgoing sequence flows.

While the token is in the element, the element reads active in the history and in the UI, and its
outputs show in the history from step 4 on. An element left by a boundary event, after a BPMN
error of its job for example, runs no `end` listener.

Several listeners of one event type are allowed. Two of them need different `type`s: a deployment
refuses two listeners of one element with the same `eventType` and `type`.

Starting a token at the element (`POST /v1/modify/start-process-instance`) enters it as every
token does, so its `start` listeners run. Cancelling the instance terminates whichever job the
token waits for, as every job of the instance; a completion sent afterwards is answered `409`.

Each job the token waits for in the element counts as one flow node execution towards
[`maxProcessInstanceFlowNodeCount`](../configuration.md), and its leaving as one more: a task with
two `start` and two `end` listeners counts six, where a task without listeners counts one. A job
of the task which a task handler registered in the engine completes at once is not waited for, so
such a task counts one less.

## Variables

A listener's job carries the variables of its element: a copy of the instance's variables as they
are when the job is created, under the results of the element's input mappings, under the
variables the earlier jobs of the element were completed with. So:

- a `start` listener sees the instance and the results of the input mappings;
- the variables a `start` listener's job is completed with are local to the element: the later
  `start` listeners and the element's own job get them as input variables, and nothing outside the
  element does;
- an `end` listener sees the instance as the output mappings left it, the element's local
  variables, and the variables the element's own job was completed with;
- the variables an `end` listener's job is completed with are seen by the later `end` listeners
  of the element only.

The variables of a listener's completion never reach the instance by themselves, as the variables
of every job: an output mapping of the element has to name them. `=reserved` as the source of an
output mapping of the task above writes what `reserve-stock` was completed with as `reserved` to
the instance. A start event has no job of its own, so nothing its `end` listeners are completed
with reaches the instance; a worker which has to set a variable there does so with
`PATCH /v1/process-instances/{key}/variables` before it completes the job.

## Listeners on a start event

```xml
<bpmn:startEvent id="StartEvent_1">
  <bpmn:extensionElements>
    <zenbpm:executionListeners>
      <zenbpm:executionListener eventType="end" type="order-started" />
    </zenbpm:executionListeners>
  </bpmn:extensionElements>
  <bpmn:outgoing>Flow_1</bpmn:outgoing>
</bpmn:startEvent>
```

A start event takes `end` listeners only, as in Camunda 8. Every trigger takes them: a none start
event, a [timer start event](./supported-elements/events/timer-events.md#timer-start-event)
and a [message start event](./supported-elements/events/message-events.md#message-start-event),
in a process that is started by the API, by a timer, by a message, or by a call activity. A process
with several start events gets the jobs of each start event a token reaches.

When the token reaches the start event, the start event's output mappings are applied first, then
the job of the first listener is created and the token waits. The output mappings are those of a
top-level start event (see [Start events](./supported-elements/events/index.mdx#start-events)): a
process started by a call activity does not apply its start event's output mappings, with a
listener or without one. While the token waits, the instance reads `active`, and nothing behind the
start event runs: a message start is answered `201` as soon as the instance exists, and a failure
behind the start event, such as a called process which is not deployed, surfaces only after the
last listener's job is completed, as an incident of that run.

A start event of a sub-process takes no listener. Starting an instance at another element, an
instantiating receive task and the iterations of a multi-instance activity create no job of a
start event listener; only the process each iteration of a multi-instance call activity starts
runs its own start event listeners, as every called process does. Modifying an instance to start
a token at a start event runs its listeners again, with new jobs, so a worker has to be idempotent.

## What a listener job carries

A listener job is a job as any other, with two differences in what a worker sees:

- `elementType` and `elementId` are those of the listener's element, such as `START_EVENT` or
  `SERVICE_TASK`; `listenerEventType` (REST) and `listener_event_type` (the gRPC job stream) are
  `start` or `end`, and absent for the element's own job;
- its `inputVariables` are the variables of the element (see [Variables](#variables)).

Its [task headers](../jobs.md#task-headers) are those of the listener's element, as in Camunda 8:
the listeners of a task with headers get them as the task's own job does, and the listeners of a
start event get none. A listener takes no headers of its own.

A listener job is never handed to a task handler registered in the engine, even one registered for
the id or the type of its element: it is for external workers only. Such a handler does the
element's own work, between the listeners.

A listener job cannot throw a BPMN error: a failure with an error code is answered `400`, and the
job stays as it was. A failure of an ended job, or of a delivery whose failure was recorded
already, is answered as for every job, with or without an error code. A failure without an error
code spends the retries as for every job (see [Jobs](../jobs.md#failures-and-retries)). When they
are gone, the incident stands on the listener's element, and resolving it hands out the job again.

## Incidents

Resolving an incident of an element with listeners goes on where the element stood. A listener
whose job was completed never runs again for the same token in the element, whichever step failed:
a listener's job, the element's own job, or the creation of the next job. This differs from
Camunda 8, which runs the earlier listeners again after some failures.

As for every element, resolving evaluates the element's input mappings again, with the variables
the instance has then, and rebuilds the variables of the job which waits; what the earlier jobs of
the element were completed with is kept. The input variables the history shows for the element are
those its own job gets its inputs from: they follow each such resolution until that job is
completed, and stay as they are while the `end` listeners run.

## Attributes

| Attribute | Meaning |
|---|---|
| `eventType` | `start` or `end` |
| `type` | the job type, a literal; surrounding whitespace is dropped |
| `retries` | the job's retries, a literal as the `retries` of a `zenbpm:taskDefinition`; absent, the engine default |
| `retryBackoff` | the job's backoff policy, a literal as the `retryBackoff` of a `zenbpm:taskDefinition`; absent, the engine default |

FEEL expressions are refused in all of them, so that creating a listener's job can never fail on
an expression.

The listeners are read in every namespace: `zenbpm:executionListeners` and
`zeebe:executionListeners` work alike, so a model drawn for Camunda 8 keeps its listeners.

## What a deployment refuses

A deployment is answered `400`, with a message naming the element, for:

| Where | Refused |
|---|---|
| an element which runs listeners | a listener of an event type the element does not run, two listeners with the same `eventType` and `type`, or a listener on an element without an outgoing sequence flow |
| an element without an `id`, or whose `id` another flow node of the process shares, a boundary event included | any listener |
| any flow node | a listener outside `extensionElements` > `executionListeners`, such as one without the `executionListeners` element or one in the shape of Camunda 7 |
| a flow node outside the flow of a process, that is neither directly in a process nor in a sub-process nested in its flow, such as one inside extension elements | any listener |
| a multi-instance activity | any listener |
| any other element: a manual task, a business rule task calling a decision, a receive task, a call activity, a sub-process and its start events, an end event, an intermediate catch event, a link event, a boundary event, a gateway | any listener |
| any element | any task listener (`taskListeners`) |
| any listener | a blank `type`, or a `type`, `retries` or `retryBackoff` which is a FEEL expression or a literal which does not parse |

For example:

```
execution listener eventType="end" on manual task id="sign" is not supported: execution listeners run on a service, send or user task, a business rule task with a task definition and a message throw event, none of them multi-instance (eventType="start" or "end"), and on a start event directly under the process (eventType="end")
```

Until this version, ZenBPM deployed such a model and silently never ran its listeners. A model drawn
for Camunda 8 with listeners on gateways, sub-processes or other events is refused from this
version on. This holds for a repeated deployment of a model stored before as well: one which
reached only some partitions before the upgrade cannot be completed by deploying it again, but only
by deploying it without the refused listeners. A listener on the `<process>` element is still
deployed and not run, as before, wherever it sits in the element's `extensionElements`, inside
another extension element too.

## Definitions deployed before

A definition stored by an earlier version is loaded without the checks of a deployment, so that it
keeps running. Its listeners on the elements named at the top of this page run for every token
which enters such an element from this version on. A token which already waited in a task before,
behind the task's job, leaves the task when that job is completed, as before: the task's `end`
listeners do not run for it. An instance of a model drawn for Camunda 8, deployed before, so waits
at its first listener from this version on until a worker serves the listener's `type`.

Its listeners in any other position are not run, as before. A stored listener which a deployment
would refuse on an element which runs listeners raises an incident on that element instead of
creating a job: one whose `type` is blank or whose `type`, `retries` or `retryBackoff` is a FEEL
expression, one beside another listener of the same `eventType` and `type`, one on an element
without an outgoing sequence flow, and one on an element whose `id` another flow node shares,
where every token at that `id` raises the incident. Resolving such an incident raises it again, as
the listener is still what it was. This holds for an instance which was started before the upgrade
and reaches the element afterwards as well: it passed such a task with an earlier version, and
does not pass it with this one. When a cluster restore copies a definition to
a partition which lacks it, the engine logs one line for each copied definition whose listeners
run. A definition a partition restores from its own backup, or which only an upgrade brings to
this version, is not logged.

## Upgrading a cluster

A node running an earlier version ignores listeners: a token which enters an element on a
partition which such a node leads passes it without the listeners' jobs. Such a node does not know
the job of a listener either. What it does, while it leads a partition, with a job an upgraded
node created there depends on the element:

- The job of a start event's listener cannot be completed and its incident cannot be resolved, and
  a failure with an error code is taken as a BPMN error, which an error event sub-process may
  catch. The token waits in the start event until an upgraded node leads the partition again.
- The job of a listener on a task or on a message throw event is taken for the element's own job.
  Completing it applies the element's output mappings to the variables of the completion and
  moves the token on: the element's own job is never created, and its remaining listeners never
  run. A failure with an error code is thrown as a BPMN error of the element, which its boundary
  events may catch.
- The own job of such an element, completed there, moves the token on without the element's `end`
  listeners.

So a cluster must not run both versions while a token can enter, or waits in, a task or a message
throw event which carries listeners. Before a process with listeners is deployed for the first
time, upgrade every node of the cluster.

That is not enough for a process deployed before the upgrade whose elements already carry
listeners: from the first upgraded node on, its listeners run wherever an upgraded node leads, and
nowhere else. Where they sit on tasks or message throw events, stop every node of the cluster,
upgrade them all, and start them again. Holding back the starts of such a process does not help:
the instances which run already reach those elements as well. Where they sit on start events
only, it does: hold back every start of the process (by the API, by a message, by a call
activity, and its timer starts) until every node runs this version.
