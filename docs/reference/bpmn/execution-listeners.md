---
sidebar_position: 3
sidebar_label: Execution listeners
---

# Execution listeners

An execution listener is a job the engine creates at a fixed point of a process instance's life,
for an external worker to report it. ZenBPM runs a listener in one position, in the XML shape of
Camunda 8:

| Position | Event type | When the job is created | What waits for the job |
|---|---|---|---|
| a start event directly under `<process>` | `end` | when a token reaches the start event | the token: it leaves the start event when the job is completed |

A listener anywhere else is refused when the process is deployed (see
[What a deployment refuses](#what-a-deployment-refuses)), except one on the `<process>` element
itself, which ZenBPM neither runs nor refuses.

## A listener on a start event

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

Every trigger takes it: a none start event, a [timer start event](./supported-elements/events/timer-events.md#timer-start-event)
and a [message start event](./supported-elements/events/message-events.md#message-start-event),
in a process that is started by the API, by a timer, by a message, or by a call activity. A process
with several start events gets a job for each start event a token reaches.

When the token reaches the start event, the start event's output mappings are applied first, then
the job is created and the token waits. The output mappings are those of a top-level start event
(see [Start events](./supported-elements/events/index.mdx#start-events)): a process started by a call
activity does not apply its start event's output mappings, with a listener or without one. While it waits, the start event reads active in the history
and in the UI, and the instance reads `active`. Nothing behind the start event runs: a message
start is answered `201` as soon as the instance exists, and a failure behind the start event, such
as a called process which is not deployed, surfaces only after the job is completed, as an
incident of that run. When the job is completed, the token leaves the start event and the instance
runs on.

A start event of a sub-process takes no listener. Starting an instance at another element
(`POST /v1/modify/start-process-instance`), an instantiating receive task and the iterations
of a multi-instance activity create no listener job; only the process each iteration of a
multi-instance call activity starts runs its own start event listener, as every called process
does. Modifying an instance to start a token at a
start event runs its listener again, with a second job, so a worker has to be idempotent.

Cancelling the instance while the job waits terminates the job with the instance, as every job of
it; a completion sent afterwards is answered `409`.

## What a listener job carries

A listener job is a job as any other, with two differences in what a worker sees:

- `elementType` is `START_EVENT`; `listenerEventType` (REST) and `listener_event_type` (the gRPC
  job stream) are `end`, and absent for the job of a task;
- its `inputVariables` are the instance's variables, as for every job, including the start event's
  outputs.

Completing a listener job changes no variable: the variables sent with the completion are kept on
the job only. A worker which has to set variables on the instance does so with
`PATCH /v1/process-instances/{key}/variables` before it completes the job.

A listener job is never handed to a task handler registered in the engine, even one registered for
the id of the start event: it is for external workers only.

A listener job cannot throw a BPMN error: a failure with an error code is answered `400`, and the
job stays as it was. A failure of an ended job, or of a delivery whose failure was recorded
already, is answered as for every job, with or without an error code. A failure without an error
code spends the retries as for every job (see
[Jobs](../jobs.md#failures-and-retries)). When they are gone, the incident stands on the start
event, and resolving it hands out the job again.

## Attributes

| Attribute | Meaning |
|---|---|
| `eventType` | `end` |
| `type` | the job type, a literal; surrounding whitespace is dropped |
| `retries` | the job's retries, a literal as the `retries` of a `zenbpm:taskDefinition`; absent, the engine default |
| `retryBackoff` | the job's backoff policy, a literal as the `retryBackoff` of a `zenbpm:taskDefinition`; absent, the engine default |

FEEL expressions are refused in all of them, so that creating a listener's job can never fail on
an expression. Resolving an incident of a listener job rebuilds the job's variables from the
instance, as for every job.

The listeners are read in every namespace: `zenbpm:executionListeners` and
`zeebe:executionListeners` work alike, so a model drawn for Camunda 8 keeps its start event
listeners. Only one listener per start event is supported.

## What a deployment refuses

A deployment is answered `400`, with a message naming the element, for:

| Where | Refused |
|---|---|
| a start event directly under `<process>` | a listener of another event type than `end`, more than one listener, or a listener on a start event without an outgoing sequence flow |
| a start event without an `id`, or whose `id` another flow node of the process shares, a boundary event included | any listener |
| a start event | a listener outside `extensionElements` > `executionListeners`, such as one without the `executionListeners` element or one in the shape of Camunda 7 |
| a start event outside the flow of a process, that is neither directly in a process nor in a sub-process nested in its flow, such as one inside extension elements | any listener |
| an embedded or event sub-process, and its start events | any listener |
| any other element | any listener |
| any element | any task listener (`taskListeners`) |
| any listener | a blank `type`, or a `type`, `retries` or `retryBackoff` which is a FEEL expression or a literal which does not parse |

For example:

```
execution listener eventType="start" on service task id="ship" is not supported: only a start event directly under the process takes an execution listener, with eventType="end"
```

Until this version, ZenBPM deployed such a model and silently never ran its listeners. A model drawn
for Camunda 8 with listeners on tasks, gateways or other events is refused from this version on.
This holds for a repeated deployment of a model stored before as well: one which reached only some
partitions before the upgrade cannot be completed by deploying it again, but only by deploying it
without the refused listeners. A listener on the `<process>` element is still deployed and not
run, as before, wherever it sits in the element's `extensionElements`, inside another extension
element too.

## Definitions deployed before

A definition stored by an earlier version is loaded without the checks of a deployment, so that it
keeps running. Its start event listeners run for every instance started from this version on;
instances which passed their start events before are not affected. Its listeners in any other
position are not run, as before. A stored start event listener which a deployment would refuse
raises an incident on its start event instead of creating a job: one whose `type` is blank or
whose `type`, `retries` or `retryBackoff` is a FEEL expression, one beside another listener of
its start event, one on a start event without an outgoing sequence flow, and one on a start event
whose `id` another flow node shares, where every token at that `id` raises the incident. When a cluster restore copies a definition to a partition which lacks it, the engine logs one
line for each copied definition whose start event listeners run. A definition a partition
restores from its own backup, or which only an upgrade brings to this version, is not logged.

## Upgrading a cluster

A node running an earlier version ignores start event listeners: an instance started on a
partition which such a node leads runs without the listener's job. Such a node does not know the
job of a listener either, so while it leads a partition, a listener job which an upgraded node
created there cannot be completed and its incident cannot be resolved, and a failure with an error
code is taken as a BPMN error, which an error event sub-process may catch. The instance waits at
its start event until an upgraded node leads the partition again. Upgrade every node of the
cluster before deploying processes with start event listeners.

Not deploying is not enough for a process deployed before the upgrade whose start events
already carry listeners: from the first upgraded node on, its listeners run wherever an upgraded
node leads, and nowhere else. Where every instance has to reach the listener's worker, hold back
every start of such a process (by the API, by a message, by a call activity, and its timer
starts) until every node runs this version.
