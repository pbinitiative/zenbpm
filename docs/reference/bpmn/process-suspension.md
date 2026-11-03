---
sidebar_position: 3
---

import ApiOperation from "@theme/ApiOperation";

# Suspending a process

A process can be suspended to stop it from starting new instances, and resumed later. Suspending applies to the BPMN process id, so it covers every version of the process, including versions deployed while it is suspended.

<ApiOperation id="api" pointer="#/paths/~1processes~1{bpmnProcessId}~1suspension/put" example={true} />

The request is idempotent: suspending a suspended process, or resuming one that is not suspended, leaves it as it is. A process id with no deployed definition answers `404`.

While a process is suspended, no new instance of it starts by any route:

| Route                                                                           | What happens                                                                                                                                                                                                                                                                                                                                                                               |
| ------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Create a process instance, or start it on elements, through the API             | The request is rejected with `409`.                                                                                                                                                                                                                                                                                                                                                        |
| Publish the message of a message start event, with or without a correlation key | The request is rejected with `409`. The message is not kept, and the subscription stays active for when the process is resumed.                                                                                                                                                                                                                                                            |
| Message of an instantiating receive task                                        | Same as the message start event.                                                                                                                                                                                                                                                                                                                                                           |
| Timer start event                                                               | The timer fires and is consumed, but no instance is created. A cycle timer carries on with its next repetition, but a firing which is skipped is used up: a cycle with a limited number of repetitions or an end date loses them, and a timer which fires once (`timeDate`) is gone. Nothing is made up on resume. A timer which fires once is logged at warning level when it is dropped. |
| Call activity calling the process                                               | The calling instance gets an incident saying that the called process is suspended. Resolving the incident after the process is resumed calls the process.                                                                                                                                                                                                                                  |

Instances which are already running are not affected: their jobs, timers and catch events keep working until they end. The exception is a call activity of such an instance which calls a suspended process, the process itself included: it gets an incident, like any other caller. A start which has passed the check at the moment the process is suspended can still create its instance.

Process definitions report the state as `suspended`.

## In a cluster

The state is stored on every partition, so it survives restarts. Each change has a version, which the first partition (the one with the lowest id) takes, and the other partitions keep the change with the highest version. When requests overlap, the one which took the higher version on the first partition is the one every partition ends with, which is not necessarily the one which was sent last.

- If a request fails with `502` after it reached the first partition, some partitions may not have the change yet. The error names them. Any other failure (a timeout, a client which went away) may also have come after the first partition took the change. Send the request again: it is idempotent, always safe, and brings every partition up to date.
- A restore brings the partitions back from backups taken at different moments, so until the first request after a restore the partitions can disagree. That request brings every partition to the same state, so send it once for each process whose state matters.
- In a cluster whose nodes run different versions, a partition leader which runs a version without process suspension answers `502`. Repeat the request after the upgrade is finished.
- `suspended` on a process definition is read from a replica of one partition, so it can briefly show the state from before a change. The same holds for a partition whose leader has just changed, until the new leader has caught up.
- The `reporting.process_suspension` table of the Postgres reporting schema receives the changes of every partition, one row per process. Its `version` column tells which change it shows, and it can lag behind the state of the partitions when requests overlap. Read the state from the process definitions.

## Embedding the engine as a library

`Engine.SetProcessSuspended` and `Engine.IsProcessSuspended` change and read the state. A storage implementation has to provide `IsProcessSuspended`, `ChangeProcessSuspension` and `ApplyProcessSuspension` (`storage.ProcessSuspensionStorageReader` and `storage.ProcessSuspensionStorageWriter`); the versions of the changes of a process must increase, and `ChangeProcessSuspension` has to return a version above the one it holds and, when it is given `storage.AboveVersion(n)`, above `n` too. Starting an instance of a suspended process, and publishing the message of one of its start events, return a `*bpmn.ProcessSuspendedError`.
