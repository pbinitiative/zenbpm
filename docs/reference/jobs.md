---
sidebar_position: 3
---

import ApiOperation from "@theme/ApiOperation";

# Jobs

Jobs are small tasks that the bpmn engine created by executing process instances. When an external job (job that cannot be handled by the engine itself) is created the token that activated it transitions into waiting state. Token then waits for the job to be completed by 3rd party application by calling API to complete job.
This is the main point of interaction of business applications with bpmn engine.

## Internal jobs

Internal jobs are jobs that run internally in the bpmn engine and do not require 3rd party applications for completion (either by calling REST or GRPC api).
They can be registered by using `NewTaskHandler` method on the bpmn engine.

If you are using the engine as a library you can register your handlers and they will be executed right away when the token hits the task node that can be handled by one of the registered handlers.

## External jobs

External jobs are jobs that require interaction with one of the ZenBPM's public APIs (REST or GRPC) by a 3rd party application.

## Working with external jobs

### REST API

Should be reserved for simpler job loads (e.g. completing user task) and task types that do not have multiple instances of workers trying to complete them.
You can load the waiting jobs with `getJobs` endpoint with `state=active` and optionally `jobType=mycooljobtype` filter:

<ApiOperation id="api" pointer="#/paths/~1jobs/get" example={true} />

This endpoint will return a list of partitions and jobs of type `mycooljobtype` that are waiting to be completed. Pagination on this endpoint is applied per partition. This means that page 1 and size 10 will return 20 jobs on fully saturated 2 partition setup.
To complete the job and move the token to the next element you have to call `completeJob` endpoint.

<ApiOperation id="api" pointer="#/paths/~1jobs~1{jobKey}~1complete/post" example={true} />

### GRPC API

Provides more robust solution to executing heavy workloads and distributes them across multiple instances of clients that can perform the same type of work. GRPC API uses Job manager to distribute jobs between nodes and clients.

Job streaming can be initiated by calling **grcp.Zenbpm JobStream** procedure. This opens a bidirectional stream between client and one of the ZenBPM nodes. Client can provide its clientID that will be used to balance job distribution in metadata. If not provided one will be generated on the server.

First thing that a client should do is send a `StreamSubscriptionRequest` message with `type` of `TYPE_SUBSCRIBE` and required `job_type`. This message can be repeated to register multiple job types.

After the client register itself for job processing the server will start sending jobs that need to be processed to the client. Every delivered job is **locked** for the client it was sent to and will not be distributed to another client until that lock lapses (see [Job locks](#job-locks)).

When client finishes the work that had to be done to complete the job, client must send `JobCompleteRequest` message. This message will complete the job in the engine and move the token to next element.

If there is an error while executing the job logic the client should send `JobFailRequest` message. This will mark the job as failed and create an incident in the engine.

If the work takes longer than the lock, the client sends a `JobExtendLockRequest` message before the lock lapses; the engine answers with a `LockExtended` message carrying the new deadline.

### Job locks

A job delivered over the stream is reserved for the receiving client for the **lock duration** of the subscription it was delivered under. `StreamSubscriptionRequest` carries two optional settings per job type:

| Field | Meaning | Default | Cap |
|---|---|---|---|
| `lock_duration_ms` | how long a delivered job of this type stays locked for this client | `jobManager.defaultLockDurationMs` (30 s) | `jobManager.maxLockDurationMs` (24 h) |
| `max_active_jobs` | how many jobs of this type this client may hold at once | `jobManager.defaultMaxActiveJobs` (10) | `jobManager.maxActiveJobsCap` (1000) |

A value of `0` means the engine's default; a value above the cap is lowered to the cap, never rejected. Subscribing again to the same job type replaces the settings; jobs already delivered keep the deadline they were delivered with. The defaults and caps are configured in the [`jobManager` section](configuration.md#job-manager-configuration-jobmanager).

**Lock deadline.** Every `WaitingJob` carries `lock_until`, the unix millisecond on the clock of the partition leader at which its lock lapses. `lock_until - now` on the client is an *estimate* of the remaining time: it is exact only as far as the two clocks agree, and it does not see the time the delivery spent in transit. It is also conservative: the leader counts the lock from the moment the delivery left it, while the reported deadline was taken just before, so the real deadline is never earlier than the reported one. Keep the clocks synchronised (NTP), and renew with a safety margin, at the latest at half the remaining time, rather than just before the deadline. When the clocks cannot be trusted, count from the moment of receipt with the lock duration you subscribed with, which needs no clock comparison at all. The Go client exposes the deadline as `job.GetLockUntil()`.

**Lock extension.** `JobExtendLockRequest{key, lock_duration_ms}` moves the deadline to *now plus the duration* (`0` = the subscription's lock duration; capped by `jobManager.maxLockDurationMs`). The new deadline is relative to now, not added to the previous one, so a worker renewing every half lock duration keeps a stable lead. The engine answers on the same stream:

- `LockExtended{key, lock_until}` on success;
- `ErrorResult` with `code` = `JOB_STREAM_ERROR_CODE_LOCK_NOT_HELD` (1) when the lock lapsed, the job was completed or failed, or it was never delivered to this client, or `JOB_STREAM_ERROR_CODE_LOCK_HELD_BY_OTHER_CLIENT` (2) when another client holds it. A refused extension leaves the job untouched. The `LockExtended` next to the error carries only the key.
- `ErrorResult` with `code` = `JOB_STREAM_ERROR_CODE_LEADER_UNAVAILABLE` (3) when the leader of the job's partition could not be reached or has just changed. Unlike a refusal, this leaves the outcome unconfirmed: a leader which lost the connection after applying the extension has moved the deadline already, and a leader change forgets the lock (see below). Retry the extension in a moment, as the REST `extendJobLock` endpoint's 502 asks you to, and count on the deadline of the last confirmed answer only. The Go client reports it as `zenclient.ErrLeaderUnavailable`.

The Go client offers `Worker.ExtendLock(ctx, jobKey, duration)`, `WithLockDuration` and `WithMaxActiveJobs` (through `RegisterWorkerWithOptions` and `WithJobType`); it does not renew locks by itself.

**Active-job cap.** The cap is counted per *client and job type*: a client holding its ten jobs of one type still receives jobs of every other type it subscribed to. The leader loads at most the sum of the free slots per round, and never more than its internal batch size of 300 jobs. Lowering the cap by subscribing again binds the next delivery, jobs already held stay held.

A leader also holds at most about 32,700 locked jobs across all its clients and job types, whatever the subscriptions add up to: every locked key is a parameter of the query which loads the next batch, and the database accepts 32,766 parameters per query. A leader at that bound logs a warning and delivers again as locks lapse or jobs complete.

The cap is enforced by each partition leader for the jobs of its own partitions. A cluster whose partitions are led by several nodes therefore lets a client hold up to the cap *per leader*; there is no cluster-wide budget. Size the cap for the number of partition leaders, or run the workers against a cluster with a single partition, until such a budget exists.

> ⚠️ **Upgrade note:** before locks became configurable, the ten-job cap was counted per client across all job types. A client subscribed to several job types may now receive more jobs in total than before. Set `max_active_jobs` per type to restore the old total.

**What a lock is not.** The lock lives in memory on the partition leader. A leader change forgets every lock and the new leader redelivers the open jobs at once, so a handler must tolerate a second delivery of a job it is still working on after a failover. Completion and failure are not bound to the lock holder: a job may be completed by anybody who knows its key, including a REST client which never held the lock. Jobs fetched through the REST `getJobs` endpoint hold no lock at all.

Workers which receive jobs over the stream but send their commands over REST extend a lock with the `extendJobLock` endpoint, passing the client id of their stream:

<ApiOperation id="api" pointer="#/paths/~1jobs~1{jobKey}~1extend-lock/post" example={true} />

## Job manager

Is a component that handles management of external jobs. Its main goal is to pull jobs that need to be processed out of the database and distribute them among clients connected to different ZenBPM nodes.
Job manager handles two sides of the communication in the cluster. Connection originates (client) from any node in the ZenBPM cluster and targets partition leaders (server) in the cluster:

- **client** side: handles worker connections that are interested in performing work on different ob types.
- **server** side: pulls out jobs that need to be processed and distributes them using round robin among workers connected to client side.
