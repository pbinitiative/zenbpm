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
To complete the job and move the token to the next element you have to call `completeJob` endpoint. Completing a job completed before answers `201` again; a job terminated or failed meanwhile no longer waits for a worker and answers `409`.

<ApiOperation id="api" pointer="#/paths/~1jobs~1{jobKey}~1complete/post" example={true} />

### GRPC API

Provides more robust solution to executing heavy workloads and distributes them across multiple instances of clients that can perform the same type of work. GRPC API uses Job manager to distribute jobs between nodes and clients.

Job streaming can be initiated by calling **grcp.Zenbpm JobStream** procedure. This opens a bidirectional stream between client and one of the ZenBPM nodes. Client can provide its clientID that will be used to balance job distribution in metadata. If not provided one will be generated on the server.

First thing that a client should do is send a `StreamSubscriptionRequest` message with `type` of `TYPE_SUBSCRIBE` and required `job_type`. This message can be repeated to register multiple job types.

After the client register itself for job processing the server will start sending jobs that need to be processed to the client. Every delivered job is **locked** for the client it was sent to and will not be distributed to another client until that lock lapses (see [Job locks](#job-locks)).

When client finishes the work that had to be done to complete the job, client must send `JobCompleteRequest` message. This message will complete the job in the engine and move the token to next element. A completion is idempotent: a job completed before is answered as completed again. A completion the engine did not carry out is answered with an `ErrorResult` next to a `WaitingJob` carrying only the key, and its `code` says why: `JOB_STREAM_ERROR_CODE_JOB_IN_TERMINAL_STATE` (6) when the job was terminated or failed meanwhile, `JOB_STREAM_ERROR_CODE_JOB_NOT_FOUND` (5) when its partition has no job with that key, `JOB_STREAM_ERROR_CODE_INVALID_REQUEST` (4) for variables which do not decode, and `JOB_STREAM_ERROR_CODE_LEADER_UNAVAILABLE` (3) when the leader of the job's partition could not be reached or has just changed - the completion may have been recorded all the same, and repeating it is safe.

If there is an error while executing the job logic the client should send `JobFailRequest` message. With an `error_code` it throws a BPMN error; without one it spends one of the job's retries, and only the failure which leaves no retries creates an incident (see [Failures and retries](#failures-and-retries)).

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

**What a lock is not.** The lock lives in memory on the partition leader. A leader change forgets every lock and the new leader redelivers the open jobs at once, so a handler must tolerate a second delivery of a job it is still working on after a failover. Completion and failure are not bound to the lock holder: a job may be completed or failed by anybody who knows its key, including a REST client which never held the lock. A failure spends the attempt whoever reports it - once within its series of attempts, when it names the attempt it belongs to and carries no error code (see [Failures and retries](#failures-and-retries)) - but releases the lock only when the reporting client holds it: a client whose lock lapsed, and which reports its failure after the job was handed to another client, does not take the job from that client. Nor does the holder's own failure when it names an attempt before the one the lock was handed out for: that is the repeat or the late report of an earlier delivery, and the holder keeps working on the attempt it has. When the job came back to the same client after its lock lapsed, the job manager cannot tell which of the two deliveries the client's next failure belongs to, so that failure keeps the lock too; the one after it, a completion, or the lapse of the lock releases it, and a retry waits at most one lock duration longer. A completion or failure refused because the job no longer waits releases the lock at once. Jobs fetched through the REST `getJobs` endpoint hold no lock at all.

Workers which receive jobs over the stream but send their commands over REST extend a lock with the `extendJobLock` endpoint, passing the client id of their stream:

<ApiOperation id="api" pointer="#/paths/~1jobs~1{jobKey}~1extend-lock/post" example={true} />

Such workers pass the client id of their stream to the `failJob` endpoint as well, as `clientId`. A REST failure without it leaves a stream lock standing: the job is handed out again only once the lock has lapsed, whatever `retryBackoff` asks for, although `GET /v1/jobs/{key}` shows no `retryAt`.

## Failures and retries

A worker which cannot finish a job reports a failure: `JobFailRequest` on the stream, the `failJob`
REST endpoint, or a `zenclient.WorkerError` from a Go worker. What happens depends on whether the
failure carries an error code.

**With an error code** the failure is a BPMN error. A matching error boundary event or error event
sub-process takes over and receives the failure's variables; without a match the job fails with an
incident. Retries are neither consulted nor spent.

**Without an error code** (absent or empty) the failure is technical and spends one attempt of the
job:

1. Every job starts with the `retries` of its `zenbpm:taskDefinition` - a non-negative integer, or a
   FEEL expression starting with `=` evaluated when the job is created. Without the attribute it
   gets `jobs.defaultRetries`, which is `1`: one attempt, so the first failure creates an incident.
2. A failure sets the remaining retries to what the request names in `retries`, or to one less
   than now. `retries` may be larger than the current value, a worker may top up, but never above
   `jobs.maxRetries`; a negative value is refused with `400`. `retries` and the backoff are
   validated in every request: an invalid value is refused even next to an error code, which
   otherwise ignores them.
3. While retries remain, the job stays `active`, no incident is created and the process instance
   is untouched. The job is not handed out again - neither over the stream nor by the library's
   `ActivateJobs` - before its `retryAt`, and is afterwards without anybody acting, within the
   job manager's polling interval of about a second, unless a stream lock still reserves it (see
   "What a lock is not" above).
   `GET /v1/jobs?state=active` still lists it; its `retryAt` says why nobody holds it. The
   failure's variables are dropped, so the next attempt starts from the same input.
4. The failure which leaves no retries fails the job with an incident whose message names the
   attempts, `<message> (job <key>: 3 attempts, retries exhausted)`, and whose `jobKey` leads to the
   job. The failure's variables stay on the job as its output variables.
5. Resolving that incident starts a fresh series: `attempts` goes back to `0`, `retryAt` is
   cleared and the retries of the task definition are evaluated again - unless an operator set new
   retries meanwhile (see below), which are kept together with the `retryAt` the operator chose.
   A `retries` expression which no longer evaluates refuses the resolution with `409` and changes
   nothing; the message names the error and the way out: correct the variables the expression
   reads, or set the job's retries, then resolve again.
   Resolving the incident of an error code nothing caught does the same, so leftover retries do not
   carry over into the new series; retries an operator set before that incident are forgotten with
   the series they belonged to. An incident the job did not raise itself, such as one of a boundary
   event of its task, leaves the series alone when it is resolved: attempts, retries and `retryAt`
   stay as they were.

A fail request without an error code which names the `attempt` it belongs to - the `attempt` of
its delivery over the stream, or the job's `attempts` plus one as read over REST - is recorded once
within a series of attempts. A request for an attempt whose failure the job recorded already
changes nothing and is answered as recorded, also once the failure it repeats exhausted the retries
and also when the instance ended meanwhile. That is the repeat after a timeout, and the late
report of a delivery whose attempt another delivery failed meanwhile: neither spends the attempt a
later delivery is working on, creates the incident while that delivery runs, or releases its lock.
An `attempt` below `1` or beyond the one the job waits for is refused like any other invalid value.
The Go client names the attempt of every failure it sends.

The attempt does not tell two deliveries of one attempt apart. After a lapsed lock or a leader
change the job is delivered again with the same attempt, and the first of the two failures to
arrive spends it: a worker which works past its lock and then fails can still exhaust the retries,
and create the incident, while the second delivery runs. A worker which needs longer than its lock
extends it (see [Job locks](#job-locks)). Attempts also start again at `1` once an incident of the
job is resolved, so a report from before the incident which arrives after the resolution is taken
for the new series: it spends the attempt when it names the one the job now waits for, and is
refused when it names a later one.

A fail request without `attempt` is not idempotent: every failure without an error code the engine
receives spends an attempt, also one reported for a job waiting out its backoff, which nobody holds
at that moment. A client which repeats such a request after a timeout may spend two attempts for one
failure. A failure reported for a job which no longer waits for a worker - the repeat of a failure
which exhausted the retries, or a job completed or terminated meanwhile - changes nothing and is
refused: `409` on the REST endpoint, `JOB_STREAM_ERROR_CODE_JOB_IN_TERMINAL_STATE` on the stream.

A `JobFailRequest` on the stream which the engine did not record is answered with an `ErrorResult`
next to a `WaitingJob` carrying only the key. Its `code` says why:
`JOB_STREAM_ERROR_CODE_INVALID_REQUEST` (4) for negative retries, a negative backoff or variables
which do not decode, which the REST endpoint answers with `400` - the message names the field and
the value; `JOB_STREAM_ERROR_CODE_JOB_NOT_FOUND` (5) when the job's partition has no job with that
key; `JOB_STREAM_ERROR_CODE_JOB_IN_TERMINAL_STATE` (6) when the job no longer waits for a worker;
`JOB_STREAM_ERROR_CODE_LEADER_UNAVAILABLE` (3) when the leader of the job's partition could not
be reached or has just changed - the failure may have been recorded all the same. Repeating a
failure without an error code spends nothing more when it names its attempt, unless an incident of
the job was resolved meanwhile; repeating one without `attempt` spends a second attempt, and one
with an error code is refused with code 6 once the first was recorded. Any other error carries no
code.

A job whose token is terminated during its backoff, by an interrupting boundary event or by
cancelling the instance, is terminated like any active job.

### Backoff

How long a failed job waits before it is handed out again is decided, most specific first, by:

1. the `retry_backoff_ms` of `JobFailRequest` or the `retryBackoff` of the REST request (an ISO-8601
   duration such as `PT10S`; `0`/`PT0S` means at once);
2. the `retryBackoff` of the `zenbpm:taskDefinition`: one ISO-8601 duration (`PT10S`, a fixed
   delay) or a comma-separated list of them (`PT10S,PT1M,PT10M`), where the n-th failure waits the
   n-th entry and the last entry repeats - `PT10S,PT20S,PT40S,PT80S` is an exponential backoff. A
   FEEL expression starting with `=` may produce such a string or a list of duration strings;
3. `jobs.defaultRetryBackoff`, `PT0S` unless configured.

Every backoff is capped by `jobs.maxRetryBackoff` (`PT24H`). Durations are hours, minutes, seconds,
days (24 hours) and weeks; years and months have no fixed length and are refused. The policy of the
task definition is fixed when the job is created, so redeploying the model does not change the
policy of a running job. A literal `retries` or `retryBackoff` which does not parse is refused when
the definition is deployed; an expression is checked when the job is created, and one which does
not evaluate to a valid value creates an incident there. A definition deployed before the engine
read these attributes is not checked again: it keeps loading whatever its literals say, and a
literal which does not parse creates an incident when a job is created, like an expression.

```xml
<zenbpm:taskDefinition type="charge-card" retries="4" retryBackoff="PT10S,PT1M,PT10M" />
```

### What a job shows

| Field | Meaning |
|---|---|
| `retries` | attempts left: failures without an error code the job may still report; the one which leaves none creates an incident |
| `attempts` | failures without an error code since the job was created or its incident resolved |
| `retryAt` | before this moment an active job is not handed out; absent when it is deliverable. A job which ended during its backoff keeps the value |
| `lastFailureMessage` | message of the latest failure without an error code |
| `retryBackoff` | the policy of the task definition; absent when the engine default applies |

Every delivery over the stream carries `retries` and `attempt`: `1` for the first delivery of a
series and one more after every failure. Two deliveries of one attempt - a lapsed lock, a leader
change - carry the same number, a delivery after a failure the next. The Go client exposes them as
`job.GetRetries()` and `job.GetAttempt()`.

Every failure without an error code is recorded: attempt, time, message, when the job was handed
out again and, on the failure which exhausted the retries, the incident key. The records are
deleted together with the process instance by the history cleanup. An unknown job answers `404`, a
job which never failed an empty page. The page, like the job itself read with `getJob`, is served by
any node of the job's partition and is eventually consistent: read right after a failure it may not
show it yet, so a reader acting on it polls until it does.

<ApiOperation id="api" pointer="#/paths/~1jobs~1{jobKey}~1failures/get" example={true} />

The engine logs one `WARN` per retried failure (job, type, instance, attempt and next delivery) and
one `ERROR` when the retries are exhausted. The metric `jobs_retried` counts the retried failures
and the histogram `job_retry_backoff` (ms) the backoffs applied, both by job `type`;
`jobs_failed` keeps counting only the failures which failed the job. Once a failure without an
error code is committed, its span carries `zenbpm.job.failure.outcome` (`retry` or `incident`),
`zenbpm.job.attempt`, `zenbpm.job.retries` (left after the failure) and, on a retry,
`zenbpm.job.retry_backoff_ms`; the worker's message is not a span attribute.

### Setting the retries of a job

An operator sets the remaining retries of an `active` or `failed` job, and optionally when it is
handed out next; without `retryAt` a job waiting out its backoff is deliverable at once. The
incident of a failed job stays open: resolve it as usual, and the resolution then keeps the retries
set here and, while it still lies ahead, the `retryAt`. A `completed` or `terminated` job answers
`409`; a count below `1` or above `jobs.maxRetries`, or a `retryAt` later than now plus
`jobs.maxRetryBackoff`, `400` - the cap which lowers every backoff refuses a deadline it cannot
lower.

<ApiOperation id="api" pointer="#/paths/~1jobs~1{jobKey}~1retries/post" example={true} />

### What stays the worker's business

The engine cannot undo what a worker did before it failed, and does not try. A job keeps its key
across attempts and its input variables do not change between them, so an effect which must happen
once per job is made idempotent on the job key, or on a key of the business operation it performs:
a worker which charged a card and then failed must find that charge again on its next attempt.

The `attempt` of a delivery is no substitute. It tells a redelivery of the same attempt (a lapsed
lock, a leader change) from a retry, within one series of attempts. It is not unique over the life
of a job: resolving an incident starts a new series at `1`. Keying an effect on the job key and the
attempt would repeat the effect on every retry.

> ⚠️ **Upgrade notes:**
>
> - A model whose `zenbpm:taskDefinition` already carries `retries` - the Camunda Modeler writes
>   the attribute - now gets those retries: its first failure without an error code no longer
>   creates an incident. Remove the attribute, or set `retries="1"`, to keep the old behaviour.
> - A failure with an empty error code, which the REST API and the Go client sent when no code was
>   given, is now a failure without an error code: it spends a retry instead of being caught by a
>   catch-all error boundary event or error event sub-process. Send an error code to throw a BPMN
>   error.
> - Jobs created before the upgrade keep one retry, the default of the new column, even when their
>   definition carries `retries`; jobs created afterwards get the definition's. Set the retries of a
>   running job with the endpoint above.
> - Failing or completing a job which no longer waits for a worker answers `409` instead of `500`,
>   and the stream answers the completion with a code. A worker using the Go client is not affected:
>   it only logs the answer. A stream completion the leader refused used to be reported to the
>   worker as a success; it is now reported as the error it was.
> - A worker which receives jobs over the stream and fails them over REST passes its stream's
>   `clientId` with the failure; without it the job still waits for its lock to lapse, whatever
>   `retryBackoff` asks for.
> - For applications embedding the engine as a library: `Engine.JobFailByKey` takes the retries
>   and the backoff of the failure as two more arguments, and a storage implementation has to
>   provide `FindJobFailures` and `SaveJobFailure` (`storage.JobStorageReader` and
>   `storage.JobStorageWriter`).

## Job manager

Is a component that handles management of external jobs. Its main goal is to pull jobs that need to be processed out of the database and distribute them among clients connected to different ZenBPM nodes.
Job manager handles two sides of the communication in the cluster. Connection originates (client) from any node in the ZenBPM cluster and targets partition leaders (server) in the cluster:

- **client** side: handles worker connections that are interested in performing work on different ob types.
- **server** side: pulls out jobs that need to be processed and distributes them using round robin among workers connected to client side.
