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

<ApiOperation id="api" pointer="#/paths/~1jobs/post" example={true} />

### GRPC API

Provides more robust solution to executing heavy workloads and distributes them across multiple instances of clients that can perform the same type of work. GRPC API uses Job manager to distribute jobs between nodes and clients.

Job streaming can be initiated by calling **grcp.Zenbpm JobStream** procedure. This opens a bidirectional stream between client and one of the ZenBPM nodes. Client can provide its clientID that will be used to balance job distribution in metadata. If not provided one will be generated on the server.

First thing that a client should do is send a `StreamSubscriptionRequest` message with `type` of `TYPE_SUBSCRIBE` and required `job_type`. This message can be repeated to register multiple job types.

After the client register itself for job processing the server will start sending jobs that need to be processed to the client. These jobs are locked for 30s and will not be distributed to another client in that time.

When client finishes the work that had to be done to complete the job, client must send `JobCompleteRequest` message. This message will complete the job in the engine and move the token to next element.

If there is an error while executing the job logic the client should send `JobFailRequest` message. This will mark the job as failed and create an incident in the engine.

## Job manager

Is a component that handles management of external jobs. Its main goal is to pull jobs that need to be processed out of the database and distribute them among clients connected to different ZenBPM nodes.
Job manager handles two sides of the communication in the cluster. Connection originates (client) from any node in the ZenBPM cluster and targets partition leaders (server) in the cluster:

- **client** side: handles worker connections that are interested in performing work on different ob types.
- **server** side: pulls out jobs that need to be processed and distributes them using round robin among workers connected to client side.
