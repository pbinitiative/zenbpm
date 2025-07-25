---
sidebar_position: 3
---
# Jobs
Jobs are small tasks that the bpmn engine created by executing process instances. When an external job (job that cannot be handled by the engine itself) is created the token that activated it transitions into waiting state. Token then waits for the job to be completed by 3rd party application by calling API to complete job.
This is the main point of interaction of business applications with bpmn engine.

## Internal jobs

## External jobs

## Working with external jobs
:::note[Future plans]
TODO: once we have public api for job handling we will have to describe it here
:::

## Job manager
Is a component that handles management of external jobs. Its main goal is to pull jobs that need to be processed out of the database and distribute them among clients connected to different ZenBPM nodes.
Job manager handles two sides of the communication in the cluster. Connection originates (client) from any node in the ZenBPM cluster and targets partition leaders (server) in the cluster:
- **client** side: handles worker connections that are interested in performing work on different ob types.
- **server** side: pulls out jobs that need to be processed and distributes them using round robin among workers connected to client side.
