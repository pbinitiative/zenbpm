---
sidebar_position: 6
---
# Storage
[RqLite](https://rqlite.io/) is used as a storage layer. Read more about partitions in [cluster section](/reference/cluster)

RqLite is a distributed relational database that combines the simplicity of SQLite with the robustness of a fault-tolerant, highly available system. It's developer-friendly, exceptionally easy to operate, and it's designed for reliability with minimal complexity.

We use the most important parts of the RqLite project as our storage solution.
[RqLite documentation](https://rqlite.io/docs/guides/config/) is a great resource to see most of the configuration possibilities.

RqLite Change Data Capture can be enabled with `cluster.cdc.enabled` and
`cluster.cdc.output`, or with `RQLITE_CDC_ENABLED` and `RQLITE_CDC_OUTPUT`.
Set the required stable source identifier with `cluster.cdc.serviceId`,
`RQLITE_CDC_SERVICE_ID`, or `service_id` in an advanced output settings file.
The output accepts an HTTP(S) endpoint, `stdout`, or a path to an advanced JSON
output settings file. CDC exports changes only; it
does not create an initial snapshot or backfill existing rows. Every voting
replica must use the same endpoint and compatible CDC settings. See the
[configuration reference](configuration.md#rqlite-change-data-capture) for
service identifiers, safe endpoint cutover, setup, and delivery semantics.

:::note[Future plans]
We plan to expose the RqLite endpoints on the system path so that the users can use those endpoints to interact with the database directly.
:::
