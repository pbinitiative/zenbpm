---
sidebar_position: 6
---
# Storage
[RqLite](https://rqlite.io/) is used as a storage layer. Read more about partitions in [cluster section](/reference/cluster)

RqLite is a distributed relational database that combines the simplicity of SQLite with the robustness of a fault-tolerant, highly available system. It's developer-friendly, exceptionally easy to operate, and it's designed for reliability with minimal complexity.

We use the most important parts of the RqLite project as our storage solution.
[RqLite documentation](https://rqlite.io/docs/guides/config/) is a great resource to see most of the configuration possibilities.

:::note[Future plans]
We plan to expose the RqLite endpoints on the system path so that the users can use those endpoints to interact with the database directly.
:::
