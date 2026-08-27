---
sidebar_position: 120
---


# Configuration
Application configuration can be supplied through:
- file: configured by env CONFIG_FILE (yaml/json format)
- env variables

---

## Root Configuration:

Top-level configuration object.

| Field         | Type         | Description                                 |
|---------------|--------------|---------------------------------------------|
| `httpServer`  | `HttpServer` | Configuration of the public REST server     |
| `grpcServer`  | `GrpcServer` | Configuration of the public GRPC server     |
| `tracing`     | `Tracing`    | Tracing and observability configuration     |
| `cluster`     | `Cluster`    | Cluster and Raft consensus configuration    |

---

## Server Configuration: `Server`

Defines settings for the public **REST API server**.

| Field                 | Type   | Env Variable                        | Default    | Description                                                                |
|-----------------------|--------|-------------------------------------|------------|----------------------------------------------------------------------------|
| `context`             | string | `REST_API_CONTEXT`                  | `/`        | Base context path for the API                                              |
| `addr`                | string | `REST_API_ADDR`                     | `:8080`    | Address the server binds to                                                |
| `maxRequestBodyBytes` | int64  | `REST_API_MAX_REQUEST_BODY_BYTES`   | `10485760` | Maximum request body size for `/v1` endpoints (10 MiB); larger bodies receive HTTP 413 |
| `logMode`             | string | `REST_API_LOG_MODE`                 | `errors`   | Which requests are logged: `errors` (status >= 400 only), `all`, or `off`  |
| `logBody`             | bool   | `REST_API_LOG_BODY`                 | `false`    | Include request/response bodies in logged requests. Buffers every body in memory, so keep off on busy servers |

---

## GRPC Server Configuration: `GrpcServer`

Defines settings for the **GRPC API server**.

| Field  | Type   | Env Variable     | Default | Description                         |
|--------|--------|------------------|---------|-------------------------------------|
| `addr` | string | `GRPC_API_ADDR`  | `:9090` | Address the GRPC server listens on  |

---

## Cluster Configuration: `Cluster`

Settings related to **clustering**, **internal communication**, and **Raft** consensus.

| Field        | Type          | Env Variable              | Default    | Description                                             |
|--------------|---------------|---------------------------|------------|---------------------------------------------------------|
| `nodeId`     | string        | `CLUSTER_NODE_ID`         | —          | Unique node identifier                                  |
| `addr`       | string        | `CLUSTER_RAFT_ADDR`       | `:8090`    | Bind address for internal Raft communication            |
| `adv`        | string        | `CLUSTER_RAFT_ADV`        | (same as `addr`) | Advertised Raft address                          |
| `raft`       | `ClusterRaft` | —                         | —          | Raft-specific cluster settings                          |
| `cdc`        | `CDC`         | —                         | —          | Change Data Capture output settings                     |
| `persistence`| `Persistence` | —                         | —          | Persistence and caching configuration                   |
| `engine`     | `Engine`      | —                         | —          | BPMN engine behaviour settings                          |

---

### Engine Configuration: `Engine`

Behaviour settings for the BPMN engines running on the node partitions.

| Field               | Type  | Env Variable                        | Default | Description                                                                                                                                                                                            |
|---------------------|-------|-------------------------------------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `maxProcessInstanceNestingDepth` | int64 | `CLUSTER_ENGINE_MAX_PROCESS_INSTANCE_NESTING_DEPTH`| `100`   | Maximum allowed nesting depth of a process instance in the parent-child chain (call activities, sub processes, multi-instance bodies). Creating a child instance deeper than the limit stops execution and raises an incident describing a potential infinite loop. Values `<= 0` disable the check. |

---

### Raft Configuration: `ClusterRaft`

Raft consensus and cluster joining settings.

| Field                    | Type          | Env Variable                         | Default             | Description                                              |
|--------------------------|---------------|--------------------------------------|---------------------|----------------------------------------------------------|
| `dir`                    | string        | `CLUSTER_RAFT_DIR`                   | `zen_bpm_node_data` | Path to local node data                                  |
| `nonVoter`               | bool          | `CLUSTER_RAFT_NON_VOTER`             | `false`             | Set node as non-voting member                            |
| `joinAttempts`           | int           | `CLUSTER_RAFT_JOIN_ATTEMPTS`         | `5`                 | Number of join attempts                                  |
| `joinInterval`           | duration      | `CLUSTER_RAFT_JOIN_INTERVAL`         | `2s`                | Time interval between join attempts                      |
| `joinAddresses`          | []string      | `CLUSTER_RAFT_JOIN_ADDRESSES`        | —                   | List of node addresses to join                           |
| `bootstrapExpect`        | int           | `CLUSTER_RAFT_BOOTSTRAP_EXPECT`      | `0`                 | Minimum nodes for bootstrap                              |
| `bootstrapExpectTimeout` | duration      | `CLUSTER_RAFT_EXPECT_BOOTSTRAP_TIMEOUT` | `10s`            | Max timeout for expected bootstrap nodes                 |

---

### RqLite Change Data Capture

Output settings for exporting RqLite changes from voting partition nodes.

| Field       | Type   | Env Variable            | Default  | Description                                                        |
|-------------|--------|-------------------------|----------|--------------------------------------------------------------------|
| `enabled`   | bool   | `RQLITE_CDC_ENABLED`    | `false`  | Enables RqLite CDC export                                          |
| `output`    | string | `RQLITE_CDC_OUTPUT`     | —        | HTTP(S) endpoint, `stdout`, or advanced JSON output settings path  |
| `serviceId` | string | `RQLITE_CDC_SERVICE_ID` | —        | Required base service identifier; ZenBPM appends the partition suffix |

CDC is disabled by default. Enable it on every voting RqLite partition node:

```yaml
cluster:
  cdc:
    enabled: true
    output: https://consumer.example.com/rqlite/cdc
    serviceId: orders-production-v1
```

The equivalent environment configuration is:

```bash
RQLITE_CDC_ENABLED=true
RQLITE_CDC_OUTPUT=https://consumer.example.com/rqlite/cdc
RQLITE_CDC_SERVICE_ID=orders-production-v1
```

`RQLITE_CDC_ENABLED` is the authoritative switch. When it is unset or `false`,
CDC is disabled even if an output is present. When it is `true`, the output is
passed to rqlite as an HTTP(S) endpoint, the special value `stdout`, or a path
to an advanced JSON output settings file. Invalid files, endpoints, or TLS
settings fail startup.
CDC startup also fails unless a non-empty service identifier is configured
through `serviceId`, `RQLITE_CDC_SERVICE_ID`, or an advanced output file.

#### Initial data and replica consistency

CDC does not create an initial snapshot or backfill. Rows already present in
SQLite when CDC is enabled are not emitted; only changes captured after CDC
starts are sent. Seed a new consumer with an explicit export or other backfill,
and reconcile that baseline with subsequent CDC events.

All voting replicas of a partition must use the same endpoint and compatible
CDC settings. In practice, deploy the same effective configuration, including
the service identifier (`serviceId` or advanced `service_id`), `table_filter`,
`row_ids_only`, and TLS and delivery settings, to every voting replica. Only the
current leader transmits events, while acknowledged high-water marks are shared
with followers. If replicas use different endpoints or filtering settings, a
leader change can split the stream between consumers or cause a follower to
discard events acknowledged through a different endpoint. CDC is rejected on
non-voting nodes.

#### Endpoint cutover

ZenBPM publishes to one CDC endpoint and does not provide built-in dual delivery
or backfill. Prefer a stable, durable receiver URL and change its downstream
routing when moving consumers. The receiver can retain events while the new
consumer is seeded and then forward them without changing the ZenBPM nodes.

If the endpoint must change in ZenBPM itself, do not roll the change through a
live cluster. Instead:

1. Pause writes that can produce CDC events.
2. Let the old endpoint acknowledge all outstanding deliveries for every
   partition, and verify its durable ledger is caught up.
3. Stop the voting replicas, apply the same new endpoint and compatible CDC
   settings to all of them, and restart them without allowing a mixed-config
   replica set to serve writes or elect a leader.
4. Resume writes only after every voting replica is using the new configuration.

The new endpoint receives only post-cutover changes. Copy or backfill the
existing state separately if it needs a complete projection.

#### Stable service identifiers

`cluster.cdc.serviceId` configures the required base identifier for direct
endpoint and `stdout` outputs. ZenBPM appends `-partition-N`, so a base value of
`orders-production-v1` emits
`orders-production-v1-partition-1` for partition 1.

An advanced rqlite CDC output settings file can also set `service_id`. For
example:

```json
{
  "endpoint": "https://consumer.example.com/rqlite/cdc",
  "service_id": "orders-production-v1"
}
```

Point every node at the file through the CDC output setting:

```bash
RQLITE_CDC_ENABLED=true
RQLITE_CDC_OUTPUT=/etc/zenbpm/rqlite-cdc-output.json
```

Use an absolute path and make the file and any referenced TLS files available
on every voting node. A non-empty `service_id` in this file takes precedence
over `cluster.cdc.serviceId` and `RQLITE_CDC_SERVICE_ID`. This preserves the
advanced output file as the authoritative source for its delivery settings. If
the file omits `service_id`, configure the base identifier through
`cluster.cdc.serviceId` or `RQLITE_CDC_SERVICE_ID` instead.

Keep the effective base identifier (`serviceId` or advanced `service_id`)
identical on all replicas and stable across restarts, leader changes, scaling,
node replacement, and endpoint cutovers while the same Raft history continues.
Choose a new unique base value before starting with fresh storage, cloning an
environment, or resetting or rewinding Raft history. Those operations can reuse
Raft indexes; retaining the old identifier could make an existing consumer
ledger mistake new changes for duplicates.

CDC batches may be delivered more than once. Consumers should acknowledge only
durably stored events and deduplicate each transaction group by
`(service_id, payload.index)`. If a consumer splits `events` into separate
records, it must add the event's array position to that key.

ZenBPM exports the persistent queue length, endpoint retry count, and confirmed
high-water mark for every CDC-enabled partition replica through
`GET /system/metrics`. See the [observability reference](observability.md#partition--rqlite)
for metric names and the stalled-delivery alert.

---

### Persistence Configuration: `Persistence`

Configuration for caching and storage.

| Field                | Type        | Env Variable                             | Default     | Description                                   |
|----------------------|-------------|------------------------------------------|-------------|-----------------------------------------------|
| `instanceHistoryTTL` | types.TTL   | `PERSISTENCE_INSTANCE_HISTORY_TTL`       | `0`         | TTL for finished process instances            |
| `procDefCacheTTL`    | types.TTL   | `PERSISTENCE_PROC_DEF_CACHE_TTL_SECONDS` | `24h`       | TTL for cached process definitions            |
| `procDefCacheSize`   | int         | `PERSISTENCE_PROC_DEF_CACHE_SIZE`        | `200`       | Max number of cached process definitions      |
| `decDefCacheTTL`     | types.TTL   | `PERSISTENCE_DEC_DEF_CACHE_TTL_SECONDS`  | `24h`       | TTL for cached dmn resource definitions       |
| `decDefCacheSize`    | int         | `PERSISTENCE_DEC_DEF_CACHE_SIZE`         | `200`       | Max number of cached dmn resource definitions |
| `rqlite`             | `*RqLite`   | —                                        | —           | Configuration for embedded RQLite database    |
| `migration`          | `Migration` | —                                        | —           | Configuration for SQL migration               |

#### SQL Migration Configuration: `Migration`

Configuration for caching and storage.

| Field | Type      | Env Variable                | Default                   | Description                               |
|-------|-----------|-----------------------------|---------------------------|-------------------------------------------|
| `dir` | `string`  | `PERSISTENCE_MIGRATION_DIR` | `internal/sql/migrations` | Configuration for SQL migration directory |

---

## Tracing Configuration: `Tracing`

Distributed tracing settings using OpenTelemetry.

| Field            | Type     | Env Variable                  | Default  | Description                                       |
|------------------|----------|-------------------------------|----------|---------------------------------------------------|
| `enabled`        | bool     | `TRACING_ENABLED`             | `false`  | Enable or disable tracing                         |
| `name`           | string   | `TRACING_APP_NAME`            | `ZenBPM` | Application name for tracing                      |
| `transferHeaders`| []string | `TRACING_TRANSFER_HEADERS`    | —        | HTTP headers to propagate through trace context   |
| `endpoint`       | string   | `OTEL_EXPORTER_OTLP_ENDPOINT` | —        | OTLP exporter endpoint (e.g., for Jaeger/Tempo)   |
| `samplerRatio`   | float64  | `TRACING_SAMPLER_RATIO`       | `1.0`    | Fraction of new traces sampled (0.0 - 1.0); child spans follow their parent's sampling decision |

---

## Error Tracking: GlitchTip

ZenBPM reports recovered panics and explicitly marked invariant violations to GlitchTip. Error tracking uses environment variables rather than the application configuration file so it can initialize before configuration parsing.

| Env Variable         | Default                 | Description                                                                         |
|----------------------|-------------------------|-------------------------------------------------------------------------------------|
| `SENTRY_ENABLED`     | `true`                  | Master switch for error reporting. Set to `false` to disable it even when DSN is set. |
| `SENTRY_DSN`         | —                       | GlitchTip project DSN. Error reporting is disabled when it is unset or empty.       |
| `SENTRY_ENVIRONMENT` | Current `PROFILE` value | Deployment environment attached to each event.                                      |

The application build version is attached as the GlitchTip release. Sentry performance tracing and automatic log forwarding are disabled because ZenBPM uses OpenTelemetry for tracing and reports only actionable unexpected errors.

---

## Example YAML Configuration

```yaml
name: zenbpm
httpServer:
  context: /
  addr: :8080
  maxRequestBodyBytes: 10485760
grpcServer:
  addr: :9090
cluster:
  addr: localhost:8090
  adv: localhost:8090
  persistence:
    migration:
        dir: internal/sql/migrations
  raft:
    dir: node-1
    bootstrapExpect: 1
    bootstrapExpectTimeout: 30s
    joinAttempts: 5
    joinAddresses: 
      - localhost:8090
  nodeId: node-1
tracing:
  enabled: true
  endpoint: localhost:4318
  name: ZenBPM
```
