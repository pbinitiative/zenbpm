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
| `persistence`| `Persistence` | —                         | —          | Persistence and caching configuration                   |

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

### Persistence Configuration: `Persistence`

Configuration for caching and storage.

| Field                | Type        | Env Variable                             | Default     | Description                                   |
|----------------------|-------------|------------------------------------------|-------------|-----------------------------------------------|
| `instanceHistoryTTL` | types.TTL   | `PERSISTENCE_INSTANCE_HISTORY_TTL`       | `0`         | TTL for finished process instances            |
| `procDefCacheTTL`    | types.TTL   | `PERSISTENCE_PROC_DEF_CACHE_TTL_SECONDS` | `24h`       | TTL for cached process definitions            |
| `procDefCacheSize`   | int         | `PERSISTENCE_PROC_DEF_CACHE_SIZE`        | `200`       | Max number of cached process definitions      |
| `decDefCacheTTL`     | types.TTL   | `PERSISTENCE_DEC_DEF_CACHE_TTL_SECONDS`  | `24h`       | TTL for cached dmn resource definitions       |
| `decDefCacheSize`    | int         | `PERSISTENCE_DEC_DEF_CACHE_SIZE`         | `200`       | Max number of cached dmn resource definitions |
| `cdcEnabled`         | bool        | `RQLITE_CDC_ENABLED`                     | `false`     | Enables RqLite CDC                            |
| `cdc`                | string      | `RQLITE_CDC_CONFIG`                      | —           | RqLite CDC endpoint, `stdout`, or JSON configuration file |
| `rqlite`             | `*RqLite`   | —                                        | —           | Configuration for embedded RQLite database    |
| `migration`          | `Migration` | —                                        | —           | Configuration for SQL migration               |

#### RqLite Change Data Capture

CDC is disabled by default. Enable it on every voting RqLite partition node by
setting `cluster.persistence.cdcEnabled` (or `RQLITE_CDC_ENABLED`) to `true`
and providing `cluster.persistence.cdc` (or `RQLITE_CDC_CONFIG`). The `cdc`
value accepts an HTTP(S) endpoint, `stdout` for local debugging, or a path to
an RqLite CDC JSON configuration file.

```yaml
cluster:
  persistence:
    cdcEnabled: true
    cdc: /etc/zenbpm/cdc.json
```

The equivalent environment configuration is:

```bash
RQLITE_CDC_ENABLED=true
RQLITE_CDC_CONFIG=/etc/zenbpm/cdc.json
```

Example `/etc/zenbpm/cdc.json`:

```json
{
  "endpoint": "https://consumer.example.com/rqlite/cdc",
  "service_id": "environment-a",
  "table_filter": "^(process_instance|job)$",
  "row_ids_only": false
}
```

The optional rqlite `service_id` identifies the ZenBPM data source. ZenBPM emits
it as `<service_id>-partition-N`, because each partition is a separate
RqLite/Raft cluster with its own event index. If `service_id` is omitted,
ZenBPM uses the backward-compatible default `zenbpm-partition-N`. A custom
`service_id` therefore requires a JSON configuration file; the endpoint-only
and `stdout` forms use the default.

All replicas of one logical ZenBPM cluster must use the same `service_id`.
Choose a globally unique value for every environment so a downstream consumer
can map it to the correct tenant and environment. Keep it unchanged across
restarts, failovers, scaling, and replacement of a node that rejoins the
existing cluster. Assign a new value when creating fresh storage, cloning an
environment, or restoring data in a way that resets or rewinds the Raft
history. The default is convenient for existing single-cluster installations
but is not globally unique.

CDC batches may be delivered more than once. Consumers should acknowledge only
durably stored events and deduplicate each transaction group by
`(service_id, payload.index)`. If a consumer splits `events` into separate
records, it must add the event's array position to that key.


---

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
