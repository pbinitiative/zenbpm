---
sidebar_position: 6
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

| Field     | Type   | Env Variable       | Default | Description                        |
|-----------|--------|--------------------|---------|------------------------------------|
| `context` | string | `REST_API_CONTEXT` | `/`     | Base context path for the API      |
| `addr`    | string | `REST_API_ADDR`    | `:8080` | Address the server binds to        |

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

| Field                    | Type          | Env Variable                          | Default            | Description                                              |
|--------------------------|---------------|----------------------------------------|---------------------|----------------------------------------------------------|
| `dir`                    | string        | `CLUSTER_RAFT_DIR`                    | `zen_bpm_node_data` | Path to local node data                                  |
| `nonVoter`               | bool          | `CLUSTER_RAFT_NON_VOTER`             | `false`             | Set node as non-voting member                            |
| `joinAttempts`           | int           | `CLUSTER_RAFT_JOIN_ATTEMPTS`         | `5`                 | Number of join attempts                                  |
| `joinInterval`           | duration      | `CLUSTER_RAFT_JOIN_INTERVAL`         | `2s`                | Time interval between join attempts                      |
| `joinAddresses`          | []string      | `CLUSTER_RAFT_JOIN_ADDRESSES`        | —                   | List of node addresses to join                           |
| `bootstrapExpect`        | int           | `CLUSTER_RAFT_BOOTSTRAP_EXPECT`      | `0`                 | Minimum nodes for bootstrap                              |
| `bootstrapExpectTimeout` | duration      | `CLUSTER_RAFT_EXPECT_BOOTSTRAP_TIMEOUT` | `10s`            | Max timeout for expected bootstrap nodes                 |

---

## Persistence Configuration: `Persistence`

Configuration for caching and storage.

| Field              | Type          | Env Variable                             | Default     | Description                                  |
|--------------------|---------------|-------------------------------------------|-------------|----------------------------------------------|
| `procDefCacheTTL`  | time.Duration | `PERSISTENCE_PROC_DEF_CACHE_TTL_SECONDS` | `24h`       | TTL for cached process definitions           |
| `procDefCacheSize` | int           | `PERSISTENCE_PROC_DEF_CACHE_SIZE`        | `200`       | Max number of cached process definitions     |
| `rqlite`           | `*RqLite`     | —                                         | —           | Configuration for embedded RQLite database   |

---

## Tracing Configuration: `Tracing`

Distributed tracing settings using OpenTelemetry.

| Field            | Type     | Env Variable                  | Default  | Description                                       |
|------------------|----------|-------------------------------|----------|---------------------------------------------------|
| `enabled`        | bool     | `TRACING_ENABLED`             | `false`  | Enable or disable tracing                         |
| `name`           | string   | `TRACING_APP_NAME`            | `ZenBPM` | Application name for tracing                      |
| `transferHeaders`| []string | `TRACING_TRANSFER_HEADERS`    | —        | HTTP headers to propagate through trace context   |
| `endpoint`       | string   | `OTEL_EXPORTER_OTLP_ENDPOINT` | —        | OTLP exporter endpoint (e.g., for Jaeger/Tempo)   |

---

## Example YAML Configuration

```yaml
name: zenbpm
httpServer:
  context: /
  addr: :8080
grpcServer:
  addr: :9090
cluster:
  addr: localhost:8090
  adv: localhost:8090
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
