# Minimal Local CDC to PostgreSQL

The stack contains only two long-running services:

- PostgreSQL.
- Redpanda Connect as the HTTP receiver, transformation layer, and PostgreSQL sink.

A Redpanda broker, topic, Console, and separate logger are not needed. PostgreSQL initializes the `reporting` schema from `postgres-init.sql`. Redpanda Connect applies incoming INSERT and UPDATE events directly to the reporting tables. DELETE events are acknowledged and discarded, so existing reporting rows are retained.

The `reporting.cdc_event` table keeps the applied INSERT and UPDATE events and deduplicates repeated deliveries with the `(service_id, raft_index, event_position)` primary key. DELETE events do not reach PostgreSQL through Redpanda Connect; the PostgreSQL function also ignores them if called directly, without changing reporting rows or writing them to the event ledger. Other events for tables outside the supplied DDL are retained with `applied = false` and a `skip_reason` instead of blocking the CDC stream.

The reporting tables intentionally do not enforce foreign keys. CDC can start
without an initial snapshot or receive related rows from different partitions,
so referenced rows are not guaranteed to be present when an event arrives.
Reference columns and their indexes are retained for reporting joins.

This stack is not secured and is intended for local development only. Published ports are available only through `127.0.0.1`, and the PostgreSQL credentials are intentionally simple.

## Start

Run the following command from the repository root:

```bash
docker-compose -f .dev/docker-compose.yml up -d
```

PostgreSQL stores its data in `tmpfs`. The database is intentionally ephemeral:
stopping or recreating the PostgreSQL container removes all data, and the next
start initializes a clean `reporting` schema from `postgres-init.sql`.

Configure the development CDC output in `conf/zenbpm/conf-dev.yaml` as follows:

```yaml
cluster:
  cdc:
    enabled: true
    output: http://localhost:4195/cdc
    serviceId: zenbpm-local-development
```

Start ZenBPM:

```bash
make run
```

Redpanda Connect returns a `200` response to ZenBPM only after all events have been written successfully to PostgreSQL. If an error occurs, the envelope is not acknowledged and rqlite may deliver it again; the SQL write is therefore idempotent.

CDC is not an initial snapshot. Rows that already exist in SQLite before CDC is
enabled are not backfilled automatically; this stack projects only changes
emitted after CDC starts.

## Inspect Data and Service Logs

Redpanda Connect service logs contain lifecycle and processing or delivery error
messages. This pipeline does not add a per-event CDC payload log:

```bash
docker-compose -f .dev/docker-compose.yml logs -f redpanda-connect
```

Current reporting rows in PostgreSQL:

```bash
docker-compose -f .dev/docker-compose.yml exec postgres \
  psql -U zenbpm -d zenbpm_cdc -c \
  'SELECT key, bpmn_process_id, version FROM reporting.process_definition ORDER BY key;'
```

CDC application status and skipped events:

```bash
docker-compose -f .dev/docker-compose.yml exec postgres \
  psql -U zenbpm -d zenbpm_cdc -c \
  'SELECT service_id, raft_index, event_position, operation, table_name, applied, skip_reason FROM reporting.cdc_event ORDER BY service_id, raft_index, event_position;'
```

For INSERT and UPDATE events, the `event_data` column in `reporting.cdc_event`
contains the source event JSON. Use that ledger when payload-level inspection is
needed. It may contain process data or other sensitive information, so restrict
database access and avoid copying it into logs.

The Connect readiness endpoint is available at `http://localhost:4195/ready`. PostgreSQL is available at `localhost:5445`, using the `zenbpm_cdc` database and `zenbpm` for both the username and password.

Quick test without ZenBPM:

```bash
curl -i \
  -H 'Content-Type: application/json' \
  --data '{"service_id":"manual-partition-1","node_id":"manual-node","payload":[{"index":42,"commit_timestamp":1787313900000,"events":[{"op":"INSERT","table":"process_definition","new_row_id":7,"after":{"key":7,"version":1,"bpmn_process_id":"invoice","bpmn_data":"<xml/>","bpmn_checksum":"YWJj","bpmn_process_name":"Invoice"}}]}]}' \
  http://localhost:4195/cdc
```

## Stop

Stop the stack and discard the ephemeral database:

```bash
docker-compose -f .dev/docker-compose.yml down
```

No `-v` flag is needed because this stack does not use a PostgreSQL data volume.
