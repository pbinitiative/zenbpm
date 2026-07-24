# Load tests (k6)

API-level throughput / latency tests for zenbpm, plus host resource monitoring.

## Prerequisites

- [k6](https://k6.io/) — `brew install k6`
- A running zenbpm node — `make run` (serves REST on `:8080`)
- Optional: the monitoring stack — `make start-monitoring`

## Run

```sh
# Defaults: 20 VUs for 1m against http://localhost:8085/v1
# Use BASE_URL=http://localhost:8080/v1 when testing a node started with make run.
make load-test

# Used for the monitoring report to compare against baseline
make load-test-prom VUS=50 GRACEFUL_STOP=10m POLL_INTERVAL_MS=100  K6_WEB_DASHBOARD_EXPORT=reports/load-test-$(date +%Y%m%d-%H%M%S).html K6_WEB_DASHBOARD=true LOAD_TEST_SCRIPT=invoice_approval_longest_flow.js DURATION=5m
```

## Test types

| Test | Script | Make targets | Description |
| --- | --- | --- | --- |
| Instance lifecycle | `instance_lifecycle.js` | `make load-test`, `make load-test-prom` | Baseline REST lifecycle test. Each iteration deploys a simple service-task process once in `setup()`, then creates one process instance, finds its active job, completes it, and verifies that the process instance reaches `completed`. Use this for general throughput and latency baselines. |
| Worst-case invoice approval | `invoice_approval_longest_flow.js` | `make load-test-invoice`, `make load-test-invoice-prom` | Heavier end-to-end invoice scenario based on acceptance Scenario 11. Each iteration drives the parent invoice liquidation process, child invoice check process, DMN evaluations, approval path, and both payment branches. A successful iteration completes 14 jobs and verifies that both child and parent instances are `completed`. |

## Configuration

For Make targets, set these as Make variables, for example
`make load-test RATE=10 DURATION=5m`. When running a script directly, pass script
fields as k6 environment variables with `k6 run -e FIELD=value`; k6 runtime fields
are read from the process environment.

| Field | Applies to | Description | Default |
| --- | --- | --- | --- |
| `LOAD_TEST_SCRIPT` | Make targets | Script file to execute from `test/load/k6`. Use `invoice_approval_longest_flow.js` for the invoice scenario. | `instance_lifecycle.js` |
| `TEST_ID` | Make targets | Value for the k6 `testid` tag, used by Prometheus/Grafana to identify a run. | Current timestamp as `YYYYMMDD-HHMMSS` |
| `BASE_URL` | Both scripts | ZenBPM REST API base URL. Use `http://localhost:8080/v1` for a node started with `make run`. | `http://localhost:8085/v1` |
| `VUS` | Both scripts, constant-VU mode | Number of virtual users when `RATE` is unset and ramping is disabled. | `20` for `instance_lifecycle.js`; `1` for `invoice_approval_longest_flow.js` |
| `DURATION` | Both scripts | Scenario duration for constant-VU and constant-arrival-rate modes. | `1m` |
| `GRACEFUL_STOP` | Both scripts | Time k6 waits for active iterations to finish after the scenario duration expires. | `2m` |
| `RATE` | Both scripts, open model | Completed process lifecycles started per second. When greater than `0`, selects `constant-arrival-rate`. | `0` |
| `PRE_ALLOCATED_VUS` | Both scripts, open model | Initial VU pool for `constant-arrival-rate`. Must cover expected in-flight lifecycles. | `max(ceil(RATE * EXPECTED_LIFECYCLE_SECONDS * VU_HEADROOM), 20)` |
| `MAX_VUS` | Both scripts, open model | Maximum VU pool k6 may allocate for `constant-arrival-rate`. | `max(ceil(RATE * EXPECTED_LIFECYCLE_SECONDS * VU_HEADROOM), ceil(RATE * 4), 100)` for `instance_lifecycle.js`; `max(ceil(RATE * 20), ceil(RATE * EXPECTED_LIFECYCLE_SECONDS * VU_HEADROOM), 100)` for `invoice_approval_longest_flow.js` |
| `EXPECTED_LIFECYCLE_SECONDS` | Both scripts, open-model validation | Expected lifecycle duration used to size and validate open-model VU capacity. | `1` for `instance_lifecycle.js`; `10` for `invoice_approval_longest_flow.js` |
| `VU_HEADROOM` | Both scripts, open-model validation | Multiplier added to expected in-flight lifecycles when calculating required VUs. | `1.2` |
| `VALIDATE_LOAD_SHAPE` | Both scripts, open-model validation | Fails early when `PRE_ALLOCATED_VUS`, `MAX_VUS`, or `GRACEFUL_STOP` are too low for the requested `RATE`. Set to `false` for intentional saturation runs. | `true` |
| `PARALLEL_JOB_COMPLETION` | `invoice_approval_longest_flow.js` | Completes simultaneously-active jobs in the same process instance with `http.batch`. Keep `false` for baseline lifecycle latency; set `true` to intentionally stress per-instance lock contention on parallel BPMN branches. | `false` |
| `RAMP_VUS_STEPS` | Both scripts, ramping-VU mode | Number of ramp stages. Ramping mode is selected when `RATE <= 0`, this is greater than `0`, and `RAMP_VUS_STEP_SIZE > 0`. | `0` |
| `RAMP_VUS_STEP_SIZE` | Both scripts, ramping-VU mode | VUs added per ramp stage. | `0` |
| `RAMP_VUS_START` | Both scripts, ramping-VU mode | Starting VU count for the ramping scenario. | `0` |
| `RAMP_VUS_STEP_DURATION` | Both scripts, ramping-VU mode | Duration of each ramp stage. | `1m` |
| `RAMP_VUS_RAMP_DOWN_DURATION` | Both scripts, ramping-VU mode | Final ramp-down duration, and `gracefulRampDown` for ramping mode. | `30s` |
| `POLL_INTERVAL_MS` | Both scripts | Sleep interval between polling attempts. | `50` for `instance_lifecycle.js`; `100` for `invoice_approval_longest_flow.js` |
| `JOB_TIMEOUT_MS` | Both scripts | Maximum time to wait for an active job to appear. | `5000` for `instance_lifecycle.js`; `30000` for `invoice_approval_longest_flow.js` |
| `COMPLETION_TIMEOUT_MS` | Both scripts | Maximum time to wait for process completion after work is driven forward. | `5000` for `instance_lifecycle.js`; `60000` for `invoice_approval_longest_flow.js` |
| `PROCESS_INSTANCE_HISTORY_TTL` | Both scripts | Value sent as `historyTimeToLive` when creating process instances. | `5m` |
| `BPMN_PATH` | `instance_lifecycle.js` only | BPMN file deployed by `setup()`. The path is resolved relative to `test/load/k6`. | `../../../pkg/bpmn/test-cases/simple_task.bpmn` |
| `SETUP_TIMEOUT` | `invoice_approval_longest_flow.js` only | k6 setup timeout for deploying invoice BPMN/DMN resources. | `2m` |
| `TEARDOWN_TIMEOUT` | `invoice_approval_longest_flow.js` only | k6 teardown timeout. | `2m` |
| `K6_WEB_DASHBOARD` | k6 runtime | Enables k6's built-in web dashboard when set to `true`. | k6 default, disabled unless set |
| `K6_WEB_DASHBOARD_EXPORT` | k6 runtime | Writes the k6 web dashboard report to the given path. | unset |
| `K6_PROMETHEUS_RW_SERVER_URL` | `load-test-prom` | Prometheus remote-write endpoint used by the Make target. Override by running k6 directly or editing the command. | `http://localhost:9101/api/v1/write` |
| `K6_PROMETHEUS_RW_TREND_STATS` | `load-test-prom` | Trend aggregations exported to Prometheus. | `p(95),p(99),avg,max` |
| `K6_PROMETHEUS_RW_STALE_MARKERS` | `load-test-prom` | Enables stale markers for Prometheus remote-write series. | `true` |

Polling defaults to every 50 ms, with 5 s timeouts for finding the job and observing
completion. Override these using `POLL_INTERVAL_MS`, `JOB_TIMEOUT_MS`, and
`COMPLETION_TIMEOUT_MS`.

The embedded rqlite default raises the WAL-size snapshot threshold to 50 MiB and the
snapshot interval to 1 minute (`raftSnapThresholdWALSize`, `raftSnapInterval` in
`internal/cluster/partition/config.go`). Smaller values make WAL-size snapshots fire more
often under sustained write load, where they race active readers/writers and log
`database checkpoint busy`; that message is retryable but noisy and can add checkpoint
churn during benchmarks.

> int64 snowflake keys exceed JS's safe integer range, so the script extracts keys as
> strings via regex rather than `JSON.parse` — see the comment at the top of the script.

## Monitoring CPU / memory / disk

`make start-monitoring` now also starts **node_exporter** (host port `9110`) and Prometheus
scrapes it as job `node`. Open Grafana at <http://localhost:9100> and import dashboard
**1860** ("Node Exporter Full") for CPU / RAM / disk usage + I/O. The app's own engine
metrics (goroutines, heap, GC) are already provisioned under the `zenbpm` folder.

- **Linux:** node_exporter reports the real host.
- **macOS (Docker Desktop):** the container reports the Docker VM, *not* the Mac. For true
  Mac-host numbers, run node_exporter natively instead:
  `brew install node_exporter && node_exporter --web.listen-address=:9110`.

### One Grafana timeline

Stream k6's own metrics (throughput, p95/p99, error rate) into the same Prometheus so they
line up with resource metrics:

```sh
make load-test-prom   # start-monitoring enables Prometheus remote-write
```

`start-monitoring` already runs Prometheus with `--web.enable-remote-write-receiver`, so
k6 can push directly. The provisioned **Load Test Report** dashboard combines k6 VUs,
throughput, p95/p99, request rates, correctness, ZenBPM CPU, RSS, Go heap, goroutines,
and open FDs. Open Grafana at <http://localhost:9100> and select the dashboard from the
`zenbpm` folder.

Every Makefile run gets a timestamp-based `testid`. Set a descriptive ID explicitly so
the run is easy to find later:
