// Instance-lifecycle load test for zenbpm.
//
// Per iteration each virtual user drives one full process lifecycle over REST:
//   1. POST /process-instances            -> create an instance
//   2. GET  /jobs?processInstanceKey=...   -> find the instance's active job
//   3. POST /jobs/{jobKey}/complete        -> complete it, driving the instance to its end event
//   4. GET  /process-instances/{key}        -> verify that the instance is completed
//
// The process definition (a single service task) is deployed once in setup().
//
// IMPORTANT: zenbpm keys are int64 snowflake IDs (e.g. 1991421078827696128).
// They exceed JS's Number.MAX_SAFE_INTEGER (2^53), so JSON.parse() silently
// rounds them and the follow-up calls 404. We therefore extract keys as STRINGS
// straight from the raw response body with a regex, and splice them back into
// request bodies as raw (unquoted) JSON numbers to keep full precision.
//
// Run:
//   make load-test                       # against http://localhost:8085/v1
//   k6 run -e VUS=50 -e DURATION=2m instance_lifecycle.js
//   make load-test-prom                  # stream metrics into Prometheus/Grafana
//
// Tunables (env): BASE_URL, VUS, RATE, PRE_ALLOCATED_VUS, MAX_VUS,
// DURATION, BPMN_PATH, EXPECTED_LIFECYCLE_SECONDS, VU_HEADROOM,
// VALIDATE_LOAD_SHAPE, RAMP_VUS_STEPS, RAMP_VUS_STEP_SIZE,
// RAMP_VUS_STEP_DURATION, PROCESS_INSTANCE_HISTORY_TTL.

import http from 'k6/http';
import { check, fail, sleep } from 'k6';
import { Trend, Counter, Rate } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080/v1';
const BPMN_PATH = __ENV.BPMN_PATH || '../../../pkg/bpmn/test-cases/simple_task.bpmn';
const POLL_INTERVAL_MS = Number(__ENV.POLL_INTERVAL_MS || 50);
const JOB_TIMEOUT_MS = Number(__ENV.JOB_TIMEOUT_MS || 5000);
const COMPLETION_TIMEOUT_MS = Number(__ENV.COMPLETION_TIMEOUT_MS || 5000);
const PROCESS_INSTANCE_HISTORY_TTL = __ENV.PROCESS_INSTANCE_HISTORY_TTL || '5m';
const RATE = Number(__ENV.RATE || 0);
const RAMP_VUS_STEPS = Number(__ENV.RAMP_VUS_STEPS || 0);
const RAMP_VUS_STEP_SIZE = Number(__ENV.RAMP_VUS_STEP_SIZE || 0);
const RAMP_VUS_START = Number(__ENV.RAMP_VUS_START || 0);
const RAMP_VUS_STEP_DURATION = __ENV.RAMP_VUS_STEP_DURATION || '1m';
const RAMP_VUS_RAMP_DOWN_DURATION = __ENV.RAMP_VUS_RAMP_DOWN_DURATION || '30s';
const EXPECTED_LIFECYCLE_SECONDS = Number(__ENV.EXPECTED_LIFECYCLE_SECONDS || 1);
const VU_HEADROOM = Number(__ENV.VU_HEADROOM || 1.2);
const VALIDATE_LOAD_SHAPE = (__ENV.VALIDATE_LOAD_SHAPE || 'true').toLowerCase() !== 'false';
const REQUIRED_OPEN_MODEL_VUS = Math.ceil(RATE * EXPECTED_LIFECYCLE_SECONDS * VU_HEADROOM);
const DEFAULT_PRE_ALLOCATED_VUS = Math.max(REQUIRED_OPEN_MODEL_VUS, 20);
const DEFAULT_MAX_VUS = Math.max(REQUIRED_OPEN_MODEL_VUS, Math.ceil(RATE * 4), 100);
const SCENARIO_DURATION = __ENV.DURATION || '1m';
const SCENARIO_GRACEFUL_STOP = __ENV.GRACEFUL_STOP || '2m';
const PRE_ALLOCATED_VUS = Number(__ENV.PRE_ALLOCATED_VUS || DEFAULT_PRE_ALLOCATED_VUS);
const MAX_VUS = Number(__ENV.MAX_VUS || DEFAULT_MAX_VUS);
const RAMP_VUS_ENABLED = RATE <= 0 && RAMP_VUS_STEPS > 0 && RAMP_VUS_STEP_SIZE > 0;

// RATE switches the test to an open workload model and means complete process
// lifecycles started per second, not individual HTTP requests per second.
// RAMP_VUS_* switches to a closed workload that increases virtual users over
// time. Without RATE or RAMP_VUS_* the original constant-VU mode remains.
function buildRampVusStages() {
  const stages = [];
  for (let i = 1; i <= RAMP_VUS_STEPS; i++) {
    stages.push({
      duration: RAMP_VUS_STEP_DURATION,
      target: RAMP_VUS_START + (i * RAMP_VUS_STEP_SIZE),
    });
  }
  stages.push({ duration: RAMP_VUS_RAMP_DOWN_DURATION, target: 0 });
  return stages;
}

const lifecycleScenario = RATE > 0
  ? {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: SCENARIO_DURATION,
      gracefulStop: SCENARIO_GRACEFUL_STOP,
      preAllocatedVUs: PRE_ALLOCATED_VUS,
      maxVUs: MAX_VUS,
    }
  : RAMP_VUS_ENABLED
    ? {
        executor: 'ramping-vus',
        startVUs: RAMP_VUS_START,
        stages: buildRampVusStages(),
        gracefulRampDown: RAMP_VUS_RAMP_DOWN_DURATION,
      }
  : {
      executor: 'constant-vus',
      vus: Number(__ENV.VUS || 20),
      duration: SCENARIO_DURATION,
      gracefulStop: SCENARIO_GRACEFUL_STOP,
    };

// Read the BPMN file once at init time (path is relative to this script).
const bpmnFile = open(BPMN_PATH);

export const options = {
  // Exclude the raw URL because snowflake IDs in dynamic paths would create
  // one Prometheus time series per process/job. Stable request names are set below.
  systemTags: ['status', 'method', 'name', 'check', 'error_code', 'scenario', 'expected_response'],
  scenarios: {
    lifecycle: lifecycleScenario,
  },
  // Starting SLA targets — tune to your hardware/topology once you have a baseline.
  thresholds: {
    http_req_failed: ['rate<0.01'],
    dropped_iterations: ['count==0'],
    lifecycle_success: ['rate==1'],
    create_instance_duration: ['p(95)<200', 'p(99)<500'],
    lifecycle_duration: ['p(95)<=10000', 'p(99)<=10000'],
  },
};

const createTrend = new Trend('create_instance_duration', true);
const completeTrend = new Trend('complete_job_duration', true);
const completionObservedTrend = new Trend('completion_observed_duration', true);
const lifecycleTrend = new Trend('lifecycle_duration', true);
const created = new Counter('instances_created');
const completionAccepted = new Counter('job_completions_accepted');
const completed = new Counter('instances_completed');
const jobTimeouts = new Counter('job_lookup_timeouts');
const completionTimeouts = new Counter('instance_completion_timeouts');
const terminalFailures = new Counter('instances_terminal_failure');
const lifecycleSuccess = new Rate('lifecycle_success');
const bigIdRegexes = Object.create(null);
const regexpSpecialChars = /[.*+?^${}()|[\]\\]/g;

function escapeRegExpLiteral(value) {
  return String(value).replace(regexpSpecialChars, '\\$&');
}

// Extract a full-precision int64 id for `field` from a raw JSON body, as a string.
// Returns null if not present.
function bigId(body, field) {
  if (body == null) {
    return null;
  }

  const cacheKey = String(field);
  let regex = bigIdRegexes[cacheKey];
  if (!regex) {
    regex = new RegExp('"' + escapeRegExpLiteral(cacheKey) + '"\\s*:\\s*(\\d+)');
    bigIdRegexes[cacheKey] = regex;
  }

  const m = String(body).match(regex);
  return m ? m[1] : null;
}

const JSON_HEADERS = { headers: { 'Content-Type': 'application/json' } };

function parseDurationMs(value) {
  const input = String(value || '').trim();
  if (input === '') {
    return null;
  }

  const multipliers = {
    ms: 1,
    s: 1000,
    m: 60 * 1000,
    h: 60 * 60 * 1000,
  };
  const pattern = /(\d+(?:\.\d+)?)(ms|s|m|h)/g;
  let cursor = 0;
  let total = 0;
  let match = pattern.exec(input);
  while (match !== null) {
    if (match.index !== cursor) {
      return null;
    }
    total += Number(match[1]) * multipliers[match[2]];
    cursor += match[0].length;
    match = pattern.exec(input);
  }
  return cursor === input.length ? total : null;
}

function formatSeconds(ms) {
  return `${Math.ceil(ms / 1000)}s`;
}

function validateOpenModelLoadShape() {
  if (RATE <= 0 || !VALIDATE_LOAD_SHAPE) {
    return;
  }

  const errors = [];
  if (PRE_ALLOCATED_VUS < REQUIRED_OPEN_MODEL_VUS) {
    errors.push(
      `PRE_ALLOCATED_VUS=${PRE_ALLOCATED_VUS} is below RATE * EXPECTED_LIFECYCLE_SECONDS * VU_HEADROOM = ${REQUIRED_OPEN_MODEL_VUS}`,
    );
  }
  if (MAX_VUS < REQUIRED_OPEN_MODEL_VUS) {
    errors.push(
      `MAX_VUS=${MAX_VUS} is below RATE * EXPECTED_LIFECYCLE_SECONDS * VU_HEADROOM = ${REQUIRED_OPEN_MODEL_VUS}`,
    );
  }

  const gracefulStopMs = parseDurationMs(SCENARIO_GRACEFUL_STOP);
  const minimumGracefulStopMs = Math.max(JOB_TIMEOUT_MS + COMPLETION_TIMEOUT_MS, EXPECTED_LIFECYCLE_SECONDS * 1000);
  if (gracefulStopMs !== null && gracefulStopMs < minimumGracefulStopMs) {
    errors.push(
      `GRACEFUL_STOP=${SCENARIO_GRACEFUL_STOP} is below ${formatSeconds(minimumGracefulStopMs)}; interrupted VUs can leave active instances`,
    );
  }

  if (errors.length > 0) {
    fail([
      'lifecycle load shape is under-provisioned:',
      ...errors.map((err) => `- ${err}`),
      'Lower RATE, raise PRE_ALLOCATED_VUS/MAX_VUS, increase GRACEFUL_STOP, or set VALIDATE_LOAD_SHAPE=false for an intentional saturation run.',
    ].join('\n'));
  }
}

function waitForJob(instanceKey) {
  const deadline = Date.now() + JOB_TIMEOUT_MS;
  do {
    const res = http.get(
      `${BASE_URL}/jobs?processInstanceKey=${instanceKey}&state=active&size=1`,
      { tags: { name: 'GET /jobs', op: 'poll_active_job' } },
    );
    const ok = check(res, { 'jobs response 200': (r) => r.status === 200 });
    if (ok) {
      const jobKey = bigId(res.body, 'key');
      if (jobKey) {
        return jobKey;
      }
    }
    sleep(POLL_INTERVAL_MS / 1000);
  } while (Date.now() < deadline);

  jobTimeouts.add(1);
  return null;
}

function waitForTerminalState(instanceKey) {
  const deadline = Date.now() + COMPLETION_TIMEOUT_MS;
  do {
    const res = http.get(`${BASE_URL}/process-instances/${instanceKey}`, {
      tags: { name: 'GET /process-instances/:key', op: 'poll_instance_state' },
    });
    const ok = check(res, { 'instance response 200': (r) => r.status === 200 });
    if (ok) {
      // Parsing the response is safe here: only `state` is used. Snowflake IDs
      // from the parsed object are deliberately ignored to avoid JS precision loss.
      const state = res.json('state');
      if (state === 'completed' || state === 'failed' || state === 'terminated') {
        return state;
      }
    }
    sleep(POLL_INTERVAL_MS / 1000);
  } while (Date.now() < deadline);

  completionTimeouts.add(1);
  return null;
}

export function setup() {
  validateOpenModelLoadShape();

  const res = http.post(`${BASE_URL}/process-definitions`, {
    resource: http.file(bpmnFile, 'simple_task.bpmn', 'application/xml'),
  }, { tags: { name: 'POST /process-definitions', op: 'deploy_process' } });
  if (!check(res, { 'deploy 2xx': (r) => r.status === 200 || r.status === 201 })) {
    fail(`deploy failed: ${res.status} ${res.body}`);
  }
  const definitionKey = bigId(res.body, 'processDefinitionKey');
  if (!definitionKey) {
    fail(`could not read processDefinitionKey from deploy response: ${res.body}`);
  }
  return { definitionKey };
}

export default function (data) {
  const started = Date.now();

  // 1. Create an instance. definitionKey is spliced in raw to preserve int64 precision.
  const createBody =
    `{"processDefinitionKey": ${data.definitionKey}, "historyTimeToLive": ${JSON.stringify(PROCESS_INSTANCE_HISTORY_TTL)}, "variables": {"name": "load-test"}}`;
  const createRes = http.post(`${BASE_URL}/process-instances`, createBody, {
    ...JSON_HEADERS,
    tags: { name: 'POST /process-instances', op: 'create_instance' },
  });
  createTrend.add(createRes.timings.duration);
  if (!check(createRes, { 'create 201': (r) => r.status === 201 })) {
    lifecycleSuccess.add(false);
    return;
  }
  created.add(1);

  const instanceKey = bigId(createRes.body, 'key');
  if (!instanceKey) {
    check(instanceKey, { 'instance key returned': (key) => key !== null });
    lifecycleSuccess.add(false);
    return;
  }

  // 2. Wait for the instance's active job. This is normally available on the
  // first request, but polling makes delayed job creation an explicit result.
  const jobKey = waitForJob(instanceKey);
  if (!check(jobKey, { 'active job found': (key) => key !== null })) {
    lifecycleSuccess.add(false);
    return;
  }

  // 3. Complete the job -> the engine runs the instance to its end event.
  const completionStarted = Date.now();
  const compRes = http.post(
    `${BASE_URL}/jobs/${jobKey}/complete`,
    '{"variables": {"variable_name": "load-test"}}',
    { ...JSON_HEADERS, tags: { name: 'POST /jobs/:key/complete', op: 'complete_job' } },
  );
  completeTrend.add(compRes.timings.duration);
  if (!check(compRes, { 'complete 201': (r) => r.status === 201 })) {
    lifecycleSuccess.add(false);
    return;
  }
  completionAccepted.add(1);

  // 4. A successful job-completion response is not counted as a completed
  // process until the process-instance endpoint confirms the terminal state.
  const terminalState = waitForTerminalState(instanceKey);
  const completionObservedDuration = Date.now() - completionStarted;
  const isCompleted = terminalState === 'completed';
  check(terminalState, {
    'instance reached completed state': (state) => state === 'completed',
  });

  if (isCompleted) {
    completed.add(1);
    completionObservedTrend.add(completionObservedDuration);
    lifecycleTrend.add(Date.now() - started);
  } else if (terminalState === 'failed' || terminalState === 'terminated') {
    terminalFailures.add(1);
  }
  lifecycleSuccess.add(isCompleted);
}
