// Worst-case invoice-approval lifecycle based on acceptance Scenario 11.
//
// Every iteration executes one complete invoice lifecycle:
//   - parent invoice-liquidation process
//   - child invoice-check process with five parallel service tasks
//   - next-step and approver DMN evaluations
//   - approval path
//   - inclusive payment gateway with both transfer and offset branches
//
// A successful iteration requires exactly 14 completed jobs and both the child
// and parent process instances in state `completed`.

import http from 'k6/http';
import { check, fail, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080/v1';
const POLL_INTERVAL_MS = Number(__ENV.POLL_INTERVAL_MS || 100);
const JOB_TIMEOUT_MS = Number(__ENV.JOB_TIMEOUT_MS || 30000);
const COMPLETION_TIMEOUT_MS = Number(__ENV.COMPLETION_TIMEOUT_MS || 60000);
const PROCESS_INSTANCE_HISTORY_TTL = __ENV.PROCESS_INSTANCE_HISTORY_TTL || '5m';
const RATE = Number(__ENV.RATE || 0);
const RAMP_VUS_STEPS = Number(__ENV.RAMP_VUS_STEPS || 0);
const RAMP_VUS_STEP_SIZE = Number(__ENV.RAMP_VUS_STEP_SIZE || 0);
const RAMP_VUS_START = Number(__ENV.RAMP_VUS_START || 0);
const RAMP_VUS_STEP_DURATION = __ENV.RAMP_VUS_STEP_DURATION || '1m';
const RAMP_VUS_RAMP_DOWN_DURATION = __ENV.RAMP_VUS_RAMP_DOWN_DURATION || '30s';
const EXPECTED_LIFECYCLE_SECONDS = Number(__ENV.EXPECTED_LIFECYCLE_SECONDS || 10);
const VU_HEADROOM = Number(__ENV.VU_HEADROOM || 1.2);
const VALIDATE_LOAD_SHAPE = (__ENV.VALIDATE_LOAD_SHAPE || 'true').toLowerCase() !== 'false';
const PARALLEL_JOB_COMPLETION = (__ENV.PARALLEL_JOB_COMPLETION || 'false').toLowerCase() === 'true';
const MAIN_PROCESS_ID = 'LikvidaceDodavatelskeFaktury';
const CHILD_PROCESS_ID = 'KontrolaDodavatelskeFaktury';
const EXPECTED_JOBS = 14;

// A lifecycle can take roughly 10 seconds under load. Preallocate enough VUs
// for the requested arrival rate plus 20% headroom; on-demand VU allocation can
// otherwise cause dropped iterations even when maxVUs is not reached.
const REQUIRED_OPEN_MODEL_VUS = Math.ceil(RATE * EXPECTED_LIFECYCLE_SECONDS * VU_HEADROOM);
const DEFAULT_PRE_ALLOCATED_VUS = Math.max(REQUIRED_OPEN_MODEL_VUS, 20);
const DEFAULT_MAX_VUS = Math.max(Math.ceil(RATE * 20), REQUIRED_OPEN_MODEL_VUS, 100);
const SCENARIO_DURATION = __ENV.DURATION || '1m';
const SCENARIO_GRACEFUL_STOP = __ENV.GRACEFUL_STOP || '2m';
const PRE_ALLOCATED_VUS = Number(__ENV.PRE_ALLOCATED_VUS || DEFAULT_PRE_ALLOCATED_VUS);
const MAX_VUS = Number(__ENV.MAX_VUS || DEFAULT_MAX_VUS);
const RAMP_VUS_ENABLED = RATE <= 0 && RAMP_VUS_STEPS > 0 && RAMP_VUS_STEP_SIZE > 0;

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

const scenario = RATE > 0
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
        vus: Number(__ENV.VUS || 1),
        duration: SCENARIO_DURATION,
        gracefulStop: SCENARIO_GRACEFUL_STOP,
      };

const resources = {
  invoiceCheck: open('./resources/invoice_approval/invoice_check.bpmn'),
  invoiceLiquidation: open('./resources/invoice_approval/invoice_liquidation.bpmn'),
  invoiceNextSteps: open('./resources/invoice_approval/invoice_next_steps.dmn'),
  invoiceRejectionNotification: open('./resources/invoice_approval/invoice_rejection_notification.dmn'),
  invoiceApprovers: open('./resources/invoice_approval/invoice_approvers.dmn'),
};

export const options = {
  // Raw request URLs contain unique process/job snowflake IDs. Omitting the
  // `url` system tag prevents a separate Prometheus series for every ID.
  systemTags: ['status', 'method', 'name', 'check', 'error_code', 'scenario', 'expected_response'],
  scenarios: { invoice_worst_case: scenario },
  setupTimeout: __ENV.SETUP_TIMEOUT || '2m',
  teardownTimeout: __ENV.TEARDOWN_TIMEOUT || '2m',
  thresholds: {
    http_req_failed: ['rate<0.01'],
    dropped_iterations: ['count==0'],
    invoice_lifecycle_success: ['rate==1'],
    invoice_all_processes_completed: ['rate==1'],
  },
};

const JSON_PARAMS = { headers: { 'Content-Type': 'application/json' } };
const parentStarted = new Counter('invoice_parent_instances_started');
const parentCompleted = new Counter('invoice_parent_instances_completed');
const childCompleted = new Counter('invoice_child_instances_completed');
const jobsCompleted = new Counter('invoice_jobs_completed');
const lifecycleFailures = new Counter('invoice_lifecycle_failures');
const jobLookupTimeouts = new Counter('invoice_job_lookup_timeouts');
const stateTimeouts = new Counter('invoice_state_timeouts');
const lifecycleSuccess = new Rate('invoice_lifecycle_success');
const allProcessesCompleted = new Rate('invoice_all_processes_completed');
const createDuration = new Trend('invoice_create_duration', true);
const jobCompletionDuration = new Trend('invoice_job_completion_duration', true);
const childDuration = new Trend('invoice_child_duration', true);
const lifecycleDuration = new Trend('invoice_lifecycle_duration', true);

// JSON.parse rounds int64 snowflake identifiers. Quote known identifier fields
// in the raw JSON before parsing so they remain exact strings.
function parseJsonPreservingIds(body) {
  if (body == null) {
    throw new Error('cannot parse JSON response: response body is null or undefined');
  }

  const quoted = body.replace(
    /("(?:key|processDefinitionKey|processInstanceKey|parentProcessInstanceKey|elementInstanceKey|decisionInstanceKey|executionToken)"\s*:\s*)(-?\d+)/g,
    '$1"$2"',
  );
  return JSON.parse(quoted);
}

function deployBpmn(filename, content) {
  const response = http.post(`${BASE_URL}/process-definitions`, {
    resource: http.file(content, filename, 'application/xml'),
  }, { tags: { name: 'POST /process-definitions', op: 'deploy_process' } });
  const ok = check(response, {
    'invoice BPMN deployment 2xx': (r) => r.status === 200 || r.status === 201,
  });
  if (!ok) {
    fail(`BPMN deployment ${filename} failed: ${response.status} ${response.body}`);
  }
}

function deployDmn(filename, content) {
  const response = http.post(`${BASE_URL}/dmn-resource-definitions`, content, {
    headers: { 'Content-Type': 'application/xml' },
    tags: { name: 'POST /dmn-resource-definitions', op: 'deploy_dmn', resource: filename },
  });
  const ok = check(response, {
    'invoice DMN deployment 2xx': (r) => r.status === 200 || r.status === 201,
  });
  if (!ok) {
    fail(`DMN deployment ${filename} failed: ${response.status} ${response.body}`);
  }
}

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
  const minimumGracefulStopMs = Math.max(COMPLETION_TIMEOUT_MS, EXPECTED_LIFECYCLE_SECONDS * 1000);
  if (gracefulStopMs !== null && gracefulStopMs < minimumGracefulStopMs) {
    errors.push(
      `GRACEFUL_STOP=${SCENARIO_GRACEFUL_STOP} is below ${formatSeconds(minimumGracefulStopMs)}; interrupted VUs can leave active invoice processes`,
    );
  }

  if (errors.length > 0) {
    fail([
      'invoice load shape is under-provisioned:',
      ...errors.map((err) => `- ${err}`),
      'Lower RATE, raise PRE_ALLOCATED_VUS/MAX_VUS, increase GRACEFUL_STOP, or set VALIDATE_LOAD_SHAPE=false for an intentional saturation run.',
    ].join('\n'));
  }
}

export function setup() {
  validateOpenModelLoadShape();

  deployBpmn('invoice_check.bpmn', resources.invoiceCheck);
  deployBpmn('invoice_liquidation.bpmn', resources.invoiceLiquidation);
  deployDmn('invoice_next_steps.dmn', resources.invoiceNextSteps);
  deployDmn('invoice_rejection_notification.dmn', resources.invoiceRejectionNotification);
  deployDmn('invoice_approvers.dmn', resources.invoiceApprovers);

  return {
    runId: `${Date.now()}`,
    // Small margin protects the final audit against sub-second clock skew.
    runStartedAt: new Date(Date.now() - 1000).toISOString(),
  };
}

function failStage(stage) {
  lifecycleFailures.add(1, { stage });
}

function getInstanceJobs(instanceKey) {
  const response = http.get(
    `${BASE_URL}/process-instances/${instanceKey}/jobs?page=1&size=100`,
    { tags: { name: 'GET /process-instances/:key/jobs', op: 'poll_jobs' } },
  );
  if (!check(response, { 'invoice jobs response 200': (r) => r.status === 200 })) {
    return null;
  }
  try {
    return parseJsonPreservingIds(response.body).items || [];
  } catch (_) {
    return null;
  }
}

function waitForActiveJobs(instanceKey, elementIds) {
  const deadline = Date.now() + JOB_TIMEOUT_MS;
  do {
    const jobs = getInstanceJobs(instanceKey);
    if (jobs !== null) {
      const found = {};
      for (const job of jobs) {
        if (job.state === 'active' && elementIds.includes(job.elementId)) {
          found[job.elementId] = job;
        }
      }
      if (elementIds.every((elementId) => found[elementId])) {
        return found;
      }
    }
    sleep(POLL_INTERVAL_MS / 1000);
  } while (Date.now() < deadline);

  jobLookupTimeouts.add(1);
  return null;
}

function waitForChild(parentKey) {
  const deadline = Date.now() + JOB_TIMEOUT_MS;
  do {
    const response = http.get(
      `${BASE_URL}/process-instances/${parentKey}/child-processes?page=1&size=10`,
      { tags: { name: 'GET /process-instances/:key/child-processes', op: 'poll_child_process' } },
    );
    const ok = check(response, { 'invoice child response 200': (r) => r.status === 200 });
    if (ok) {
      try {
        const page = parseJsonPreservingIds(response.body);
        for (const partition of page.partitions || []) {
          for (const instance of partition.items || []) {
            if (instance.bpmnProcessId === CHILD_PROCESS_ID) {
              return instance.key;
            }
          }
        }
      } catch (_) {
        // Ignore transient malformed/empty responses and retry until the deadline.
      }
    }
    sleep(POLL_INTERVAL_MS / 1000);
  } while (Date.now() < deadline);

  jobLookupTimeouts.add(1);
  return null;
}

function waitForTerminalState(instanceKey, kind) {
  const deadline = Date.now() + COMPLETION_TIMEOUT_MS;
  do {
    const response = http.get(`${BASE_URL}/process-instances/${instanceKey}`, {
      tags: { name: 'GET /process-instances/:key', op: 'poll_process_state', process_kind: kind },
    });
    const ok = check(response, { 'invoice state response 200': (r) => r.status === 200 });
    if (ok) {
      try {
        const instance = parseJsonPreservingIds(response.body);
        if (instance.state === 'completed' || instance.state === 'failed' || instance.state === 'terminated') {
          return instance;
        }
      } catch (_) {
        // Ignore transient malformed/empty responses and retry until the deadline.
      }
    }
    sleep(POLL_INTERVAL_MS / 1000);
  } while (Date.now() < deadline);

  stateTimeouts.add(1, { process_kind: kind });
  return null;
}

function completeJob(job, variables) {
  const response = http.post(
    `${BASE_URL}/jobs/${job.key}/complete`,
    JSON.stringify({ variables }),
    {
      ...JSON_PARAMS,
      tags: { name: 'POST /jobs/:key/complete', op: 'complete_job', element_id: job.elementId },
    },
  );
  jobCompletionDuration.add(response.timings.duration, { element_id: job.elementId });
  const ok = check(response, { 'invoice job completion 201': (r) => r.status === 201 });
  if (ok) {
    jobsCompleted.add(1, { element_id: job.elementId });
    return 1;
  }
  return -1;
}

function completeElement(instanceKey, elementId, variables) {
  const found = waitForActiveJobs(instanceKey, [elementId]);
  if (!check(found, { 'invoice expected job found': (jobs) => jobs !== null })) {
    return -1;
  }
  return completeJob(found[elementId], variables);
}

function completeElementGroup(instanceKey, specifications) {
  const elementIds = specifications.map((spec) => spec.elementId);
  const found = waitForActiveJobs(instanceKey, elementIds);
  if (!check(found, { 'invoice grouped jobs found': (jobs) => jobs !== null })) {
    return -1;
  }

  if (!PARALLEL_JOB_COMPLETION) {
    let successful = 0;
    for (const spec of specifications) {
      const result = completeJob(found[spec.elementId], spec.variables);
      if (result < 0) {
        return -1;
      }
      successful += result;
    }
    return successful;
  }

  const requests = specifications.map((spec) => ({
    method: 'POST',
    url: `${BASE_URL}/jobs/${found[spec.elementId].key}/complete`,
    body: JSON.stringify({ variables: spec.variables }),
    params: {
      ...JSON_PARAMS,
      tags: { name: 'POST /jobs/:key/complete', op: 'complete_job', element_id: spec.elementId },
    },
  }));
  const responses = http.batch(requests);

  let successful = 0;
  for (let i = 0; i < responses.length; i++) {
    const response = responses[i];
    const elementId = specifications[i].elementId;
    jobCompletionDuration.add(response.timings.duration, { element_id: elementId });
    const ok = check(response, { 'invoice grouped parallel job completion 201': (r) => r.status === 201 });
    if (ok) {
      successful++;
      jobsCompleted.add(1, { element_id: elementId });
    }
  }
  return successful === specifications.length ? successful : -1;
}

function createParent(data) {
  const invoiceNumber = `PERF-${data.runId}-${__VU}-${__ITER}`;
  const body = JSON.stringify({
    bpmnProcessId: MAIN_PROCESS_ID,
    businessKey: invoiceNumber,
    historyTimeToLive: PROCESS_INSTANCE_HISTORY_TTL,
    variables: { invoiceNumber },
  });
  const response = http.post(`${BASE_URL}/process-instances`, body, {
    ...JSON_PARAMS,
    tags: { name: 'POST /process-instances', op: 'create_invoice_parent' },
  });
  createDuration.add(response.timings.duration);
  if (!check(response, { 'invoice parent create 201': (r) => r.status === 201 })) {
    return null;
  }
  try {
    const instance = parseJsonPreservingIds(response.body);
    return { key: instance.key, invoiceNumber };
  } catch (_) {
    return null;
  }
}

export default function (data) {
  const startedAt = Date.now();
  let success = false;
  let completedJobCount = 0;

  try {
    const parent = createParent(data);
    if (!parent || !parent.key) {
      failStage('create_parent');
      return;
    }
    parentStarted.add(1);

    let result = completeElement(parent.key, 'UserTask_ManualniNahraniFaktury', {
      invoiceNumber: parent.invoiceNumber,
      invoiceTotalNetAmount: 55000,
      invoiceOrderReference: `ORD-${parent.invoiceNumber}`,
    });
    if (result < 0) {
      failStage('upload_invoice');
      return;
    }
    completedJobCount += result;

    result = completeElement(parent.key, 'ServiceTask_VytezeniFaktury', {});
    if (result < 0) {
      failStage('ocr');
      return;
    }
    completedJobCount += result;

    result = completeElement(parent.key, 'UserTask_ManualniKorekcePoVytezeni', {});
    if (result < 0) {
      failStage('manual_correction');
      return;
    }
    completedJobCount += result;

    const childStartedAt = Date.now();
    const childKey = waitForChild(parent.key);
    if (!check(childKey, { 'invoice child process found': (key) => key !== null })) {
      failStage('find_child');
      return;
    }

    result = completeElementGroup(childKey, [
      { elementId: 'ServiceTask_OvereniExistenceObjednavky', variables: { orderExists: true } },
      { elementId: 'ServiceTask_KontrolaSpolehlivostiPlatceDPH', variables: { isReliableVatPayer: true } },
      { elementId: 'ServiceTask_KontrolaRegistraceBankovnihoUctu', variables: { isBankAccountRegistered: true } },
      { elementId: 'ServiceTask_OvereniUdajuDodavatele', variables: { supplierDataMatches: true } },
      { elementId: 'ServiceTask_MatematickaKontrolaFaktury', variables: { hasAmountMatch: true } },
    ]);
    if (result < 0) {
      failStage('parallel_invoice_checks');
      return;
    }
    completedJobCount += result;

    const childInstance = waitForTerminalState(childKey, 'child');
    if (!check(childInstance, {
      'invoice child completed': (instance) => instance !== null && instance.state === 'completed',
    })) {
      failStage('child_not_completed');
      return;
    }
    childCompleted.add(1);
    childDuration.add(Date.now() - childStartedAt);

    result = completeElement(parent.key, 'UserTask_SchvaleniFaktury', { schvaleno: true });
    if (result < 0) {
      failStage('approve_invoice');
      return;
    }
    completedJobCount += result;

    result = completeElement(parent.key, 'SendTask_OznameniSchvaleniFaktury', {});
    if (result < 0) {
      failStage('approval_notification');
      return;
    }
    completedJobCount += result;

    result = completeElement(parent.key, 'UserTask_KonfiguraceUctovaniFaktury', {
      payPrevodem: true,
      payZapoctem: true,
    });
    if (result < 0) {
      failStage('configure_accounting');
      return;
    }
    completedJobCount += result;

    result = completeElement(parent.key, 'ServiceTask_ZauctovaniDodavatelskeFaktury', {});
    if (result < 0) {
      failStage('account_invoice');
      return;
    }
    completedJobCount += result;

    result = completeElementGroup(parent.key, [
      { elementId: 'ServiceTask_VytvoreniNavrhuPrikazuPlatby', variables: {} },
      { elementId: 'UserTask_ZapocetFaktury', variables: {} },
    ]);
    if (result < 0) {
      failStage('both_payment_branches');
      return;
    }
    completedJobCount += result;

    if (!check(completedJobCount, {
      'invoice exactly 14 jobs completed': (count) => count === EXPECTED_JOBS,
    })) {
      failStage('unexpected_job_count');
      return;
    }

    const parentInstance = waitForTerminalState(parent.key, 'parent');
    if (!check(parentInstance, {
      'invoice parent completed': (instance) => instance !== null && instance.state === 'completed',
      'invoice both payment flags retained': (instance) => instance !== null &&
        instance.variables && instance.variables.payPrevodem === true && instance.variables.payZapoctem === true,
    })) {
      failStage('parent_not_completed');
      return;
    }

    parentCompleted.add(1);
    lifecycleDuration.add(Date.now() - startedAt);
    success = true;
  } finally {
    lifecycleSuccess.add(success);
  }
}

function countInstances(processId, state, createdFrom) {
  const query = [
    `bpmnProcessId=${encodeURIComponent(processId)}`,
    `state=${encodeURIComponent(state)}`,
    `createdFrom=${encodeURIComponent(createdFrom)}`,
    'includeChildProcesses=true',
    'page=1',
    'size=1',
  ].join('&');
  const response = http.get(`${BASE_URL}/process-instances?${query}`, {
    tags: { name: 'GET /process-instances (completion audit)', op: 'final_completion_audit', process_id: processId, state },
  });
  if (!check(response, { 'invoice completion audit response 200': (r) => r.status === 200 })) {
    return null;
  }
  try {
    return parseJsonPreservingIds(response.body).totalCount;
  } catch (_) {
    return null;
  }
}

function stateCounts(processId, createdFrom) {
  return {
    active: countInstances(processId, 'active', createdFrom),
    completed: countInstances(processId, 'completed', createdFrom),
    failed: countInstances(processId, 'failed', createdFrom),
    terminated: countInstances(processId, 'terminated', createdFrom),
  };
}

export function teardown(data) {
  const parent = stateCounts(MAIN_PROCESS_ID, data.runStartedAt);
  const child = stateCounts(CHILD_PROCESS_ID, data.runStartedAt);
  const parentOk = parent.completed > 0 && parent.active === 0 && parent.failed === 0 && parent.terminated === 0;
  const childOk = child.completed > 0 && child.active === 0 && child.failed === 0 && child.terminated === 0;
  const countsMatch = parent.completed === child.completed;
  const ok = parentOk && childOk && countsMatch;

  check({ parent, child }, {
    'invoice audit all parents completed': () => parentOk,
    'invoice audit all children completed': () => childOk,
    'invoice audit one completed child per parent': () => countsMatch,
  });
  allProcessesCompleted.add(ok);
  console.log(`invoice completion audit: ${JSON.stringify({ parent, child, ok })}`);
}
