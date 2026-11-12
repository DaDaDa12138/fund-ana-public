const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '../frontend/operations.js'), 'utf8');
const sectionSource = fs.readFileSync(path.join(__dirname, '../frontend/operations-scheduler-detail.js'), 'utf8');
const sharedSource = fs.readFileSync(path.join(__dirname, '../frontend/shared.js'), 'utf8');

function declaration(name, from = source) {
  const found = new RegExp(`^(?:async )?function ${name}\\(`, 'm').exec(from);
  assert.ok(found, name);
  const rest = from.slice(found.index + found[0].length);
  const next = /\n(?:async )?function /.exec(rest);
  assert.ok(next, name);
  return from.slice(found.index, found.index + found[0].length + next.index);
}

function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return {promise, resolve, reject};
}

async function settle() {
  for (let i = 0; i < 16; i++) await new Promise(resolve => setTimeout(resolve, 0));
}

function harness({scheduler, dashboard} = {}) {
  const calls = {scheduler: [], dashboard: [], workflow: 0, schedulerFailures: 0, dashboardFailures: 0};
  const context = vm.createContext({
    document: {visibilityState: 'visible'},
    window: {
      setTimeout,
      loadSchedulerStatusDetail: async ({section}) => {
        calls.scheduler.push(section);
        return scheduler ? scheduler(section, calls.scheduler.length) : {section, probe: 'loaded'};
      },
    },
    operationsDetailCollapsedState: {workflowLogs: true, longTermGovernanceSamples: true,
      exclusionSamples: true, lowVolPositiveRatioSamples: true},
    loadDashboard: async ({sampleLimit}) => {
      calls.dashboard.push(sampleLimit);
      return dashboard ? dashboard(sampleLimit, calls.dashboard.length) : {probe: 'loaded'};
    },
    loadStatus: async () => { calls.workflow++; return {status: 'idle', logs: []}; },
    getOperationsSchedulerState: () => ({}),
    renderSchedulerDetailFailureStates: () => { calls.schedulerFailures++; },
    renderDashboardDetailFailureStates: () => { calls.dashboardFailures++; },
  });
  for (const name of ['setStatus', 'renderOperationsHero', 'renderAutomationAutonomy', 'renderScheduler',
    'renderManualRunPanel', 'renderCurrentFinalSelectionGovernanceQueue', 'renderRegressionSmoke',
    'renderAutomationLayers', 'renderLongTermGovernance', 'renderIntegrityAudits', 'renderExclusionSummary',
    'renderLowVolPositiveRatioWatch', 'renderOperationsDashboardFallbacks', 'scheduleTick']) {
    context[name] = () => {};
  }
  vm.runInContext(sectionSource, context);
  vm.runInContext(`const {OPERATIONS_DETAIL_SECTION_SCHEDULER_SECTIONS, normalizeOperationsSchedulerSections,
    hasOperationsSchedulerSectionData, mergeOperationsSchedulerDetailPayloads,
    loadOperationsSchedulerDetailSections} = window.OperationsSchedulerDetail;
    ${source.slice(source.indexOf('let polling = null;'), source.indexOf('const OPERATIONS_DETAIL_SECTION_CONFIG'))}
    ${['hasWorkflowDetailData', 'hasSchedulerDetailData', 'getExpandedOperationsSchedulerSections',
      'getRequestedOperationsSchedulerSections', 'shouldRefreshWorkflowDetail', 'shouldRefreshSchedulerDetail',
      'shouldRefreshDashboardDetail', 'getOperationsDashboardSampleLimit', 'disconnectOperationsLazySectionObserver',
      'requestOperationsHeavyResource', 'refreshOperationsHeavy'].map(name => declaration(name)).join('\n')}`, context);
  return {calls, context, run: expression => vm.runInContext(expression, context)};
}

test('opening governance loads dashboard after an already active scheduler request', async () => {
  const pending = deferred();
  const app = harness({scheduler: () => pending.promise});
  app.run(`requestOperationsHeavyResource('scheduler', {schedulerSections:['long_term_governance']});
    requestOperationsHeavyResource('dashboard');`);
  assert.equal(app.calls.dashboard.length, 0);
  pending.resolve({section: 'long_term_governance', probe: 'loaded'});
  await settle();
  assert.equal(app.calls.dashboard.length, 1);
  assert.equal(app.run('cachedOperationsDashboard.probe'), 'loaded');
  assert.equal(app.calls.workflow, 0);
});

test('a scheduler section requested during another section request is eventually loaded', async () => {
  const pending = deferred();
  const app = harness({scheduler: (section, n) => n === 1 ? pending.promise : {section, probe: 'loaded'}});
  app.run(`requestOperationsHeavyResource('scheduler', {schedulerSections:['long_term_governance']});
    requestOperationsHeavyResource('scheduler', {schedulerSections:['maintenance']});`);
  pending.resolve({section: 'long_term_governance', probe: 'loaded'});
  await settle();
  assert.ok(app.calls.scheduler.includes('maintenance'));
  assert.equal(app.run(`hasOperationsSchedulerSectionData(cachedSchedulerStatusDetail, 'maintenance')`), true);
});

test('expanded samples requested during summary load use the larger limit without forcing other resources', async () => {
  const pending = deferred();
  const app = harness({dashboard: (sampleLimit, n) => n === 1 ? pending.promise : {sampleLimit}});
  app.run(`requestOperationsHeavyResource('dashboard');
    operationsDetailCollapsedState.longTermGovernanceSamples = false;
    requestOperationsHeavyResource('dashboard');`);
  pending.resolve({sampleLimit: 0});
  await settle();
  assert.deepEqual(app.calls.dashboard, [0, 8]);
  assert.equal(app.run('cachedOperationsDashboardSampleLimit'), 8);
  assert.equal(app.calls.workflow, 0);
  assert.deepEqual(app.calls.scheduler, []);
});

test('a failed scheduler request does not lose queued dashboard demand or retry forever', async () => {
  const pending = deferred();
  const app = harness({scheduler: (_, n) => n === 1 ? pending.promise : Promise.reject(new Error('offline section'))});
  app.run(`requestOperationsHeavyResource('scheduler', {schedulerSections:['long_term_governance']});
    requestOperationsHeavyResource('dashboard');`);
  pending.reject(new Error('offline section'));
  await settle();
  assert.equal(app.calls.dashboard.length, 1);
  assert.equal(app.run('cachedOperationsDashboard.probe'), 'loaded');
  assert.equal(app.calls.scheduler.length, 2);
  assert.equal(app.calls.schedulerFailures, 2);
  await settle();
  assert.equal(app.calls.scheduler.length, 2);
});

test('overlapping demand for the same cached resource does not duplicate requests', async () => {
  const pending = deferred();
  const app = harness({dashboard: () => pending.promise});
  app.run(`for (let i = 0; i < 5; i++) requestOperationsHeavyResource('dashboard');`);
  pending.resolve({probe: 'loaded'});
  await settle();
  assert.deepEqual(app.calls.dashboard, [0]);
  assert.equal(app.run('operationsHeavyRefreshPromise'), null);
});

test('explicit force refresh queued during a load is honored and awaitable', async () => {
  const pending = deferred();
  const app = harness({dashboard: (_, n) => n === 1 ? pending.promise : {probe: 'refreshed'}});
  app.run(`requestOperationsHeavyResource('dashboard');`);
  const forced = app.run('refreshOperationsHeavy(true)');
  pending.resolve({probe: 'initial'});
  await forced;
  assert.equal(app.run('cachedOperationsDashboard.probe'), 'refreshed');
  assert.deepEqual(app.calls.dashboard, [0, 0]);
});

function summaryRequestHarness() {
  let now = 0, nextTimer = 0;
  const timers = new Map(), requests = [];
  const context = vm.createContext({
    AbortController, DOMException, structuredClone, URLSearchParams,
    window: {
      setTimeout: (callback, delay) => {
        const id = ++nextTimer;
        timers.set(id, {callback, at: now + delay});
        return id;
      },
      clearTimeout: id => timers.delete(id),
    },
    fetch: (url, {signal}) => {
      const pending = deferred();
      signal.addEventListener('abort', () => pending.reject(signal.reason), {once: true});
      requests.push({url, signal, complete: () => pending.resolve({
        ok: true, headers: {get: () => 'application/json'},
        json: async () => ({status: 'io_probe_complete'}),
      })});
      return pending.promise;
    },
  });
  vm.runInContext(['cloneJsonPayload', 'fetchJson', 'loadSchedulerStatus', 'loadDashboard', 'loadSchedulerStatusDetail']
    .map(name => declaration(name, sharedSource)).join('\n'), context);
  return {requests, run: expression => vm.runInContext(expression, context),
    advance(ms) {
      now += ms;
      for (const [id, timer] of timers) {
        if (timer.at <= now) { timers.delete(id); timer.callback(); }
      }
    },
  };
}

for (const loader of ['loadSchedulerStatus()', 'loadDashboard({sampleLimit:8})',
  "loadSchedulerStatusDetail({section:'long_term_governance',sampleLimit:0})"]) {
  test(`16-second cold response loads once without an automatic retry: ${loader}`, async () => {
    const app = summaryRequestHarness();
    const outcome = app.run(loader).then(value => ({value}), error => ({error}));
    app.advance(16000);
    assert.equal(app.requests[0].signal.aborted, false);
    app.requests[0].complete();
    assert.equal((await outcome).value.status, 'io_probe_complete');
    assert.equal(app.requests.length, 1);
    assert.equal((await app.run(loader)).status, 'io_probe_complete');
    assert.equal(app.requests.length, 1);
  });
}

test('an unresponsive scheduler still times out and a failed response is never cached as success', async () => {
  const app = summaryRequestHarness();
  const outcome = app.run('loadSchedulerStatus()').then(value => ({value}), error => ({error}));
  app.advance(30001);
  assert.match((await outcome).error.message, /请求超时或已取消/);
  assert.equal(app.run('fetchJson._responseCache.size'), 0);
  const retry = app.run('loadSchedulerStatus()');
  assert.equal(app.requests.length, 2);
  app.requests[1].complete();
  assert.equal((await retry).status, 'io_probe_complete');
});

test('ordinary GET requests keep the existing timeout budget', async () => {
  const app = summaryRequestHarness();
  const outcome = app.run("fetchJson('/api/io-probe')").then(value => ({value}), error => ({error}));
  app.advance(12001);
  assert.match((await outcome).error.message, /请求超时或已取消/);
});
