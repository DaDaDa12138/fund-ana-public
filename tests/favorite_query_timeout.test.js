// Virtual time and synthetic I/O exercise the shipped browser request path.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '../frontend/shared.js'), 'utf8');
const fetchSource = source.slice(source.indexOf('async function fetchJson('),
  source.indexOf('async function loadRecommendationsLatest('));
const querySource = source.slice(source.indexOf('async function queryFavoriteFundScores('),
  source.indexOf('async function loadFavoriteMetricDetails('));
const valuationSource = source.slice(source.indexOf('async function loadTrackerValuations('),
  source.indexOf('function formatTrackerValuationStatusMessage('));

function harness() {
  let now = 0;
  let nextId = 0;
  const timers = new Map();
  const requests = [];
  const context = vm.createContext({
    AbortController, DOMException, Map, Promise, Date, URLSearchParams,
    cloneJsonPayload: value => JSON.parse(JSON.stringify(value)),
    window: {
      setTimeout(callback, delay) {
        const id = ++nextId;
        timers.set(id, {callback, due: now + delay});
        return id;
      },
      clearTimeout: id => timers.delete(id),
    },
    fetch: (url, options) => new Promise((resolve, reject) => {
      const abort = () => reject(options.signal.reason);
      options.signal.addEventListener('abort', abort, {once: true});
      if (options.signal.aborted) abort();
      requests.push({url, options, respond(payload, status = 200) {
        options.signal.removeEventListener('abort', abort);
        resolve({ok: status < 400, headers: {get: () => 'application/json'},
          json: async () => payload});
      }});
    }),
  });
  vm.runInContext(`const TRACKER_VALUATION_MAX_REQUEST_CODES = 50;\n${fetchSource}\n${querySource}\n${valuationSource}\n` +
    'globalThis.api = {fetchJson, queryFavoriteFundScores, loadTrackerValuations};', context);
  return {
    api: context.api, requests, timers,
    advance(milliseconds) {
      now += milliseconds;
      for (const [id, timer] of timers) {
        if (timer.due <= now) {
          timers.delete(id);
          timer.callback();
        }
      }
    },
  };
}

async function main() {
  const normal = harness();
  const request = normal.api.queryFavoriteFundScores({codes: ['fixture'], save_history: false});
  assert.equal(normal.requests[0].url, '/api/favorites/score');
  assert.equal(normal.requests[0].options.method, 'POST');
  assert.equal(normal.requests[0].options.timeoutMs, undefined); // Internal option is not sent to fetch.
  const body = JSON.parse(normal.requests[0].options.body);
  assert.equal(body.response_mode, 'compact');
  assert.equal(body.save_history, false);
  normal.advance(20000);
  assert.equal(normal.requests[0].options.signal.aborted, false);
  normal.advance(6600); // Reproduce the original successful server latency.
  normal.requests[0].respond({rows: [{missing: null, zero: 0}]});
  const payload = await request;
  assert.equal(payload.rows[0].missing, null);
  assert.equal(payload.rows[0].zero, 0);
  assert.equal(normal.timers.size, 0);

  const timeout = harness();
  const timedOut = timeout.api.queryFavoriteFundScores({codes: ['fixture']});
  const rejection = assert.rejects(timedOut, /请求超时或已取消: \/api\/favorites\/score/);
  timeout.advance(119999);
  assert.equal(timeout.requests[0].options.signal.aborted, false);
  timeout.advance(1);
  await rejection;
  const retry = timeout.api.queryFavoriteFundScores({codes: ['fixture']});
  assert.equal(timeout.requests.length, 2); // Failure did not leave a stale inflight entry.
  timeout.requests[1].respond({rows: []});
  await retry;
  assert.equal(timeout.timers.size, 0);

  const cancelled = harness();
  const controller = new AbortController();
  const pending = cancelled.api.fetchJson('/api/favorites/score', {
    method: 'POST', timeoutMs: 120000, signal: controller.signal,
  });
  const cancelledRejection = assert.rejects(pending, /请求超时或已取消/);
  controller.abort(new DOMException('Synthetic user cancellation', 'AbortError'));
  await cancelledRejection;
  assert.equal(cancelled.requests[0].options.signal.aborted, true);
  assert.equal(cancelled.timers.size, 0);

  const failed = harness();
  const failedRequest = failed.api.queryFavoriteFundScores({codes: ['fixture']});
  failed.requests[0].respond({error: 'Synthetic upstream failure'}, 503);
  await assert.rejects(failedRequest, /Synthetic upstream failure/);
  assert.equal(failed.timers.size, 0);

  const partial = harness();
  const batchCodes = Array.from({length: 51}, (_, index) => String(index + 1).padStart(6, '0'));
  const partialRequest = partial.api.loadTrackerValuations(batchCodes, {continueOnError: true});
  assert.equal(partial.requests.length, 1);
  partial.requests[0].respond({
    status: 'ok', requested_count: 50, returned_count: 50, cached_count: 0,
    uncached_count: 50, available_count: 1, missing_count: 49,
    upstream_requested_count: 50, upstream_returned_count: 50,
    batch_count: 1, valuations: {[batchCodes[0]]: {available: true, estimated_change_pct: '0'}},
  });
  await new Promise(setImmediate);
  assert.equal(partial.requests.length, 2); // Batches remain sequential.
  partial.requests[1].respond({error: 'Synthetic second-batch failure'}, 503);
  const partialPayload = await partialRequest;
  assert.equal(partialPayload.status, 'partial');
  assert.equal(partialPayload.failed_batch_count, 1);
  assert.equal(partialPayload.valuations[batchCodes[0]].estimated_change_pct, '0');
  assert.equal(partialPayload.valuations[batchCodes[50]].reason, 'upstream_failed');
  assert.equal(partialPayload.available_count, 1);
  assert.equal(partialPayload.missing_count, 50);
  assert.equal(partial.timers.size, 0);

  for (const [method, budget] of [['GET', 12000], ['POST', 20000]]) {
    const defaults = harness();
    const defaultRequest = defaults.api.fetchJson('/fixture', {method});
    const defaultRejection = assert.rejects(defaultRequest, /请求超时或已取消/);
    defaults.advance(budget - 1);
    assert.equal(defaults.requests[0].options.signal.aborted, false);
    defaults.advance(1);
    await defaultRejection;
  }
  process.stdout.write('Favorite query timeout behavior passed (virtual time, no network).\n');
}

main().catch(error => { console.error(error); process.exitCode = 1; });
