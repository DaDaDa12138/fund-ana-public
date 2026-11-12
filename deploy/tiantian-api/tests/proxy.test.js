'use strict';

// All network requests are replaced with an Axios adapter inside a Docker
// container with --network=none. Source distributions use synthetic records;
// an optional local real-response fixture retains its independent source SHA.
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { createHash } = require('node:crypto');
const axios = require('axios');
const { configureTransport, createHandler, loadModules } = require('../server');
const provenance = require('../upstream.json');
const realNavPath = path.join(__dirname, 'fixtures/nav.json');
const hasRealNav = fs.existsSync(realNavPath);
const navBytes = hasRealNav ? fs.readFileSync(realNavPath) : Buffer.from(JSON.stringify({
  ErrCode: 0, Datas: [{ fixture: 'synthetic only', DWJZ: '1.0000', LJJZ: null, JZZZL: '0' }],
}));
const navSource = hasRealNav ? require('./fixtures/nav.source.json') : null;
const nav = JSON.parse(navBytes);

async function invoke(handler, url, method = 'GET') {
  const response = { headersSent: false, destroyed: false,
    writeHead(status, headers) { this.status = status; this.headers = headers; this.headersSent = true; },
    end(body) { this.body = body; },
  };
  await handler({ url, method }, response);
  response.payload = JSON.parse(response.body);
  return response;
}

test('optional real NAV fixture remains bound to the complete captured API response', { skip: !hasRealNav }, () => {
  assert.equal(createHash('sha256').update(navBytes).digest('hex'), navSource.body_sha256);
  assert.equal(nav.ErrCode, 0);
  assert.equal(nav.Datas.length, 5);
  assert.ok(nav.Datas.some((row) => row.JZZZL === '0'));
});

test('all documented fund adapters and their shared source match the pinned commit', () => {
  const modules = loadModules();
  assert.equal(Object.keys(modules).length, 31);
  assert.ok(Object.hasOwn(modules, '/fundMNHisNetList'));
  assert.ok(Object.hasOwn(modules, '/companyApi2'));
  assert.ok(!Object.hasOwn(modules, '/stockGet'));
  if (navSource) {
    assert.equal(provenance.commit, navSource.comparison_adapter_commit);
    for (const [name, digest] of Object.entries(navSource.existing_proxy_source_file_sha256)) {
      assert.equal(provenance.files[name], digest);
    }
  }
});

test('provided NAV JSON and complete response accounting are preserved', async () => {
  const logged = [];
  const handler = createHandler({ modules: { '/fundMNHisNetList': async () => nav }, log: (row) => logged.push(row) });
  const response = await invoke(handler, '/fundMNHisNetList?FCODE=000015&pagesize=5');
  assert.equal(response.status, 200);
  assert.deepEqual(response.payload, nav);
  assert.equal(logged.length, 1);
  assert.match(logged[0].observed_at, /\+08:00$/);
  assert.equal(logged[0].timezone, 'Asia/Shanghai');
  assert.ok(Math.abs(Date.parse(logged[0].observed_at) - Date.now()) < 5000);
  assert.equal(logged[0].upstream_attempted, true);
  assert.equal(logged[0].response_sha256, createHash('sha256').update(response.body).digest('hex'));
  assert.equal(logged[0].response_bytes, response.body.length);
  assert.equal(response.headers['Cache-Control'], 'no-store');
});

for (const payload of [null, [], { ErrCode: 1, ErrMsg: 'isolated upstream rejection', Datas: null },
  { absent: null, zero: 0, empty: '', textZero: '0' }]) {
  test(`source null/empty/failure values remain unchanged: ${JSON.stringify(payload)}`, async () => {
    const response = await invoke(createHandler({ modules: { '/fundMNRank': async () => payload }, log() {} }), '/fundMNRank');
    assert.equal(response.status, 200);
    assert.deepEqual(response.payload, payload);
  });
}

for (const payload of [undefined, '<html>upstream unavailable</html>', { invalid: Infinity }, { invalid: undefined }]) {
  test(`invalid upstream payload cannot become synthetic business values: ${String(payload)}`, async () => {
    const response = await invoke(createHandler({ modules: { '/fundMNRank': async () => payload }, log() {} }), '/fundMNRank');
    assert.equal(response.status, 502);
    assert.equal(response.payload.error.code, 'UPSTREAM_INVALID_JSON');
    assert.ok(!Object.hasOwn(response.payload, 'Datas'));
  });
}

for (const [code, expectedStatus] of [['ETIMEDOUT', 504], ['ECONNABORTED', 504], ['ECONNREFUSED', 502], ['ERR_BAD_RESPONSE', 502]]) {
  test(`transport failure is explicit and never returned as an empty successful result: ${code}`, async () => {
    const handler = createHandler({ modules: { '/fundMNRank': async () => {
      throw Object.assign(new Error('isolated transport failure'), { code, response: { status: 503 } });
    } }, log() {} });
    const response = await invoke(handler, '/fundMNRank');
    assert.equal(response.status, expectedStatus);
    assert.equal(response.payload.upstreamStatus, 503);
    assert.equal(response.payload.endpoint, '/fundMNRank');
    assert.equal(response.payload.error.code, code);
    assert.ok(!Object.hasOwn(response.payload, 'Datas'));
  });
}

for (const endpoint of ['/healthz', '/health']) {
  test(`${endpoint} only proves the local listener and does not claim upstream success`, async () => {
    let calls = 0;
    const handler = createHandler({ modules: { '/fundMNRank': async () => { calls += 1; throw new Error('upstream unavailable'); } }, log() {} });
    const response = await invoke(handler, endpoint);
    assert.equal(response.status, 200);
    assert.equal(response.payload.upstream_checked, false);
    assert.equal(calls, 0);
    assert.ok(!Object.hasOwn(response.payload, 'endpointMetrics'));
    assert.ok(!Object.hasOwn(response.payload, 'endpointSummary'));
  });
}

for (const [url, method, status] of [['/stockGet', 'GET', 404], ['/fundMNRank', 'POST', 405],
  ['http://example.invalid/fundMNRank', 'GET', 400], ['/missing', 'GET', 404]]) {
  test(`unsupported request performs no upstream call: ${method} ${url}`, async () => {
    let calls = 0;
    const handler = createHandler({ modules: { '/fundMNRank': async () => { calls += 1; return nav; } }, log() {} });
    assert.equal((await invoke(handler, url, method)).status, status);
    assert.equal(calls, 0);
  });
}

test('provider methods, parameter case, leading zeros and dates pass through actual pinned adapters', async () => {
  configureTransport();
  const originalAdapter = axios.defaults.adapter;
  const captured = [];
  axios.defaults.adapter = async (config) => {
    captured.push(config);
    return { status: 200, statusText: 'OK', headers: {}, config, data: nav };
  };
  try {
    const handler = createHandler({ modules: loadModules(), log() {} });
    await invoke(handler, '/fundMNHisNetList?FCODE=000015&pageIndex=2&pagesize=5');
    await invoke(handler, '/fundMNRank?FundType=31&pageIndex=1&pageSize=30&BUY=true&SortColumn=SYL_1N');
    await invoke(handler, '/fundMNPeriodIncrease?FCODE=000015&RANGE=n');
    await invoke(handler, '/fundMSNMangerInfo?FCODE=30276903');
    const [history, rank, annual, manager] = captured;
    assert.equal(history.method, 'post');
    assert.equal(history.url, 'https://fundmobapi.eastmoney.com/FundMNewApi/FundMNHisNetList');
    const historyParams = new URLSearchParams(history.data);
    assert.equal(historyParams.get('FCODE'), '000015');
    assert.equal(historyParams.get('pageIndex'), '2');
    assert.equal(historyParams.get('pagesize'), '5');
    assert.equal(historyParams.get('pageSize'), null);
    assert.equal(rank.method, 'get');
    assert.equal(rank.params.FundType, '31');
    assert.equal(rank.params.BUY, 'true');
    assert.equal(rank.params.pageSize, '30');
    assert.equal(annual.params.RANGE, 'n');
    assert.equal(manager.method, 'post');
    assert.equal(new URLSearchParams(manager.data).get('FCODE'), '30276903');
    assert.ok(captured.every((config) => config.timeout === 25000));
  } finally {
    axios.defaults.adapter = originalAdapter;
  }
});

test('concurrency rejection releases capacity when original requests finish', async () => {
  let release;
  const pending = new Promise((resolve) => { release = resolve; });
  const handler = createHandler({ modules: { '/fundMNRank': async () => { await pending; return nav; } }, log() {} });
  const requests = Array.from({ length: 32 }, () => invoke(handler, '/fundMNRank'));
  const rejected = await invoke(handler, '/fundMNRank');
  assert.equal(rejected.status, 503);
  assert.equal(rejected.payload.error.type, 'proxy_busy');
  release();
  assert.ok((await Promise.all(requests)).every((response) => response.status === 200));
  assert.equal((await invoke(handler, '/fundMNRank')).status, 200);
});
