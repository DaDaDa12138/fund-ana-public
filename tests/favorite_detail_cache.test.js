// Synthetic I/O markers exercise the shipped browser functions, not fund data.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const root = path.resolve(__dirname, '..');
const favorites = fs.readFileSync(path.join(root, 'frontend/favorites.js'), 'utf8');
const shared = fs.readFileSync(path.join(root, 'frontend/shared.js'), 'utf8');
const functions = favorites.slice(favorites.indexOf('function favoriteMetricDetailCacheKey('),
  favorites.indexOf('function attachFavoriteStyleReferencesFromPayload('));
const globals = ['FAVORITE_PERIODIC_DETAIL_CACHE', 'FAVORITE_PERIODIC_DETAIL_INFLIGHT',
  'FAVORITE_PERIODIC_DETAIL_TTL_MS', 'favoriteMetricDetailGeneration']
  .map(name => favorites.match(new RegExp(`(?:const|let) ${name} = [^;]+;`))[0]).join('\n');

function harness(loader) {
  let now = 1000;
  const calls = [];
  const context = vm.createContext({Map, Promise, Date: {now: () => now},
    normalizeFavoriteCode: code => String(code || '').trim(),
    cloneJsonPayload: value => JSON.parse(JSON.stringify(value)),
    loadFavoriteMetricDetails: async (codes, options) => {
      calls.push({codes, options});
      return loader ? loader(codes, options, calls.length) : {details: {fixture: {request: calls.length, empty: null, zero: 0}}};
    }});
  vm.runInContext(`${globals}\nlet favoriteLastPayload = null;\n${functions}\n` +
    `globalThis.api = {load: ensureFavoriteMetricDetailLoaded, reset: resetFavoriteMetricDetailCache,\n` +
    `renderToken: favoriteMetricDetailRenderToken,\n` +
    `setVersion: value => { favoriteLastPayload = {source_basis: {digest: value}}; }};`, context);
  return {api: context.api, calls, context, clock: value => {now = value;}};
}

function tooltipHarness(kind, loader) {
  const result = harness(loader || ((codes, options, attempt) => ({details: {fixture: {
    weekly_recent: {records: [{io_attempt: attempt}], total_volatility_pct: 0},
    annual_fixture: [{io_attempt: attempt}], volatility_fixture: 0,
  }}})));
  Object.assign(result.context, {
    FAVORITE_ANNUAL_BREAKDOWN_DETAIL_KEY: 'annual_fixture',
    FAVORITE_ANNUAL_VOLATILITY_DETAIL_KEY: 'volatility_fixture',
    buildFavoriteMetricLoadingTooltip: () => 'loading',
    buildFavoritePeriodicReturnTooltipMarkup: options => JSON.stringify(options),
    buildSinceInceptionAnnualizedTooltipMarkupWithVolatility: (display, records, volatility) => JSON.stringify({records, volatility}),
  });
  const card = {innerHTML: '', querySelector: () => null};
  const handlers = {};
  const element = {isConnected: true, dataset: kind === 'periodic'
    ? {favoriteMetricCode: 'fixture', favoriteMetricDetailKey: 'weekly_recent'}
    : {favoriteAnnualDetailCode: 'fixture', favoriteAnnualVolatility: 'fallback'},
    querySelector: () => card, addEventListener: (event, handler) => {handlers[event] = handler;}};
  const root = {querySelectorAll: selector => selector.includes('data-favorite-metric-code') === (kind === 'periodic') ? [element] : []};
  const source = favorites.slice(favorites.indexOf('function bindFavoriteMetricTooltipHydration('),
    favorites.indexOf('function applyFavoriteHistoryCollapsedState('));
  vm.runInContext(`${source}\nglobalThis.bind = bindFavoriteMetricTooltipHydration;`, result.context);
  result.context.bind(root);
  return {...result, hydrate: handlers.mouseenter, card, element};
}

async function main() {
  const normal = harness();
  const [first, concurrent] = await Promise.all([normal.api.load('fixture'), normal.api.load('fixture')]);
  assert.equal(normal.calls.length, 1);
  assert.equal(first.request, concurrent.request);
  first.zero = 17;
  assert.equal((await normal.api.load('fixture')).zero, 0);
  assert.equal((await normal.api.load('fixture')).empty, null);
  const firstRenderToken = normal.api.renderToken('fixture');
  assert.ok(firstRenderToken);
  normal.clock(361000);
  assert.equal(normal.api.renderToken('fixture'), '');
  assert.equal((await normal.api.load('fixture')).request, 2);
  assert.notEqual(normal.api.renderToken('fixture'), firstRenderToken);
  normal.clock(100);
  assert.equal(normal.api.renderToken('fixture'), '');
  assert.equal((await normal.api.load('fixture')).request, 3);

  const versioned = harness();
  versioned.api.setVersion('source-a');
  await versioned.api.load('fixture');
  versioned.api.setVersion('source-b');
  await versioned.api.load('fixture');
  assert.equal(versioned.calls.length, 2);
  assert.notEqual(versioned.calls[0].options.sourceVersion, versioned.calls[1].options.sourceVersion);

  const pending = [];
  const delayed = harness(() => new Promise(resolve => pending.push(resolve)));
  const old = delayed.api.load('fixture');
  delayed.api.reset();
  const fresh = delayed.api.load('fixture');
  assert.equal(delayed.calls.length, 2);
  pending[0]({details: {fixture: {request: 'old'}}});
  await old;
  const alsoFresh = delayed.api.load('fixture');
  assert.equal(delayed.calls.length, 2); // Old finally did not delete the new inflight request.
  pending[1]({details: {fixture: {request: 'fresh'}}});
  assert.equal((await fresh).request, 'fresh');
  assert.equal((await alsoFresh).request, 'fresh');
  assert.equal((await delayed.api.load('fixture')).request, 'fresh');

  const missing = harness(() => ({details: {}}));
  await missing.api.load('fixture');
  await missing.api.load('fixture');
  assert.equal(missing.calls.length, 2);
  const failed = harness((codes, options, attempt) => {
    if (attempt === 1) throw new Error('I/O fixture failure');
    return {details: {fixture: {request: attempt}}};
  });
  await assert.rejects(failed.api.load('fixture'), /fixture failure/);
  assert.equal((await failed.api.load('fixture')).request, 2);

  const fetches = [];
  const sharedContext = vm.createContext({fetchJson: async (url, options) => {fetches.push({url, options}); return {};}});
  const loaderSource = shared.slice(shared.indexOf('async function loadFavoriteMetricDetails('),
    shared.indexOf('async function loadTrackerValuations('));
  vm.runInContext(`${loaderSource}\nglobalThis.loader = loadFavoriteMetricDetails;`, sharedContext);
  await sharedContext.loader(['fixture'], {sourceVersion: 'query-a'});
  await sharedContext.loader(['fixture'], {sourceVersion: 'query-b'});
  assert.notEqual(fetches[0].options.cacheKey, fetches[1].options.cacheKey);
  assert.equal(fetches[0].options.method, 'POST');
  assert.equal(fetches[0].options.cacheTtlMs, undefined);
  const submit = favorites.slice(favorites.indexOf('async function submitFavoriteQuery('));
  assert.ok(submit.indexOf('resetFavoriteMetricDetailCache();') < submit.indexOf('await queryFavoriteFundScores('));
  const trackerFetches = [];
  const trackerContext = vm.createContext({URLSearchParams,
    fetchJson: async (url, options) => {trackerFetches.push({url, options}); return {};}});
  const trackerSource = shared.slice(shared.indexOf('async function loadDashboard('),
    shared.indexOf('async function loadFavoriteFundHistory('));
  vm.runInContext(`${trackerSource}\nglobalThis.api = {tracker: loadTrackerCurrent, dashboard: loadDashboard};`, trackerContext);
  await trackerContext.api.tracker({full: true, page: 2, pageSize: 30, group: 'fixture'});
  assert.equal(trackerFetches[0].url, '/api/tracker/current?full=1&row_aliases=0&page=2&page_size=30&group=fixture');
  await trackerContext.api.dashboard({full: true});
  assert.equal(trackerFetches[1].url.includes('row_aliases'), false);
  for (const kind of ['periodic', 'annual']) {
    const tooltip = tooltipHarness(kind);
    await tooltip.hydrate();
    const rendered = tooltip.card.innerHTML;
    assert.ok(rendered.includes('"0"')); // Numeric zero must survive the tooltip conversion.
    await tooltip.hydrate();
    assert.equal(tooltip.calls.length, 1);
    tooltip.clock(361000);
    await tooltip.hydrate();
    assert.equal(tooltip.calls.length, 2);
    assert.notEqual(tooltip.card.innerHTML, rendered);
    let resolve;
    const racing = tooltipHarness(kind, () => new Promise(done => {resolve = done;}));
    const pendingRender = racing.hydrate();
    racing.api.reset();
    const before = racing.card.innerHTML;
    resolve({details: {fixture: {weekly_recent: {records: [{io_attempt: 'stale'}]}}}});
    await pendingRender;
    assert.equal(racing.card.innerHTML, before);
    assert.equal(racing.element.dataset.favoriteMetricRenderToken, undefined);
  }
  console.log('12 scenarios passed: 8 cache/request contracts and periodic/annual tooltip expiry, zero and stale-response checks');
}

main().catch(error => {console.error(error); process.exitCode = 1;});
