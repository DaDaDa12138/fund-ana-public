// Synthetic I/O markers exercise rendering and request ordering without fund data.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const root = path.resolve(__dirname, '..');
const shared = fs.readFileSync(path.join(root, 'frontend/shared.js'), 'utf8');
const favorites = fs.readFileSync(path.join(root, 'frontend/favorites.js'), 'utf8');
const tracker = fs.readFileSync(path.join(root, 'frontend/tracker.js'), 'utf8');
const tableSource = shared.slice(shared.indexOf('function renderCompactExpandableTable('),
  shared.indexOf('function renderSimpleTable('));
const historySource = favorites.slice(favorites.indexOf('async function refreshFavoriteHistory('),
  favorites.indexOf('async function submitFavoriteQuery('));
const querySource = favorites.slice(favorites.indexOf('async function submitFavoriteQuery('),
  favorites.indexOf('function bindFavoritePageEvents('));
const favoriteValuationSource = favorites.slice(favorites.indexOf('function favoriteValuationsFromScoreSnapshot('),
  favorites.indexOf('const FAVORITE_FOCUS_CODE'));
const initSource = favorites.slice(favorites.indexOf('async function initFavoritesPage('),
  favorites.indexOf('initFavoritesPage().catch('));

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((done, fail) => {resolve = done; reject = fail;});
  return {promise, resolve, reject};
}

function tableHarness(compact) {
  const calls = {details: 0, headers: 0, cells: 0, hydrated: []};
  const context = vm.createContext({Set,
    isCompactTableViewport: () => compact,
    escapeHtml: value => String(value ?? ''),
    buildAttributeMarkup: () => '',
    renderCompactTableSortHeader: () => {calls.headers++; return '<th>marker</th>';},
    resolveTableCellRenderer: (renderers, column, value) => {
      calls.cells++;
      return value === null || value === undefined ? '-' : String(value);
    },
    renderMetricColumnHeader: value => value,
    scheduleStickyTableLayout: () => {},
    compareSortableValues: (left, right, direction) => (left - right) * direction,
  });
  vm.runInContext(`${tableSource}\nglobalThis.render = renderCompactExpandableTable;`, context);
  const container = {
    markup: '', details: [],
    set innerHTML(value) {
      this.markup = value;
      this.details = [...value.matchAll(/data-compact-detail-index="(\d+)"/g)].map(match => ({
        dataset: {compactDetailIndex: match[1]}, open: false, markup: '',
        handlers: {}, addEventListener(event, callback) {this.handlers[event] = callback;},
        insertAdjacentHTML(position, value) {this.markup += value;},
      }));
    },
    get innerHTML() {return this.markup;},
    querySelectorAll(selector) {return selector === '[data-compact-detail-index]' ? this.details : [];},
    querySelector: () => null,
  };
  const options = {
    coreColumns: ['identity', 'rank', '详情'], mobileCardColumns: ['identity'],
    rowKey: row => row.identity, expandedRows: new Set(),
    detailGroups: [{title: 'I/O marker', fields: ['missing', 'zero']}],
    detailValueFormatter: (column, value) => {
      calls.details++;
      return value === null || value === undefined ? '-' : String(value);
    },
    onDetailRendered: (detail, row) => calls.hydrated.push(row.identity),
  };
  return {render: (rows, overrides = {}) => context.render(container, rows, {...options, ...overrides}), container, calls};
}

function favoriteHarness() {
  const scores = [];
  const valuations = [];
  const bootstrap = deferred();
  const histories = [];
  const calls = {renders: [], eventsBound: 0, history: [], badges: [], banners: []};
  const dom = () => ({innerHTML: '', textContent: '', hidden: false, disabled: false, value: ''});
  const context = vm.createContext({Date, Promise, Set,
    favoriteQueryGeneration: 0, favoriteHistoryLoadGeneration: 0,
    favoriteHeroSnapshot: {}, favoriteExportTables: [], favoriteLastQueryContext: null,
    favoriteLastPayload: null, favoriteAllRows: [], favoriteFilterState: {},
    favoriteQueryParams: new URLSearchParams(''),
    parseFavoriteCodes: value => String(value || '').split(',').filter(Boolean),
    normalizeFavoriteCode: value => String(value),
    defaultFavoriteFilterState: () => ({}),
    resetFavoriteMetricDetailCache: () => {}, syncFavoriteExportState: () => {},
    renderFavoritesHeroMeta: () => {},
    setFavoriteLoadingState: () => {}, formatNumber: value => value,
    applyPayloadDataContract: () => {},
    attachFavoriteStyleReferencesFromPayload: rows => rows,
    normalizeFavoriteStyleThresholds: value => value || [],
    getFavoritePrimaryCode: row => row.identity,
    rerenderFavoriteResults: () => calls.renders.push(context.favoriteAllRows.map(row => row.__valuation_state)),
    renderFavoriteQueryBanner: message => calls.banners.push(message),
    renderFavoriteHistory: records => calls.history.push(records),
    renderFavoriteHistoryFailure: reason => calls.history.push({failed: reason}),
    renderFavoriteQueryFailure: reason => calls.failure = reason,
    formatTrackerValuationStatusMessage: () => 'I/O result',
    queryFavoriteFundScores: () => {
      const pending = deferred(); scores.push(pending); return pending.promise;
    },
    loadTrackerValuations: codes => {
      const pending = deferred(); valuations.push({pending, codes}); return pending.promise;
    },
    loadBootstrapSummary: () => bootstrap.promise,
    loadFavoriteFundHistory: () => {
      const pending = deferred(); histories.push(pending); return pending.promise;
    },
    bindFavoritePageEvents: () => calls.eventsBound++,
    applyFavoriteHistoryCollapsedState: () => {},
    renderRunBadge: () => calls.badges.push('run'),
    renderHealthBadge: () => calls.badges.push('health'),
    renderUnavailableBadge: () => calls.badges.push('unavailable'),
    getBootstrapWorkflow: value => value, getBootstrapHealth: value => value,
  });
  for (const name of [
    'favoriteQuerySubmitBtn', 'favoriteExportStatus', 'favoriteScoreMeta', 'favoriteScoreStats',
    'favoriteScoreNarrative', 'favoriteFilterBar', 'favoriteFilterDetails', 'favoriteContextSummaryNote',
    'favoriteViewBar', 'favoriteStyleThresholdSummary', 'favoriteListHints', 'favoriteTableToolbar',
    'favoriteSourceLegend', 'favoriteScoreTable', 'favoriteCodesInput', 'favoriteQueryLabel',
    'favoritesHeaderRunBadge', 'favoritesHeaderHealthBadge',
  ]) context[name] = dom();
  vm.runInContext(`${favoriteValuationSource}\n${historySource}\n${querySource}\n${initSource}\n` +
    'globalThis.api = {submit: submitFavoriteQuery, init: initFavoritesPage, history: refreshFavoriteHistory};', context);
  return {context, api: context.api, scores, valuations, bootstrap, histories, calls};
}

async function verifyIndependentBootstrap(file, initFunction, mainLoaders, firstPayloadMarker) {
  const source = fs.readFileSync(path.join(root, `frontend/${file}`), 'utf8');
  const start = source.indexOf(`async function ${initFunction}(`);
  const prefix = source.slice(start, source.indexOf(firstPayloadMarker, start));
  const bootstrap = deferred();
  const main = deferred();
  const calls = {content: 0, badges: [], errors: []};
  const context = vm.createContext({Promise,
    loadBootstrapSummary: () => bootstrap.promise,
    getBootstrapWorkflow: value => value, getBootstrapHealth: value => value,
    renderRunBadge: target => calls.badges.push(['run', target]),
    renderHealthBadge: target => calls.badges.push(['health', target]),
    renderUnavailableBadge: (target, label) => calls.errors.push([target, label]),
    buildStatCard: () => '', renderRetryableEmptyState: () => {},
    shouldLoadRecommendationDetail: () => false,
    trackerGroupFromUrl: () => 'I/O marker', GROUP_STABLE: 'I/O marker',
    trackerValuationBtn: {disabled: false}, trackerHeroMeta: {}, trackerPageTable: {},
    recommendationsHeroMeta: {}, recommendationCards: {},
    reportsHeroMeta: {}, publicationTable: {},
    taskSummaryHeroMeta: {}, taskStageTable: {}, taskAuditNarrative: {}, taskAuditTable: {},
    mainRendered: () => calls.content++,
  });
  for (const loader of mainLoaders) context[loader] = () => main.promise;
  context.loadFavoriteFundHistory = () => Promise.resolve({records: []});
  for (const target of ['trackerHeaderRunBadge', 'trackerHeaderHealthBadge',
    'recommendationsHeaderRunBadge', 'recommendationsHeaderHealthBadge',
    'reportsHeaderRunBadge', 'reportsHeaderHealthBadge',
    'taskSummaryHeaderRunBadge', 'taskSummaryHeaderHealthBadge']) context[target] = target;
  vm.runInContext(`${prefix}\nmainRendered();\n}\nglobalThis.init = ${initFunction};`, context);
  const pending = context.init();
  main.resolve({marker: 'main-content'});
  await pending;
  assert.equal(calls.content, 1, `${file}: main content should not wait for bootstrap`);
  assert.equal(calls.badges.length, 0);
  bootstrap.reject(new Error('synthetic status failure'));
  await new Promise(setImmediate);
  assert.equal(calls.content, 1);
  assert.equal(calls.errors.length, 2, `${file}: badge errors must still render`);
}

async function main() {
  const rows = [{identity: 'marker-a', rank: 2, missing: null, zero: 0},
    {identity: 'marker-b', rank: 1, missing: null, zero: 0}];
  const desktop = tableHarness(false);
  desktop.render(rows);
  assert.equal(desktop.calls.details, 0);
  assert.equal(desktop.container.innerHTML.includes('favorite-detail-row'), false);
  desktop.render(rows, {expandedRows: new Set(['marker-b'])});
  assert.equal(desktop.calls.details, 2);
  assert.ok(desktop.container.innerHTML.includes('data-compact-detail-key="marker-b"'));
  assert.ok(desktop.container.innerHTML.includes('<span>0</span>'));
  assert.ok(desktop.container.innerHTML.includes('<span>-</span>'));

  const mobile = tableHarness(true);
  const sorted = mobile.render(rows, {sortableColumns: ['rank'], sortState: {column: 'rank', direction: 'asc'}});
  assert.equal(mobile.calls.details, 0);
  assert.equal(mobile.calls.headers, 0);
  assert.equal(mobile.calls.cells, 2); // Mobile builds only the two visible identity cells.
  assert.equal(sorted.sortedRows[0].identity, 'marker-b');
  const detail = mobile.container.details[0];
  detail.handlers.toggle();
  assert.equal(mobile.calls.details, 0);
  detail.open = true;
  detail.handlers.toggle();
  assert.equal(mobile.calls.details, 2);
  assert.deepEqual(mobile.calls.hydrated, ['marker-b']);
  assert.ok(detail.markup.includes('<span>0</span>'));
  detail.open = false;
  detail.handlers.toggle();
  detail.open = true;
  detail.handlers.toggle();
  assert.equal(mobile.calls.details, 2); // Reopening keeps the already rendered details.

  const early = favoriteHarness();
  const init = early.api.init();
  assert.equal(early.calls.eventsBound, 1);
  assert.ok(early.context.favoriteScoreNarrative.innerHTML.includes('待查询'));
  const query = early.api.submit({codes: ['marker-a'], saveHistory: false});
  early.scores[0].resolve({rows: [rows[0]], history: [{marker: 'new-query'}]});
  await new Promise(setImmediate);
  assert.equal(early.calls.failure, undefined);
  assert.equal(early.calls.renders.length, 1);
  assert.deepEqual(Array.from(early.calls.renders[0]), ['loading']);
  await query;
  assert.equal(early.context.favoriteQuerySubmitBtn.disabled, false);
  assert.equal(early.valuations.length, 1);
  early.histories[0].resolve({records: [{marker: 'old-init'}]});
  early.bootstrap.resolve({});
  await init;
  assert.equal(early.calls.history.length, 1);
  assert.equal(early.calls.history[0][0].marker, 'new-query');
  assert.equal(early.context.favoriteAllRows[0].__valuation_state, 'loading');
  early.valuations[0].pending.resolve({status: 'ok', valuations: {'marker-a': {available: false, reason: 'valuation_not_provided'}}});
  await new Promise(setImmediate);
  assert.equal(early.calls.renders.length, 2);
  assert.equal(early.context.favoriteAllRows[0].__valuation_state, 'missing');

  const snapshotReuse = favoriteHarness();
  const snapshotQuery = snapshotReuse.api.submit({codes: ['snapshot-ready', 'snapshot-missing', 'snapshot-stale', 'not-covered'], saveHistory: false});
  const evidence = {
    endpoint: 'https://example.invalid/valuation',
    fetched_at: '2026-10-18T14:00:00+08:00',
    response_sha256: 'a'.repeat(64),
    response_artifact: 'reports/audit/snapshot.response.json',
  };
  snapshotReuse.scores[0].resolve({rows: [
    {identity: 'snapshot-ready', __favorite_live_source: 'tiantian_fund_valuation_last',
      __favorite_live_source_evidence: evidence, __valuation_state: 'ready', '实时估值': '0',
      '实时估算净值': '1.0123', '实时估值时间': '2026-10-18 14:00:00'},
    {identity: 'snapshot-missing', __favorite_live_source: 'tiantian_fund_valuation_last',
      __favorite_live_source_evidence: evidence, __valuation_state: 'unresolved',
      __valuation_reason: 'valuation_not_provided'},
    {identity: 'snapshot-stale', __favorite_live_source: 'tiantian_fund_valuation_last',
      __favorite_live_source_evidence: evidence, __valuation_state: 'unresolved',
      __valuation_reason: 'valuation_stale', '实时估值': '5.00', '实时估算净值': '1.1234'},
    {identity: 'not-covered'},
  ]});
  await snapshotQuery;
  assert.deepEqual(Array.from(snapshotReuse.valuations[0].codes), ['not-covered']);
  assert.equal(snapshotReuse.context.favoriteAllRows[0]['实时估值'], '0');
  assert.equal(snapshotReuse.context.favoriteAllRows[0].__valuation_state, 'ready');
  assert.equal(snapshotReuse.context.favoriteAllRows[1]['实时估值'], '暂无估值');
  assert.equal(snapshotReuse.context.favoriteAllRows[1].__valuation_state, 'missing');
  assert.equal(snapshotReuse.context.favoriteAllRows[2]['实时估值'], '');
  assert.equal(snapshotReuse.context.favoriteAllRows[2].__valuation_state, 'unresolved');
  assert.equal(snapshotReuse.context.favoriteAllRows[3].__valuation_state, 'loading');

  const queryRace = favoriteHarness();
  const olderQuery = queryRace.api.submit({codes: ['older'], saveHistory: false});
  queryRace.scores[0].resolve({rows: [{identity: 'older'}]});
  await olderQuery;
  const newerQuery = queryRace.api.submit({codes: ['newer'], saveHistory: false});
  queryRace.scores[1].resolve({rows: [{identity: 'newer'}]});
  await newerQuery;
  queryRace.valuations[0].pending.resolve({status: 'ok', valuations: {older: {available: true, estimated_change_pct: '9'}}});
  await new Promise(setImmediate);
  assert.deepEqual(Array.from(queryRace.context.favoriteAllRows.map(row => row.identity)), ['newer']);
  assert.equal(queryRace.context.favoriteAllRows[0].__valuation_state, 'loading');
  queryRace.valuations[1].pending.resolve({status: 'ok', valuations: {newer: {available: true, estimated_change_pct: '0'}}});
  await new Promise(setImmediate);
  assert.equal(queryRace.context.favoriteAllRows[0]['实时估值'], '0');
  assert.equal(queryRace.context.favoriteAllRows[0].__valuation_state, 'ready');

  const historyRace = favoriteHarness();
  const first = historyRace.api.history();
  const second = historyRace.api.history();
  historyRace.histories[1].resolve({records: [{marker: 'fresh'}]});
  await second;
  historyRace.histories[0].reject(new Error('stale I/O failure'));
  await first;
  assert.equal(historyRace.calls.history.length, 1);
  assert.equal(historyRace.calls.history[0][0].marker, 'fresh');

  // Exercise the shipped tracker button wiring across two page initializations.
  const trackerBinding = tracker.slice(tracker.indexOf('  rerenderTrackerForViewport = applyRender;'),
    tracker.indexOf('  const trackerFile ='));
  let previousCalls = 0;
  let currentCalls = 0;
  const button = {};
  const trackerContext = vm.createContext({trackerValuationBtn: button, applyRender: () => {},
    refreshValuations: async () => previousCalls++, valuationRequested: false});
  vm.runInContext(trackerBinding, trackerContext);
  trackerContext.refreshValuations = async () => currentCalls++;
  vm.runInContext(trackerBinding, trackerContext);
  await button.onclick();
  assert.equal(previousCalls, 0);
  assert.equal(currentCalls, 1);
  assert.equal(trackerContext.valuationRequested, true);
  await verifyIndependentBootstrap('tracker.js', 'initTrackerPage', ['loadTrackerCurrent'], '  const tracker = trackerResult.value;');
  await verifyIndependentBootstrap('recommendations.js', 'initRecommendationsPage', ['loadRecommendationsLatest'], '  let payload = recommendationsResult.value');
  await verifyIndependentBootstrap('reports.js', 'initReportsPage', ['loadDashboardReportsDetail', 'loadPublicationSummary'], '  const dashboard = dashboardResult.value;');
  await verifyIndependentBootstrap('task-summary.js', 'initTaskSummaryPage', ['loadDashboardTaskSummaryDetail'], '  const dashboard = dashboardResult.value');
  console.log('Front-end loading checks passed: lazy details, mobile work, immediate interaction, stale auxiliary responses, render count, tracker button wiring and four independent bootstrap paths.');
}

main().catch(error => {console.error(error); process.exitCode = 1;});
