const favoritesHeaderRunBadge = document.getElementById('favoritesHeaderRunBadge');
const favoritesHeaderHealthBadge = document.getElementById('favoritesHeaderHealthBadge');
const favoritesHeroMeta = document.getElementById('favoritesHeroMeta');
const favoriteQueryLabel = document.getElementById('favoriteQueryLabel');
const favoriteCodesInput = document.getElementById('favoriteCodesInput');
const favoriteQuerySubmitBtn = document.getElementById('favoriteQuerySubmitBtn');
const favoriteCodesClearBtn = document.getElementById('favoriteCodesClearBtn');
const favoriteHistoryRefreshBtn = document.getElementById('favoriteHistoryRefreshBtn');
const favoriteHistoryToggleBtn = document.getElementById('favoriteHistoryToggleBtn');
const favoriteHistoryList = document.getElementById('favoriteHistoryList');
const favoriteQueryBanner = document.getElementById('favoriteQueryBanner');
const favoriteScoreMeta = document.getElementById('favoriteScoreMeta');
const favoriteScoreStats = document.getElementById('favoriteScoreStats');
const favoriteScoreNarrative = document.getElementById('favoriteScoreNarrative');
const favoriteLoadingState = document.getElementById('favoriteLoadingState');
const favoriteSourceLegend = document.getElementById('favoriteSourceLegend');
const favoriteTableToolbar = document.getElementById('favoriteTableToolbar');
const favoriteScoreTable = document.getElementById('favoriteScoreTable');
const favoriteFilterBar = document.getElementById('favoriteFilterBar');
const favoriteFilterDetails = document.getElementById('favoriteFilterDetails');
const favoriteFilterSummary = document.getElementById('favoriteFilterSummary');
const favoriteContextSummaryNote = document.getElementById('favoriteContextSummaryNote');
const favoriteViewBar = document.getElementById('favoriteViewBar');
const favoriteStyleThresholdSummary = document.getElementById('favoriteStyleThresholdSummary');
const favoriteListHints = document.getElementById('favoriteListHints');
const favoriteExportBtn = document.getElementById('favoriteExportBtn');
const favoriteExportStatus = document.getElementById('favoriteExportStatus');
const favoriteQueryParams = new URLSearchParams(window.location.search);
const FAVORITE_PERIODIC_DETAIL_CACHE = new Map();
const FAVORITE_PERIODIC_DETAIL_INFLIGHT = new Map();
const FAVORITE_PERIODIC_DETAIL_TTL_MS = 300000;
let favoriteMetricDetailGeneration = 0;
const FAVORITE_ANNUAL_BREAKDOWN_DETAIL_KEY = 'since_inception_annual_return_breakdown';
const FAVORITE_ANNUAL_VOLATILITY_DETAIL_KEY = 'since_inception_annual_return_volatility_pct';
const FAVORITE_LIVE_RETURN_FIELD_MAP = Object.freeze({
  '近1周(%)': {
    liveField: '__favorite_live_return_1w_pct',
    sourceField: '__favorite_return_1w_source',
    label: '实时近1周',
  },
  '近1月(%)': {
    liveField: '__favorite_live_return_1m_pct',
    sourceField: '__favorite_return_1m_source',
    label: '实时近1月',
  },
  '近3月(%)': {
    liveField: '__favorite_live_return_3m_pct',
    sourceField: '__favorite_return_3m_source',
    label: '实时近3月',
  },
  '近6月(%)': {
    liveField: '__favorite_live_return_6m_pct',
    sourceField: '__favorite_return_6m_source',
    label: '实时近6月',
  },
});

const FAVORITE_OPTIONAL_MAIN_COLUMN_OPTIONS = Object.freeze([
  { id: 'scale', column: '基金规模(亿)', label: '规模' },
  { id: 'occurrence', column: '出现次数&出现频率(%)', label: '频次' },
  { id: 'group', column: '正式榜分组', label: '榜分组' },
]);
const FAVORITE_DEFAULT_OPTIONAL_MAIN_COLUMN_IDS = Object.freeze(
  FAVORITE_OPTIONAL_MAIN_COLUMN_OPTIONS.map((item) => item.id)
);

const FAVORITE_CORE_BASE_TABLE_COLUMNS = buildFundCoreColumns({
  preScoreColumns: ['当前层级', '建议动作'],
  metricColumns: ['平台成立以来收益年化(%)', '1年-年化收益(%)', '近1周(%)', '近1月(%)', '近3月(%)', '近6月(%)', '实时估值'],
  trailingColumns: ['债基风格标签'],
});
const FAVORITE_EXPANDED_BASE_TABLE_COLUMNS = buildFundCoreColumns({
  preScoreColumns: ['当前层级', '建议动作'],
  metricColumns: ['平台成立以来收益年化(%)', '1年-年化收益(%)', '近1周(%)', '近1月(%)', '近3月(%)', '近6月(%)', '实时估值', '1年-夏普比率'],
});

const FAVORITE_DETAIL_GROUPS = Object.freeze([
  {
    title: FUND_DETAIL_SECTION_TITLES.time,
    fields: buildFundTimeDetailFields(['近1周(%)', '近1月(%)', '近3月(%)', '近6月(%)']),
  },
  {
    title: FUND_DETAIL_SECTION_TITLES.label,
    fields: [...FUND_SHARED_LABEL_DETAIL_FIELDS, '基金类型', '基金规模(亿)', '出现次数&出现频率(%)', '投资优先池得分'],
  },
  {
    title: FUND_DETAIL_SECTION_TITLES.risk,
    fields: [...FUND_SHARED_RISK_DETAIL_FIELDS],
  },
  {
    title: FUND_DETAIL_SECTION_TITLES.subject,
    fields: ['基金经理', '基金公司'],
  },
  {
    title: FUND_DETAIL_SECTION_TITLES.recommendation,
    fields: ['推荐解读'],
    render: (row, helpers) => renderRecommendationInfoDetailSection(row, helpers, {
      includeReason: false,
    }),
  },
]);

const FAVORITE_SORTABLE_COLUMNS = buildFundSortableColumns(['实时估算净值', '最新单位净值']);

const FAVORITE_INITIAL_SORT_DIRECTIONS = buildFundInitialSortDirections({
  '实时估算净值': 'desc',
  '最新单位净值': 'desc',
});
const FAVORITE_VIEW_MODES = Object.freeze({
  mixed: 'mixed',
  grouped: 'grouped',
});
const FAVORITE_HEADER_REFERENCE_FIELDS = Object.freeze([
  '正式投资评分',
  '1年-年化收益(%)',
  '平台成立以来收益年化(%)',
  '近1周(%)',
  '近1月(%)',
  '近3月(%)',
  '近6月(%)',
  '1年-夏普比率',
  '1年-最大回撤(%)',
  '1年-波动率',
]);
const FAVORITE_HEADER_LABEL_OVERRIDES = Object.freeze({
  '正式投资评分': '正式评分',
  '1年-年化收益(%)': '1年年化',
  '平台成立以来收益年化(%)': '平台成立年化',
  '近1周(%)': '近1周',
  '近1月(%)': '近1月',
  '1年-夏普比率': '1年策略夏普',
  '1年-最大回撤(%)': '最大回撤',
  '1年-波动率': '波动率',
});
const FAVORITE_COMPACTNESS_MODES = Object.freeze({
  ultra: 'ultra',
  standard: 'standard',
  compare: 'compare',
});
const FAVORITE_VIEW_PARAM_KEY = 'view';
const FAVORITE_DENSITY_PARAM_KEY = 'density';
const FAVORITE_COMPACTNESS_PARAM_KEY = 'compactness';
const FAVORITE_COMPACTNESS_STORAGE_KEY = 'fund_ana.favorite_table_compactness';
const FAVORITE_OPTIONAL_COLUMNS_PARAM_KEY = 'aux_cols';
const FAVORITE_OPTIONAL_COLUMNS_STORAGE_KEY = 'fund_ana.favorite_table_aux_cols';
const FAVORITE_POSITIVE_RETURN_HIGHLIGHT_STORAGE_KEY = 'fund_ana.favorite_positive_return_highlight';
const FAVORITE_POSITIVE_RETURN_FIELDS = Object.freeze(['近3月(%)', '近1周(%)', '近1月(%)']);
const DEFAULT_FAVORITE_FILTER_STATE = Object.freeze({
  level: '',
  source: '',
  recommendationStatus: 'all',
  annualizedAvailability: 'all',
  scoreFloor: '',
});
const FAVORITE_FILTER_PARAM_KEYS = Object.freeze({
  level: 'filter_level',
  source: 'filter_source',
  recommendationStatus: 'filter_recommendation',
  annualizedAvailability: 'filter_annualized',
  scoreFloor: 'filter_score_floor',
});
const RECOMMENDATION_RETURN_CONTEXT_PARAM_KEYS = Object.freeze({
  group: 'rec_group',
  sortPreset: 'rec_sort_preset',
  sortColumn: 'rec_sort_column',
  sortDirection: 'rec_sort_direction',
  filterQuery: 'rec_filter_query',
  filterStyle: 'rec_filter_style',
  filterStability: 'rec_filter_stability',
  filterQuality: 'rec_filter_quality',
  filterCompany: 'rec_filter_company',
  filterFavoriteStatus: 'rec_filter_favorite',
  filterScoreFloor: 'rec_filter_score_floor',
  filterAnnualizedAvailability: 'rec_filter_annualized',
});

let favoriteSortState = {
  column: '正式投资评分',
  direction: 'desc',
};
let favoriteRows = [];
let favoriteAllRows = [];
let favoriteLastPayload = null;
let favoriteLastQueryContext = null;
let favoriteExportTables = [];
let favoriteExportBusy = false;
let favoriteFilterState = favoriteFilterStateFromUrl();
let favoriteViewMode = favoriteViewModeFromUrl();
let favoriteDensityMode = favoriteDensityModeFromUrl();
let favoriteCompactnessMode = favoriteCompactnessModeFromUrl();
let favoriteOptionalMainColumnIds = favoriteOptionalMainColumnsFromUrl();
let favoriteStyleThresholds = [];
let favoriteGlobalStyleThreshold = {};
let favoriteMissingStyleThresholds = [];
let favoriteExpandedCodes = new Set();
let favoriteHistoryCollapsed = window.innerWidth <= 720;
let favoritePageEventsBound = false;
let favoriteQueryGeneration = 0;
let favoriteHistoryLoadGeneration = 0;
let favoritePositiveReturnHighlightEnabled = localStorage.getItem(FAVORITE_POSITIVE_RETURN_HIGHLIGHT_STORAGE_KEY) === '1';
let favoriteHistoryQueryCount = 0;
let favoriteHistoryAvailable = true;
let favoriteHeroSnapshot = {
  mode: 'idle',
  runId: '',
  requestedCount: 0,
  matchedCount: 0,
  visibleCount: 0,
  styleCount: 0,
};

function favoriteValuationsFromScoreSnapshot(rows) {
  const result = {};
  for (const row of Array.isArray(rows) ? rows : []) {
    const code = getFavoritePrimaryCode(row);
    const evidence = row?.__favorite_live_source_evidence;
    const fetchedAt = Date.parse(String(evidence?.fetched_at || ''));
    const hasEvidence = row?.__favorite_live_source === 'tiantian_fund_valuation_last'
      && Number.isFinite(fetchedAt)
      && /^[a-f0-9]{64}$/iu.test(String(evidence?.response_sha256 || ''))
      && String(evidence?.response_artifact || '').trim()
      && String(evidence?.endpoint || '').trim();
    if (!code || !hasEvidence) continue;

    const reason = String(row?.__valuation_reason || '').trim();
    if (row?.__valuation_state === 'ready') {
      const changePct = String(row?.['实时估值'] ?? '').trim();
      const estimatedNav = String(row?.['实时估算净值'] ?? '').trim();
      const valuationDate = String(row?.['实时估值时间'] ?? '').trim();
      if (!changePct || !estimatedNav || !valuationDate) continue;
      result[code] = {
        available: true,
        estimated_change_pct: changePct,
        estimated_nav: estimatedNav,
        valuation_date: valuationDate,
        source: row.__favorite_live_source,
        response_sha256: evidence.response_sha256,
        request_fetched_at: evidence.fetched_at,
      };
      continue;
    }
    if (!reason) continue;
    result[code] = {
      available: false,
      reason,
      message: String(row?.__valuation_status_label || '').trim(),
      valuation_date: String(row?.['实时估值时间'] ?? '').trim(),
      source: row.__favorite_live_source,
      response_sha256: evidence.response_sha256,
      request_fetched_at: evidence.fetched_at,
    };
  }
  return result;
}

function mergeFavoriteRowsWithValuations(rows, valuations, valuationState = 'ready') {
  const valuationMap = valuations && typeof valuations === 'object' ? valuations : {};
  return (Array.isArray(rows) ? rows : []).map((row) => {
    const code = getFavoritePrimaryCode(row);
    const valuation = valuationMap[code] || {};
    const hasObservation = Object.prototype.hasOwnProperty.call(valuationMap, code);
    let displayValue = '';
    let uiState = valuationState === 'loading' && !hasObservation ? 'loading' : 'unresolved';
    let uiStatusLabel = uiState === 'loading' ? '请求中' : '未取得有效估值';
    let uiReason = '';
    if (valuation.available) {
      displayValue = String(valuation.estimated_change_pct ?? '').trim();
      uiState = 'ready';
      uiStatusLabel = '已返回';
    } else if (
      valuation.reason === 'valuation_not_provided' || valuation.reason === 'upstream_missing'
    ) {
      displayValue = '暂无估值';
      uiState = 'missing';
      uiStatusLabel = valuation.reason === 'valuation_not_provided' ? '该基金当前未提供盘中估值' : '上游未返回该基金';
      uiReason = String(valuation.reason || '').trim();
    } else if (valuation.reason === 'upstream_failed' || valuationState === 'failed') {
      uiState = 'failed';
      uiStatusLabel = '请求失败';
      uiReason = String(valuation.reason || 'upstream_failed').trim();
    } else if (hasObservation && valuation.reason) {
      uiState = 'unresolved';
      uiStatusLabel = '估值未通过有效性校验';
      uiReason = String(valuation.reason).trim();
    }
    return {
      ...row,
      实时估值: displayValue,
      实时估值时间: String(valuation.valuation_date ?? '').trim(),
      实时估算净值: String(valuation.estimated_nav ?? '').trim(),
      __valuation_state: uiState,
      __valuation_status_label: uiStatusLabel,
      __valuation_reason: uiReason,
    };
  });
}

async function refreshFavoriteValuationsInBackground(codes, allCodes, snapshotValuations, payload, queryGeneration) {
  const snapshotCount = Object.keys(snapshotValuations).length;
  try {
    const valuationPayload = await loadTrackerValuations(codes, { continueOnError: true });
    if (queryGeneration !== favoriteQueryGeneration) return;
    const combinedValuations = {
      ...snapshotValuations,
      ...(valuationPayload?.valuations || {}),
    };
    favoriteAllRows = mergeFavoriteRowsWithValuations(favoriteAllRows, combinedValuations, 'ready');
    payload.rows = favoriteAllRows;
    payload.funds = favoriteAllRows;
    rerenderFavoriteResults();
    const availableCount = Object.values(combinedValuations).filter((item) => item?.available).length;
    const status = formatTrackerValuationStatusMessage(
      valuationPayload,
      availableCount,
      allCodes.length,
      {
        suffix: `实时估值仅作盘中辅助观察；已复用评分阶段 ${snapshotCount} 条已校验状态`,
      },
    );
    renderFavoriteQueryBanner(status, valuationPayload?.status === 'ok' && availableCount
      ? 'info-banner-success'
      : 'info-banner-warning');
  } catch (error) {
    if (queryGeneration !== favoriteQueryGeneration) return;
    const failedValuations = Object.fromEntries(codes.map((code) => [code, {
      available: false,
      reason: 'upstream_failed',
      message: error?.message || '实时估值请求失败',
      source: 'tiantian_fund_valuation_last',
    }]));
    favoriteAllRows = mergeFavoriteRowsWithValuations(
      favoriteAllRows,
      { ...snapshotValuations, ...failedValuations },
      'ready',
    );
    payload.rows = favoriteAllRows;
    payload.funds = favoriteAllRows;
    rerenderFavoriteResults();
    renderFavoriteQueryBanner(
      `评分已完成；${error?.message ? `实时估值补查失败：${error.message}` : '实时估值补查失败'}；已保留评分阶段 ${snapshotCount} 条来源状态`,
      'info-banner-warning',
    );
  }
}
const FAVORITE_FOCUS_CODE = normalizeFavoriteCode(favoriteQueryParams.get('focus_code') || '');

function favoriteMetricDetailCacheKey(code) {
  const sourceVersion = favoriteLastPayload?.source_basis?.digest
    || favoriteLastPayload?.cache_basis?.digest || '';
  return [favoriteMetricDetailGeneration, sourceVersion, normalizeFavoriteCode(code)].join('::');
}

function resetFavoriteMetricDetailCache() {
  favoriteMetricDetailGeneration += 1;
  FAVORITE_PERIODIC_DETAIL_CACHE.clear();
  FAVORITE_PERIODIC_DETAIL_INFLIGHT.clear();
}

function favoriteMetricDetailRenderToken(code) {
  const cacheKey = favoriteMetricDetailCacheKey(code);
  const cached = FAVORITE_PERIODIC_DETAIL_CACHE.get(cacheKey);
  const age = cached ? Date.now() - cached.storedAt : null;
  return cached && age >= 0 && age < FAVORITE_PERIODIC_DETAIL_TTL_MS
    ? `${cacheKey}::${cached.storedAt}` : '';
}

async function ensureFavoriteMetricDetailLoaded(code) {
  const normalizedCode = normalizeFavoriteCode(code);
  if (!normalizedCode) return {};
  const cacheKey = favoriteMetricDetailCacheKey(normalizedCode);
  const cached = FAVORITE_PERIODIC_DETAIL_CACHE.get(cacheKey);
  const age = cached ? Date.now() - cached.storedAt : null;
  if (cached && age >= 0 && age < FAVORITE_PERIODIC_DETAIL_TTL_MS) {
    return cloneJsonPayload(cached.value);
  }
  FAVORITE_PERIODIC_DETAIL_CACHE.delete(cacheKey);
  if (FAVORITE_PERIODIC_DETAIL_INFLIGHT.has(cacheKey)) {
    return FAVORITE_PERIODIC_DETAIL_INFLIGHT.get(cacheKey);
  }
  const requestPromise = loadFavoriteMetricDetails([normalizedCode], { sourceVersion: cacheKey })
    .then((payload) => {
      const detail = (payload?.details || {})[normalizedCode] || {};
      if (favoriteMetricDetailCacheKey(normalizedCode) === cacheKey && Object.keys(detail).length) {
        FAVORITE_PERIODIC_DETAIL_CACHE.set(cacheKey, { storedAt: Date.now(), value: cloneJsonPayload(detail) });
      }
      return detail;
    })
    .finally(() => {
      if (FAVORITE_PERIODIC_DETAIL_INFLIGHT.get(cacheKey) === requestPromise) {
        FAVORITE_PERIODIC_DETAIL_INFLIGHT.delete(cacheKey);
      }
    });
  FAVORITE_PERIODIC_DETAIL_INFLIGHT.set(cacheKey, requestPromise);
  return requestPromise;
}

function attachFavoriteStyleReferencesFromPayload(rows, referenceMap) {
  const items = Array.isArray(rows) ? rows : [];
  const references = referenceMap && typeof referenceMap === 'object' ? referenceMap : {};
  items.forEach((row) => {
    if (!row || typeof row !== 'object') return;
    const existing = row.__style_metric_references;
    if (existing && typeof existing === 'object' && Object.keys(existing).length) return;
    const code = getFavoritePrimaryCode(row);
    const reference = references[code];
    if (reference && typeof reference === 'object') {
      row.__style_metric_references = reference;
    }
  });
  return items;
}

function buildFavoriteMetricLoadingTooltip(title, description) {
  return buildFavoritePeriodicReturnTooltipMarkup({
    title,
    description,
    records: [],
    emptyText: '正在加载真实净值衍生明细…',
  });
}

function buildFavoritePeriodicMetricDescription(field, display, row, baseDescription) {
  const meta = FAVORITE_LIVE_RETURN_FIELD_MAP[field];
  const normalizedBase = String(baseDescription || '').trim();
  if (!meta) return normalizedBase;
  const source = String(row?.[meta.sourceField] || '').trim();
  const sourceLabel = favoriteLiveSourceLabel(row);
  const liveValue = String(row?.[meta.liveField] ?? '').trim();
  const liveDate = String(row?.__favorite_live_return_date || '').trim();
  const liveTimeText = liveDate ? `收益截至 ${liveDate}` : '收益基准日未提供';
  const snapshotDate = String(row?.__favorite_snapshot_return_date || row?.['区间收益基准日'] || '').trim();
  const snapshotDateText = snapshotDate ? `收益截至 ${snapshotDate}` : '历史收益基准日未记录';
  const snapshotText = formatSignedPercentOrDash(display);
  if (source === 'live_api' && liveValue) {
    return [
      `当前主表优先展示 ${sourceLabel} ${meta.label} ${formatSignedPercentText(liveValue)}（${liveTimeText}）。`,
      '正式评分、风险指标和同风格参考线仍来自项目当前正式分析快照。',
      normalizedBase,
    ].filter(Boolean).join(' ');
  }
  if (source === 'live_fallback' && liveValue) {
    return [
      `当前主表该列暂无正式快照值，暂以 ${sourceLabel} ${meta.label} ${formatSignedPercentText(liveValue)}（${liveTimeText}）补充展示。`,
      normalizedBase,
    ].filter(Boolean).join(' ');
  }
  if (liveValue && formatSignedPercentText(liveValue) !== snapshotText) {
    return [
      `主表当前展示正式快照值 ${snapshotText}（${snapshotDateText}）；${sourceLabel} ${meta.label} 为 ${formatSignedPercentText(liveValue)}（${liveTimeText}），仅作辅助观察，不参与当前参考线比较。`,
      normalizedBase,
    ].filter(Boolean).join(' ');
  }
  return [
    `主表当前展示正式快照值 ${snapshotText}（${snapshotDateText}），与同风格参考线按同一快照口径比较。`,
    normalizedBase,
  ].filter(Boolean).join(' ');
}

function renderFavoritePeriodicReturnTooltipCell(value, row, options = {}) {
  const display = String(value || '').trim();
  if (!display) {
    return '<span class="favorite-detail-value-empty">-</span>';
  }
  const code = getFavoritePrimaryCode(row);
  const detailKey = String(options.detailKey || '').trim();
  const title = String(options.title || '').trim() || display;
  const description = buildFavoritePeriodicMetricDescription(
    String(options.field || '').trim(),
    display,
    row,
    String(options.description || '').trim(),
  );
  const tooltipClass = String(options.tooltipClass || '').trim();
  const emptyText = String(options.emptyText || '暂无明细').trim();
  const metricFields = [String(options.field || '').trim(), ...(Array.isArray(options.referenceFields) ? options.referenceFields : [])]
    .map((item) => String(item || '').trim())
    .filter(Boolean);
  const benchmarkMarkup = buildStyleMetricReferenceSummaryMarkup(row, [...new Set(metricFields)]);
  const cardMarkup = detailKey
    ? `${buildFavoriteMetricLoadingTooltip(title, description)}${benchmarkMarkup}`
    : buildFavoritePeriodicReturnTooltipMarkup({
      title,
      description,
      records: [],
      emptyText,
      benchmarkMarkup,
    });
  const buttonText = formatSignedPercentText(display);
  const lazyAttributes = detailKey
    ? `
      data-favorite-metric-code="${escapeHtml(code)}"
      data-favorite-metric-detail-key="${escapeHtml(detailKey)}"
      data-favorite-metric-title="${escapeHtml(title)}"
      data-favorite-metric-description="${escapeHtml(description)}"
      data-favorite-metric-empty-text="${escapeHtml(emptyText)}"
    `
    : '';
  return `
    <div
      class="score-tooltip favorite-score-tooltip favorite-periodic-tooltip-anchor ${escapeHtml(tooltipClass)}"
      tabindex="0"
      ${lazyAttributes}
    >
      <button type="button" class="score-tooltip-trigger favorite-score-tooltip-trigger" aria-controls="globalFloatingTooltip" aria-expanded="false">
        ${escapeHtml(buttonText)}
      </button>
      <div class="score-tooltip-card favorite-score-tooltip-card favorite-periodic-tooltip-card" role="tooltip">
        ${cardMarkup}
      </div>
    </div>
  `;
}

function bindFavoriteMetricTooltipHydration(root = document) {
  if (!root || typeof root.querySelectorAll !== 'function') return;
  root.querySelectorAll('[data-favorite-metric-code][data-favorite-metric-detail-key]').forEach((element) => {
    if (element.dataset.favoriteMetricBound === 'true') return;
    element.dataset.favoriteMetricBound = 'true';
    const hydrate = async () => {
      const code = String(element.dataset.favoriteMetricCode || '').trim();
      const detailKey = String(element.dataset.favoriteMetricDetailKey || '').trim();
      if (!code || !detailKey) return;
      const card = element.querySelector('.favorite-periodic-tooltip-card');
      if (!card) return;
      const renderToken = favoriteMetricDetailRenderToken(code);
      if (renderToken && element.dataset.favoriteMetricRenderToken === renderToken) return;
      const requestKey = favoriteMetricDetailCacheKey(code);
      const benchmarkMarkup = card.querySelector('.favorite-style-benchmark-block')?.outerHTML || '';
      try {
        card.innerHTML = buildFavoriteMetricLoadingTooltip(
          String(element.dataset.favoriteMetricTitle || '').trim(),
          String(element.dataset.favoriteMetricDescription || '').trim(),
        );
        const detailPayload = await ensureFavoriteMetricDetailLoaded(code);
        if (!element.isConnected || requestKey !== favoriteMetricDetailCacheKey(code)) return;
        const detail = detailPayload?.[detailKey] || {};
        card.innerHTML = buildFavoritePeriodicReturnTooltipMarkup({
          title: String(element.dataset.favoriteMetricTitle || '').trim(),
          description: String(element.dataset.favoriteMetricDescription || '').trim(),
          records: Array.isArray(detail.records) ? detail.records : [],
          totalVolatilityPct: String(detail.total_volatility_pct ?? '').trim(),
          volatilityLabel: detailKey === 'weekly_recent' ? '近两月周收益总波动率' : '月化收益总波动率',
          emptyText: String(element.dataset.favoriteMetricEmptyText || '暂无明细').trim(),
          benchmarkMarkup,
        });
        element.dataset.favoriteMetricRenderToken = favoriteMetricDetailRenderToken(code);
      } catch (_error) {
        if (!element.isConnected || requestKey !== favoriteMetricDetailCacheKey(code)) return;
        card.innerHTML = buildFavoritePeriodicReturnTooltipMarkup({
          title: String(element.dataset.favoriteMetricTitle || '').trim(),
          description: String(element.dataset.favoriteMetricDescription || '').trim(),
          records: [],
          emptyText: '明细加载失败，请稍后重试',
          benchmarkMarkup,
        });
      }
      if (typeof refreshFloatingTooltipContentFromSource === 'function') {
        refreshFloatingTooltipContentFromSource(element);
      }
      if (typeof requestFloatingTooltipPosition === 'function') {
        requestFloatingTooltipPosition();
      }
    };
    element.addEventListener('mouseenter', hydrate, { passive: true });
    element.addEventListener('focusin', hydrate);
    element.addEventListener('click', hydrate);
  });
  root.querySelectorAll('[data-favorite-annual-detail-code]').forEach((element) => {
    if (element.dataset.favoriteAnnualBound === 'true') return;
    element.dataset.favoriteAnnualBound = 'true';
    const hydrate = async () => {
      const code = String(element.dataset.favoriteAnnualDetailCode || '').trim();
      if (!code) return;
      const card = element.querySelector('.favorite-annualized-tooltip-card');
      if (!card) return;
      const renderToken = favoriteMetricDetailRenderToken(code);
      if (renderToken && element.dataset.favoriteMetricRenderToken === renderToken) return;
      const requestKey = favoriteMetricDetailCacheKey(code);
      const display = String(element.dataset.favoriteAnnualDisplay || '').trim();
      const fallbackVolatility = String(element.dataset.favoriteAnnualVolatility || '').trim();
      const accumNavRatio = String(element.dataset.favoriteAnnualAccumNav ?? '').trim();
      const benchmarkMarkup = card.querySelector('.favorite-style-benchmark-block')?.outerHTML || '';
      try {
        card.innerHTML = `
          ${buildSinceInceptionAnnualizedTooltipMarkupWithVolatility(display, [], fallbackVolatility, {
            emptyText: '正在加载真实年度收益明细…', accumNavRatio,
          })}
          ${benchmarkMarkup}
        `;
        const detailPayload = await ensureFavoriteMetricDetailLoaded(code);
        if (!element.isConnected || requestKey !== favoriteMetricDetailCacheKey(code)) return;
        const annualBreakdown = Array.isArray(detailPayload?.[FAVORITE_ANNUAL_BREAKDOWN_DETAIL_KEY])
          ? detailPayload[FAVORITE_ANNUAL_BREAKDOWN_DETAIL_KEY]
          : [];
        const annualVolatility = String(
          detailPayload?.[FAVORITE_ANNUAL_VOLATILITY_DETAIL_KEY] ?? fallbackVolatility ?? ''
        ).trim();
        card.innerHTML = `
          ${buildSinceInceptionAnnualizedTooltipMarkupWithVolatility(display, annualBreakdown, annualVolatility, {
            emptyText: '暂无完整年度收益明细', accumNavRatio,
          })}
          ${benchmarkMarkup}
        `;
        element.dataset.favoriteMetricRenderToken = favoriteMetricDetailRenderToken(code);
      } catch (_error) {
        if (!element.isConnected || requestKey !== favoriteMetricDetailCacheKey(code)) return;
        card.innerHTML = `
          ${buildSinceInceptionAnnualizedTooltipMarkupWithVolatility(display, [], fallbackVolatility, {
            emptyText: '年度收益明细加载失败，请稍后重试',
          })}
          ${benchmarkMarkup}
        `;
      }
      if (typeof refreshFloatingTooltipContentFromSource === 'function') {
        refreshFloatingTooltipContentFromSource(element);
      }
      if (typeof requestFloatingTooltipPosition === 'function') {
        requestFloatingTooltipPosition();
      }
    };
    element.addEventListener('mouseenter', hydrate, { passive: true });
    element.addEventListener('focusin', hydrate);
    element.addEventListener('click', hydrate);
  });
}

function applyFavoriteHistoryCollapsedState() {
  const panel = favoriteHistoryList?.closest('.favorite-history-panel');
  if (!panel) return;
  panel.classList.toggle('is-collapsed', favoriteHistoryCollapsed);
  if (favoriteHistoryToggleBtn) {
    favoriteHistoryToggleBtn.textContent = favoriteHistoryCollapsed ? '展开记录' : '收起记录';
    favoriteHistoryToggleBtn.setAttribute('aria-expanded', favoriteHistoryCollapsed ? 'false' : 'true');
  }
}

function defaultFavoriteFilterState() {
  return {
    level: DEFAULT_FAVORITE_FILTER_STATE.level,
    source: DEFAULT_FAVORITE_FILTER_STATE.source,
    recommendationStatus: DEFAULT_FAVORITE_FILTER_STATE.recommendationStatus,
    annualizedAvailability: DEFAULT_FAVORITE_FILTER_STATE.annualizedAvailability,
    scoreFloor: DEFAULT_FAVORITE_FILTER_STATE.scoreFloor,
  };
}

function favoriteHasActiveFilters(filterState = {}) {
  return Boolean(
    String(filterState?.level || '').trim()
    || String(filterState?.source || '').trim()
    || String(filterState?.recommendationStatus || 'all').trim() !== 'all'
    || String(filterState?.annualizedAvailability || 'all').trim() !== 'all'
    || String(filterState?.scoreFloor || '').trim()
  );
}

function favoriteFilterStateFromUrl() {
  const params = new URLSearchParams(window.location.search);
  const recommendationStatus = String(params.get(FAVORITE_FILTER_PARAM_KEYS.recommendationStatus) || '').trim();
  const annualizedAvailability = String(params.get(FAVORITE_FILTER_PARAM_KEYS.annualizedAvailability) || '').trim();
  const scoreFloor = String(params.get(FAVORITE_FILTER_PARAM_KEYS.scoreFloor) || '').trim();
  return {
    level: String(params.get(FAVORITE_FILTER_PARAM_KEYS.level) || '').trim(),
    source: String(params.get(FAVORITE_FILTER_PARAM_KEYS.source) || '').trim(),
    recommendationStatus: ['all', 'recommendation', 'non_recommendation'].includes(recommendationStatus)
      ? recommendationStatus
      : DEFAULT_FAVORITE_FILTER_STATE.recommendationStatus,
    annualizedAvailability: ['all', 'available', 'missing'].includes(annualizedAvailability)
      ? annualizedAvailability
      : DEFAULT_FAVORITE_FILTER_STATE.annualizedAvailability,
    scoreFloor: ['', '75', '80', '85', '90'].includes(scoreFloor)
      ? scoreFloor
      : DEFAULT_FAVORITE_FILTER_STATE.scoreFloor,
  };
}

function favoriteViewModeFromUrl() {
  const view = String(new URLSearchParams(window.location.search).get(FAVORITE_VIEW_PARAM_KEY) || '').trim();
  if (view === FAVORITE_VIEW_MODES.mixed) return FAVORITE_VIEW_MODES.mixed;
  return FAVORITE_VIEW_MODES.grouped;
}

function favoriteDensityModeFromUrl() {
  const density = String(new URLSearchParams(window.location.search).get(FAVORITE_DENSITY_PARAM_KEY) || '').trim();
  if (density === FUND_TABLE_DENSITY_MODES.core) return FUND_TABLE_DENSITY_MODES.core;
  return FUND_TABLE_DENSITY_MODES.expanded;
}

function favoriteCompactnessModeFromUrl() {
  const params = new URLSearchParams(window.location.search);
  const compactness = String(params.get(FAVORITE_COMPACTNESS_PARAM_KEY) || '').trim();
  if (Object.values(FAVORITE_COMPACTNESS_MODES).includes(compactness)) {
    return compactness;
  }
  try {
    const stored = String(window.localStorage.getItem(FAVORITE_COMPACTNESS_STORAGE_KEY) || '').trim();
    if (Object.values(FAVORITE_COMPACTNESS_MODES).includes(stored)) {
      return stored;
    }
  } catch (_error) {
    // Ignore storage access failures and fall back to default.
  }
  return FAVORITE_COMPACTNESS_MODES.standard;
}

function persistFavoriteCompactnessMode(mode) {
  try {
    window.localStorage.setItem(FAVORITE_COMPACTNESS_STORAGE_KEY, mode);
  } catch (_error) {
    // Ignore storage access failures.
  }
}

function normalizeFavoriteOptionalMainColumnIds(rawIds) {
  const requested = new Set(
    (Array.isArray(rawIds) ? rawIds : [])
      .map((item) => String(item || '').trim())
      .filter(Boolean)
  );
  return FAVORITE_OPTIONAL_MAIN_COLUMN_OPTIONS
    .map((item) => item.id)
    .filter((id) => requested.has(id));
}

function favoriteOptionalMainColumnsFromUrl() {
  const params = new URLSearchParams(window.location.search);
  const fromUrl = normalizeFavoriteOptionalMainColumnIds(
    String(params.get(FAVORITE_OPTIONAL_COLUMNS_PARAM_KEY) || '')
      .split(',')
  );
  if (fromUrl.length || params.has(FAVORITE_OPTIONAL_COLUMNS_PARAM_KEY)) {
    return fromUrl;
  }
  try {
    const stored = normalizeFavoriteOptionalMainColumnIds(
      String(window.localStorage.getItem(FAVORITE_OPTIONAL_COLUMNS_STORAGE_KEY) || '')
        .split(',')
    );
    if (stored.length) {
      return stored;
    }
  } catch (_error) {
    // Ignore storage access failures and fall back to default.
  }
  return [...FAVORITE_DEFAULT_OPTIONAL_MAIN_COLUMN_IDS];
}

function persistFavoriteOptionalMainColumnIds(ids) {
  try {
    window.localStorage.setItem(
      FAVORITE_OPTIONAL_COLUMNS_STORAGE_KEY,
      normalizeFavoriteOptionalMainColumnIds(ids).join(',')
    );
  } catch (_error) {
    // Ignore storage access failures.
  }
}

function buildFavoriteOptionalMainColumns() {
  const selected = new Set(normalizeFavoriteOptionalMainColumnIds(favoriteOptionalMainColumnIds));
  return FAVORITE_OPTIONAL_MAIN_COLUMN_OPTIONS
    .filter((item) => selected.has(item.id))
    .map((item) => item.column);
}

function mergeFavoriteMainTableColumns(baseColumns, extraColumns) {
  const items = Array.isArray(baseColumns) ? baseColumns : [];
  const extras = Array.isArray(extraColumns) ? extraColumns : [];
  const detailIndex = items.indexOf('详情');
  const leading = detailIndex >= 0 ? items.slice(0, detailIndex) : items.slice();
  const trailing = detailIndex >= 0 ? items.slice(detailIndex) : [];
  return [...new Set([...leading, ...extras, ...trailing])];
}

function syncFavoriteViewUrl() {
  if (!favoriteLastQueryContext?.codes?.length) return;
  const params = new URLSearchParams(window.location.search);
  params.set('codes', favoriteLastQueryContext.codes.join(','));
  params.set('auto_query', '1');
  params.set('save_history', '0');
  if (favoriteLastQueryContext.label) {
    params.set('label', favoriteLastQueryContext.label);
  } else {
    params.delete('label');
  }
  if (favoriteLastQueryContext.codes.length === 1) {
    params.set('focus_code', favoriteLastQueryContext.codes[0]);
  } else {
    params.delete('focus_code');
  }
  params.delete('from');

  const filterEntries = [
    [FAVORITE_FILTER_PARAM_KEYS.level, favoriteFilterState.level],
    [FAVORITE_FILTER_PARAM_KEYS.source, favoriteFilterState.source],
    [FAVORITE_FILTER_PARAM_KEYS.recommendationStatus, favoriteFilterState.recommendationStatus !== 'all' ? favoriteFilterState.recommendationStatus : ''],
    [FAVORITE_FILTER_PARAM_KEYS.annualizedAvailability, favoriteFilterState.annualizedAvailability !== 'all' ? favoriteFilterState.annualizedAvailability : ''],
    [FAVORITE_FILTER_PARAM_KEYS.scoreFloor, favoriteFilterState.scoreFloor],
  ];
  filterEntries.forEach(([key, value]) => {
    if (value) params.set(key, value);
    else params.delete(key);
  });
  if (favoriteViewMode === FAVORITE_VIEW_MODES.mixed) {
    params.set(FAVORITE_VIEW_PARAM_KEY, FAVORITE_VIEW_MODES.mixed);
  } else {
    params.delete(FAVORITE_VIEW_PARAM_KEY);
  }
  if (favoriteDensityMode === FUND_TABLE_DENSITY_MODES.core) {
    params.set(FAVORITE_DENSITY_PARAM_KEY, FUND_TABLE_DENSITY_MODES.core);
  } else {
    params.delete(FAVORITE_DENSITY_PARAM_KEY);
  }
  if (favoriteCompactnessMode !== FAVORITE_COMPACTNESS_MODES.standard) {
    params.set(FAVORITE_COMPACTNESS_PARAM_KEY, favoriteCompactnessMode);
  } else {
    params.delete(FAVORITE_COMPACTNESS_PARAM_KEY);
  }
  const normalizedOptionalColumnIds = normalizeFavoriteOptionalMainColumnIds(favoriteOptionalMainColumnIds);
  if (normalizedOptionalColumnIds.length !== FAVORITE_DEFAULT_OPTIONAL_MAIN_COLUMN_IDS.length) {
    params.set(FAVORITE_OPTIONAL_COLUMNS_PARAM_KEY, normalizedOptionalColumnIds.join(','));
  } else {
    params.delete(FAVORITE_OPTIONAL_COLUMNS_PARAM_KEY);
  }

  const nextQuery = params.toString();
  const nextUrl = `${window.location.pathname}${nextQuery ? `?${nextQuery}` : ''}`;
  window.history.replaceState(null, '', nextUrl);
}

function normalizeFavoriteCode(value) {
  const text = String(value || '').trim();
  return /^\d+$/u.test(text) ? text.padStart(6, '0') : text;
}

function parseFavoriteCodes(text) {
  return [...new Set(
    String(text || '')
      .split(/[\s,，;；]+/u)
      .map((item) => item.trim())
      .filter(Boolean)
      .map((item) => (/^\d+$/.test(item) ? item.padStart(6, '0') : item))
  )];
}

function renderFavoriteQueryBanner(message, className = '') {
  if (!favoriteQueryBanner) return;
  favoriteQueryBanner.className = `info-banner ${className}`.trim();
  favoriteQueryBanner.innerHTML = `
    <strong>查询状态</strong>
    <span>${escapeHtml(message)}</span>
  `;
}

function setFavoriteLoadingState(loading, message = '') {
  if (!favoriteLoadingState) return;
  favoriteLoadingState.classList.toggle('is-hidden', !loading);
  if (message) {
    favoriteLoadingState.innerHTML = `
      <strong>查询状态</strong>
      <span>${escapeHtml(message)}</span>
    `;
  } else {
    favoriteLoadingState.innerHTML = `
      <strong>查询状态</strong>
      <span>正在读取收藏基金评分、风格门槛和对比结果。</span>
    `;
  }
}

function buildRecommendationFocusUrl(code) {
  const currentParams = new URLSearchParams(window.location.search);
  const params = new URLSearchParams({
    focus_code: normalizeFavoriteCode(code),
    from: 'favorites',
  });
  const contextEntries = [
    [RECOMMENDATION_RETURN_CONTEXT_PARAM_KEYS.group, 'group'],
    [RECOMMENDATION_RETURN_CONTEXT_PARAM_KEYS.sortPreset, 'sort_preset'],
    [RECOMMENDATION_RETURN_CONTEXT_PARAM_KEYS.sortColumn, 'sort_column'],
    [RECOMMENDATION_RETURN_CONTEXT_PARAM_KEYS.sortDirection, 'sort_direction'],
    [RECOMMENDATION_RETURN_CONTEXT_PARAM_KEYS.filterQuery, 'filter_query'],
    [RECOMMENDATION_RETURN_CONTEXT_PARAM_KEYS.filterStyle, 'filter_style'],
    [RECOMMENDATION_RETURN_CONTEXT_PARAM_KEYS.filterStability, 'filter_stability'],
    [RECOMMENDATION_RETURN_CONTEXT_PARAM_KEYS.filterQuality, 'filter_quality'],
    [RECOMMENDATION_RETURN_CONTEXT_PARAM_KEYS.filterCompany, 'filter_company'],
    [RECOMMENDATION_RETURN_CONTEXT_PARAM_KEYS.filterFavoriteStatus, 'filter_favorite'],
    [RECOMMENDATION_RETURN_CONTEXT_PARAM_KEYS.filterScoreFloor, 'filter_score_floor'],
    [RECOMMENDATION_RETURN_CONTEXT_PARAM_KEYS.filterAnnualizedAvailability, 'filter_annualized'],
  ];
  contextEntries.forEach(([sourceKey, targetKey]) => {
    const value = String(currentParams.get(sourceKey) || '').trim();
    if (value) params.set(targetKey, value);
  });
  return `/recommendations?${params.toString()}`;
}

function renderFavoriteNameCell(value, row) {
  const name = String(value || '').trim() || '-';
  const code = normalizeFavoriteCode(row?.['基金代码'] || '');
  const inRecommendationPool = String(row?.['是否优质推荐池'] || '').trim() === '是';
  return `
    <div class="table-cell-stack table-cell-stack-inline">
      <span class="table-name" title="${escapeHtml(name)}">${escapeHtml(name)}</span>
      ${inRecommendationPool && code ? `<a class="table-inline-link table-inline-link-compact" href="${escapeHtml(buildRecommendationFocusUrl(code))}">定位</a>` : ''}
    </div>
  `;
}

function renderFavoriteDigestCell(value) {
  const text = String(value || '').trim();
  return `<span class="table-note" title="${escapeHtml(text)}">${escapeHtml(text)}</span>`;
}

function getFavoritePrimaryCode(row) {
  return normalizeFavoriteCode(row?.['基金代码'] || '');
}

function isFavoriteRowExpanded(row) {
  const code = getFavoritePrimaryCode(row);
  return Boolean(code) && favoriteExpandedCodes.has(code);
}

function toggleFavoriteRowExpanded(code) {
  const normalized = normalizeFavoriteCode(code);
  if (!normalized) return;
  if (favoriteExpandedCodes.has(normalized)) favoriteExpandedCodes.delete(normalized);
  else favoriteExpandedCodes.add(normalized);
  renderFavoriteRows(favoriteRows);
}

function setAllFavoriteRowsExpanded(expanded, rowsOverride = null) {
  const next = new Set();
  const targetRows = Array.isArray(rowsOverride) ? rowsOverride : favoriteRows;
  if (expanded) {
    targetRows.forEach((row) => {
      const code = getFavoritePrimaryCode(row);
      if (code) next.add(code);
    });
  }
  favoriteExpandedCodes = next;
  renderFavoriteRows(favoriteRows);
}

function renderFavoriteExpandCell(row) {
  const code = getFavoritePrimaryCode(row);
  const expanded = isFavoriteRowExpanded(row);
  return `
    <button
      type="button"
      class="favorite-expand-toggle ${expanded ? 'is-expanded' : ''}"
      data-compact-expand-key="${escapeHtml(code)}"
      aria-expanded="${expanded ? 'true' : 'false'}"
    >
      ${expanded ? '收起' : '展开'}
    </button>
  `;
}

function formatFavoriteDetailValue(column, value, row) {
  const rendered = renderCell(column, value, row);
  return rendered || '<span class="favorite-detail-value-empty">-</span>';
}

function favoriteRowHasAllPositiveReturns(row) {
  return FAVORITE_POSITIVE_RETURN_FIELDS.every((field) => {
    const value = Number(String(row?.[field] ?? '').replace(/[%％,]/g, '').trim());
    return Number.isFinite(value) && value > 0;
  });
}

function favoritePositiveReturnCellClass(column, _value, row) {
  if (!favoritePositiveReturnHighlightEnabled) return '';
  if (!FAVORITE_POSITIVE_RETURN_FIELDS.includes(column)) return '';
  return favoriteRowHasAllPositiveReturns(row) ? 'favorite-positive-return-highlight' : '';
}

function favoritePositiveReturnToggleMarkup() {
  return `
    <label class="favorite-highlight-toggle">
      <input type="checkbox" data-favorite-positive-highlight="1" ${favoritePositiveReturnHighlightEnabled ? 'checked' : ''} />
      <span>三项收益均为正时标黄</span>
    </label>
  `;
}

function bindFavoritePositiveReturnToggle(root) {
  root?.querySelectorAll('[data-favorite-positive-highlight="1"]').forEach((input) => {
    input.addEventListener('change', () => {
      favoritePositiveReturnHighlightEnabled = Boolean(input.checked);
      localStorage.setItem(
        FAVORITE_POSITIVE_RETURN_HIGHLIGHT_STORAGE_KEY,
        favoritePositiveReturnHighlightEnabled ? '1' : '0',
      );
      renderFavoriteRows(favoriteRows);
    });
  });
}

function renderFavoriteTableToolbar(rows, options = {}) {
  if (!favoriteTableToolbar) return;
  const items = Array.isArray(rows) ? rows : [];
  if (!items.length) {
    favoriteTableToolbar.innerHTML = '';
    return;
  }
  const expandedCount = items.filter((row) => isFavoriteRowExpanded(row)).length;
  const allExpanded = items.length > 0 && expandedCount === items.length;
  const title = options.title || '高密度视图';
  const hint = options.hint || '默认只展示核心决策列，更多字段按行展开查看';
  favoriteTableToolbar.innerHTML = `
    <div class="favorite-table-toolbar">
      <div class="favorite-table-toolbar-copy">
        <strong>${escapeHtml(title)}</strong>
        <span>${escapeHtml(hint)}</span>
      </div>
      <div class="favorite-table-toolbar-actions">
        ${favoritePositiveReturnToggleMarkup()}
        <button type="button" class="secondary" data-favorite-expand-all="1">${allExpanded ? '全部收起' : '全部展开'}</button>
      </div>
    </div>
  `;
  favoriteTableToolbar.querySelector('[data-favorite-expand-all="1"]')?.addEventListener('click', () => {
    setAllFavoriteRowsExpanded(!allExpanded, items);
  });
  bindFavoritePositiveReturnToggle(favoriteTableToolbar);
}

function focusFavoriteTarget(code) {
  const targetCode = normalizeFavoriteCode(code);
  if (!targetCode || !favoriteScoreTable) return false;
  const target = favoriteScoreTable.querySelector(`[data-fund-code="${targetCode}"]`);
  if (!target) return false;
  target.classList.add('is-focus-target');
  target.scrollIntoView({ behavior: 'smooth', block: 'center', inline: 'nearest' });
  return true;
}

function renderFavoriteHistory(records) {
  if (!favoriteHistoryList) return;
  const items = (Array.isArray(records) ? records : []).filter((item) => String(item?.entry_type || 'query').trim() === 'query');
  favoriteHistoryAvailable = true;
  favoriteHistoryQueryCount = items.length;
  renderFavoritesHeroMeta();
  if (!items.length) {
    favoriteHistoryList.innerHTML = '<div class="empty-state-inline">暂无历史查询</div>';
    return;
  }
  favoriteHistoryList.innerHTML = items.map((item) => `
    <article class="favorite-history-item">
      <button type="button" class="favorite-history-load" data-codes="${escapeHtml((item.codes || []).join(','))}" data-label="${escapeHtml(item.label || '')}" title="载入这条查询的名称与基金代码">
        <span class="favorite-history-heading">
          <strong>${escapeHtml(item.label || '未命名查询')}</strong>
          <span class="favorite-history-load-hint">载入</span>
        </span>
        <span class="favorite-history-meta">
          <span>${escapeHtml(String(item.codes?.length || 0))} 只基金</span>
          <span>${escapeHtml(formatDateTime(item.updated_at || item.created_at || ''))}</span>
        </span>
      </button>
      <details class="favorite-history-codes">
        <summary>基金代码</summary>
        <p class="favorite-history-code-list">${escapeHtml((item.codes || []).join('、'))}</p>
      </details>
    </article>
  `).join('');
  favoriteHistoryList.querySelectorAll('.favorite-history-load').forEach((button) => {
    button.addEventListener('click', () => {
      favoriteQueryLabel.value = button.dataset.label || '';
      favoriteCodesInput.value = String(button.dataset.codes || '').split(',').join('\n');
    });
  });
}

function renderFavoriteHistoryFailure(message, onRetry = refreshFavoriteHistory) {
  favoriteHistoryAvailable = false;
  favoriteHistoryQueryCount = 0;
  renderFavoritesHeroMeta();
  renderRetryableEmptyState(favoriteHistoryList, {
    title: '历史查询加载失败',
    message: message || '请稍后重试',
    actionLabel: '重新加载',
    onRetry,
  });
}

function renderFavoriteQueryFailure(message, onRetry) {
  favoriteExportTables = [];
  syncFavoriteExportState();
  favoriteScoreMeta.textContent = '查询失败';
  favoriteScoreStats.innerHTML = '';
  favoriteScoreNarrative.innerHTML = '';
  favoriteHeroSnapshot = {
    ...favoriteHeroSnapshot,
    mode: 'failed',
    matchedCount: 0,
    visibleCount: 0,
    styleCount: 0,
  };
  renderFavoritesHeroMeta();
  if (favoriteFilterBar) favoriteFilterBar.innerHTML = '';
  if (favoriteFilterDetails) favoriteFilterDetails.hidden = true;
  if (favoriteContextSummaryNote) favoriteContextSummaryNote.textContent = '';
  if (favoriteViewBar) favoriteViewBar.innerHTML = '';
  if (favoriteStyleThresholdSummary) favoriteStyleThresholdSummary.innerHTML = '';
  if (favoriteListHints) favoriteListHints.innerHTML = '';
  if (favoriteTableToolbar) favoriteTableToolbar.innerHTML = '';
  favoriteSourceLegend.innerHTML = '';
  renderRetryableEmptyState(favoriteScoreTable, {
    title: '收藏基金查询失败',
    message: message || '请稍后重试',
    actionLabel: '重新查询',
    onRetry,
  });
}

function renderFavoritesHeroMeta(snapshot = favoriteHeroSnapshot) {
  if (!favoritesHeroMeta) return;
  const mode = String(snapshot?.mode || 'idle').trim() || 'idle';
  const stateMap = {
    idle: {
      value: '待查询',
      hint: '输入基金代码后查询',
    },
    loading: {
      value: '查询中',
      hint: `正在处理 ${formatNumber(snapshot?.requestedCount || 0)} 只`,
    },
    ready: {
      value: '已返回',
      hint: snapshot?.runId ? `版本 ${snapshot.runId}` : '最新结果',
    },
    failed: {
      value: '查询失败',
      hint: '请稍后重试',
    },
  };
  const state = stateMap[mode] || stateMap.idle;
  favoritesHeroMeta.innerHTML = [
    buildStatCard('当前状态', state.value, state.hint),
    buildStatCard('当前输入', formatNumber(snapshot?.requestedCount), '本次查询'),
    buildStatCard(
      '命中基金',
      mode === 'ready' ? formatNumber(snapshot?.matchedCount) : '-',
      mode === 'ready' ? '命中项目样本' : '等待结果'
    ),
    buildStatCard(
      '当前可见',
      mode === 'ready' ? formatNumber(snapshot?.visibleCount) : '-',
      mode === 'ready'
        ? `覆盖风格 ${formatNumber(snapshot?.styleCount)} 个`
        : '筛选后列表'
    ),
    buildStatCard(
      '历史查询',
      favoriteHistoryAvailable ? formatNumber(favoriteHistoryQueryCount) : '异常',
      favoriteHistoryAvailable ? '可复用记录' : '接口暂不可用'
    ),
  ].join('');
}

function buildFavoriteFilterOptions(rows) {
  const items = Array.isArray(rows) ? rows : [];
  return {
    levelOptions: [...new Set(items.map((item) => String(item['当前层级'] || '').trim()).filter(Boolean))],
    sourceOptions: [...new Set(items.map((item) => String(item['对比来源'] || '').trim()).filter(Boolean))],
  };
}

function filterFavoriteRows(rows, filterState) {
  const items = Array.isArray(rows) ? rows : [];
  return items.filter((row) => {
    const level = String(row['当前层级'] || '').trim();
    const source = String(row['对比来源'] || '').trim();
    const recommendationStatus = String(row['是否优质推荐池'] || '').trim();
    const annualized = String(row['平台成立以来收益年化(%)'] ?? '').trim();
    const score = parseMetricDisplayNumber(row['正式投资评分']);

    if (filterState.level && level !== filterState.level) return false;
    if (filterState.source && source !== filterState.source) return false;
    if (filterState.recommendationStatus === 'recommendation' && recommendationStatus !== '是') return false;
    if (filterState.recommendationStatus === 'non_recommendation' && recommendationStatus === '是') return false;
    if (filterState.annualizedAvailability === 'available' && !annualized) return false;
    if (filterState.annualizedAvailability === 'missing' && annualized) return false;
    if (filterState.scoreFloor) {
      const floor = Number(filterState.scoreFloor);
      if (Number.isFinite(floor) && (!Number.isFinite(score) || score < floor)) return false;
    }
    return true;
  });
}

function normalizeFavoriteStyle(value) {
  return normalizeFundStyleLabel(value);
}

function normalizeFavoriteStyleThresholds(entries) {
  return normalizeFundStyleThresholdEntries(entries);
}

function buildFavoriteStyleThresholdMap(entries) {
  return buildFundStyleThresholdMap(entries);
}

function getFavoriteVisibleStyleThresholds(rows) {
  return getVisibleFundStyleThresholds(rows, favoriteStyleThresholds);
}

function formatFavoriteThresholdNumber(value, digits = 2, suffix = '') {
  return formatFundStyleThresholdNumber(value, digits, suffix);
}

function getFavoriteReferenceMetric(row, field) {
  const referencePayload = row?.['__style_metric_references'];
  if (!referencePayload || typeof referencePayload !== 'object') return null;
  const metrics = referencePayload.metrics && typeof referencePayload.metrics === 'object'
    ? referencePayload.metrics
    : {};
  const metric = metrics[field];
  return metric && typeof metric === 'object' ? metric : null;
}

function buildFavoriteVisibleHeaderReferenceMap(rows) {
  return buildVisibleStyleMetricHeaderReferenceMap(rows, FAVORITE_HEADER_REFERENCE_FIELDS);
}

function getFavoriteHeaderDisplayLabel(column) {
  const normalized = String(column || '').trim();
  return FAVORITE_HEADER_LABEL_OVERRIDES[normalized] || normalized;
}

function buildFavoriteHeaderReferenceMarkup(column, referenceSummaryMap = {}, options = {}) {
  return buildStyleMetricHeaderReferenceMarkup(column, referenceSummaryMap, {
    ...options,
    labelResolver: getFavoriteHeaderDisplayLabel,
  });
}

function buildFavoriteThresholdMetricRows(entry) {
  return buildFundStyleThresholdMetricRows(entry);
}

function renderFavoriteThresholdCard(entry, options = {}) {
  return renderFundStyleThresholdCard(entry, options);
}

function renderFavoriteViewBar(rows) {
  const settingsOpen = favoriteViewBar?.querySelector('.favorite-display-settings')?.open === true;
  const focusedControl = favoriteViewBar?.contains(document.activeElement) ? document.activeElement : null;
  const focusAttribute = [
    'data-fund-style-view-mode', 'data-fund-table-density-mode',
    'data-fund-table-compactness-mode', 'data-favorite-optional-column-id',
  ].find((attribute) => focusedControl?.hasAttribute(attribute));
  const focusValue = focusAttribute ? focusedControl.getAttribute(focusAttribute) : null;
  renderFundStyleViewBar(favoriteViewBar, {
    viewMode: favoriteViewMode,
    densityMode: favoriteDensityMode,
    compactnessMode: favoriteCompactnessMode,
    compactnessOptions: [
      { value: FAVORITE_COMPACTNESS_MODES.ultra, label: '超紧凑' },
      { value: FAVORITE_COMPACTNESS_MODES.standard, label: '标准' },
      { value: FAVORITE_COMPACTNESS_MODES.compare, label: '对比增强' },
    ],
    compactnessNotes: {
      [FAVORITE_COMPACTNESS_MODES.ultra]: '优先压缩收益和辅助列宽，适合快速扫表。',
      [FAVORITE_COMPACTNESS_MODES.standard]: '保持默认均衡布局，兼顾浏览和对比。',
      [FAVORITE_COMPACTNESS_MODES.compare]: '放宽关键收益、规模与出现频次列，适合横向深度比对。',
    },
    rows,
    onChange: (nextMode) => {
      favoriteViewMode = nextMode;
      rerenderFavoriteResults();
    },
    onDensityChange: (nextMode) => {
      favoriteDensityMode = nextMode;
      rerenderFavoriteResults();
    },
    onCompactnessChange: (nextMode) => {
      favoriteCompactnessMode = nextMode;
      persistFavoriteCompactnessMode(nextMode);
      rerenderFavoriteResults();
    },
    groupedNote: `建议先看每个风格的动态门槛，再在同风格内比较正式投资评分与${FUND_COPYWRITING.sinceInceptionAnnualizedLabel}。`,
    mixedNote: '混合视图适合总览，但不同风格间的正式投资评分不宜直接横向解读。',
  });
  const toggleGroup = favoriteViewBar?.querySelector('.favorite-view-toggle-group');
  const note = favoriteViewBar?.querySelector('.favorite-view-note');
  if (toggleGroup) {
    const activeIds = new Set(normalizeFavoriteOptionalMainColumnIds(favoriteOptionalMainColumnIds));
    toggleGroup.insertAdjacentHTML('beforeend', `
      <div class="favorite-view-toggle favorite-view-toggle-columns">
        <span class="favorite-view-toggle-label">主表辅助列</span>
        ${FAVORITE_OPTIONAL_MAIN_COLUMN_OPTIONS.map((item) => `
          <button
            type="button"
            class="favorite-view-toggle-btn ${activeIds.has(item.id) ? 'is-active' : ''}"
            data-favorite-optional-column-id="${escapeHtml(item.id)}"
            aria-pressed="${activeIds.has(item.id) ? 'true' : 'false'}"
          >${escapeHtml(item.label)}</button>
        `).join('')}
      </div>
    `);
    toggleGroup.querySelectorAll('[data-favorite-optional-column-id]').forEach((button) => {
      button.addEventListener('click', () => {
        const columnId = String(button.dataset.favoriteOptionalColumnId || '').trim();
        if (!columnId) return;
        const nextIds = new Set(normalizeFavoriteOptionalMainColumnIds(favoriteOptionalMainColumnIds));
        if (nextIds.has(columnId)) nextIds.delete(columnId);
        else nextIds.add(columnId);
        favoriteOptionalMainColumnIds = normalizeFavoriteOptionalMainColumnIds([...nextIds]);
        persistFavoriteOptionalMainColumnIds(favoriteOptionalMainColumnIds);
        rerenderFavoriteResults();
      });
    });
  }
  if (note) {
    const activeLabels = FAVORITE_OPTIONAL_MAIN_COLUMN_OPTIONS
      .filter((item) => normalizeFavoriteOptionalMainColumnIds(favoriteOptionalMainColumnIds).includes(item.id))
      .map((item) => item.label);
    note.insertAdjacentHTML(
      'beforeend',
      `<span>${escapeHtml(`当前主表辅助列：${activeLabels.length ? activeLabels.join('、') : '全部隐藏'}`)}</span>`
    );
  }
  if (toggleGroup && note) {
    const settings = document.createElement('details');
    settings.className = 'compact-disclosure favorite-display-settings';
    settings.open = settingsOpen;
    settings.innerHTML = '<summary>显示设置</summary><div class="favorite-display-settings-body"></div>';
    const body = settings.querySelector('.favorite-display-settings-body');
    Array.from(toggleGroup.children).slice(1).forEach((control) => body.append(control));
    body.append(note);
    favoriteViewBar.append(settings);
    if (focusAttribute) {
      favoriteViewBar.querySelector(`[${focusAttribute}="${CSS.escape(focusValue)}"]`)?.focus({ preventScroll: true });
    }
  }
}

function renderFavoriteStyleThresholdSummary(rows) {
  renderFundStyleThresholdSummary(favoriteStyleThresholdSummary, {
    rows,
    styleThresholds: favoriteStyleThresholds,
    globalStyleThreshold: favoriteGlobalStyleThreshold,
    missingStyleThresholds: favoriteMissingStyleThresholds,
    viewMode: favoriteViewMode,
  });
  if (favoriteContextSummaryNote) {
    favoriteContextSummaryNote.textContent = favoriteStyleThresholdSummary?.querySelector('.info-banner-warning')
      ? '部分风格门槛待确认' : '';
  }
}

function renderFavoriteFilterBar(rows, filterState, onChange, onReset) {
  if (!favoriteFilterBar) return;
  const focusedControl = favoriteFilterBar.contains(document.activeElement) ? document.activeElement : null;
  const focusedFilter = focusedControl?.dataset.filterId;
  const focusedReset = focusedControl?.dataset.filterReset === '1';
  const { levelOptions, sourceOptions } = buildFavoriteFilterOptions(rows);
  const buildSelect = (id, label, value, options, placeholder) => `
    <label class="recommendation-filter-field">
      <span>${escapeHtml(label)}</span>
      <select data-filter-id="${escapeHtml(id)}">
        <option value="">${escapeHtml(placeholder)}</option>
        ${options.map((item) => `
          <option value="${escapeHtml(item)}" ${item === value ? 'selected' : ''}>${escapeHtml(item)}</option>
        `).join('')}
      </select>
    </label>
  `;
  favoriteFilterBar.innerHTML = `
    <div class="recommendation-filter-grid">
      ${buildSelect('level', '当前层级', filterState.level, levelOptions, '全部层级')}
      ${buildSelect('source', '对比来源', filterState.source, sourceOptions, '全部来源')}
      <label class="recommendation-filter-field">
        <span>推荐归属</span>
        <select data-filter-id="recommendationStatus">
          <option value="all" ${filterState.recommendationStatus === 'all' ? 'selected' : ''}>全部</option>
          <option value="recommendation" ${filterState.recommendationStatus === 'recommendation' ? 'selected' : ''}>仅优质推荐池</option>
          <option value="non_recommendation" ${filterState.recommendationStatus === 'non_recommendation' ? 'selected' : ''}>仅非推荐池</option>
        </select>
      </label>
      <label class="recommendation-filter-field">
        <span>${escapeHtml(FUND_COPYWRITING.sinceInceptionAnnualizedLabel)}</span>
        <select data-filter-id="annualizedAvailability">
          <option value="all" ${filterState.annualizedAvailability === 'all' ? 'selected' : ''}>全部</option>
          <option value="available" ${filterState.annualizedAvailability === 'available' ? 'selected' : ''}>仅显示可用</option>
          <option value="missing" ${filterState.annualizedAvailability === 'missing' ? 'selected' : ''}>仅显示待确认</option>
        </select>
      </label>
      <label class="recommendation-filter-field">
        <span>评分下限</span>
        <select data-filter-id="scoreFloor">
          <option value="" ${!filterState.scoreFloor ? 'selected' : ''}>不限</option>
          <option value="90" ${filterState.scoreFloor === '90' ? 'selected' : ''}>90+</option>
          <option value="85" ${filterState.scoreFloor === '85' ? 'selected' : ''}>85+</option>
          <option value="80" ${filterState.scoreFloor === '80' ? 'selected' : ''}>80+</option>
          <option value="75" ${filterState.scoreFloor === '75' ? 'selected' : ''}>75+</option>
        </select>
      </label>
    </div>
    <div class="recommendation-filter-actions">
      <button type="button" class="secondary" data-filter-reset="1">重置筛选</button>
    </div>
  `;
  favoriteFilterBar.querySelectorAll('select[data-filter-id]').forEach((element) => {
    element.addEventListener('change', () => {
      const id = element.dataset.filterId || '';
      if (!id) return;
      onChange(id, element.value);
    });
  });
  favoriteFilterBar.querySelector('[data-filter-reset="1"]')?.addEventListener('click', onReset);
  if (favoriteFilterDetails) favoriteFilterDetails.hidden = false;
  if (favoriteFilterSummary) favoriteFilterSummary.textContent = favoriteFilterDescriptions().join('；') || '全部结果';
  if (focusedFilter) favoriteFilterBar.querySelector(`[data-filter-id="${CSS.escape(focusedFilter)}"]`)?.focus({ preventScroll: true });
  else if (focusedReset) favoriteFilterBar.querySelector('[data-filter-reset="1"]')?.focus({ preventScroll: true });
}

function renderFavoriteListHints(rows, totalRows) {
  if (!favoriteListHints) return;
  const items = Array.isArray(rows) ? rows : [];
  if (!items.length && !totalRows) {
    favoriteListHints.innerHTML = '';
    return;
  }
  const levelCounts = {
    优质推荐池: items.filter((item) => String(item['当前层级'] || '').trim() === '优质推荐池').length,
    正式主榜: items.filter((item) => String(item['当前层级'] || '').trim() === '正式主榜').length,
    项目外样本: items.filter((item) => String(item['当前层级'] || '').trim() === '项目外样本').length,
    未收录: items.filter((item) => String(item['当前层级'] || '').trim() === '未收录').length,
  };
  const annualizedAvailableCount = items.filter((item) => String(item['平台成立以来收益年化(%)'] ?? '').trim()).length;
  const styleCount = new Set(items.map((item) => normalizeFavoriteStyle(item['债基风格标签'])).filter((style) => style && style !== '未标注风格')).size;
  const hints = [
    `当前可见 ${formatNumber(items.length)} / ${formatNumber(totalRows || 0)} 只`,
    `覆盖风格 ${formatNumber(styleCount)} 个`,
    `${FUND_COPYWRITING.sinceInceptionAnnualizedAvailableCountLabel} ${formatNumber(annualizedAvailableCount)} 只`,
    favoriteDensityMode === FUND_TABLE_DENSITY_MODES.expanded ? '当前扩展字段主表' : '当前核心字段主表',
    favoriteCompactnessMode === FAVORITE_COMPACTNESS_MODES.ultra ? '紧凑度：超紧凑' : favoriteCompactnessMode === FAVORITE_COMPACTNESS_MODES.compare ? '紧凑度：对比增强' : '紧凑度：标准',
    `辅助列：${buildFavoriteOptionalMainColumns().length || 0} 项`,
    `优质推荐池 ${formatNumber(levelCounts['优质推荐池'])} 只`,
    `正式主榜 ${formatNumber(levelCounts['正式主榜'])} 只`,
  ];
  if (levelCounts['项目外样本'] > 0) hints.push(`项目外样本 ${formatNumber(levelCounts['项目外样本'])} 只`);
  if (levelCounts['未收录'] > 0) hints.push(`未收录 ${formatNumber(levelCounts['未收录'])} 只`);
  renderSummaryHintChips(favoriteListHints, hints.slice(0, 5));
}

function buildFavoriteTableOptions(rows, options = {}) {
  const activeOptionalColumns = buildFavoriteOptionalMainColumns();
  const headerReferenceMap = options.headerReferenceMap && typeof options.headerReferenceMap === 'object'
    ? options.headerReferenceMap
    : buildFavoriteVisibleHeaderReferenceMap(rows);
  const allowMixedRange = options.allowMixedReferenceRange === true;
  const activeCoreColumns = mergeFavoriteMainTableColumns(
    favoriteDensityMode === FUND_TABLE_DENSITY_MODES.expanded
      ? FAVORITE_EXPANDED_BASE_TABLE_COLUMNS
      : FAVORITE_CORE_BASE_TABLE_COLUMNS,
    activeOptionalColumns,
  );
  return {
    coreColumns: activeCoreColumns,
    detailGroups: FAVORITE_DETAIL_GROUPS,
    tableClass: 'data-table recommendation-table favorite-score-table favorite-compact-table compact-expand-table',
    tableAttributes: {
      'data-density-mode': favoriteDensityMode,
      'data-compactness-mode': favoriteCompactnessMode,
      'data-optional-column-count': String(activeOptionalColumns.length),
    },
    wrapClass: 'recommendation-table-wrap favorite-score-table-wrap favorite-compact-wrap compact-expand-wrap',
    toolbarContainer: options.toolbarContainer || null,
    toolbarClass: 'favorite-table-toolbar compact-table-toolbar',
    toolbarTitle: options.toolbarTitle || (favoriteDensityMode === FUND_TABLE_DENSITY_MODES.expanded ? '扩展视图' : '高密度视图'),
    toolbarHint: options.toolbarHint || (favoriteDensityMode === FUND_TABLE_DENSITY_MODES.expanded
      ? '主表直接展开更多收益、规模和出现频次字段，适合深度比对。'
      : '默认只展示核心决策列，更多字段按行展开查看'),
    showToolbar: options.showToolbar !== false,
    mobileCardColumns: buildFundMobileCardColumns({
      identityExtraColumns: ['当前层级', '建议动作'],
      trailingColumns: activeOptionalColumns,
    }),
    headerContentRenderers: {
      '正式投资评分': () => buildFavoriteHeaderReferenceMarkup('正式投资评分', headerReferenceMap, { allowMixedRange }),
      '1年-年化收益(%)': () => buildFavoriteHeaderReferenceMarkup('1年-年化收益(%)', headerReferenceMap, { allowMixedRange }),
      '平台成立以来收益年化(%)': () => buildFavoriteHeaderReferenceMarkup('平台成立以来收益年化(%)', headerReferenceMap, { allowMixedRange }),
      '近1周(%)': () => buildFavoriteHeaderReferenceMarkup('近1周(%)', headerReferenceMap, { allowMixedRange }),
      '近1月(%)': () => buildFavoriteHeaderReferenceMarkup('近1月(%)', headerReferenceMap, { allowMixedRange }),
      '近3月(%)': () => buildFavoriteHeaderReferenceMarkup('近3月(%)', headerReferenceMap, { allowMixedRange }),
      '近6月(%)': () => buildFavoriteHeaderReferenceMarkup('近6月(%)', headerReferenceMap, { allowMixedRange }),
      '1年-夏普比率': () => buildFavoriteHeaderReferenceMarkup('1年-夏普比率', headerReferenceMap, { allowMixedRange }),
      '1年-最大回撤(%)': () => buildFavoriteHeaderReferenceMarkup('1年-最大回撤(%)', headerReferenceMap, { allowMixedRange }),
      '1年-波动率': () => buildFavoriteHeaderReferenceMarkup('1年-波动率', headerReferenceMap, { allowMixedRange }),
    },
    sortableColumns: FAVORITE_SORTABLE_COLUMNS,
    initialSortDirections: FAVORITE_INITIAL_SORT_DIRECTIONS,
    sortState: favoriteSortState,
    sortAccessors: {
      '出现次数&出现频率(%)': (row) => String(row['出现次数&出现频率(%)'] || '').split('次')[0],
    },
    rowKey: (row) => getFavoritePrimaryCode(row),
    expandedRows: favoriteExpandedCodes,
    rowClassName: (row) => {
      const source = String(row['对比来源'] || '').trim();
      const code = getFavoritePrimaryCode(row);
      const classes = [];
      if (source === '共同命中') classes.push('favorite-row-shared');
      else if (source === '优质推荐池') classes.push('favorite-row-recommendation');
      else classes.push('favorite-row-user');
      if (FAVORITE_FOCUS_CODE && code === FAVORITE_FOCUS_CODE) classes.push('is-focus-target');
      return classes.join(' ');
    },
    rowAttributes: (row) => ({
      'data-fund-code': getFavoritePrimaryCode(row),
    }),
    cellRenderers: {
      基金名称: (value, row) => renderFavoriteNameCell(value, row),
      债基风格标签: (value, row) => renderStyleBenchmarkTooltipCell(value, row),
      正式投资评分: (value, row) => renderFormalScoreTooltipCell(value, row, {
        code: getFavoritePrimaryCode(row),
      }),
      实时估值: (value, row) => renderRealtimeValuationTooltipCell(value, row, {
        columnLabel: '实时估值',
        showEstimatedNav: true,
        showLatestNav: true,
      }),
      '平台成立以来收益年化(%)': (value, row) => renderSinceInceptionAnnualizedTooltipCell(value, row),
      '近1周(%)': (value, row) => renderFavoritePeriodicReturnTooltipCell(value, row, {
        field: '近1周(%)',
        detailKey: 'weekly_recent',
        title: `近1周涨跌 ${formatSignedPercentOrDash(value)}`,
        description: '下方展示该基金近两个月每一周的真实净值涨跌率，并给出周收益总波动率。',
        emptyText: '近两月周收益明细不足，暂无法展示',
      }),
      '近1月(%)': (value, row) => renderFavoritePeriodicReturnTooltipCell(value, row, {
        field: '近1月(%)',
        detailKey: 'monthly_ytd',
        title: `近1月涨跌 ${formatSignedPercentOrDash(value)}`,
        description: '下方展示该基金自当年起每个月的真实净值涨跌率，并给出月化收益总波动率。',
        emptyText: '当年月度收益明细不足，暂无法展示',
        referenceFields: ['近1月(%)', '1年-波动率'],
      }),
      '近3月(%)': (value, row) => renderFavoritePeriodicReturnTooltipCell(value, row, {
        field: '近3月(%)',
        title: `近3月涨跌 ${formatSignedPercentOrDash(value)}`,
        description: '该列展示正式快照近三个月收益，并与同风格统一数据基准比较；不使用实时估值补齐或改写。',
        emptyText: '该指标当前只展示正式快照值和真实基准线，不额外生成三个月明细。',
        referenceFields: ['近3月(%)', '1年-年化收益(%)', '1年-夏普比率', '1年-最大回撤(%)', '1年-波动率'],
      }),
      '近6月(%)': (value, row) => renderFavoritePeriodicReturnTooltipCell(value, row, {
        field: '近6月(%)',
        title: `近6月涨跌 ${formatSignedPercentOrDash(value)}`,
        description: '该列优先展示实时 API 返回的近六个月收益；同风格正式参考线仍来自当前分析快照。',
        emptyText: '实时 API 当前未返回近六个月收益。',
        referenceFields: ['近6月(%)', '1年-年化收益(%)', '1年-夏普比率', '1年-最大回撤(%)', '1年-波动率'],
      }),
      推荐解读: (value) => renderFavoriteDigestCell(value),
      详情: (_, row) => renderFavoriteExpandCell(row),
    },
    cellClassName: favoritePositiveReturnCellClass,
    toolbarActionsMarkup: favoritePositiveReturnToggleMarkup(),
    detailValueFormatter: formatFavoriteDetailValue,
    onDetailRendered: (detail) => bindFavoriteMetricTooltipHydration(detail),
    onSort: (nextSortState) => {
      favoriteSortState = nextSortState;
      renderFavoriteRows(favoriteRows);
    },
    onToggleExpand: (code) => {
      toggleFavoriteRowExpanded(code);
    },
    onToggleExpandAll: (expanded) => {
      setAllFavoriteRowsExpanded(expanded, rows);
    },
  };
}

function renderFavoriteGroupedRows(rows) {
  const items = Array.isArray(rows) ? rows : [];
  const styleThresholdMap = buildFavoriteStyleThresholdMap(favoriteStyleThresholds);
  const groupMap = new Map();
  items.forEach((row) => {
    const style = normalizeFavoriteStyle(row?.['债基风格标签']);
    if (!groupMap.has(style)) groupMap.set(style, []);
    groupMap.get(style).push(row);
  });
  const orderedStyles = [
    ...getFavoriteVisibleStyleThresholds(items).map((entry) => String(entry['债基风格标签'] || '').trim()),
    ...[...groupMap.keys()].filter((style) => !styleThresholdMap.has(style)).sort((a, b) => a.localeCompare(b, 'zh-CN')),
  ];
  renderFundStyleGroupedTables(favoriteScoreTable, items, {
    orderedStyles,
    renderSectionSupplement: (styleRows, style) => {
      const thresholdEntry = styleThresholdMap.get(style);
      return thresholdEntry
        ? `<details class="compact-disclosure favorite-style-disclosure">
            <summary aria-label="${escapeHtml(style)}评分门槛">评分门槛</summary>
            ${renderFavoriteThresholdCard(thresholdEntry)}
          </details>`
        : `
          <div class="info-banner info-banner-warning favorite-style-threshold-missing">
            <strong>${escapeHtml(style)}</strong>
            <span>当前最新 workflow 未返回该风格的独立动态门槛摘要，建议结合全局基准谨慎解读。</span>
          </div>
        `;
    },
    renderTable: (container, styleRows) => {
      const options = buildFavoriteTableOptions(styleRows, {
        showToolbar: false,
        headerReferenceMap: buildFavoriteVisibleHeaderReferenceMap(styleRows),
        allowMixedReferenceRange: false,
      });
      const result = renderCompactExpandableTable(container, styleRows, options);
      favoriteExportTables.push({ name: normalizeFavoriteStyle(styleRows[0]?.['债基风格标签']), result, options, container });
    },
  });
}

function renderFavoriteRows(rows) {
  favoriteExportTables = [];
  syncFavoriteExportState();
  favoriteRows = Array.isArray(rows) ? rows : [];
  if (!favoriteRows.length) {
    renderFavoriteTableToolbar(favoriteRows);
    const hasActiveFilters = favoriteHasActiveFilters(favoriteFilterState);
    if (favoriteAllRows.length > 0 && hasActiveFilters) {
      favoriteScoreTable.innerHTML = `
        <div class="empty-state-inline recommendation-empty-state">
          <strong>筛选后暂无匹配基金</strong>
          <span>${escapeHtml(`当前列表 ${formatNumber(favoriteAllRows.length)} 只，筛选后 0 只。`)}</span>
          <small>建议重置部分筛选条件，或切换视图继续查看。</small>
          <div class="recommendation-empty-state-actions">
            <button type="button" class="secondary" data-favorite-empty-action="reset-filters">重置筛选</button>
          </div>
        </div>
      `;
      favoriteScoreTable.querySelector('[data-favorite-empty-action="reset-filters"]')?.addEventListener('click', () => {
        favoriteFilterState = defaultFavoriteFilterState();
        rerenderFavoriteResults();
      });
      return;
    }
    if (favoriteLastPayload) {
      favoriteScoreTable.innerHTML = `
        <div class="empty-state-inline">
          <strong>当前查询暂无命中基金</strong>
          <span>请检查基金代码，或调整输入后重新查询。</span>
        </div>
      `;
      return;
    }
    favoriteScoreTable.innerHTML = `
      <div class="empty-state-inline">
        <strong>暂无查询</strong>
        <span>输入基金代码后开始查询。</span>
      </div>
    `;
    return;
  }
  const toolbarOptions = favoriteViewMode === FAVORITE_VIEW_MODES.grouped
    ? {
        title: favoriteDensityMode === FUND_TABLE_DENSITY_MODES.expanded ? '按风格分组扩展视图' : '按风格分组视图',
        hint: favoriteDensityMode === FUND_TABLE_DENSITY_MODES.expanded
          ? '分组下直接展开更多收益、规模和出现频次字段，适合逐风格深度对比。'
          : '先看每个风格的动态门槛，再在同风格内部比较正式投资评分和风险收益指标。',
      }
    : {
        title: favoriteDensityMode === FUND_TABLE_DENSITY_MODES.expanded ? '扩展混合视图' : '高密度混合视图',
        hint: favoriteDensityMode === FUND_TABLE_DENSITY_MODES.expanded
          ? '适合直接横向查看更多关键字段，但不同风格间评分仍不宜直接横向比较。'
          : '适合总览全部收藏基金，但不同风格间的正式投资评分不宜直接横向比较。',
      };
  if (favoriteViewMode === FAVORITE_VIEW_MODES.grouped) {
    renderFavoriteTableToolbar(favoriteRows, toolbarOptions);
    renderFavoriteGroupedRows(favoriteRows);
  } else {
    const options = buildFavoriteTableOptions(favoriteRows, {
      toolbarContainer: favoriteTableToolbar,
      toolbarTitle: toolbarOptions.title,
      toolbarHint: toolbarOptions.hint,
      headerReferenceMap: buildFavoriteVisibleHeaderReferenceMap(favoriteRows),
      allowMixedReferenceRange: false,
    });
    const result = renderCompactExpandableTable(favoriteScoreTable, favoriteRows, options);
    favoriteExportTables.push({ name: '收藏基金', result, options, container: favoriteScoreTable });
  }
  if (FAVORITE_FOCUS_CODE) {
    window.requestAnimationFrame(() => {
      if (focusFavoriteTarget(FAVORITE_FOCUS_CODE)) {
        renderFavoriteQueryBanner(`已定位基金 ${FAVORITE_FOCUS_CODE}`, 'info-banner-success');
      }
    });
  }
  bindFavoriteMetricTooltipHydration(favoriteScoreTable);
  bindFavoritePositiveReturnToggle(favoriteScoreTable);
  bindFavoritePositiveReturnToggle(favoriteTableToolbar);
  syncFavoriteExportState();
}

function syncFavoriteExportState() {
  if (!favoriteExportBtn) return;
  favoriteExportBtn.disabled = favoriteExportBusy || favoriteQuerySubmitBtn.disabled
    || !favoriteExportTables.some((table) => table.result.sortedRows.length);
  favoriteExportBtn.textContent = favoriteExportBusy ? '正在导出…' : '导出 Excel';
}

function favoriteFilterDescriptions() {
  return [
    favoriteFilterState.level && `层级：${favoriteFilterState.level}`,
    favoriteFilterState.source && `来源：${favoriteFilterState.source}`,
    favoriteFilterState.recommendationStatus !== 'all' && `推荐关系：${favoriteFilterState.recommendationStatus === 'recommendation' ? '优质推荐池' : '非推荐池'}`,
    favoriteFilterState.annualizedAvailability !== 'all' && `成立年化：${favoriteFilterState.annualizedAvailability === 'available' ? '有数据' : '缺数据'}`,
    favoriteFilterState.scoreFloor && `评分下限：${favoriteFilterState.scoreFloor}`,
  ].filter(Boolean);
}

function buildFavoriteExportSnapshot() {
  const filters = favoriteFilterDescriptions();
  return FavoriteExcelExport.buildSnapshot(favoriteExportTables, {
    查询名称: favoriteLastQueryContext?.label || '收藏基金',
    查询完成时间: favoriteLastQueryContext?.completedAt
      ? `${formatDateTime(favoriteLastQueryContext.completedAt)} +08:00` : '',
    分析版本: String(favoriteLastPayload?.run_id || ''),
    视图: favoriteViewMode === FAVORITE_VIEW_MODES.grouped ? '按风格分组' : '混合列表',
    排序: favoriteSortState.column ? `${getFavoriteHeaderDisplayLabel(favoriteSortState.column)} / ${favoriteSortState.direction === 'desc' ? '降序' : '升序'}` : '查询顺序',
    筛选: filters.join('；') || '全部结果',
    数据来源: payloadSourceContractSummary(favoriteLastPayload),
  }, getFavoriteHeaderDisplayLabel);
}

async function exportFavoriteQuery() {
  if (!favoriteExportBtn || favoriteExportBtn.disabled) return;
  favoriteExportBusy = true;
  syncFavoriteExportState();
  favoriteExportStatus.textContent = '';
  try {
    const snapshot = buildFavoriteExportSnapshot();
    await FavoriteExcelExport.download(snapshot);
    const count = snapshot.tables.reduce((sum, table) => sum + table.rows.length, 0);
    favoriteExportStatus.textContent = `已导出 ${count} 只`;
  } catch (error) {
    favoriteExportStatus.textContent = error?.name === 'AbortError' ? '导出超时，请重试' : (error?.message || '导出失败，请重试');
  } finally {
    favoriteExportBusy = false;
    syncFavoriteExportState();
  }
}

function renderFavoriteSourceLegend(rows) {
  if (!favoriteSourceLegend) return;
  const items = Array.isArray(rows) ? rows : [];
  const sourceCounts = {
    用户输入: items.filter((item) => String(item['对比来源'] || '').trim() === '用户输入').length,
    共同命中: items.filter((item) => String(item['对比来源'] || '').trim() === '共同命中').length,
    优质推荐池: items.filter((item) => String(item['对比来源'] || '').trim() === '优质推荐池').length,
  };
  favoriteSourceLegend.innerHTML = [
    `<span class="favorite-source-chip favorite-source-chip-user">用户输入：${escapeHtml(formatNumber(sourceCounts['用户输入'] || 0))} 只</span>`,
    `<span class="favorite-source-chip favorite-source-chip-shared">共同命中：${escapeHtml(formatNumber(sourceCounts['共同命中'] || 0))} 只</span>`,
    `<span class="favorite-source-chip favorite-source-chip-recommendation">仅优质推荐池：${escapeHtml(formatNumber(sourceCounts['优质推荐池'] || 0))} 只</span>`,
    '<span class="favorite-source-chip">用户输入基金的名称、净值、规模、经理、区间收益和估值优先使用实时 API；评分与风险指标使用正式分析快照。</span>',
  ].join('');
}

function renderFavoriteScoreSummary(payload, rows = []) {
  const counts = payload?.counts || {};
  const items = Array.isArray(rows) ? rows : [];
  const visibleCount = items.length;
  const styleCount = new Set(items.map((item) => normalizeFavoriteStyle(item['债基风格标签'])).filter((style) => style && style !== '未标注风格')).size;
  favoriteHeroSnapshot = {
    mode: 'ready',
    runId: payload?.run_id || '',
    requestedCount: payload?.requested_count,
    matchedCount: payload?.matched_count,
    visibleCount,
    styleCount,
  };
  renderFavoritesHeroMeta();
  favoriteScoreMeta.textContent = `输入 ${formatNumber(payload?.requested_count)} 只 · 命中 ${formatNumber(payload?.matched_count)} 只 · 当前显示 ${formatNumber(visibleCount)} 只`;
  favoriteScoreStats.innerHTML = [
    buildStatCard('优质推荐池', formatNumber(counts['优质推荐池']), '直接进入当前推荐页优先池'),
    buildStatCard('主榜与诊断池', formatNumber(sumPresentNumbers(counts['正式主榜'], counts['自动诊断池'])), `正式主榜 ${formatNumber(counts['正式主榜'])} / 自动诊断池 ${formatNumber(counts['自动诊断池'])}`),
    buildStatCard('持有期排除', formatNumber(counts['持有期排除']), '因持有期超过门槛被排除'),
    buildStatCard('项目外与未收录', formatNumber(sumPresentNumbers(counts['项目外样本'], counts['未收录'])), `项目外 ${formatNumber(counts['项目外样本'])} / 未收录 ${formatNumber(counts['未收录'])}`),
    buildStatCard('自动并入推荐池', formatNumber(payload?.auto_appended_recommendation_count), '自动补入当前最新优质基金推荐列表中、但不在用户输入里的基金'),
    buildStatCard('当前可见', formatNumber(visibleCount), '筛选后列表'),
  ].join('');
  favoriteScoreNarrative.innerHTML = `
    <strong>分析来源</strong>
    <span>${escapeHtml(`版本 ${payload?.run_id || '-'} / ${payloadSourceContractSummary(payload)}。实时估值仅作辅助。`)}</span>
  `;
}

function rerenderFavoriteResults() {
  if (!favoriteLastPayload) return;
  syncFavoriteViewUrl();
  renderFavoriteFilterBar(
    favoriteAllRows,
    favoriteFilterState,
    (key, value) => {
      favoriteFilterState = {
        ...favoriteFilterState,
        [key]: value,
      };
      rerenderFavoriteResults();
    },
    () => {
      favoriteFilterState = defaultFavoriteFilterState();
      rerenderFavoriteResults();
    },
  );
  const filteredRows = filterFavoriteRows(favoriteAllRows, favoriteFilterState);
  renderFavoriteViewBar(filteredRows);
  renderFavoriteStyleThresholdSummary(filteredRows);
  renderFavoriteListHints(filteredRows, favoriteAllRows.length);
  renderFavoriteScoreSummary(favoriteLastPayload, filteredRows);
  renderFavoriteSourceLegend(filteredRows);
  renderFavoriteRows(filteredRows);
}

async function refreshFavoriteHistory() {
  const queryGeneration = favoriteQueryGeneration;
  const historyGeneration = ++favoriteHistoryLoadGeneration;
  try {
    const payload = await loadFavoriteFundHistory();
    if (queryGeneration !== favoriteQueryGeneration || historyGeneration !== favoriteHistoryLoadGeneration) return;
    renderFavoriteHistory(payload?.records || []);
  } catch (error) {
    if (queryGeneration !== favoriteQueryGeneration || historyGeneration !== favoriteHistoryLoadGeneration) return;
    renderFavoriteHistoryFailure(error?.message || '历史查询加载失败');
  }
}

async function submitFavoriteQuery(options = {}) {
  const presetCodes = Array.isArray(options.codes) ? options.codes.map((item) => normalizeFavoriteCode(item)).filter(Boolean) : [];
  const codes = presetCodes.length ? [...new Set(presetCodes)] : parseFavoriteCodes(favoriteCodesInput.value);
  const label = typeof options.label === 'string' ? options.label.trim() : String(favoriteQueryLabel.value || '').trim();
  const saveHistory = typeof options.saveHistory === 'boolean' ? options.saveHistory : true;
  if (!codes.length) {
    renderFavoriteQueryBanner('输入代码', 'info-banner-warning');
    return;
  }
  favoriteQueryGeneration += 1;
  const queryGeneration = favoriteQueryGeneration;
  resetFavoriteMetricDetailCache();
  favoriteQuerySubmitBtn.disabled = true;
  favoriteExportTables = [];
  if (favoriteExportStatus) favoriteExportStatus.textContent = '';
  syncFavoriteExportState();
  favoriteHeroSnapshot = {
    ...favoriteHeroSnapshot,
    mode: 'loading',
    requestedCount: codes.length,
    matchedCount: 0,
    visibleCount: 0,
    styleCount: 0,
  };
  renderFavoritesHeroMeta();
  renderFavoriteQueryBanner(`查询中：${codes.length} 只`);
  setFavoriteLoadingState(true, `正在处理 ${codes.length} 只基金，并同步评分摘要与风格门槛。`);
  favoriteScoreMeta.textContent = `查询中 / ${formatNumber(codes.length)} 只`;
  favoriteScoreStats.innerHTML = '';
  favoriteScoreNarrative.innerHTML = `
    <strong>查询状态</strong>
    <span>正在读取基金评分、风格门槛和历史对比结果。</span>
  `;
  if (favoriteFilterBar) favoriteFilterBar.innerHTML = '';
  if (favoriteFilterDetails) favoriteFilterDetails.hidden = true;
  if (favoriteContextSummaryNote) favoriteContextSummaryNote.textContent = '';
  if (favoriteViewBar) favoriteViewBar.innerHTML = '';
  if (favoriteStyleThresholdSummary) favoriteStyleThresholdSummary.innerHTML = '';
  if (favoriteListHints) favoriteListHints.innerHTML = '';
  if (favoriteTableToolbar) favoriteTableToolbar.innerHTML = '';
  favoriteSourceLegend.innerHTML = '';
  favoriteScoreTable.innerHTML = `
    <div class="favorite-query-loading-shell">
      <strong>正在生成收藏基金列表</strong>
      <span>查询完成后会自动展示分组门槛、筛选器和评分明细。</span>
    </div>
  `;
  try {
    const payload = await queryFavoriteFundScores({
      label,
      codes,
      save_history: saveHistory,
      response_mode: 'compact',
      include_recommendation_pool: true,
      include_autostock: true,
    });
    renderFavoriteQueryBanner(
      payload?.unresolved_count
        ? `完成：未命中 ${payload.unresolved_count} 只`
        : '查询完成',
      payload?.unresolved_count ? 'info-banner-warning' : 'info-banner-success',
    );
    favoriteLastQueryContext = {
      codes,
      label,
    };
    favoriteLastPayload = payload || {};
    applyPayloadDataContract(payload);
    favoriteAllRows = attachFavoriteStyleReferencesFromPayload(
      Array.isArray(payload?.rows) ? payload.rows : [],
      payload?.style_metric_references,
    );
    if (payload && typeof payload === 'object') {
      payload.rows = favoriteAllRows;
      payload.funds = favoriteAllRows;
    }
    favoriteStyleThresholds = normalizeFavoriteStyleThresholds(payload?.style_thresholds);
    favoriteGlobalStyleThreshold = payload?.global_style_threshold && typeof payload.global_style_threshold === 'object'
      ? { ...payload.global_style_threshold }
      : {};
    favoriteMissingStyleThresholds = Array.isArray(payload?.missing_style_thresholds)
      ? payload.missing_style_thresholds.map((item) => String(item || '').trim()).filter(Boolean)
      : [];
    favoriteFilterState = options.preserveFilterState ? favoriteFilterState : defaultFavoriteFilterState();
    renderFavoriteHistory(payload?.history || []);
    const allValuationCodes = [...new Set(
      favoriteAllRows
        .map((row) => getFavoritePrimaryCode(row))
        .filter(Boolean),
    )];
    const snapshotValuations = favoriteValuationsFromScoreSnapshot(favoriteAllRows);
    const snapshotCodes = new Set(Object.keys(snapshotValuations));
    const valuationCodes = allValuationCodes.filter((code) => !snapshotCodes.has(code));
    favoriteAllRows = mergeFavoriteRowsWithValuations(favoriteAllRows, snapshotValuations, 'loading');
    payload.rows = favoriteAllRows;
    payload.funds = favoriteAllRows;
    rerenderFavoriteResults();
    if (valuationCodes.length) {
      renderFavoriteQueryBanner(
        `评分已完成；复用 ${snapshotCodes.size} 条评分阶段估值状态，后台补查 ${valuationCodes.length} 只`,
        'info-banner-warning',
      );
      void refreshFavoriteValuationsInBackground(
        valuationCodes,
        allValuationCodes,
        snapshotValuations,
        payload,
        queryGeneration,
      );
    } else {
      const availableCount = Object.values(snapshotValuations).filter((item) => item?.available).length;
      renderFavoriteQueryBanner(
        `评分已完成；评分阶段已校验 ${snapshotCodes.size} 条估值状态，其中 ${availableCount} 只有效估值；其他数据保持缺失或未通过校验`,
        availableCount ? 'info-banner-success' : 'info-banner-warning',
      );
    }
  } catch (error) {
    if (queryGeneration !== favoriteQueryGeneration) return;
    renderFavoriteQueryBanner(error.message || '查询失败', 'info-banner-error');
    renderFavoriteQueryFailure(
      error?.message || '查询失败',
      () => submitFavoriteQuery({
        codes,
        label,
        saveHistory,
        preserveFilterState: true,
      }),
    );
  } finally {
    if (queryGeneration === favoriteQueryGeneration) {
      setFavoriteLoadingState(false);
      favoriteQuerySubmitBtn.disabled = false;
      if (favoriteExportTables.length && favoriteLastQueryContext) {
        favoriteLastQueryContext.completedAt = new Date().toISOString();
      }
      syncFavoriteExportState();
    }
  }
}

function bindFavoritePageEvents() {
  if (favoritePageEventsBound) return;
  favoritePageEventsBound = true;

  favoriteQuerySubmitBtn.addEventListener('click', submitFavoriteQuery);
  favoriteExportBtn?.addEventListener('click', exportFavoriteQuery);
  favoriteCodesClearBtn.addEventListener('click', () => {
    favoriteCodesInput.value = '';
    favoriteQueryLabel.value = '';
  });
  favoriteHistoryRefreshBtn.addEventListener('click', refreshFavoriteHistory);
  favoriteHistoryToggleBtn?.addEventListener('click', () => {
    favoriteHistoryCollapsed = !favoriteHistoryCollapsed;
    applyFavoriteHistoryCollapsedState();
  });
  let compactViewport = isCompactTableViewport();
  window.addEventListener('resize', () => {
    const nextCompactViewport = isCompactTableViewport();
    if (nextCompactViewport === compactViewport) return;
    compactViewport = nextCompactViewport;
    if (favoriteLastPayload && !favoriteQuerySubmitBtn.disabled && favoriteExportTables.length) {
      rerenderFavoriteResults();
    }
  });
}

async function initFavoritesPage() {
  favoriteHeroSnapshot = {
    mode: 'idle',
    runId: '',
    requestedCount: 0,
    matchedCount: 0,
    visibleCount: 0,
    styleCount: 0,
  };
  renderFavoritesHeroMeta();

  favoriteScoreStats.innerHTML = '';
  favoriteScoreNarrative.innerHTML = `
    <strong>待查询</strong>
    <span>输入代码后优先查询实时 API；评分、回撤、夏普等分析指标仍使用正式分析快照。</span>
  `;
  setFavoriteLoadingState(false);
  if (favoriteFilterBar) favoriteFilterBar.innerHTML = '';
  if (favoriteFilterDetails) favoriteFilterDetails.hidden = true;
  if (favoriteContextSummaryNote) favoriteContextSummaryNote.textContent = '';
  if (favoriteViewBar) favoriteViewBar.innerHTML = '';
  if (favoriteStyleThresholdSummary) favoriteStyleThresholdSummary.innerHTML = '';
  if (favoriteListHints) favoriteListHints.innerHTML = '';
  if (favoriteTableToolbar) favoriteTableToolbar.innerHTML = '';
  favoriteSourceLegend.innerHTML = `
    <span class="favorite-source-chip favorite-source-chip-user">用户输入</span>
    <span class="favorite-source-chip favorite-source-chip-shared">共同命中</span>
    <span class="favorite-source-chip favorite-source-chip-recommendation">仅优质推荐池</span>
  `;
  favoriteScoreTable.innerHTML = '<div class="empty-state-inline">暂无查询</div>';

  bindFavoritePageEvents();
  applyFavoriteHistoryCollapsedState();

  const preloadCodes = parseFavoriteCodes(favoriteQueryParams.get('codes') || '');
  const autoQuery = favoriteQueryParams.get('auto_query') === '1';
  const preloadLabel = String(favoriteQueryParams.get('label') || '').trim();
  const saveHistory = favoriteQueryParams.get('save_history') !== '0';
  if (preloadCodes.length) {
    favoriteCodesInput.value = preloadCodes.join('\n');
    if (preloadLabel) favoriteQueryLabel.value = preloadLabel;
  }
  const bootstrapPromise = loadBootstrapSummary().then((payload) => {
    renderRunBadge(favoritesHeaderRunBadge, getBootstrapWorkflow(payload));
    renderHealthBadge(favoritesHeaderHealthBadge, getBootstrapHealth(payload));
  }, () => {
    renderUnavailableBadge(favoritesHeaderRunBadge, '运行状态', '接口暂不可用');
    renderUnavailableBadge(favoritesHeaderHealthBadge, '数据源状态', '接口暂不可用');
  });
  const historyPromise = refreshFavoriteHistory();
  if (autoQuery && preloadCodes.length) {
    await submitFavoriteQuery({
      codes: preloadCodes,
      label: preloadLabel,
      saveHistory,
      preserveFilterState: true,
    });
  }
  await Promise.allSettled([bootstrapPromise, historyPromise]);
}

initFavoritesPage().catch((error) => {
  renderFavoriteQueryBanner(error.message || '加载失败', 'info-banner-error');
});
