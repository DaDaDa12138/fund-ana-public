const trackerPageStats = document.getElementById('trackerPageStats');
const trackerPageMeta = document.getElementById('trackerPageMeta');
const trackerPageNarrative = document.getElementById('trackerPageNarrative');
const trackerPageHighlights = document.getElementById('trackerPageHighlights');
const trackerPageTable = document.getElementById('trackerPageTable');
let trackerCompactViewport = isCompactTableViewport();
let rerenderTrackerForViewport = null;
window.addEventListener('resize', () => {
  const compact = isCompactTableViewport();
  if (compact === trackerCompactViewport) return;
  trackerCompactViewport = compact;
  rerenderTrackerForViewport?.();
});
const trackerViewBar = document.getElementById('trackerViewBar');
const trackerStyleThresholdSummary = document.getElementById('trackerStyleThresholdSummary');
const trackerListHints = document.getElementById('trackerListHints');
const trackerPagePreviewBtn = document.getElementById('trackerPagePreviewBtn');
const trackerPageDownloadBtn = document.getElementById('trackerPageDownloadBtn');
const trackerHeaderRunBadge = document.getElementById('trackerHeaderRunBadge');
const trackerHeaderHealthBadge = document.getElementById('trackerHeaderHealthBadge');
const trackerHeroMeta = document.getElementById('trackerHeroMeta');
const trackerGroupSwitch = document.getElementById('trackerGroupSwitch');
const trackerFilterBar = document.getElementById('trackerFilterBar');
const trackerValuationBanner = document.getElementById('trackerValuationBanner');
const trackerValuationBtn = document.getElementById('trackerValuationBtn');

const GROUP_STABLE = '纯债稳健榜';
const GROUP_ENHANCED = '增强债弹性榜';

const VALUATION_STATE_IDLE = 'idle';
const VALUATION_STATE_LOADING = 'loading';
const VALUATION_STATE_READY = 'ready';
const VALUATION_STATE_FAILED = 'failed';
const TOP_DECILE_METRIC_RULES = {
  '1年-年化收益(%)': 'desc',
  '近1月(%)': 'desc',
  '近3月(%)': 'desc',
  '1年-夏普比率': 'desc',
  '1年-最大回撤(%)': 'asc',
  '1年-波动率': 'asc',
};
const TRACKER_CORE_TABLE_COLUMNS = buildFundCoreColumns({
  metricColumns: ['平台成立以来收益年化(%)', '1年-年化收益(%)', '实时估值'],
  trailingColumns: ['债基风格标签'],
});
const TRACKER_EXPANDED_TABLE_COLUMNS = buildFundCoreColumns({
  metricColumns: ['平台成立以来收益年化(%)', '1年-年化收益(%)', '近3月(%)', '近1月(%)', '实时估值', '1年-夏普比率'],
  trailingColumns: ['基金规模(亿)', '出现次数&出现频率(%)', '最新净值日期', '正式榜分组', '债基风格标签'],
});
const TRACKER_HEADER_REFERENCE_FIELDS = Object.freeze([
  '正式投资评分',
  '1年-年化收益(%)',
  '平台成立以来收益年化(%)',
  '近3月(%)',
  '近1月(%)',
  '1年-夏普比率',
  '1年-最大回撤(%)',
  '1年-波动率',
]);
const TRACKER_HEADER_LABEL_OVERRIDES = Object.freeze({
  '正式投资评分': '正式评分',
  '1年-年化收益(%)': '1年年化',
  '平台成立以来收益年化(%)': '平台成立年化',
  '近3月(%)': '近3月',
  '近1月(%)': '近1月',
  '1年-夏普比率': '1年策略夏普',
  '1年-最大回撤(%)': '最大回撤',
  '1年-波动率': '波动率',
  '基金规模(亿)': '规模',
  '出现次数&出现频率(%)': '频次',
});
const TRACKER_MOBILE_CARD_COLUMNS = buildFundMobileCardColumns({
  trailingColumns: ['基金规模(亿)', '出现次数&出现频率(%)'],
});
const TRACKER_DETAIL_GROUPS = Object.freeze([
  {
    title: FUND_DETAIL_SECTION_TITLES.risk,
    fields: [...FUND_SHARED_RISK_DETAIL_FIELDS, '1年-最长回撤观测数'],
  },
  {
    title: FUND_DETAIL_SECTION_TITLES.time,
    fields: buildFundTimeDetailFields(['近3月(%)', '近1月(%)']),
  },
  {
    title: FUND_DETAIL_SECTION_TITLES.label,
    fields: [...FUND_SHARED_LABEL_DETAIL_FIELDS, '基金规模(亿)', '出现次数&出现频率(%)'],
  },
  {
    title: '经理数据（覆盖为有值/绑定人数）',
    fields: ['经理从业与评分状态', '经理最短从业天数', '经理平均综合评分', '经理评分覆盖', '经理从业数据覆盖'],
  },
]);
const DEFAULT_TRACKER_FILTER_STATE = Object.freeze({
  query: '',
  scoreFloor: '',
  recentStrength: 'all',
  valuationStatus: 'all',
});
const TRACKER_VIEW_PARAM_KEY = 'fund_view';
const TRACKER_VIEW_PARAM_KEYS = Object.freeze({
  group: 'group',
  view: TRACKER_VIEW_PARAM_KEY,
  density: 'density',
  filterQuery: 'filter_query',
  filterScoreFloor: 'filter_score_floor',
  filterRecentStrength: 'filter_recent_strength',
  filterValuationStatus: 'filter_valuation',
});

function groupRows(rows, group) {
  if (!Array.isArray(rows)) return [];
  return rows.filter((item) => (item['正式榜分组'] || GROUP_STABLE) === group);
}

function defaultTrackerFilterState() {
  return {
    query: DEFAULT_TRACKER_FILTER_STATE.query,
    scoreFloor: DEFAULT_TRACKER_FILTER_STATE.scoreFloor,
    recentStrength: DEFAULT_TRACKER_FILTER_STATE.recentStrength,
    valuationStatus: DEFAULT_TRACKER_FILTER_STATE.valuationStatus,
  };
}

function trackerGroupFromUrl() {
  const params = new URLSearchParams(window.location.search);
  const group = String(params.get(TRACKER_VIEW_PARAM_KEYS.group) || '').trim();
  return group === GROUP_ENHANCED ? GROUP_ENHANCED : GROUP_STABLE;
}

function trackerViewModeFromUrl() {
  const params = new URLSearchParams(window.location.search);
  const view = String(params.get(TRACKER_VIEW_PARAM_KEYS.view) || '').trim();
  return view === FUND_STYLE_VIEW_MODES.grouped ? FUND_STYLE_VIEW_MODES.grouped : FUND_STYLE_VIEW_MODES.mixed;
}

function trackerDensityModeFromUrl() {
  const params = new URLSearchParams(window.location.search);
  const density = String(params.get(TRACKER_VIEW_PARAM_KEYS.density) || '').trim();
  return density === FUND_TABLE_DENSITY_MODES.expanded ? FUND_TABLE_DENSITY_MODES.expanded : FUND_TABLE_DENSITY_MODES.core;
}

function trackerFilterStateFromUrl() {
  const params = new URLSearchParams(window.location.search);
  const scoreFloor = String(params.get(TRACKER_VIEW_PARAM_KEYS.filterScoreFloor) || '').trim();
  const recentStrength = String(params.get(TRACKER_VIEW_PARAM_KEYS.filterRecentStrength) || '').trim();
  const valuationStatus = String(params.get(TRACKER_VIEW_PARAM_KEYS.filterValuationStatus) || '').trim();
  return {
    query: String(params.get(TRACKER_VIEW_PARAM_KEYS.filterQuery) || '').trim(),
    scoreFloor: ['', '75', '80', '85', '90'].includes(scoreFloor)
      ? scoreFloor
      : DEFAULT_TRACKER_FILTER_STATE.scoreFloor,
    recentStrength: ['all', 'strong', 'weak'].includes(recentStrength)
      ? recentStrength
      : DEFAULT_TRACKER_FILTER_STATE.recentStrength,
    valuationStatus: ['all', 'available', 'missing'].includes(valuationStatus)
      ? valuationStatus
      : DEFAULT_TRACKER_FILTER_STATE.valuationStatus,
  };
}

function syncTrackerViewUrl({ activeGroup, filterState, viewMode, densityMode }) {
  const params = new URLSearchParams(window.location.search);
  if (activeGroup === GROUP_ENHANCED) {
    params.set(TRACKER_VIEW_PARAM_KEYS.group, GROUP_ENHANCED);
  } else {
    params.delete(TRACKER_VIEW_PARAM_KEYS.group);
  }
  if (viewMode === FUND_STYLE_VIEW_MODES.grouped) {
    params.set(TRACKER_VIEW_PARAM_KEYS.view, FUND_STYLE_VIEW_MODES.grouped);
  } else {
    params.delete(TRACKER_VIEW_PARAM_KEYS.view);
  }
  if (densityMode === FUND_TABLE_DENSITY_MODES.expanded) {
    params.set(TRACKER_VIEW_PARAM_KEYS.density, FUND_TABLE_DENSITY_MODES.expanded);
  } else {
    params.delete(TRACKER_VIEW_PARAM_KEYS.density);
  }
  const entries = [
    [TRACKER_VIEW_PARAM_KEYS.filterQuery, filterState?.query || ''],
    [TRACKER_VIEW_PARAM_KEYS.filterScoreFloor, filterState?.scoreFloor || ''],
    [TRACKER_VIEW_PARAM_KEYS.filterRecentStrength, filterState?.recentStrength !== 'all' ? filterState?.recentStrength : ''],
    [TRACKER_VIEW_PARAM_KEYS.filterValuationStatus, filterState?.valuationStatus !== 'all' ? filterState?.valuationStatus : ''],
  ];
  entries.forEach(([key, value]) => {
    if (value) params.set(key, value);
    else params.delete(key);
  });
  const nextQuery = params.toString();
  const nextUrl = `${window.location.pathname}${nextQuery ? `?${nextQuery}` : ''}`;
  window.history.replaceState(null, '', nextUrl);
}

function toNumber(value) {
  if (value === null || value === undefined || String(value).trim() === '') return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function formatSignedPercentValue(value) {
  const num = toNumber(value);
  if (num === null) return '-';
  const prefix = num > 0 ? '+' : '';
  return `${prefix}${num.toFixed(2)}%`;
}

function getTrackerHeaderDisplayLabel(column) {
  const normalized = String(column || '').trim();
  return TRACKER_HEADER_LABEL_OVERRIDES[normalized] || normalized;
}

function buildTrackerHeaderReferenceMap(rows) {
  return buildVisibleStyleMetricHeaderReferenceMap(rows, TRACKER_HEADER_REFERENCE_FIELDS);
}

function buildTrackerHeaderReferenceMarkup(column, referenceSummaryMap = {}, options = {}) {
  return buildStyleMetricHeaderReferenceMarkup(column, referenceSummaryMap, {
    ...options,
    labelResolver: getTrackerHeaderDisplayLabel,
  });
}

function renderSignedPercent(value) {
  const num = toNumber(value);
  if (num === null) return '-';
  const cls = num > 0 ? 'num-pos' : num < 0 ? 'num-neg' : 'num-zero';
  return `<span class="${cls}">${escapeHtml(formatSignedPercentValue(value))}</span>`;
}

function buildHighlightCards(rows) {
  const formalScoreColumn = '正式投资评分';
  const recentlyStrongCount = rows.filter((item) => {
    const recentReturn = toNumber(item['近3月(%)']);
    return recentReturn !== null && recentReturn >= 1;
  }).length;
  const styleCounts = {};
  rows.forEach((item) => {
    const score = toNumber(item[formalScoreColumn]);
    if (score === null) return;
    const bucket = score >= 85 ? '高分段' : score >= 70 ? '中高分段' : '观察段';
    styleCounts[bucket] = (styleCounts[bucket] || 0) + 1;
  });
  const dominantStyle = Object.entries(styleCounts).sort((a, b) => b[1] - a[1])[0];
  const strongestRows = rows
    .filter((item) => toNumber(item[formalScoreColumn]) !== null)
    .sort((left, right) => toNumber(right[formalScoreColumn]) - toNumber(left[formalScoreColumn]))
    .slice(0, 3);
  return `
    <div class="tracker-highlight-card">
      <strong>近3月较强</strong>
      <span>${escapeHtml(formatNumber(recentlyStrongCount))}</span>
      <small>近 3 月 >= 1%</small>
    </div>
    <div class="tracker-highlight-card">
      <strong>主导评分段</strong>
      <span>${escapeHtml(dominantStyle ? `${dominantStyle[0]} · ${dominantStyle[1]}只` : '-')}</span>
      <small>占比最高</small>
    </div>
    <div class="tracker-highlight-card tracker-highlight-card-wide">
      <strong>优先关注 3 只</strong>
      <small>${strongestRows.map((item) => `${item['基金名称']}（评分 ${item[formalScoreColumn]}）`).join(' / ') || '暂无数据'}</small>
    </div>
  `;
}

function renderTrackerFilterBar(filterState, onChange, onReset) {
  if (!trackerFilterBar) return;
  trackerFilterBar.innerHTML = `
    <div class="tracker-filter-grid">
      <label class="tracker-filter-field">
        <span>基金检索</span>
        <input
          type="search"
          value="${escapeHtml(filterState.query || '')}"
          data-tracker-filter-id="query"
          placeholder="输入基金代码或名称"
        />
      </label>
      <label class="tracker-filter-field">
        <span>评分下限</span>
        <select data-tracker-filter-id="scoreFloor">
          <option value="" ${!filterState.scoreFloor ? 'selected' : ''}>不限</option>
          <option value="90" ${filterState.scoreFloor === '90' ? 'selected' : ''}>90+</option>
          <option value="85" ${filterState.scoreFloor === '85' ? 'selected' : ''}>85+</option>
          <option value="80" ${filterState.scoreFloor === '80' ? 'selected' : ''}>80+</option>
          <option value="75" ${filterState.scoreFloor === '75' ? 'selected' : ''}>75+</option>
        </select>
      </label>
      <label class="tracker-filter-field">
        <span>近3月强度</span>
        <select data-tracker-filter-id="recentStrength">
          <option value="all" ${filterState.recentStrength === 'all' ? 'selected' : ''}>全部</option>
          <option value="strong" ${filterState.recentStrength === 'strong' ? 'selected' : ''}>仅近3月 >= 1%</option>
          <option value="weak" ${filterState.recentStrength === 'weak' ? 'selected' : ''}>仅近3月 < 1%</option>
        </select>
      </label>
      <label class="tracker-filter-field">
        <span>估值状态</span>
        <select data-tracker-filter-id="valuationStatus">
          <option value="all" ${filterState.valuationStatus === 'all' ? 'selected' : ''}>全部</option>
          <option value="available" ${filterState.valuationStatus === 'available' ? 'selected' : ''}>仅已补估值</option>
          <option value="missing" ${filterState.valuationStatus === 'missing' ? 'selected' : ''}>仅估值缺失</option>
        </select>
      </label>
    </div>
    <div class="recommendation-filter-actions">
      <button type="button" class="secondary" data-tracker-filter-reset="1">重置筛选</button>
    </div>
  `;
  trackerFilterBar.querySelectorAll('[data-tracker-filter-id]').forEach((element) => {
    const eventName = element.tagName === 'INPUT' ? 'input' : 'change';
    element.addEventListener(eventName, () => {
      const id = element.dataset.trackerFilterId || '';
      if (!id) return;
      onChange(id, element.value);
    });
  });
  trackerFilterBar.querySelector('[data-tracker-filter-reset="1"]')?.addEventListener('click', onReset);
}

function isTrackerValuationMarkedMissing(valuation) {
  return Boolean(valuation && !valuation.available && (String(valuation.reason || '').startsWith('valuation_')
    || ['upstream_missing', 'upstream_empty'].includes(valuation.reason)));
}

function filterTrackerRows(rows, filterState, valuations = {}, options = {}) {
  const items = Array.isArray(rows) ? rows : [];
  const query = String(filterState?.query || '').trim().toLowerCase();
  const scoreFloor = parseMetricDisplayNumber(filterState?.scoreFloor);
  const recentStrength = String(filterState?.recentStrength || 'all').trim();
  const valuationStatus = String(filterState?.valuationStatus || 'all').trim();
  const ignoreValuationStatus = options.ignoreValuationStatus === true;
  return items.filter((row) => {
    const code = String(row['基金代码'] || '').trim().toLowerCase();
    const normalizedCode = String(row['基金代码'] || '').trim();
    const name = String(row['基金名称'] || '').trim().toLowerCase();
    const score = parseMetricDisplayNumber(row['正式投资评分']);
    const recent3m = parseMetricDisplayNumber(row['近3月(%)']);
    const valuation = valuations[normalizedCode] || null;
    if (query && !code.includes(query) && !name.includes(query)) return false;
    if (scoreFloor !== null && Number.isFinite(scoreFloor) && filterState?.scoreFloor && (!Number.isFinite(score) || score < scoreFloor)) return false;
    if (recentStrength === 'strong' && (!Number.isFinite(recent3m) || recent3m < 1)) return false;
    if (recentStrength === 'weak' && (recent3m === null || !Number.isFinite(recent3m) || recent3m >= 1)) return false;
    if (!ignoreValuationStatus) {
      if (valuationStatus === 'available' && !valuation?.available) return false;
      if (valuationStatus === 'missing' && !isTrackerValuationMarkedMissing(valuation)) return false;
    }
    return true;
  });
}

function bindTrackerFilterBar(currentFilterState, applyRender, afterRenderChange) {
  renderTrackerFilterBar(
    currentFilterState,
    (key, value) => {
      currentFilterState[key] = value;
      applyRender();
      if (typeof afterRenderChange === 'function') {
        void afterRenderChange();
      }
    },
    () => {
      const nextState = defaultTrackerFilterState();
      currentFilterState.query = nextState.query;
      currentFilterState.scoreFloor = nextState.scoreFloor;
      currentFilterState.recentStrength = nextState.recentStrength;
      currentFilterState.valuationStatus = nextState.valuationStatus;
      bindTrackerFilterBar(currentFilterState, applyRender, afterRenderChange);
      applyRender();
      if (typeof afterRenderChange === 'function') {
        void afterRenderChange();
      }
    },
  );
}

function renderTrackerMembershipNarrative(summary) {
  const narrative = summary?.narrative_summary || {};
  const changeSummary = summary?.latest_membership_change_summary || {};
  const rolling = changeSummary?.rolling_change_stability_summary || {};
  const latestChanges = narrative.latest_changes || summary?.latest_changes || '暂无正式发布名单变化摘要。';
  const overview = narrative.overview || '正式主榜累计跟踪用于识别连续稳定出现、缺席后回归和本期掉出。';
  const riskHint = narrative.risk_hint || '';
  const transitionCounts = Array.isArray(summary?.latest_transition_counts)
    ? summary.latest_transition_counts
    : [];
  const transitionText = transitionCounts
    .filter((item) => item && item.label)
    .map((item) => `${item.label}${formatNumber(item.count || 0)}只`)
    .join(' / ');
  const rollingSummary = rolling.available
    ? `近${formatNumber(rolling.window_size)}期均值：新入/回归 ${formatNumber(rolling.mean_current_change_rate_pct)}%，掉出 ${formatNumber(rolling.mean_dropped_rate_pct)}%；当前偏离 ${Number(rolling.current_vs_rolling_change_gap_pct) > 0 ? '+' : ''}${formatNumber(rolling.current_vs_rolling_change_gap_pct)}pct。`
    : '';
  const rollingAlert = rolling.available && Array.isArray(rolling.rolling_alert_codes) && rolling.rolling_alert_codes.length
    ? '提示：当前名单波动高于近期基线，需继续观察是否连续多期异常后再调整主链策略。'
    : '';
  return `
    <strong>名单变化</strong>
    <span>${escapeHtml(latestChanges)}</span>
    <small>${escapeHtml(transitionText || overview)}</small>
    ${riskHint ? `<small>${escapeHtml(riskHint)}</small>` : ''}
    ${rollingSummary ? `<small>${escapeHtml(rollingSummary)}</small>` : ''}
    ${rollingAlert ? `<small>${escapeHtml(rollingAlert)}</small>` : ''}
    ${changeSummary.available === false ? '<small>提示：至少需要两个唯一正式快照，才能完整判断连续、回归和掉出。</small>' : ''}
  `;
}

function buildTrackerSnapshotComparisonLabel(row) {
  const snapshotDate = String(row?.['最新净值日期'] || '').trim();
  return snapshotDate ? `正式快照（净值日期 ${snapshotDate}）` : '正式快照';
}

function renderTrackerListHints(
  rows,
  totalRows,
  activeGroup,
  filterState,
  viewMode,
  densityMode = FUND_TABLE_DENSITY_MODES.core,
  payloadScopeLabel = '',
) {
  if (!trackerListHints) return;
  const items = Array.isArray(rows) ? rows : [];
  if (!items.length && !totalRows) {
    trackerListHints.innerHTML = '';
    return;
  }
  const styleCount = new Set(
    items
      .map((item) => normalizeFundStyleLabel(item['债基风格标签']))
      .filter((style) => style && style !== '未标注风格'),
  ).size;
  const annualizedAvailableCount = items.filter((item) => String(item['平台成立以来收益年化(%)'] ?? '').trim()).length;
  const valuationAvailableCount = items.filter((item) => String(item['实时估值'] || '').trim()).length;
  const activeFilterCount = countTrackerActiveFilters(filterState);
  const scopeHint = String(payloadScopeLabel || '').trim();
  const densityHint = densityMode === FUND_TABLE_DENSITY_MODES.expanded
    ? '扩展视图：主表含更多字段'
    : '核心视图：基准见行展开';
  const hints = [
    scopeHint,
    densityHint,
    `分组：${String(activeGroup || GROUP_STABLE).trim() || GROUP_STABLE}`,
    `当前分组 ${formatNumber(items.length)} / ${formatNumber(totalRows || 0)} 只`,
    `覆盖风格 ${formatNumber(styleCount)} 个`,
    `${FUND_COPYWRITING.sinceInceptionAnnualizedAvailableCountLabel} ${formatNumber(annualizedAvailableCount)} 只`,
    `已补估值 ${formatNumber(valuationAvailableCount)} 只`,
    viewMode === FUND_STYLE_VIEW_MODES.grouped ? '当前按风格分组' : '当前为混合总表',
  ];
  if (activeFilterCount > 0) hints.push(`启用筛选 ${formatNumber(activeFilterCount)} 项`);
  renderSummaryHintChips(trackerListHints, hints.slice(0, 5));
}

function countTrackerActiveFilters(filterState = {}) {
  return [
    String(filterState?.query || '').trim(),
    String(filterState?.scoreFloor || '').trim(),
    String(filterState?.recentStrength || 'all').trim() !== 'all' ? filterState?.recentStrength : '',
    String(filterState?.valuationStatus || 'all').trim() !== 'all' ? filterState?.valuationStatus : '',
  ].filter(Boolean).length;
}

function buildTrackerEmptyState(activeGroup, totalRows, filterState) {
  const groupName = String(activeGroup || GROUP_STABLE).trim() || GROUP_STABLE;
  const activeFilterCount = countTrackerActiveFilters(filterState);
  const nextGroup = groupName === GROUP_STABLE ? GROUP_ENHANCED : GROUP_STABLE;
  if (activeFilterCount > 0) {
    return `
      <div class="empty-state-inline recommendation-empty-state">
        <strong>筛选后暂无匹配基金</strong>
        <span>${escapeHtml(`${groupName} 当前分组 ${formatNumber(totalRows || 0)} 只，筛选后 0 只。`)}</span>
        <small>建议重置部分筛选条件，或切换到其他分组继续查看。</small>
        <div class="recommendation-empty-state-actions">
          <button type="button" class="secondary" data-tracker-empty-action="reset-filters">重置筛选</button>
          <button type="button" class="secondary" data-tracker-empty-action="next-group" data-tracker-next-group="${escapeHtml(nextGroup)}">切换到 ${escapeHtml(nextGroup)}</button>
        </div>
      </div>
    `;
  }
  return `
    <div class="empty-state-inline recommendation-empty-state">
      <strong>${escapeHtml(groupName)} 当前暂无在榜基金</strong>
      <span>当前分组暂无可展示数据，可切换到其他分组继续查看。</span>
      <div class="recommendation-empty-state-actions">
        <button type="button" class="secondary" data-tracker-empty-action="next-group" data-tracker-next-group="${escapeHtml(nextGroup)}">切换到 ${escapeHtml(nextGroup)}</button>
      </div>
    </div>
  `;
}

function trackerGroupTotalCount(counts = {}, group = '', fallback = 0) {
  const stableCount = Number(counts.stable_bond_final_count);
  const enhancedCount = Number(counts.enhanced_bond_final_count);
  if (group === GROUP_STABLE && Number.isFinite(stableCount)) return stableCount;
  if (group === GROUP_ENHANCED && Number.isFinite(enhancedCount)) return enhancedCount;
  const activeCount = Number(counts.project_tracker_current_active_count);
  if (Number.isFinite(activeCount)) return activeCount;
  const normalizedFallback = Number(fallback);
  return Number.isFinite(normalizedFallback) ? normalizedFallback : 0;
}

function isTrackerCompactPayload(tracker = {}) {
  return String(tracker?.payload_scope?.mode || '').trim() === 'compact';
}

function appendTrackerDetailScopeAction({ isCompact, visibleCount, totalCount, onLoadFull }) {
  if (!isCompact || !trackerPageTable) return;
  const scope = document.createElement('div');
  scope.className = 'empty-state-inline recommendation-empty-state tracker-detail-scope-state';
  scope.innerHTML = `
    <strong>明细范围</strong>
    <span>预览 ${escapeHtml(formatNumber(visibleCount || 0))} / ${escapeHtml(formatNumber(totalCount || 0))}；完整明细会加载全部行级基准与展开内容。</span>
    <div class="recommendation-empty-state-actions">
      <button type="button" class="secondary" data-tracker-load-full-detail="1">完整明细</button>
    </div>
  `;
  trackerPageTable.appendChild(scope);
  const button = scope.querySelector('[data-tracker-load-full-detail="1"]');
  if (!button || typeof onLoadFull !== 'function') return;
  button.addEventListener('click', async () => {
    if (button.disabled) return;
    button.disabled = true;
    button.textContent = '加载中';
    try {
      await onLoadFull();
    } finally {
      if (button.isConnected) {
        button.disabled = false;
        button.textContent = '完整明细';
      }
    }
  });
}

function appendTrackerPaginationScopeAction({ pagination, onPageChange }) {
  if (!trackerPageTable || !pagination || typeof pagination !== 'object') return;
  const page = Number(pagination.page) || 1;
  const pageSize = Number(pagination.page_size) || 50;
  const totalCount = Number(pagination.filtered_total_count ?? pagination.total_count) || 0;
  const returnedCount = Number(pagination.returned_count) || 0;
  const hasMore = Boolean(pagination.has_more);
  const group = String(pagination.group || GROUP_STABLE).trim() || GROUP_STABLE;
  const scope = document.createElement('div');
  scope.className = 'empty-state-inline recommendation-empty-state tracker-detail-scope-state';
  scope.innerHTML = `
    <strong>分页明细</strong>
    <span>${escapeHtml(group)} / 第 ${escapeHtml(formatNumber(page))} 页 / 本页 ${escapeHtml(formatNumber(returnedCount))} 只 / 共 ${escapeHtml(formatNumber(totalCount))} 只；基准与展开内容按当前页真实明细展示。</span>
    <div class="recommendation-empty-state-actions">
      <button type="button" class="secondary" data-tracker-page-prev="1"${page <= 1 ? ' disabled' : ''}>上一页</button>
      <button type="button" class="secondary" data-tracker-page-next="1"${!hasMore ? ' disabled' : ''}>下一页</button>
    </div>
  `;
  trackerPageTable.appendChild(scope);
  if (typeof onPageChange !== 'function') return;
  scope.querySelector('[data-tracker-page-prev="1"]')?.addEventListener('click', () => {
    if (page > 1) onPageChange(Math.max(1, page - 1), pageSize);
  });
  scope.querySelector('[data-tracker-page-next="1"]')?.addEventListener('click', () => {
    if (hasMore) onPageChange(page + 1, pageSize);
  });
}

function renderTrackerGroupSwitch(activeGroup, onChange) {
  if (!trackerGroupSwitch) return;
  const groups = [GROUP_STABLE, GROUP_ENHANCED];
  trackerGroupSwitch.innerHTML = groups.map((group) => `
    <button
      type="button"
      class="${group === activeGroup ? 'is-active' : ''}"
      data-group="${group}"
      aria-pressed="${group === activeGroup ? 'true' : 'false'}"
    >${group}</button>
  `).join('');

  trackerGroupSwitch.querySelectorAll('button').forEach((button) => {
    button.addEventListener('click', () => {
      const nextGroup = button.dataset.group || GROUP_STABLE;
      if (nextGroup !== activeGroup) onChange(nextGroup);
    });
  });
}

function getTrackerColumnClass(column) {
  const map = {
    序号: 'col-seq',
    基金代码: 'col-code',
    基金名称: 'col-name',
    正式投资评分: 'col-score',
    '出现次数&出现频率(%)': 'col-occurrence',
    实时估值: 'col-valuation',
    '1年-年化收益(%)': 'col-return-1y',
    '平台成立以来收益年化(%)': 'col-return-since',
    '近1月(%)': 'col-return-1m',
    '近3月(%)': 'col-return-3m',
    '1年-夏普比率': 'col-sharpe',
    '1年-最大回撤(%)': 'col-drawdown',
    '1年-波动率': 'col-volatility',
    最新净值日期: 'col-nav-date',
    '基金规模(亿)': 'col-scale',
  };
  return map[column] || '';
}

function getTrackerMetricKey(row, column) {
  const code = String(row['基金代码'] || '').trim();
  return `${code}::${column}`;
}

function buildTopDecileMetricLookup(rows) {
  const lookup = new Set();
  Object.entries(TOP_DECILE_METRIC_RULES).forEach(([column, direction]) => {
    const numericRows = rows
      .map((row) => ({
        key: getTrackerMetricKey(row, column),
        value: toNumber(row[column]),
      }))
      .filter((item) => item.key && item.value !== null);

    if (!numericRows.length) return;

    const sorted = [...numericRows].sort((left, right) => (
      direction === 'asc' ? left.value - right.value : right.value - left.value
    ));
    const cutoffIndex = Math.max(0, Math.ceil(sorted.length * 0.1) - 1);
    const threshold = sorted[cutoffIndex]?.value;
    if (threshold === undefined) return;

    numericRows.forEach((item) => {
      if ((direction === 'asc' && item.value <= threshold) || (direction === 'desc' && item.value >= threshold)) {
        lookup.add(item.key);
      }
    });
  });
  return lookup;
}

function renderTrackerCell(column, row, highlightLookup) {
  const content = column === '正式投资评分'
    ? renderFormalScoreTooltipCell(row[column], row, {
      code: String(row['基金代码'] || '').trim(),
    })
    : column === '实时估值'
      ? renderRealtimeValuationTooltipCell(row[column], row, {
        columnLabel: '实时估值',
        snapshotComparisonLabel: buildTrackerSnapshotComparisonLabel(row),
        valuationState: row.__valuation_state,
        valuationStatusLabel: row.__valuation_status_label,
        valuationReason: row.__valuation_reason,
      })
    : column === '债基风格标签'
      ? renderStyleBenchmarkTooltipCell(row[column], row)
    : renderCell(column, row[column]);
  if (highlightLookup?.has(getTrackerMetricKey(row, column))) {
    return `<span class="metric-top-decile">${content}</span>`;
  }
  return content;
}

function renderTrackerExpandToggle(row) {
  const expanded = Boolean(row?.__expanded);
  return `
    <button
      type="button"
      class="favorite-expand-toggle ${expanded ? 'is-expanded' : ''}"
      data-compact-expand-key="${escapeHtml(String(row?.__expandKey || ''))}"
      aria-expanded="${expanded ? 'true' : 'false'}"
    >
      ${expanded ? '收起' : '展开'}
    </button>
  `;
}

function renderTrackerTableShell(container, columns, rows, expandedCodes, onToggleExpand, onToggleExpandAll, densityMode = FUND_TABLE_DENSITY_MODES.core) {
  const highlightLookup = buildTopDecileMetricLookup(rows);
  const tableRows = rows.map((row) => ({
    ...row,
    详情: '',
    __expandKey: String(row['基金代码'] || '').trim(),
    __expanded: expandedCodes.has(String(row['基金代码'] || '').trim()),
  }));
  const headerReferenceMap = buildTrackerHeaderReferenceMap(tableRows);
  renderCompactExpandableTable(container, tableRows, {
    coreColumns: (densityMode === FUND_TABLE_DENSITY_MODES.expanded ? TRACKER_EXPANDED_TABLE_COLUMNS : TRACKER_CORE_TABLE_COLUMNS)
      .filter((column) => column === '详情' || columns.includes(column)),
    detailGroups: TRACKER_DETAIL_GROUPS,
    tableClass: 'data-table tracker-table tracker-table-wide tracker-compact-table compact-expand-table',
    tableAttributes: {
      'data-density-mode': densityMode,
    },
    wrapClass: 'tracker-table-wrap tracker-compact-wrap compact-expand-wrap',
    toolbarClass: 'favorite-table-toolbar compact-table-toolbar tracker-table-toolbar',
    toolbarTitle: densityMode === FUND_TABLE_DENSITY_MODES.expanded ? '扩展跟踪视图' : '紧凑跟踪视图',
    toolbarHint: densityMode === FUND_TABLE_DENSITY_MODES.expanded
      ? '主表直接展开更多收益、规模和出现频次字段，适合深度跟踪比对。'
      : '主表只保留跟踪核心列，风险和补充信息按行展开查看',
    headerContentRenderers: {
      '正式投资评分': () => buildTrackerHeaderReferenceMarkup('正式投资评分', headerReferenceMap, { allowMixedRange: false }),
      '1年-年化收益(%)': () => buildTrackerHeaderReferenceMarkup('1年-年化收益(%)', headerReferenceMap, { allowMixedRange: false }),
      '平台成立以来收益年化(%)': () => buildTrackerHeaderReferenceMarkup('平台成立以来收益年化(%)', headerReferenceMap, { allowMixedRange: false }),
      '近3月(%)': () => buildTrackerHeaderReferenceMarkup('近3月(%)', headerReferenceMap, { allowMixedRange: false }),
      '近1月(%)': () => buildTrackerHeaderReferenceMarkup('近1月(%)', headerReferenceMap, { allowMixedRange: false }),
      '1年-夏普比率': () => buildTrackerHeaderReferenceMarkup('1年-夏普比率', headerReferenceMap, { allowMixedRange: false }),
      '1年-最大回撤(%)': () => buildTrackerHeaderReferenceMarkup('1年-最大回撤(%)', headerReferenceMap, { allowMixedRange: false }),
      '1年-波动率': () => buildTrackerHeaderReferenceMarkup('1年-波动率', headerReferenceMap, { allowMixedRange: false }),
    },
    mobileCardColumns: TRACKER_MOBILE_CARD_COLUMNS.filter((column) => columns.includes(column)),
    renderCards: true,
    rowDataResolver: (row) => row,
    rowKey: (row) => String(row['基金代码'] || '').trim(),
    expandedRows: expandedCodes,
    rowAttributes: (row) => ({
      'data-fund-code': String(row['基金代码'] || '').trim(),
    }),
    cellRenderers: {
      ...Object.fromEntries(columns.map((column) => [column, (value, row) => renderTrackerCell(column, row, highlightLookup)])),
      '平台成立以来收益年化(%)': (value, row) => renderSinceInceptionAnnualizedTooltipCell(value, row),
      '1年-年化收益(%)': (value, row) => renderBenchmarkAwareReturnTooltipCell(value, row, {
        field: '1年-年化收益(%)',
        title: `1年年化收益 ${formatSignedPercentOrDash(value)}`,
        snapshotLabel: buildTrackerSnapshotComparisonLabel(row),
        description: '用于衡量当前在榜基金近一年收益效率，并与同风格统一数据基准比较。',
        referenceFields: ['1年-年化收益(%)', '1年-夏普比率', '1年-最大回撤(%)', '1年-波动率'],
      }),
      '近1月(%)': (value, row) => renderBenchmarkAwareReturnTooltipCell(value, row, {
        field: '近1月(%)',
        title: `近1月涨跌 ${formatSignedPercentOrDash(value)}`,
        snapshotLabel: buildTrackerSnapshotComparisonLabel(row),
        description: '该列展示正式快照近一个月收益，并与同风格真实样本参考线比较。',
        referenceFields: ['近1月(%)', '1年-波动率'],
      }),
      '近3月(%)': (value, row) => renderBenchmarkAwareReturnTooltipCell(value, row, {
        field: '近3月(%)',
        title: `近3月涨跌 ${formatSignedPercentOrDash(value)}`,
        snapshotLabel: buildTrackerSnapshotComparisonLabel(row),
        description: '该列展示正式快照近三个月收益，并与同风格真实样本参考线比较。',
        referenceFields: ['近3月(%)', '1年-年化收益(%)', '1年-夏普比率', '1年-最大回撤(%)', '1年-波动率'],
      }),
      详情: (_, row) => renderTrackerExpandToggle(row),
    },
    onToggleExpand,
    onToggleExpandAll,
  });
}

function buildTrackerColumns(activeGroup) {
  void activeGroup;
  return [
    '序号',
    '基金代码',
    '基金名称',
    '正式投资评分',
    '平台成立以来收益年化(%)',
    '1年-年化收益(%)',
    '近3月(%)',
    '近1月(%)',
    '实时估值',
    '基金规模(亿)',
    '出现次数&出现频率(%)',
    '1年-夏普比率',
    '1年-最大回撤(%)',
    '1年-波动率',
    '最新净值日期',
    '债基风格标签',
  ];
}

function enrichRowsWithValuation(rows, valuations, valuationState) {
  return rows.map((row, index) => {
    const code = String(row['基金代码'] || '').trim();
    const valuation = (valuations || {})[code] || {};
    let text = '';
    let uiState = valuationState === VALUATION_STATE_LOADING ? 'loading' : '';
    let uiStatusLabel = valuationState === VALUATION_STATE_LOADING ? '请求中' : '';
    let uiReason = '';
    if (valuationState === VALUATION_STATE_READY && valuation.available) {
      text = formatSignedPercentValue(valuation.estimated_change_pct);
      uiState = 'ready';
      uiStatusLabel = '已返回';
    } else if (valuationState === VALUATION_STATE_READY && valuation.reason === 'valuation_not_provided') {
      text = '暂无估值';
      uiState = 'missing';
      uiStatusLabel = '该基金当前未提供盘中估值';
      uiReason = valuation.reason || '';
    } else if (valuationState === VALUATION_STATE_READY && valuation.reason === 'upstream_missing') {
      text = '暂无估值';
      uiState = 'missing';
      uiStatusLabel = '上游未返回该基金';
      uiReason = valuation.reason || '';
    } else if (valuationState === VALUATION_STATE_READY && valuation.reason === 'upstream_failed') {
      uiState = 'failed';
      uiStatusLabel = '请求失败';
      uiReason = valuation.reason || '';
    } else if (valuationState === VALUATION_STATE_READY) {
      uiState = 'unresolved';
      uiStatusLabel = '本次未返回';
    } else if (valuationState === VALUATION_STATE_FAILED) {
      uiState = 'failed';
      uiStatusLabel = '请求失败';
    } else if (valuationState === VALUATION_STATE_IDLE) {
      uiState = 'not_requested';
      uiStatusLabel = '未请求';
    }
    return {
      ...row,
      序号: index + 1,
      实时估值: text,
      实时估值时间: valuation.valuation_date || '',
      实时估算净值: valuation.estimated_nav || '',
      __valuation_state: uiState,
      __valuation_status_label: uiStatusLabel,
      __valuation_reason: uiReason,
    };
  });
}

function updateValuationBanner(message, options = {}) {
  if (!trackerValuationBanner) return;
  const { loading = false, failed = false, retryable = false, onRetry = null } = options;
  trackerValuationBanner.classList.toggle('is-loading', loading);
  trackerValuationBanner.classList.toggle('is-failed', failed);
  trackerValuationBanner.innerHTML = `
    <strong>实时估值</strong>
    <span>${escapeHtml(message)}</span>
    ${retryable ? '<button type="button" class="secondary tracker-valuation-retry">重试</button>' : ''}
  `;
  if (retryable) {
    trackerValuationBanner.querySelector('.tracker-valuation-retry')?.addEventListener('click', () => {
      if (typeof onRetry === 'function') onRetry();
    });
  }
}

function renderTrackerGroupContent(allRows, activeGroup, valuations, valuationState, expandedCodes, onToggleExpand, onToggleExpandAll, viewMode = FUND_STYLE_VIEW_MODES.mixed, densityMode = FUND_TABLE_DENSITY_MODES.core, emptyStateHtml = '') {
  const rows = Array.isArray(allRows) ? allRows : [];
  const enrichedRows = enrichRowsWithValuation(rows, valuations, valuationState);
  const columns = buildTrackerColumns(activeGroup).filter((column) => column in (enrichedRows[0] || {}));
  trackerPageHighlights.innerHTML = buildHighlightCards(rows);
  if (!enrichedRows.length) {
    trackerPageTable.innerHTML = emptyStateHtml || '<div class="empty-state-inline">暂无数据</div>';
  } else {
    if (viewMode === FUND_STYLE_VIEW_MODES.grouped) {
      renderFundStyleGroupedTables(trackerPageTable, enrichedRows, {
        renderTable: (container, styleRows) => {
          renderTrackerTableShell(container, columns, styleRows, expandedCodes, onToggleExpand, onToggleExpandAll, densityMode);
        },
      });
    } else {
      renderTrackerTableShell(trackerPageTable, columns, enrichedRows, expandedCodes, onToggleExpand, onToggleExpandAll, densityMode);
    }
  }
  return { totalRows: rows.length, rows, enrichedRows };
}

async function initTrackerPage(options = {}) {
  rerenderTrackerForViewport = null;
  if (trackerValuationBtn) {
    trackerValuationBtn.onclick = null;
    trackerValuationBtn.disabled = true;
  }
  const requestedGroup = String(options?.group || trackerGroupFromUrl()).trim() || GROUP_STABLE;
  const requestFullDetail = Boolean(options?.full);
  const requestedPage = Number.isFinite(Number(options?.page)) && Number(options.page) > 0
    ? Math.floor(Number(options.page))
    : 1;
  const requestedPageSize = Number.isFinite(Number(options?.pageSize ?? options?.page_size)) && Number(options?.pageSize ?? options?.page_size) > 0
    ? Math.floor(Number(options?.pageSize ?? options?.page_size))
    : 50;
  void loadBootstrapSummary().then((payload) => {
    renderRunBadge(trackerHeaderRunBadge, getBootstrapWorkflow(payload));
    renderHealthBadge(trackerHeaderHealthBadge, getBootstrapHealth(payload));
  }).catch(() => {
    renderUnavailableBadge(trackerHeaderRunBadge, '运行状态', '接口暂不可用');
    renderUnavailableBadge(trackerHeaderHealthBadge, '数据源状态', '接口暂不可用');
  });
  const [trackerResult] = await Promise.allSettled([
    loadTrackerCurrent(requestFullDetail ? {
      full: true,
      group: requestedGroup,
      page: requestedPage,
      pageSize: requestedPageSize,
    } : {
      full: false,
      group: requestedGroup,
      pageSize: 50,
    }),
  ]);

  if (trackerResult.status !== 'fulfilled') {
    trackerHeroMeta.innerHTML = buildStatCard('当前主发布版本', '加载失败', '稍后重试');
    renderRetryableEmptyState(trackerPageTable, {
      title: '跟踪榜单加载失败',
      message: trackerResult.reason?.message || '请稍后重试',
      actionLabel: '重新加载',
      onRetry: () => initTrackerPage(options),
    });
    return;
  }

  const tracker = trackerResult.value;
  applyPayloadDataContract(tracker);
  const trackerSourceSummary = payloadSourceContractSummary(tracker);
  renderFormalScoreContractInto('trackerFormalScoreContract', tracker?.formal_score_contract);
  const compactMode = isTrackerCompactPayload(tracker);
  const rows = tracker?.rows || [];
  const summary = tracker?.tracker_summary || {};
  const counts = tracker?.counts || {};
  const compactPagination = tracker?.pagination && typeof tracker.pagination === 'object'
    ? tracker.pagination
    : {};
  const pagedDetailMode = !compactMode && String(compactPagination.pagination_scope || '').trim() === 'detail';
  const styleThresholds = normalizeFundStyleThresholdEntries(tracker?.style_thresholds);
  const globalStyleThreshold = tracker?.global_style_threshold && typeof tracker.global_style_threshold === 'object'
    ? { ...tracker.global_style_threshold }
    : {};
  const missingStyleThresholds = Array.isArray(tracker?.missing_style_thresholds)
    ? tracker.missing_style_thresholds.map((item) => String(item || '').trim()).filter(Boolean)
    : [];

  trackerHeroMeta.innerHTML = [
    buildStatCard('当前主发布版本', tracker?.run_id || '-', '只认规范保留版本'),
    buildStatCard('最新快照', summary.latest_nav_snapshot_date || '-', '累计跟踪快照'),
    buildStatCard('当前在榜', counts.project_tracker_current_active_count ?? '-', '当前仍在榜基金'),
  ].join('');

  trackerPageMeta.textContent = `版本 ${tracker?.run_id || '-'} / 快照 ${summary.latest_nav_snapshot_date || '-'} / ${trackerSourceSummary}`;
  trackerPageNarrative.innerHTML = renderTrackerMembershipNarrative(summary);

  let activeGroup = requestedGroup === GROUP_ENHANCED ? GROUP_ENHANCED : GROUP_STABLE;
  const trackerFilterState = trackerFilterStateFromUrl();
  let currentVisibleRows = [];
  let currentValuationTargetRows = [];
  let valuationMap = {};
  let valuationState = VALUATION_STATE_IDLE;
  let valuationRequested = false;
  let valuationLastPayload = null;
  let trackerViewMode = trackerViewModeFromUrl();
  let trackerDensityMode = trackerDensityModeFromUrl();
  const valuationCache = {};
  const expandedCodeMap = {
    [GROUP_STABLE]: new Set(),
    [GROUP_ENHANCED]: new Set(),
  };

  const activeExpandedCodes = () => expandedCodeMap[activeGroup] || new Set();
  const activeValuationCache = () => valuationCache[activeGroup] || {};
  const currentVisibleCodes = () => currentVisibleRows
    .map((item) => String(item['基金代码'] || '').trim())
    .filter(Boolean);
  const currentValuationTargetCodes = () => currentValuationTargetRows
    .map((item) => String(item['基金代码'] || '').trim())
    .filter(Boolean);
  const missingVisibleValuationCodes = () => {
    const cache = activeValuationCache();
    const sourceCodes = shouldRequireValuationData()
      ? currentValuationTargetCodes()
      : currentVisibleCodes();
    return sourceCodes.filter((code) => !(code in cache));
  };
  const currentVisibleValuationStats = () => {
    const codes = shouldRequireValuationData()
      ? currentValuationTargetCodes()
      : currentVisibleCodes();
    const cache = activeValuationCache();
    const resolvedCodes = codes.filter((code) => code in cache);
    const availableCount = resolvedCodes.filter((code) => cache[code]?.available).length;
    const missingCount = resolvedCodes.filter((code) => (
      cache[code]?.reason === 'valuation_not_provided' || cache[code]?.reason === 'upstream_missing'
    )).length;
    return {
      totalCount: codes.length,
      resolvedCount: resolvedCodes.length,
      availableCount,
      missingCount,
      unresolvedCount: Math.max(0, codes.length - resolvedCodes.length),
    };
  };
  const shouldRequireValuationData = () => String(trackerFilterState.valuationStatus || 'all').trim() !== 'all';
  const syncValuationBannerForVisibleRows = () => {
    if (valuationState === VALUATION_STATE_FAILED) return;
    if (!valuationRequested) {
      updateValuationBanner(
        shouldRequireValuationData()
          ? '已启用估值筛选，正在准备估值数据'
          : '点击后补估值'
      );
      return;
    }
    const stats = currentVisibleValuationStats();
    if (!stats.totalCount) {
      updateValuationBanner('暂无可估值基金');
      return;
    }
    if (valuationState === VALUATION_STATE_LOADING) {
      updateValuationBanner(
        stats.resolvedCount
          ? `估值中：已补 ${stats.resolvedCount} / ${stats.totalCount}`
          : `估值中：${stats.totalCount} 只`,
        { loading: true },
      );
      return;
    }
    if (valuationLastPayload) {
      updateValuationBanner(formatTrackerValuationStatusMessage(valuationLastPayload, stats.availableCount, stats.totalCount));
      return;
    }
    if (stats.unresolvedCount > 0) {
      updateValuationBanner(`待补 ${stats.resolvedCount} / ${stats.totalCount}`);
      return;
    }
    updateValuationBanner(
      stats.missingCount
        ? `已补 ${stats.availableCount} / ${stats.totalCount}`
        : `已补 ${stats.availableCount} 只`
    );
  };

  const applyRender = () => {
    syncTrackerViewUrl({
      activeGroup,
      filterState: trackerFilterState,
      viewMode: trackerViewMode,
      densityMode: trackerDensityMode,
    });
    const rawGroupRows = compactMode || pagedDetailMode ? rows : groupRows(rows, activeGroup);
    const groupTotalRows = compactMode
      ? trackerGroupTotalCount(counts, activeGroup, compactPagination.total_count || rawGroupRows.length)
      : pagedDetailMode
        ? Number(compactPagination.filtered_total_count ?? compactPagination.total_count ?? rawGroupRows.length) || rawGroupRows.length
        : rawGroupRows.length;
    renderFundStyleViewBar(trackerViewBar, {
      viewMode: trackerViewMode,
      densityMode: trackerDensityMode,
      rows: rawGroupRows,
      onChange: (nextMode) => {
        trackerViewMode = nextMode;
        expandedCodeMap[activeGroup] = new Set();
        applyRender();
      },
      onDensityChange: (nextMode) => {
        trackerDensityMode = nextMode;
        expandedCodeMap[activeGroup] = new Set();
        applyRender();
      },
      groupedNote: FUND_COPYWRITING.sameStyleAnnualizedCompareNote,
      mixedNote: '混合视图便于总览，但不同风格间的正式投资评分不宜直接横向比较。',
    });
    renderFundStyleThresholdSummary(trackerStyleThresholdSummary, {
      rows: rawGroupRows,
      styleThresholds,
      globalStyleThreshold,
      missingStyleThresholds,
      viewMode: trackerViewMode,
      description: '以下门槛来自当前最新 workflow，用于解释当前在榜基金按风格分组后的评分与留榜标准。',
    });
    const baseFilteredRows = filterTrackerRows(rawGroupRows, trackerFilterState, activeValuationCache(), {
      ignoreValuationStatus: true,
    });
    const filteredGroupRows = shouldRequireValuationData()
      ? filterTrackerRows(baseFilteredRows, trackerFilterState, activeValuationCache())
      : baseFilteredRows;
    currentValuationTargetRows = baseFilteredRows;
    renderTrackerListHints(
      filteredGroupRows,
      groupTotalRows,
      activeGroup,
      trackerFilterState,
      trackerViewMode,
      trackerDensityMode,
      compactMode ? '摘要预览' : (pagedDetailMode ? '分页明细' : '完整明细'),
    );
    const renderResult = renderTrackerGroupContent(
      filteredGroupRows,
      activeGroup,
      valuationMap,
      valuationState,
      activeExpandedCodes(),
      (code) => {
        const normalized = String(code || '').trim();
        if (!normalized) return;
        const next = new Set(activeExpandedCodes());
        if (next.has(normalized)) next.delete(normalized);
        else next.add(normalized);
        expandedCodeMap[activeGroup] = next;
        applyRender();
      },
      (expanded, visibleRows) => {
        expandedCodeMap[activeGroup] = expanded
          ? new Set((Array.isArray(visibleRows) ? visibleRows : []).map((row) => String(row['基金代码'] || '').trim()).filter(Boolean))
          : new Set();
        applyRender();
      },
      trackerViewMode,
      trackerDensityMode,
      buildTrackerEmptyState(activeGroup, groupTotalRows, trackerFilterState),
    );
    currentVisibleRows = renderResult.rows;
    appendTrackerDetailScopeAction({
      isCompact: compactMode,
      visibleCount: filteredGroupRows.length,
      totalCount: groupTotalRows,
      onLoadFull: () => initTrackerPage({ full: true, group: activeGroup, page: 1, pageSize: requestedPageSize }),
    });
    appendTrackerPaginationScopeAction({
      pagination: pagedDetailMode ? compactPagination : null,
      onPageChange: (nextPage, nextPageSize) => initTrackerPage({
        full: true,
        group: activeGroup,
        page: nextPage,
        pageSize: nextPageSize,
      }),
    });
    const styleCount = new Set(filteredGroupRows.map((item) => normalizeFundStyleLabel(item['债基风格标签'])).filter((style) => style && style !== '未标注风格')).size;
    const activeFilterCount = countTrackerActiveFilters(trackerFilterState);
    const viewLabel = trackerViewMode === FUND_STYLE_VIEW_MODES.grouped ? '风格分组' : '混合总表';
    const densityLabel = trackerDensityMode === FUND_TABLE_DENSITY_MODES.expanded ? '扩展视图' : '核心视图';
    trackerPageMeta.textContent = `版本 ${tracker?.run_id || '-'} / 快照 ${summary.latest_nav_snapshot_date || '-'} / 当前分组 ${formatNumber(groupTotalRows)} 只 / 可见 ${formatNumber(filteredGroupRows.length)} 只 / ${trackerSourceSummary}`;
    trackerPageStats.innerHTML = [
      buildStatCard('当前分组', activeGroup, compactMode ? `${formatNumber(filteredGroupRows.length)} 只预览 / ${formatNumber(groupTotalRows)} 只在榜` : `${formatNumber(groupTotalRows)} 只当前在榜`),
      buildStatCard('当前可见', formatNumber(filteredGroupRows.length), `覆盖风格 ${formatNumber(styleCount)} 个`),
      buildStatCard('双分榜分布', counts.stable_bond_final_count ?? '-', `纯债稳健 ${formatNumber(counts.stable_bond_final_count)} / 增强债弹性 ${formatNumber(counts.enhanced_bond_final_count)}`),
      buildStatCard('当前视图', `${viewLabel} / ${densityLabel}`, activeFilterCount > 0 ? `启用筛选 ${formatNumber(activeFilterCount)} 项` : '未启用额外筛选'),
      buildStatCard('累计跟踪期数', counts.project_tracker_total_periods ?? '-', '仅按主发布版本累计'),
    ].join('');
    if (!filteredGroupRows.length) {
      trackerPageTable.querySelector('[data-tracker-empty-action="reset-filters"]')?.addEventListener('click', () => {
        const nextState = defaultTrackerFilterState();
        trackerFilterState.query = nextState.query;
        trackerFilterState.scoreFloor = nextState.scoreFloor;
        trackerFilterState.recentStrength = nextState.recentStrength;
        trackerFilterState.valuationStatus = nextState.valuationStatus;
        bindTrackerFilterBar(trackerFilterState, applyRender, async () => {
          await ensureValuationDataForFilters();
          await refreshMissingVisibleValuationsIfNeeded();
        });
        applyRender();
      });
      trackerPageTable.querySelector('[data-tracker-empty-action="next-group"]')?.addEventListener('click', () => {
        const nextGroup = String(trackerPageTable.querySelector('[data-tracker-empty-action="next-group"]')?.dataset.trackerNextGroup || '').trim();
        if (nextGroup) void handleGroupChange(nextGroup);
      });
    }
    if (trackerValuationBtn) {
      trackerValuationBtn.disabled = valuationState === VALUATION_STATE_LOADING;
      trackerValuationBtn.textContent = valuationState === VALUATION_STATE_LOADING ? '补估值中' : '补估值';
    }
    syncValuationBannerForVisibleRows();
  };

  const refreshValuations = async () => {
    const codes = shouldRequireValuationData()
      ? currentValuationTargetCodes()
      : currentVisibleCodes();
    if (!codes.length) {
      valuationMap = activeValuationCache();
      valuationState = valuationRequested ? VALUATION_STATE_READY : VALUATION_STATE_IDLE;
      valuationLastPayload = null;
      applyRender();
      return;
    }
    const cache = activeValuationCache();
    const missingCodes = codes.filter((code) => !(code in cache));
    if (!missingCodes.length) {
      valuationMap = cache;
      valuationState = VALUATION_STATE_READY;
      valuationLastPayload = null;
      applyRender();
      return;
    }
    valuationState = VALUATION_STATE_LOADING;
    valuationMap = cache;
    applyRender();
    try {
      const result = await loadTrackerValuations(missingCodes);
      valuationMap = {
        ...cache,
        ...(result?.valuations || {}),
      };
      valuationCache[activeGroup] = valuationMap;
      valuationLastPayload = result || null;
      valuationState = VALUATION_STATE_READY;
      applyRender();
    } catch (error) {
      valuationState = VALUATION_STATE_FAILED;
      valuationMap = cache;
      valuationLastPayload = null;
      applyRender();
      updateValuationBanner(
        error?.message ? `估值失败：${error.message}` : '估值失败',
        {
          failed: true,
          retryable: true,
          onRetry: () => {
            refreshValuations();
          },
        }
      );
    }
  };

  const refreshMissingVisibleValuationsIfNeeded = async () => {
    if (!valuationRequested || valuationState === VALUATION_STATE_LOADING) return;
    if (!missingVisibleValuationCodes().length) return;
    await refreshValuations();
  };
  const ensureValuationDataForFilters = async () => {
    if (!shouldRequireValuationData()) return;
    if (!valuationRequested) {
      valuationRequested = true;
    }
    await refreshMissingVisibleValuationsIfNeeded();
  };

  const handleGroupChange = async (nextGroup) => {
    if (compactMode || pagedDetailMode) {
      await initTrackerPage({
        full: pagedDetailMode,
        group: nextGroup,
        page: 1,
        pageSize: requestedPageSize,
      });
      return;
    }
    activeGroup = nextGroup;
    if (valuationRequested && valuationCache[activeGroup]) {
      valuationMap = valuationCache[activeGroup];
      valuationState = VALUATION_STATE_READY;
      valuationLastPayload = null;
    } else {
      valuationMap = {};
      valuationState = VALUATION_STATE_IDLE;
      valuationLastPayload = null;
    }
    renderTrackerGroupSwitch(activeGroup, handleGroupChange);
    applyRender();
    await ensureValuationDataForFilters();
    await refreshMissingVisibleValuationsIfNeeded();
  };

  renderTrackerGroupSwitch(activeGroup, handleGroupChange);
  bindTrackerFilterBar(trackerFilterState, applyRender, async () => {
    await ensureValuationDataForFilters();
    await refreshMissingVisibleValuationsIfNeeded();
  });
  applyRender();

  rerenderTrackerForViewport = applyRender;
  if (trackerValuationBtn) {
    trackerValuationBtn.onclick = async () => {
      valuationRequested = true;
      await refreshValuations();
    };
  }

  const trackerFile = tracker?.tracker_file || '';
  trackerPagePreviewBtn.href = trackerFile ? viewerUrl(trackerFile, '当前在榜') : '#';
  trackerPageDownloadBtn.href = trackerFile ? fileUrl(trackerFile, true) : '#';
  trackerPagePreviewBtn.classList.toggle('is-disabled', !trackerFile);
  trackerPageDownloadBtn.classList.toggle('is-disabled', !trackerFile);
}

initTrackerPage();
