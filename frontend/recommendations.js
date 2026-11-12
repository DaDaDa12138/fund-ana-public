const recommendationsMeta = document.getElementById('recommendationsMeta');
const recommendationsStats = document.getElementById('recommendationsStats');
const recommendationsNarrative = document.getElementById('recommendationsNarrative');
const recommendationsSignals = document.getElementById('recommendationsSignals');
const recommendationsHeroMeta = document.getElementById('recommendationsHeroMeta');
const recommendationsHeaderRunBadge = document.getElementById('recommendationsHeaderRunBadge');
const recommendationsHeaderHealthBadge = document.getElementById('recommendationsHeaderHealthBadge');
const recommendationGroupSwitch = document.getElementById('recommendationGroupSwitch');
const recommendationGroupStats = document.getElementById('recommendationGroupStats');
const recommendationGroupNarrative = document.getElementById('recommendationGroupNarrative');
const recommendationCards = document.getElementById('recommendationCards');
const recommendationViewBar = document.getElementById('recommendationViewBar');
const recommendationStyleThresholdSummary = document.getElementById('recommendationStyleThresholdSummary');
const recommendationSortPresetSwitch = document.getElementById('recommendationSortPresetSwitch');
const recommendationFilterBar = document.getElementById('recommendationFilterBar');
const recommendationChangePanel = document.getElementById('recommendationChangePanel');
const recommendationComparePanel = document.getElementById('recommendationComparePanel');
const recommendationChangeToggleBtn = document.getElementById('recommendationChangeToggleBtn');
const recommendationCompareToggleBtn = document.getElementById('recommendationCompareToggleBtn');
const recommendationListHints = document.getElementById('recommendationListHints');
const recommendationActionBanner = document.getElementById('recommendationActionBanner');
const recommendationDownloadBtn = document.getElementById('recommendationDownloadBtn');
const recommendationValuationBtn = document.getElementById('recommendationValuationBtn');
const recommendationValuationBanner = document.getElementById('recommendationValuationBanner');
const recommendationExportBtn = document.getElementById('recommendationExportBtn');
const recommendationQueryParams = new URLSearchParams(window.location.search);
let recommendationCompactViewport = isCompactTableViewport();
let rerenderRecommendationsForViewport = null;
window.addEventListener('resize', () => {
  const compact = isCompactTableViewport();
  if (compact === recommendationCompactViewport) return;
  recommendationCompactViewport = compact;
  rerenderRecommendationsForViewport?.();
});

const VALUATION_STATE_IDLE = 'idle';
const VALUATION_STATE_LOADING = 'loading';
const VALUATION_STATE_READY = 'ready';
const VALUATION_STATE_FAILED = 'failed';
const DEFAULT_RECOMMENDATION_SORT_STATE = Object.freeze({
  column: '正式投资评分',
  direction: 'desc',
});
const DEFAULT_RECOMMENDATION_SORT_PRESET = 'balanced';
const DEFAULT_RECOMMENDATION_FILTER_STATE = Object.freeze({
  query: '',
  style: '',
  stability: '',
  quality: '',
  company: '',
  favoriteStatus: 'all',
  scoreFloor: '',
  annualizedAvailability: 'all',
});
const SORTABLE_RECOMMENDATION_COLUMNS = buildFundSortableColumns(['建议权重']);
const RECOMMENDATION_INITIAL_SORT_DIRECTIONS = buildFundInitialSortDirections({
  '建议权重': 'desc',
});
const RECOMMENDATION_IDENTITY_COLUMNS = Object.freeze([
  '对比',
  '基金代码',
  '基金名称',
  '收藏',
]);
const RECOMMENDATION_DECISION_COLUMNS = Object.freeze([
  '正式投资评分',
  '建议权重',
  '数据质量提示',
  '投资优先池得分',
]);
const RECOMMENDATION_STYLE_COLUMNS = Object.freeze([
  '正式榜分组',
  '债基风格标签',
  '长期稳定性标签',
  '基金公司',
]);
const RECOMMENDATION_RETURN_COLUMNS = Object.freeze([
  '平台成立以来收益年化(%)',
  '1年-年化收益(%)',
  '近3月(%)',
  '近1月(%)',
  '实时估值',
]);
const RECOMMENDATION_RISK_COLUMNS = Object.freeze([
  '1年-夏普比率',
  '1年-最大回撤(%)',
  '1年-波动率',
  '费率合计(%)',
  '最新净值日期',
  '基金规模(亿)',
]);
const RECOMMENDATION_EXPLANATION_COLUMNS = Object.freeze([
  '推荐解读',
  '出现次数&出现频率(%)',
]);
const RECOMMENDATION_HEADER_REFERENCE_FIELDS = Object.freeze([
  '正式投资评分',
  '1年-年化收益(%)',
  '平台成立以来收益年化(%)',
  '近3月(%)',
  '近1月(%)',
  '1年-夏普比率',
  '1年-最大回撤(%)',
  '1年-波动率',
]);

function recommendationFeeDisplayValue(fund = {}) {
  const exact = fund?.['费率合计(%)'];
  if (exact !== null && exact !== undefined && String(exact).trim() !== '') return exact;
  const lowerBound = fund?.['费率下界(%)'];
  if (lowerBound !== null && lowerBound !== undefined && String(lowerBound).trim() !== '') {
    return `≥${String(lowerBound).trim()}`;
  }
  return '';
}
const RECOMMENDATION_HEADER_LABEL_OVERRIDES = Object.freeze({
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
const RECOMMENDATION_TABLE_COLUMNS = [
  ...RECOMMENDATION_IDENTITY_COLUMNS.slice(0, 1),
  '序号',
  ...RECOMMENDATION_IDENTITY_COLUMNS.slice(1),
  ...RECOMMENDATION_DECISION_COLUMNS,
  ...RECOMMENDATION_STYLE_COLUMNS,
  ...RECOMMENDATION_RETURN_COLUMNS,
  ...RECOMMENDATION_RISK_COLUMNS,
  ...RECOMMENDATION_EXPLANATION_COLUMNS,
];
const RECOMMENDATION_CORE_TABLE_COLUMNS = buildFundCoreColumns({
  leadingColumns: ['对比'],
  preScoreColumns: ['收藏'],
  postScoreColumns: ['建议权重'],
  metricColumns: ['平台成立以来收益年化(%)', '1年-年化收益(%)', '实时估值'],
  trailingColumns: ['债基风格标签'],
});
const RECOMMENDATION_EXPANDED_TABLE_COLUMNS = buildFundCoreColumns({
  leadingColumns: ['对比'],
  preScoreColumns: ['收藏'],
  postScoreColumns: ['建议权重', '数据质量提示'],
  metricColumns: ['平台成立以来收益年化(%)', '1年-年化收益(%)', '近3月(%)', '近1月(%)', '实时估值', '1年-夏普比率'],
  trailingColumns: ['基金规模(亿)', '出现次数&出现频率(%)', '正式榜分组', '债基风格标签', '投资优先池得分'],
});
const RECOMMENDATION_DETAIL_GROUPS = Object.freeze([
  {
    title: FUND_DETAIL_SECTION_TITLES.time,
    fields: buildFundTimeDetailFields(['近3月(%)', '近1月(%)']),
  },
  {
    title: FUND_DETAIL_SECTION_TITLES.label,
    fields: [...FUND_SHARED_LABEL_DETAIL_FIELDS, '基金规模(亿)', '出现次数&出现频率(%)'],
  },
  {
    title: FUND_DETAIL_SECTION_TITLES.risk,
    fields: [...FUND_SHARED_RISK_DETAIL_FIELDS],
  },
  {
    title: FUND_DETAIL_SECTION_TITLES.recommendation,
    fields: ['推荐解读', '入选原因', '风险提示'],
    render: (row, helpers) => renderRecommendationInfoDetailSection(row, helpers),
  },
]);
const RECOMMENDATION_MOBILE_CARD_COLUMNS = buildFundMobileCardColumns({
  identityExtraColumns: ['对比', '收藏'],
  postScoreColumns: ['建议权重', '数据质量提示'],
  metricColumns: ['实时估值'],
  trailingColumns: ['正式榜分组', '债基风格标签', '推荐解读', '平台成立以来收益年化(%)'],
});
const RECOMMENDATION_VIEW_PARAM_KEY = 'fund_view';
const RECOMMENDATION_GROUP_SHORT_HINTS = Object.freeze({
  核心优先池: '优先名单',
  纯债稳健扩展: '纯债补充',
  增强债弹性扩展: '增强补充',
});
const RECOMMENDATION_STABILITY_SHORT_LABELS = Object.freeze({
  长期稳定性强: '长期强',
  长期稳定性中等: '长期中',
  长期稳定性偏弱: '长期弱',
});
const RECOMMENDATION_SORT_PRESETS = Object.freeze({
  balanced: {
    label: '综合平衡',
    description: '默认综合排序，适合作为通用起点。',
    sortState: { column: '正式投资评分', direction: 'desc' },
  },
  steady: {
    label: '稳健优先',
    description: '低回撤、低波动、高夏普优先。',
    sortState: { column: '1年-夏普比率', direction: 'desc' },
  },
  long_term: {
    label: '长期复利',
    description: '成立以来年化和长期稳定性优先。',
    sortState: { column: '平台成立以来收益年化(%)', direction: 'desc' },
  },
  flexible: {
    label: '收益弹性',
    description: '1年年化效率和弹性优先。',
    sortState: { column: '1年-年化收益(%)', direction: 'desc' },
  },
});
const RECOMMENDATION_SORT_PRESET_ORDER = ['balanced', 'steady', 'long_term', 'flexible'];
const RECOMMENDATION_COMPARE_SECTIONS = Object.freeze([
  {
    title: '评分与结论',
    fields: ['正式投资评分', '建议权重', '投资优先池得分'],
  },
  {
    title: '风格与定位',
    fields: ['正式榜分组', '债基风格标签', '长期稳定性标签'],
  },
  {
    title: '收益表现',
    fields: ['平台成立以来收益年化(%)', '累计净值比年化(%)', '1年-年化收益(%)', '近1月(%)', '近3月(%)'],
  },
  {
    title: '风险收益',
    fields: ['1年-夏普比率', '1年-最大回撤(%)', '1年-波动率'],
  },
  {
    title: '质量提示',
    fields: ['数据质量提示', '推荐解读'],
  },
]);
const RECOMMENDATION_COMPARE_BENCHMARK_RULES = Object.freeze({
  '正式投资评分': { direction: 'desc', label: '同风格高位' },
  '投资优先池得分': { direction: 'desc', label: '同风格高位' },
  '平台成立以来收益年化(%)': { direction: 'desc', label: '同风格最佳' },
  '1年-年化收益(%)': { direction: 'desc', label: '同风格最佳' },
  '1年-夏普比率': { direction: 'desc', label: '同风格最佳' },
  '1年-最大回撤(%)': { direction: 'asc', label: '同风格最低' },
  '1年-波动率': { direction: 'asc', label: '同风格最低' },
});
const RECOMMENDATION_VIEW_PARAM_KEYS = Object.freeze({
  group: 'group',
  view: RECOMMENDATION_VIEW_PARAM_KEY,
  density: 'density',
  sortPreset: 'sort_preset',
  sortColumn: 'sort_column',
  sortDirection: 'sort_direction',
  compareCodes: 'compare_codes',
  filterQuery: 'filter_query',
  filterStyle: 'filter_style',
  filterStability: 'filter_stability',
  filterQuality: 'filter_quality',
  filterCompany: 'filter_company',
  filterFavoriteStatus: 'filter_favorite',
  filterScoreFloor: 'filter_score_floor',
  filterAnnualizedAvailability: 'filter_annualized',
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

function normalizeRecommendationCode(value) {
  const text = String(value || '').trim();
  return /^\d+$/u.test(text) ? text.padStart(6, '0') : text;
}

const RECOMMENDATION_FOCUS_CODE = normalizeRecommendationCode(recommendationQueryParams.get('focus_code') || '');
const recommendationDetailPayloadCache = new Map();
let recommendationMobileChangeCollapsed = window.innerWidth <= 720;
let recommendationMobileCompareCollapsed = window.innerWidth <= 720;

function applyRecommendationMobileSectionState() {
  const changeShell = recommendationChangePanel?.closest('.recommendation-mobile-section-shell');
  const compareShell = recommendationComparePanel?.closest('.recommendation-mobile-section-shell');
  if (changeShell) changeShell.classList.toggle('is-collapsed', recommendationMobileChangeCollapsed);
  if (compareShell) compareShell.classList.toggle('is-collapsed', recommendationMobileCompareCollapsed);
  if (recommendationChangeToggleBtn) {
    recommendationChangeToggleBtn.textContent = recommendationMobileChangeCollapsed ? '展开名单变化' : '收起名单变化';
    recommendationChangeToggleBtn.setAttribute('aria-expanded', recommendationMobileChangeCollapsed ? 'false' : 'true');
  }
  if (recommendationCompareToggleBtn) {
    recommendationCompareToggleBtn.textContent = recommendationMobileCompareCollapsed ? '展开对比面板' : '收起对比面板';
    recommendationCompareToggleBtn.setAttribute('aria-expanded', recommendationMobileCompareCollapsed ? 'false' : 'true');
  }
}

function defaultRecommendationSortPreset() {
  return DEFAULT_RECOMMENDATION_SORT_PRESET;
}

function getRecommendationHeaderDisplayLabel(column) {
  const normalized = String(column || '').trim();
  return RECOMMENDATION_HEADER_LABEL_OVERRIDES[normalized] || normalized;
}

function buildRecommendationHeaderReferenceMap(rows) {
  return buildVisibleStyleMetricHeaderReferenceMap(rows, RECOMMENDATION_HEADER_REFERENCE_FIELDS);
}

function buildRecommendationHeaderReferenceMarkup(column, referenceSummaryMap = {}, options = {}) {
  return buildStyleMetricHeaderReferenceMarkup(column, referenceSummaryMap, {
    ...options,
    labelResolver: getRecommendationHeaderDisplayLabel,
  });
}

function defaultRecommendationFilterState() {
  return {
    query: DEFAULT_RECOMMENDATION_FILTER_STATE.query,
    style: DEFAULT_RECOMMENDATION_FILTER_STATE.style,
    stability: DEFAULT_RECOMMENDATION_FILTER_STATE.stability,
    quality: DEFAULT_RECOMMENDATION_FILTER_STATE.quality,
    company: DEFAULT_RECOMMENDATION_FILTER_STATE.company,
    favoriteStatus: DEFAULT_RECOMMENDATION_FILTER_STATE.favoriteStatus,
    scoreFloor: DEFAULT_RECOMMENDATION_FILTER_STATE.scoreFloor,
    annualizedAvailability: DEFAULT_RECOMMENDATION_FILTER_STATE.annualizedAvailability,
  };
}

function recommendationFilterStateFromUrl() {
  const favoriteStatus = String(recommendationQueryParams.get(RECOMMENDATION_VIEW_PARAM_KEYS.filterFavoriteStatus) || '').trim();
  const scoreFloor = String(recommendationQueryParams.get(RECOMMENDATION_VIEW_PARAM_KEYS.filterScoreFloor) || '').trim();
  const annualizedAvailability = String(recommendationQueryParams.get(RECOMMENDATION_VIEW_PARAM_KEYS.filterAnnualizedAvailability) || '').trim();
  return {
    query: String(recommendationQueryParams.get(RECOMMENDATION_VIEW_PARAM_KEYS.filterQuery) || '').trim(),
    style: String(recommendationQueryParams.get(RECOMMENDATION_VIEW_PARAM_KEYS.filterStyle) || '').trim(),
    stability: String(recommendationQueryParams.get(RECOMMENDATION_VIEW_PARAM_KEYS.filterStability) || '').trim(),
    quality: String(recommendationQueryParams.get(RECOMMENDATION_VIEW_PARAM_KEYS.filterQuality) || '').trim(),
    company: String(recommendationQueryParams.get(RECOMMENDATION_VIEW_PARAM_KEYS.filterCompany) || '').trim(),
    favoriteStatus: ['all', 'favorite', 'not_favorite'].includes(favoriteStatus)
      ? favoriteStatus
      : DEFAULT_RECOMMENDATION_FILTER_STATE.favoriteStatus,
    scoreFloor: ['', '75', '80', '85', '90'].includes(scoreFloor)
      ? scoreFloor
      : DEFAULT_RECOMMENDATION_FILTER_STATE.scoreFloor,
    annualizedAvailability: ['all', 'available', 'missing'].includes(annualizedAvailability)
      ? annualizedAvailability
      : DEFAULT_RECOMMENDATION_FILTER_STATE.annualizedAvailability,
  };
}

function recommendationSortStateFromUrl() {
  const preset = String(recommendationQueryParams.get(RECOMMENDATION_VIEW_PARAM_KEYS.sortPreset) || '').trim();
  const sortColumn = String(recommendationQueryParams.get(RECOMMENDATION_VIEW_PARAM_KEYS.sortColumn) || '').trim();
  const sortDirection = String(recommendationQueryParams.get(RECOMMENDATION_VIEW_PARAM_KEYS.sortDirection) || '').trim();
  if (SORTABLE_RECOMMENDATION_COLUMNS.includes(sortColumn) && ['asc', 'desc'].includes(sortDirection)) {
    return { column: sortColumn, direction: sortDirection };
  }
  if (RECOMMENDATION_SORT_PRESETS[preset]) {
    return recommendationSortPresetSortState(preset);
  }
  return defaultRecommendationSortState();
}

function recommendationViewModeFromUrl() {
  const view = String(recommendationQueryParams.get(RECOMMENDATION_VIEW_PARAM_KEY) || '').trim();
  return view === FUND_STYLE_VIEW_MODES.grouped ? FUND_STYLE_VIEW_MODES.grouped : FUND_STYLE_VIEW_MODES.mixed;
}

function recommendationDensityModeFromUrl() {
  const density = String(recommendationQueryParams.get(RECOMMENDATION_VIEW_PARAM_KEYS.density) || '').trim();
  return density === FUND_TABLE_DENSITY_MODES.expanded ? FUND_TABLE_DENSITY_MODES.expanded : FUND_TABLE_DENSITY_MODES.core;
}

function recommendationCompareCodesFromUrl() {
  return [...new Set(
    String(recommendationQueryParams.get(RECOMMENDATION_VIEW_PARAM_KEYS.compareCodes) || '')
      .split(',')
      .map((item) => normalizeRecommendationCode(item))
      .filter(Boolean)
  )].slice(0, 4);
}

function initialRecommendationGroupName(groups, focusCode, explicitGroup = '') {
  const explicitGroupName = String(explicitGroup || '').trim();
  if (explicitGroupName && groups.some((item) => String(item?.name || '').trim() === explicitGroupName)) {
    return explicitGroupName;
  }
  const groupFromUrl = String(recommendationQueryParams.get(RECOMMENDATION_VIEW_PARAM_KEYS.group) || '').trim();
  if (groupFromUrl && groups.some((item) => String(item?.name || '').trim() === groupFromUrl)) {
    return groupFromUrl;
  }
  const focusGroup = findRecommendationGroupNameByCode(groups, focusCode);
  return focusGroup || groups[0]?.name || '';
}

function syncRecommendationViewUrl({ activeGroup, sortState, sortPreset, filterState, viewMode, densityMode, compareCodes }) {
  const params = new URLSearchParams(window.location.search);
  if (activeGroup) params.set(RECOMMENDATION_VIEW_PARAM_KEYS.group, activeGroup);
  else params.delete(RECOMMENDATION_VIEW_PARAM_KEYS.group);

  if (RECOMMENDATION_FOCUS_CODE) params.set('focus_code', RECOMMENDATION_FOCUS_CODE);
  else params.delete('focus_code');
  params.delete('from');

  const normalizedPreset = normalizeRecommendationSortPreset(sortState);
  const sortPresetValue = sortPreset === 'custom' ? normalizedPreset : sortPreset;
  if (sortPresetValue && sortPresetValue !== DEFAULT_RECOMMENDATION_SORT_PRESET) {
    params.set(RECOMMENDATION_VIEW_PARAM_KEYS.sortPreset, sortPresetValue);
  } else {
    params.delete(RECOMMENDATION_VIEW_PARAM_KEYS.sortPreset);
  }
  if (sortPresetValue === 'custom' && sortState?.column && sortState?.direction) {
    params.set(RECOMMENDATION_VIEW_PARAM_KEYS.sortColumn, sortState.column);
    params.set(RECOMMENDATION_VIEW_PARAM_KEYS.sortDirection, sortState.direction);
  } else {
    params.delete(RECOMMENDATION_VIEW_PARAM_KEYS.sortColumn);
    params.delete(RECOMMENDATION_VIEW_PARAM_KEYS.sortDirection);
  }

  const filterEntries = [
    [RECOMMENDATION_VIEW_PARAM_KEYS.filterQuery, filterState.query],
    [RECOMMENDATION_VIEW_PARAM_KEYS.filterStyle, filterState.style],
    [RECOMMENDATION_VIEW_PARAM_KEYS.filterStability, filterState.stability],
    [RECOMMENDATION_VIEW_PARAM_KEYS.filterQuality, filterState.quality],
    [RECOMMENDATION_VIEW_PARAM_KEYS.filterCompany, filterState.company],
    [RECOMMENDATION_VIEW_PARAM_KEYS.filterFavoriteStatus, filterState.favoriteStatus !== 'all' ? filterState.favoriteStatus : ''],
    [RECOMMENDATION_VIEW_PARAM_KEYS.filterScoreFloor, filterState.scoreFloor],
    [RECOMMENDATION_VIEW_PARAM_KEYS.filterAnnualizedAvailability, filterState.annualizedAvailability !== 'all' ? filterState.annualizedAvailability : ''],
  ];
  filterEntries.forEach(([key, value]) => {
    if (value) params.set(key, value);
    else params.delete(key);
  });

  if (viewMode === FUND_STYLE_VIEW_MODES.grouped) {
    params.set(RECOMMENDATION_VIEW_PARAM_KEYS.view, FUND_STYLE_VIEW_MODES.grouped);
  } else {
    params.delete(RECOMMENDATION_VIEW_PARAM_KEYS.view);
  }
  if (densityMode === FUND_TABLE_DENSITY_MODES.expanded) {
    params.set(RECOMMENDATION_VIEW_PARAM_KEYS.density, FUND_TABLE_DENSITY_MODES.expanded);
  } else {
    params.delete(RECOMMENDATION_VIEW_PARAM_KEYS.density);
  }

  const normalizedCompareCodes = [...new Set((Array.isArray(compareCodes) ? compareCodes : [])
    .map((item) => normalizeRecommendationCode(item))
    .filter(Boolean))].slice(0, 4);
  if (normalizedCompareCodes.length) {
    params.set(RECOMMENDATION_VIEW_PARAM_KEYS.compareCodes, normalizedCompareCodes.join(','));
  } else {
    params.delete(RECOMMENDATION_VIEW_PARAM_KEYS.compareCodes);
  }

  const nextQuery = params.toString();
  const nextUrl = `${window.location.pathname}${nextQuery ? `?${nextQuery}` : ''}`;
  window.history.replaceState(null, '', nextUrl);
}

function normalizeRecommendationStatus(status) {
  const text = String(status || '').trim();
  if (text === 'strict_match') return '严格匹配';
  if (text === 'soft_pass_with_warnings') return '软告警';
  if (text === 'hard_block') return '已拦截';
  if (text === 'not_checked') return '未校验';
  if (text === 'unknown') return '待确认';
  return text || '待确认';
}

function compactReasonText(text) {
  return String(text || '')
    .split(/[；;]+/u)
    .map((item) => item.trim())
    .filter(Boolean)
    .slice(0, 3)
    .join('；');
}

function compactRiskText(text) {
  const compact = compactReasonText(text);
  if (!compact || compact === '当前未见额外显式风险提示') return '';
  return compact;
}

function shortRecommendationGroupHint(group) {
  const groupName = String(group?.name || '').trim();
  if (groupName && RECOMMENDATION_GROUP_SHORT_HINTS[groupName]) {
    return RECOMMENDATION_GROUP_SHORT_HINTS[groupName];
  }
  return compactUiText(String(group?.description || '').trim(), 8) || '分组';
}

function normalizeRecommendationStabilityLabel(text) {
  const value = String(text || '').trim();
  if (!value) return '';
  return RECOMMENDATION_STABILITY_SHORT_LABELS[value] || compactUiText(value, 6);
}

function recommendationSortPresetSortState(key) {
  const preset = RECOMMENDATION_SORT_PRESETS[key] || RECOMMENDATION_SORT_PRESETS[DEFAULT_RECOMMENDATION_SORT_PRESET];
  return {
    column: preset.sortState.column,
    direction: preset.sortState.direction,
  };
}

function normalizeRecommendationSortPreset(sortState) {
  const entry = Object.entries(RECOMMENDATION_SORT_PRESETS).find(([, preset]) => (
    preset.sortState.column === sortState?.column && preset.sortState.direction === sortState?.direction
  ));
  return entry?.[0] || 'custom';
}

function recommendationSortPresetLabel(key) {
  if (key === 'custom') return '自定义';
  return RECOMMENDATION_SORT_PRESETS[key]?.label || '自定义';
}

function buildRecommendationQualityFlags(fund) {
  const flags = [];
  const sinceInceptionAnnualized = String(fund['平台成立以来收益年化(%)'] || '').trim();
  if (!sinceInceptionAnnualized) {
    flags.push(`${FUND_COPYWRITING.sinceInceptionAnnualizedLabel}待确认`);
  }
  const latestNavDate = String(fund['最新净值日期'] || '').trim();
  if (latestNavDate) {
    const latest = new Date(`${latestNavDate}T00:00:00`);
    const now = new Date();
    const diff = Math.floor((now.getTime() - latest.getTime()) / (24 * 60 * 60 * 1000));
    if (Number.isFinite(diff) && diff > 10) {
      flags.push('净值日期偏旧');
    }
  }
  const holdingStatus = String(fund['持有期状态'] || '').trim();
  if (holdingStatus.includes('占位') || holdingStatus.includes('未识别')) {
    flags.push('持有期待确认');
  }
  const validation = fundValidationContract(fund);
  const validationStatus = validation.rawStatus;
  if (validationStatus === 'soft_pass_with_warnings') {
    flags.push('校验软告警');
  } else if (validationStatus === 'hard_block') {
    flags.push('校验已拦截');
  } else if (validation.neutral && validationStatus !== 'strict_match') {
    flags.push(validationStatus ? '校验待确认' : '校验状态缺失');
  }
  const confidence = String(fund['项目质量信心'] || '').trim();
  if (!confidence) {
    flags.push('质量信心缺失');
  }
  return [...new Set(flags)];
}

function buildRecommendationQualityHintText(fund) {
  const flags = buildRecommendationQualityFlags(fund);
  return flags.length ? flags.join(' / ') : '口径完整';
}

function renderRecommendationQualityHint(value) {
  const items = String(value || '')
    .split('/')
    .map((item) => item.trim())
    .filter(Boolean);
  if (!items.length || (items.length === 1 && items[0] === '口径完整')) {
    return '<span class="table-pill pill-confidence-high">口径完整</span>';
  }
  return items.map((item) => {
    const cls = item.includes('拦截') || item.includes('偏旧')
      ? 'pill-no'
      : item.includes('待确认') || item.includes('告警') || item.includes('缺失')
        ? 'pill-confidence-mid'
        : 'pill-style';
    return `<span class="table-pill ${cls}">${escapeHtml(item)}</span>`;
  }).join(' ');
}

function summarizeRecommendationActiveFilterHints(filterState = {}) {
  const hints = [];
  const query = String(filterState.query || '').trim();
  if (query) hints.push(`检索：${query}`);
  if (filterState.style) hints.push(`风格：${filterState.style}`);
  if (filterState.stability) hints.push(`长期稳定性：${filterState.stability}`);
  if (filterState.quality) hints.push(`数据质量：${filterState.quality}`);
  if (filterState.company) hints.push(`基金公司：${filterState.company}`);
  if (filterState.favoriteStatus === 'favorite') hints.push('收藏状态：仅已收藏');
  if (filterState.favoriteStatus === 'not_favorite') hints.push('收藏状态：仅未收藏');
  if (filterState.scoreFloor) hints.push(`评分下限：${filterState.scoreFloor}+`);
  if (filterState.annualizedAvailability === 'available') hints.push(FUND_COPYWRITING.sinceInceptionAnnualizedAvailableHint);
  if (filterState.annualizedAvailability === 'missing') hints.push(FUND_COPYWRITING.sinceInceptionAnnualizedMissingHint);
  return hints;
}

function summarizeRecommendationListHints(
  funds,
  totalCount,
  sortPresetKey,
  favoriteCodeSet = new Set(),
  filterState = {},
  viewMode = FUND_STYLE_VIEW_MODES.grouped,
  densityMode = FUND_TABLE_DENSITY_MODES.core,
  activeGroupLabel = '',
  payloadScopeLabel = '',
) {
  const rows = Array.isArray(funds) ? funds : [];
  const activeFilterHints = summarizeRecommendationActiveFilterHints(filterState);
  const styleCount = new Set(rows.map((item) => normalizeFundStyleLabel(item['债基风格标签'])).filter((style) => style && style !== '未标注风格')).size;
  const annualizedAvailableCount = rows.filter((item) => String(item['平台成立以来收益年化(%)'] ?? '').trim()).length;
  const scopeHint = String(payloadScopeLabel || '').trim();
  const densityHint = densityMode === FUND_TABLE_DENSITY_MODES.expanded
    ? '扩展视图：主表含更多字段'
    : '核心视图：基准见行展开';
  if (!rows.length) {
    return [
      scopeHint,
      densityHint,
      activeGroupLabel ? `分组：${activeGroupLabel}` : '',
      `当前分组 ${formatNumber(0)} / ${formatNumber(totalCount || 0)} 只`,
      `排序视角：${recommendationSortPresetLabel(sortPresetKey)}`,
      viewMode === FUND_STYLE_VIEW_MODES.grouped ? '当前按风格分组' : '当前为混合总表',
      ...activeFilterHints,
    ].filter(Boolean);
  }
  const longTermPending = rows.filter((item) => !String(item['平台成立以来收益年化(%)'] ?? '').trim()).length;
  const holdingPending = rows.filter((item) => buildRecommendationQualityFlags(item).includes('持有期待确认')).length;
  const staleNav = rows.filter((item) => buildRecommendationQualityFlags(item).includes('净值日期偏旧')).length;
  const qualitySoftWarnings = rows.filter((item) => buildRecommendationQualityFlags(item).includes('校验软告警')).length;
  const favoriteVisibleCount = rows.filter((item) => {
    const code = String(item['基金代码'] || '').trim();
    return code && favoriteCodeSet.has(code);
  }).length;
  const hints = [
    scopeHint,
    densityHint,
    activeGroupLabel ? `分组：${activeGroupLabel}` : '',
    `当前分组 ${formatNumber(rows.length)} / ${formatNumber(totalCount || rows.length)} 只`,
    `覆盖风格 ${formatNumber(styleCount)} 个`,
    `${FUND_COPYWRITING.sinceInceptionAnnualizedAvailableCountLabel} ${formatNumber(annualizedAvailableCount)} 只`,
    `排序视角：${recommendationSortPresetLabel(sortPresetKey)}`,
    viewMode === FUND_STYLE_VIEW_MODES.grouped ? '当前按风格分组' : '当前为混合总表',
    ...activeFilterHints,
    `已收藏命中 ${formatNumber(favoriteVisibleCount)} 只`,
    `${FUND_COPYWRITING.sinceInceptionAnnualizedPendingCountLabel} ${formatNumber(longTermPending)} 只`,
  ];
  if (holdingPending > 0) hints.push(`持有期待确认 ${formatNumber(holdingPending)} 只`);
  if (staleNav > 0) hints.push(`净值偏旧 ${formatNumber(staleNav)} 只`);
  if (qualitySoftWarnings > 0) hints.push(`校验软告警 ${formatNumber(qualitySoftWarnings)} 只`);
  return hints.filter(Boolean);
}

function renderRecommendationSortPresetSwitch(activePreset, onChange) {
  if (!recommendationSortPresetSwitch) return;
  recommendationSortPresetSwitch.innerHTML = RECOMMENDATION_SORT_PRESET_ORDER.map((key) => `
    <button
      type="button"
      class="${key === activePreset ? 'is-active' : ''}"
      data-preset="${escapeHtml(key)}"
      aria-pressed="${key === activePreset ? 'true' : 'false'}"
      title="${escapeHtml(RECOMMENDATION_SORT_PRESETS[key].description)}"
    >${escapeHtml(RECOMMENDATION_SORT_PRESETS[key].label)}</button>
  `).join('');
  recommendationSortPresetSwitch.querySelectorAll('button').forEach((button) => {
    button.addEventListener('click', () => {
      const nextPreset = button.dataset.preset || '';
      if (nextPreset && nextPreset !== activePreset) onChange(nextPreset);
    });
  });
}

function renderRecommendationListHintsPanel(
  funds,
  totalCount,
  sortPresetKey,
  favoriteCodeSet,
  filterState = {},
  viewMode = FUND_STYLE_VIEW_MODES.grouped,
  densityMode = FUND_TABLE_DENSITY_MODES.core,
  activeGroupLabel = '',
  payloadScopeLabel = '',
) {
  if (!recommendationListHints) return;
  const hints = summarizeRecommendationListHints(
    funds,
    totalCount,
    sortPresetKey,
    favoriteCodeSet,
    filterState,
    viewMode,
    densityMode,
    activeGroupLabel,
    payloadScopeLabel,
  ).slice(0, 5);
  renderSummaryHintChips(recommendationListHints, hints);
}

function renderRecommendationEmptyState(groups, currentGroup, filterState, totalFunds) {
  const activeFilters = summarizeRecommendationActiveFilterHints(filterState);
  const groupName = String(currentGroup?.name || '').trim() || '当前分组';
  const originalCount = formatNumber(totalFunds || 0);
  const groupList = Array.isArray(groups) ? groups : [];
  const currentIndex = groupList.findIndex((item) => String(item?.name || '').trim() === groupName);
  const nextGroup = currentIndex >= 0 && groupList.length > 1
    ? groupList[(currentIndex + 1) % groupList.length]
    : null;
  const nextGroupName = String(nextGroup?.name || '').trim();
  const reasonText = activeFilters.length
    ? `当前筛选：${activeFilters.join(' / ')}`
    : '当前分组没有可展示基金。';
  const suggestionText = filterState?.query
    ? `建议先清空检索词，${nextGroupName ? `或切换到${nextGroupName}继续查找。` : '或切换到其他分组继续查找。'}`
      : activeFilters.length
        ? `建议重置部分筛选条件，${nextGroupName ? `或切换到${nextGroupName}继续比较。` : '或切换到其他分组继续比较。'}`
        : (nextGroupName ? `建议切换到${nextGroupName}继续查看。` : '建议切换到其他分组继续查看。');
  return `
    <div class="empty-state-inline recommendation-empty-state">
      <strong>当前分组暂无匹配基金</strong>
      <span>${escapeHtml(`${groupName} 原始 ${originalCount} 只，当前筛选后 0 只。`)}</span>
      <small>${escapeHtml(reasonText)}</small>
      <small>${escapeHtml(suggestionText)}</small>
      <div class="recommendation-empty-state-actions">
        ${filterState?.query ? '<button type="button" class="secondary" data-rec-empty-action="clear-query">清空检索词</button>' : ''}
        ${activeFilters.length ? '<button type="button" class="secondary" data-rec-empty-action="reset-filters">重置筛选</button>' : ''}
        ${nextGroupName ? `<button type="button" class="secondary" data-rec-empty-action="next-group" data-rec-next-group="${escapeHtml(nextGroupName)}">切换到 ${escapeHtml(nextGroupName)}</button>` : ''}
      </div>
    </div>
  `;
}

function renderRecommendationChangePanel(trackerSummary = {}) {
  if (!recommendationChangePanel) return;
  const changeSummary = trackerSummary?.latest_membership_change_summary || {};
  const transitionCounts = Array.isArray(trackerSummary?.latest_transition_counts)
    ? trackerSummary.latest_transition_counts
    : [];
  if (!changeSummary.available) {
    recommendationChangePanel.innerHTML = `
      <div class="subtle-panel recommendation-change-empty">
        <strong>上一版变化</strong>
        <span>当前唯一正式快照不足，暂时无法判断新增、回归和掉出。</span>
      </div>
    `;
    return;
  }
  const rolling = changeSummary?.rolling_change_stability_summary || {};
  const transitionItems = transitionCounts.length
    ? transitionCounts.map((item) => `
      <span class="table-pill pill-style">${escapeHtml(item.label)} ${escapeHtml(formatNumber(item.count || 0))}只</span>
    `).join('')
    : '';
  const sampleRows = [
    { label: '新增样本', codes: changeSummary.new_entry_codes_sample || [] },
    { label: '回归样本', codes: changeSummary.returned_codes_sample || [] },
    { label: '掉出样本', codes: changeSummary.dropped_codes_sample || [] },
  ].filter((item) => Array.isArray(item.codes) && item.codes.length);
  const rollingText = rolling.available
    ? `近${formatNumber(rolling.window_size)}期均值：新入/回归 ${formatNumber(rolling.mean_current_change_rate_pct)}%，掉出 ${formatNumber(rolling.mean_dropped_rate_pct)}%；当前偏离 ${Number(rolling.current_vs_rolling_change_gap_pct) > 0 ? '+' : ''}${formatNumber(rolling.current_vs_rolling_change_gap_pct)}pct。`
    : '';
  recommendationChangePanel.innerHTML = `
    <div class="card-title-row recommendation-change-header">
      <div>
        <div class="card-title">上一版变化</div>
        <div class="card-subtitle">正式主榜 ${escapeHtml(String(changeSummary.previous_period || '-'))} -> ${escapeHtml(String(changeSummary.current_period || '-'))}</div>
      </div>
    </div>
    <div class="recommendation-change-grid">
      <div class="subtle-panel">
        <strong>本期变化</strong>
        <span>${escapeHtml(trackerSummary.latest_changes || '暂无摘要')}</span>
        ${transitionItems ? `<div class="recommendation-change-tags">${transitionItems}</div>` : ''}
      </div>
      <div class="subtle-panel">
        <strong>波动提示</strong>
        <span>${escapeHtml((trackerSummary.narrative_summary || {}).risk_hint || '当前暂无额外波动提示。')}</span>
        ${rollingText ? `<small>${escapeHtml(rollingText)}</small>` : ''}
      </div>
    </div>
    ${sampleRows.length ? `
      <div class="recommendation-change-samples">
        ${sampleRows.map((item) => `
          <div class="recommendation-change-sample-row">
            <strong>${escapeHtml(item.label)}</strong>
            <span>${escapeHtml(item.codes.join(' / '))}</span>
          </div>
        `).join('')}
      </div>
    ` : ''}
  `;
}

function buildRecommendationFilterOptions(funds) {
  const rows = Array.isArray(funds) ? funds : [];
  const styleOptions = [...new Set(rows.map((item) => String(item['债基风格标签'] || '').trim()).filter(Boolean))].sort((a, b) => a.localeCompare(b, 'zh-CN'));
  const stabilityOptions = [...new Set(rows.map((item) => String(item['长期稳定性标签'] || '').trim()).filter(Boolean))].sort((a, b) => a.localeCompare(b, 'zh-CN'));
  const companyOptions = [...new Set(rows.map((item) => String(item['基金公司'] || '').trim()).filter(Boolean))].sort((a, b) => a.localeCompare(b, 'zh-CN'));
  const qualityOptions = [...new Set(
    rows.flatMap((item) => buildRecommendationQualityFlags(item))
      .filter(Boolean)
  )].sort((a, b) => a.localeCompare(b, 'zh-CN'));
  return {
    styleOptions,
    stabilityOptions,
    companyOptions,
    qualityOptions,
  };
}

function filterRecommendationFunds(funds, filterState, favoriteCodeSet = new Set()) {
  const rows = Array.isArray(funds) ? funds : [];
  const query = String(filterState.query || '').trim().toLowerCase();
  return rows.filter((fund) => {
    const code = String(fund['基金代码'] || '').trim();
    const codeLower = code.toLowerCase();
    const name = String(fund['基金名称'] || '').trim();
    const nameLower = name.toLowerCase();
    const style = String(fund['债基风格标签'] || '').trim();
    const stability = String(fund['长期稳定性标签'] || '').trim();
    const company = String(fund['基金公司'] || '').trim();
    const qualityFlags = buildRecommendationQualityFlags(fund);
    const annualized = parseMetricDisplayNumber(fund['平台成立以来收益年化(%)']);
    const score = parseMetricDisplayNumber(fund['正式投资评分']);

    if (query && !codeLower.includes(query) && !nameLower.includes(query)) return false;
    if (filterState.style && style !== filterState.style) return false;
    if (filterState.stability && stability !== filterState.stability) return false;
    if (filterState.company && company !== filterState.company) return false;
    if (filterState.favoriteStatus === 'favorite' && !favoriteCodeSet.has(code)) return false;
    if (filterState.favoriteStatus === 'not_favorite' && favoriteCodeSet.has(code)) return false;
    if (filterState.quality && !qualityFlags.includes(filterState.quality)) return false;
    if (filterState.annualizedAvailability === 'available' && annualized === null) return false;
    if (filterState.annualizedAvailability === 'missing' && annualized !== null) return false;
    if (filterState.scoreFloor) {
      const floor = Number(filterState.scoreFloor);
      if (Number.isFinite(floor) && (!Number.isFinite(score) || score < floor)) return false;
    }
    return true;
  });
}

function renderRecommendationFilterBar(funds, filterState, onChange, onReset) {
  if (!recommendationFilterBar) return;
  const options = buildRecommendationFilterOptions(funds);
  const buildSelect = (id, label, value, selectOptions, placeholder) => `
    <label class="recommendation-filter-field">
      <span>${escapeHtml(label)}</span>
      <select data-filter-id="${escapeHtml(id)}">
        <option value="">${escapeHtml(placeholder)}</option>
        ${selectOptions.map((item) => `
          <option value="${escapeHtml(item)}" ${item === value ? 'selected' : ''}>${escapeHtml(item)}</option>
        `).join('')}
      </select>
    </label>
  `;
  recommendationFilterBar.innerHTML = `
    <div class="recommendation-filter-grid">
      <label class="recommendation-filter-field recommendation-filter-field-search">
        <span>代码 / 名称</span>
        <input
          type="search"
          value="${escapeHtml(filterState.query || '')}"
          data-filter-id="query"
          placeholder="输入基金代码或名称"
        />
      </label>
      ${buildSelect('style', '风格', filterState.style, options.styleOptions, '全部风格')}
      ${buildSelect('stability', '长期稳定性', filterState.stability, options.stabilityOptions, '全部稳定性')}
      ${buildSelect('quality', '数据质量', filterState.quality, options.qualityOptions, '全部质量状态')}
      ${buildSelect('company', '基金公司', filterState.company, options.companyOptions, '全部公司')}
      <label class="recommendation-filter-field">
        <span>收藏状态</span>
        <select data-filter-id="favoriteStatus">
          <option value="all" ${filterState.favoriteStatus === 'all' ? 'selected' : ''}>全部</option>
          <option value="favorite" ${filterState.favoriteStatus === 'favorite' ? 'selected' : ''}>仅已收藏</option>
          <option value="not_favorite" ${filterState.favoriteStatus === 'not_favorite' ? 'selected' : ''}>仅未收藏</option>
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
      <label class="recommendation-filter-field">
        <span>${escapeHtml(FUND_COPYWRITING.sinceInceptionAnnualizedLabel)}</span>
        <select data-filter-id="annualizedAvailability">
          <option value="all" ${filterState.annualizedAvailability === 'all' ? 'selected' : ''}>全部</option>
          <option value="available" ${filterState.annualizedAvailability === 'available' ? 'selected' : ''}>仅显示可用</option>
          <option value="missing" ${filterState.annualizedAvailability === 'missing' ? 'selected' : ''}>仅显示待确认</option>
        </select>
      </label>
    </div>
    <div class="recommendation-filter-actions">
      <button type="button" class="secondary" data-filter-reset="1">重置筛选</button>
    </div>
  `;
  recommendationFilterBar.querySelectorAll('[data-filter-id]').forEach((element) => {
    const eventName = element.tagName === 'INPUT' ? 'input' : 'change';
    element.addEventListener(eventName, () => {
      const id = element.dataset.filterId || '';
      if (!id) return;
      onChange(id, element.value);
    });
  });
  recommendationFilterBar.querySelector('[data-filter-reset="1"]')?.addEventListener('click', onReset);
}

function renderRecommendationCompareToggle(code, checked, disabled) {
  const safeCode = escapeHtml(code);
  return `
    <label class="recommendation-compare-toggle ${disabled ? 'is-disabled' : ''}">
      <input type="checkbox" data-compare-code="${safeCode}" ${checked ? 'checked' : ''} ${disabled ? 'disabled' : ''} />
      <span>${checked ? '已选' : '加入'}</span>
    </label>
  `;
}

function buildFavoriteCodeSet(historyPayload = {}) {
  const records = Array.isArray(historyPayload?.records) ? historyPayload.records : [];
  const set = new Set();
  records.forEach((item) => {
    if (String(item?.entry_type || '').trim() !== 'favorite') return;
    const codes = Array.isArray(item?.codes) ? item.codes : [];
    codes.forEach((code) => {
      const text = String(code || '').trim();
      if (text) set.add(text);
    });
  });
  return set;
}

async function saveFavoriteHistoryEntry(payload) {
  return fetchJson('/api/favorites/history', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
}

function renderRecommendationActionBanner(message, className = '') {
  if (!recommendationActionBanner) return;
  recommendationActionBanner.className = `info-banner recommendation-action-banner ${className}`.trim();
  recommendationActionBanner.innerHTML = `
    <strong>操作建议</strong>
    <span>${escapeHtml(message)}</span>
  `;
}

function recommendationPublicationStatus(payload = {}) {
  return payload?.publication_status_summary && typeof payload.publication_status_summary === 'object'
    ? payload.publication_status_summary
    : {};
}

function publicationRunValue(record = {}, fallback = '-') {
  const runId = String(record?.run_id || fallback || '-').trim() || '-';
  return runId;
}

function publicationNavDate(record = {}, fallback = '-') {
  const snapshot = record?.snapshot && typeof record.snapshot === 'object' ? record.snapshot : {};
  return String(record?.effective_nav_date || snapshot?.candidate_date || fallback || '-').trim() || '-';
}

function latestSuccessfulPublicationLabel(publicationStatus = {}) {
  if (!publicationStatus?.latest_successful_run_id) return '暂无最新运行';
  if (publicationStatus.latest_successful_is_currently_published) return '已正式发布';
  if (publicationStatus.latest_successful_publish_blocked) return '发布阻断';
  if (publicationStatus.latest_successful_is_publishable) return '发布指针异常';
  return '待确认';
}

function recommendationSnapshotText(payload = {}) {
  const publicationStatus = recommendationPublicationStatus(payload);
  const trackerSummary = payload?.tracker_summary || {};
  return String(
    publicationStatus.current_effective_nav_date
      || trackerSummary.latest_nav_snapshot_date
      || '-',
  ).trim() || '-';
}

function recommendationVersionMetaText(payload = {}, extraParts = []) {
  const publicationStatus = recommendationPublicationStatus(payload);
  const latestRunId = publicationStatus.latest_successful_run_id || '';
  const latestNavDate = publicationStatus.latest_successful_effective_nav_date || '';
  const baseParts = [
    `正式发布 ${publicationStatus.current_published_run_id || payload?.run_id || '-'}`,
    `净值 ${recommendationSnapshotText(payload)}`,
    payloadSourceContractSummary(payload),
  ];
  if (latestRunId && latestRunId !== (publicationStatus.current_published_run_id || payload?.run_id)) {
    baseParts.push(`最新运行 ${latestRunId}`);
  }
  if (latestNavDate && latestNavDate !== recommendationSnapshotText(payload)) {
    baseParts.push(`最新净值 ${latestNavDate}`);
  }
  return [...baseParts, ...extraParts.filter(Boolean)].join(' / ');
}

function renderRecommendationHeroMeta(payload = {}, displayCount = 0, reportTiming = {}, groupCount = 0) {
  const publicationStatus = recommendationPublicationStatus(payload);
  const currentRecord = publicationStatus.current_published || {};
  const latestRecord = publicationStatus.latest_successful || {};
  const latestRunId = publicationStatus.latest_successful_run_id || latestRecord.run_id || '';
  const currentRunId = publicationStatus.current_published_run_id || currentRecord.run_id || payload?.run_id || '-';
  const latestRunValue = latestRunId || '-';
  const latestHint = latestRunId && latestRunId !== currentRunId
    ? `${latestSuccessfulPublicationLabel(publicationStatus)} / 净值 ${publicationStatus.latest_successful_effective_nav_date || publicationNavDate(latestRecord)}`
    : '与正式发布一致';
  recommendationsHeroMeta.innerHTML = [
    buildStatCard('正式发布版本', currentRunId, `净值 ${publicationStatus.current_effective_nav_date || publicationNavDate(currentRecord, recommendationSnapshotText(payload))}`),
    buildStatCard('最新筛选运行', latestRunValue, latestHint),
    buildStatCard('展示基金数', formatNumber(displayCount), `${formatNumber(groupCount)} 个分组`),
    buildStatCard('构建耗时', reportTiming?.available ? formatSeconds(reportTiming.total_seconds, 1) : '-', '当前正式发布报告'),
  ].join('');
}

function formatPublicationSignal(signal = {}) {
  const label = String(signal?.label || signal?.field || '').trim();
  const baseline = signal?.baseline_value ?? '-';
  const candidate = signal?.candidate_value ?? '-';
  const dropCount = signal?.drop_count;
  const dropRatioPct = signal?.drop_ratio_pct;
  const ratioText = Number.isFinite(Number(dropRatioPct)) ? formatPercent(Number(dropRatioPct), 2) : '';
  return [
    label ? `${label} ${baseline}→${candidate}` : `${baseline}→${candidate}`,
    dropCount !== null && dropCount !== undefined ? `减少 ${formatNumber(dropCount)}` : '',
    ratioText ? `降幅 ${ratioText}` : '',
  ].filter(Boolean).join('，');
}

function renderRecommendationPublicationBanner(payload = {}, fallbackMessage = '') {
  const publicationStatus = recommendationPublicationStatus(payload);
  if (!publicationStatus?.latest_successful_run_id) {
    renderRecommendationActionBanner(fallbackMessage || '推荐摘要已加载，等待最新自动运行状态');
    return;
  }

  if (!publicationStatus.formal_publication_stale_against_latest_successful) {
    renderRecommendationActionBanner(
      fallbackMessage || `最新筛选运行 ${publicationStatus.latest_successful_run_id} 已是当前正式发布版本`,
      'info-banner-success',
    );
    return;
  }

  const guard = publicationStatus.contraction_guard || {};
  const signals = Array.isArray(guard.signals)
    ? guard.signals.map(formatPublicationSignal).filter(Boolean)
    : [];
  const detail = signals.length
    ? signals.join('；')
    : (publicationStatus.primary_blocker?.detail || publicationStatus.detail || '');
  const message = [
    `正式发布仍为 ${publicationStatus.current_published_run_id || payload?.run_id || '-'}，净值 ${publicationStatus.current_effective_nav_date || recommendationSnapshotText(payload)}`,
    `最新筛选运行 ${publicationStatus.latest_successful_run_id}，净值 ${publicationStatus.latest_successful_effective_nav_date || '-'}`,
    publicationStatus.status_label,
    detail,
  ].filter(Boolean).join('；');
  renderRecommendationActionBanner(message, 'info-banner-warning');
}

function buildFavoritePageUrl(code) {
  const currentParams = new URLSearchParams(window.location.search);
  const params = new URLSearchParams({
    codes: normalizeRecommendationCode(code),
    label: `推荐页收藏 ${normalizeRecommendationCode(code)}`,
    auto_query: '1',
    save_history: '0',
    focus_code: normalizeRecommendationCode(code),
    from: 'recommendations',
  });
  const favoriteScoreFloor = String(currentParams.get(RECOMMENDATION_VIEW_PARAM_KEYS.filterScoreFloor) || '').trim();
  const favoriteAnnualized = String(currentParams.get(RECOMMENDATION_VIEW_PARAM_KEYS.filterAnnualizedAvailability) || '').trim();
  if (favoriteScoreFloor) params.set('filter_score_floor', favoriteScoreFloor);
  if (favoriteAnnualized && favoriteAnnualized !== 'all') params.set('filter_annualized', favoriteAnnualized);

  const contextEntries = [
    [RECOMMENDATION_VIEW_PARAM_KEYS.group, RECOMMENDATION_RETURN_CONTEXT_PARAM_KEYS.group],
    [RECOMMENDATION_VIEW_PARAM_KEYS.sortPreset, RECOMMENDATION_RETURN_CONTEXT_PARAM_KEYS.sortPreset],
    [RECOMMENDATION_VIEW_PARAM_KEYS.sortColumn, RECOMMENDATION_RETURN_CONTEXT_PARAM_KEYS.sortColumn],
    [RECOMMENDATION_VIEW_PARAM_KEYS.sortDirection, RECOMMENDATION_RETURN_CONTEXT_PARAM_KEYS.sortDirection],
    [RECOMMENDATION_VIEW_PARAM_KEYS.filterQuery, RECOMMENDATION_RETURN_CONTEXT_PARAM_KEYS.filterQuery],
    [RECOMMENDATION_VIEW_PARAM_KEYS.filterStyle, RECOMMENDATION_RETURN_CONTEXT_PARAM_KEYS.filterStyle],
    [RECOMMENDATION_VIEW_PARAM_KEYS.filterStability, RECOMMENDATION_RETURN_CONTEXT_PARAM_KEYS.filterStability],
    [RECOMMENDATION_VIEW_PARAM_KEYS.filterQuality, RECOMMENDATION_RETURN_CONTEXT_PARAM_KEYS.filterQuality],
    [RECOMMENDATION_VIEW_PARAM_KEYS.filterCompany, RECOMMENDATION_RETURN_CONTEXT_PARAM_KEYS.filterCompany],
    [RECOMMENDATION_VIEW_PARAM_KEYS.filterFavoriteStatus, RECOMMENDATION_RETURN_CONTEXT_PARAM_KEYS.filterFavoriteStatus],
    [RECOMMENDATION_VIEW_PARAM_KEYS.filterScoreFloor, RECOMMENDATION_RETURN_CONTEXT_PARAM_KEYS.filterScoreFloor],
    [RECOMMENDATION_VIEW_PARAM_KEYS.filterAnnualizedAvailability, RECOMMENDATION_RETURN_CONTEXT_PARAM_KEYS.filterAnnualizedAvailability],
  ];
  contextEntries.forEach(([sourceKey, targetKey]) => {
    const value = String(currentParams.get(sourceKey) || '').trim();
    if (value) params.set(targetKey, value);
  });
  return `/favorites?${params.toString()}`;
}

function renderRecommendationFavoriteToggle(code, saved) {
  const safeCode = escapeHtml(code);
  return `
    <button
      type="button"
      class="recommendation-favorite-toggle ${saved ? 'is-saved' : ''}"
      data-favorite-code="${safeCode}"
      data-favorite-action="${saved ? 'open' : 'save'}"
      aria-pressed="${saved ? 'true' : 'false'}"
    >${saved ? '去收藏页' : '收藏'}</button>
  `;
}

function renderRecommendationExpandToggle(row) {
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

function findRecommendationGroupNameByCode(groups, code) {
  const targetCode = normalizeRecommendationCode(code);
  if (!targetCode) return '';
  for (const group of Array.isArray(groups) ? groups : []) {
    const funds = Array.isArray(group?.funds) ? group.funds : [];
    if (funds.some((item) => normalizeRecommendationCode(item?.['基金代码']) === targetCode)) {
      return String(group?.name || '').trim();
    }
  }
  return '';
}

function focusRecommendationTarget(code) {
  const targetCode = normalizeRecommendationCode(code);
  if (!targetCode || !recommendationCards) return false;
  const target = recommendationCards.querySelector(`[data-fund-code="${escapeHtml(targetCode)}"]`);
  if (!target) return false;
  target.classList.add('is-focus-target');
  target.scrollIntoView({ behavior: 'smooth', block: 'center', inline: 'nearest' });
  return true;
}

function buildRecommendationCompareValueMeta(column, fund) {
  const value = parseScoreMetricValue(fund?.[column]);
  switch (column) {
    case '正式投资评分':
      if (value === null) return null;
      if (value >= 90) return { tone: 'strong', badge: '高分' };
      if (value >= 85) return { tone: 'soft', badge: '优势' };
      if (value < 75) return { tone: 'weak', badge: '偏弱' };
      return { tone: 'neutral', badge: '观察' };
    case '投资优先池得分':
      if (value === null) return null;
      if (value >= 95) return { tone: 'strong', badge: '优先' };
      if (value >= 90) return { tone: 'soft', badge: '靠前' };
      if (value < 80) return { tone: 'weak', badge: '偏低' };
      return null;
    case '平台成立以来收益年化(%)':
      if (value === null) return null;
      if (value >= 5) return { tone: 'strong', badge: '长期强' };
      if (value >= 2.5) return { tone: 'soft', badge: '长期稳' };
      if (value < 1.5) return { tone: 'weak', badge: '待确认' };
      return null;
    case '1年-年化收益(%)':
      if (value === null) return null;
      if (value >= 6) return { tone: 'strong', badge: '弹性强' };
      if (value >= 2) return { tone: 'soft', badge: '稳健' };
      if (value < 1.5) return { tone: 'weak', badge: '偏弱' };
      return null;
    case '1年-夏普比率':
      if (value === null) return null;
      if (value >= 3) return { tone: 'strong', badge: '高夏普' };
      if (value >= 2) return { tone: 'soft', badge: '稳健' };
      if (value < 1.5) return { tone: 'weak', badge: '偏低' };
      return null;
    case '1年-最大回撤(%)':
      if (value === null) return null;
      if (value <= 0.005) return { tone: 'strong', badge: '回撤低' };
      if (value <= 0.015) return { tone: 'soft', badge: '可控' };
      if (value >= 0.03) return { tone: 'weak', badge: '偏大' };
      return null;
    case '1年-波动率':
      if (value === null) return null;
      if (value <= 0.01) return { tone: 'strong', badge: '低波' };
      if (value <= 0.02) return { tone: 'soft', badge: '平稳' };
      if (value >= 0.04) return { tone: 'weak', badge: '偏高' };
      return null;
    default:
      return null;
  }
}

function buildRecommendationCompareBenchmarks(funds) {
  const rows = Array.isArray(funds) ? funds : [];
  const result = {};
  const rowsByStyle = new Map();
  rows.forEach((fund) => {
    const style = String(fund?.['债基风格标签'] || '').trim() || '未标注风格';
    if (!rowsByStyle.has(style)) rowsByStyle.set(style, []);
    rowsByStyle.get(style).push(fund);
  });
  Object.entries(RECOMMENDATION_COMPARE_BENCHMARK_RULES).forEach(([column, rule]) => {
    const winnerCodes = [];
    rowsByStyle.forEach((styleRows) => {
      if (!Array.isArray(styleRows) || styleRows.length < 2) return;
      const numericRows = styleRows
        .map((fund) => ({
          code: normalizeRecommendationCode(fund?.['基金代码'] || ''),
          value: parseScoreMetricValue(fund?.[column]),
        }))
        .filter((item) => item.code && item.value !== null);
      if (numericRows.length < 2) return;
      const targetValue = rule.direction === 'asc'
        ? Math.min(...numericRows.map((item) => item.value))
        : Math.max(...numericRows.map((item) => item.value));
      numericRows
        .filter((item) => item.value === targetValue)
        .forEach((item) => winnerCodes.push(item.code));
    });
    if (!winnerCodes.length) return;
    result[column] = {
      winnerCodes,
      label: rule.label,
    };
  });
  return result;
}

function renderRecommendationCompareValue(column, fund, qualityHintText, benchmarkMap = {}) {
  if (column === '数据质量提示') {
    return renderRecommendationQualityHint(qualityHintText);
  }
  if (column === '推荐解读') {
    const digest = String(fund?.['推荐解读'] || buildRecommendationDigest(fund) || '').trim();
    return digest
      ? `<span class="table-note">${escapeHtml(digest)}</span>`
      : '<span class="favorite-detail-value-empty">-</span>';
  }
  const rawValue = fund?.[column];
  const text = String(rawValue || '').trim();
  if (!text) {
    return '<span class="favorite-detail-value-empty">-</span>';
  }
  if (['正式榜分组', '债基风格标签'].includes(column)) {
    return `<span class="table-pill pill-style">${escapeHtml(text)}</span>`;
  }
  if (column === '长期稳定性标签') {
    const cls = text.includes('强')
      ? 'pill-confidence-high'
      : text.includes('中')
        ? 'pill-confidence-mid'
        : 'pill-confidence-low';
    return `<span class="table-pill ${cls}">${escapeHtml(text)}</span>`;
  }

  const metricMarkup = ['实时估值', '近1月(%)', '近3月(%)', '1年-年化收益(%)', '平台成立以来收益年化(%)'].includes(column)
    ? renderSignedPercentValue(rawValue)
    : renderCell(column, rawValue);
  const meta = buildRecommendationCompareValueMeta(column, fund);
  const benchmark = benchmarkMap[column];
  const code = normalizeRecommendationCode(fund?.['基金代码'] || '');
  const benchmarkHit = Boolean(
    benchmark
    && code
    && Array.isArray(benchmark.winnerCodes)
    && benchmark.winnerCodes.includes(code)
  );
  if (!meta?.badge && !benchmarkHit) {
    return metricMarkup;
  }
  return `
    <div class="recommendation-compare-value-stack">
      <div class="recommendation-compare-value-main recommendation-compare-value-main-${escapeHtml(meta.tone || 'neutral')}">
        ${metricMarkup}
      </div>
      <div class="recommendation-compare-pill-row">
        ${meta?.badge ? `<span class="table-pill recommendation-compare-pill recommendation-compare-pill-${escapeHtml(meta.tone || 'neutral')}">${escapeHtml(meta.badge)}</span>` : ''}
        ${benchmarkHit ? `<span class="table-pill recommendation-compare-pill recommendation-compare-pill-benchmark">${escapeHtml(benchmark.label || '对比优')}</span>` : ''}
      </div>
    </div>
  `;
}

function renderRecommendationComparePanel(funds, compareCodes, onToggle, onClear) {
  if (!recommendationComparePanel) return;
  const rows = Array.isArray(funds) ? funds : [];
  const selectedFunds = compareCodes
    .map((code) => rows.find((item) => String(item['基金代码'] || '').trim() === code))
    .filter(Boolean)
    .sort((left, right) => {
      const scoreDiff = (parseScoreMetricValue(right?.['正式投资评分']) ?? -Infinity) - (parseScoreMetricValue(left?.['正式投资评分']) ?? -Infinity);
      if (scoreDiff !== 0) return scoreDiff;
      return (parseScoreMetricValue(right?.['平台成立以来收益年化(%)']) ?? -Infinity) - (parseScoreMetricValue(left?.['平台成立以来收益年化(%)']) ?? -Infinity);
    });
  const compareStyleCounts = selectedFunds.reduce((map, fund) => {
    const style = String(fund?.['债基风格标签'] || '').trim() || '未标注风格';
    map.set(style, (map.get(style) || 0) + 1);
    return map;
  }, new Map());
  if (!selectedFunds.length) {
    recommendationComparePanel.innerHTML = `
      <div class="subtle-panel recommendation-compare-empty">
        <strong>对比模式</strong>
        <span>勾选 2 到 4 只基金后，可并排查看核心指标。</span>
      </div>
    `;
    return;
  }
  recommendationComparePanel.innerHTML = `
    <div class="card-title-row recommendation-compare-header">
      <div>
        <div class="card-title">对比模式</div>
        <div class="card-subtitle">当前已选 ${selectedFunds.length} / 4，按正式投资评分优先排序；基准提示仅在同风格样本 >= 2 时显示</div>
      </div>
      <button type="button" class="secondary" data-compare-clear="1">清空对比</button>
    </div>
    <div class="recommendation-compare-grid">
      ${(() => {
        const benchmarkMap = buildRecommendationCompareBenchmarks(selectedFunds);
        return selectedFunds.map((fund) => {
          const qualityHintText = buildRecommendationQualityHintText(fund);
          const style = String(fund?.['债基风格标签'] || '').trim() || '未标注风格';
          const sameStyleCount = Number(compareStyleCounts.get(style) || 0);
          const sameStyleHint = sameStyleCount >= 2
            ? `同风格样本 ${sameStyleCount} 只，已启用同风格基准`
            : `同风格样本仅 ${sameStyleCount} 只，本卡不显示同风格基准`;
        return `
          <article class="recommendation-compare-card">
            <div class="recommendation-compare-card-header">
              <strong>${escapeHtml(String(fund['基金名称'] || '-'))}</strong>
              <button type="button" class="secondary recommendation-compare-remove" data-compare-remove="${escapeHtml(String(fund['基金代码'] || ''))}">移除</button>
            </div>
            <div class="recommendation-compare-card-code">${escapeHtml(String(fund['基金代码'] || ''))}</div>
            <div class="recommendation-compare-card-style-note">${escapeHtml(sameStyleHint)}</div>
            <div class="recommendation-compare-sections">
              ${RECOMMENDATION_COMPARE_SECTIONS.map((section) => `
                <section class="recommendation-compare-section">
                  <div class="recommendation-compare-section-title">${escapeHtml(section.title)}</div>
                  <div class="recommendation-compare-metrics">
                    ${section.fields.map((column) => `
                      <div class="recommendation-compare-item">
                        <span>${escapeHtml(column)}</span>
                        <div class="recommendation-compare-item-value">${renderRecommendationCompareValue(column, fund, qualityHintText, benchmarkMap)}</div>
                      </div>
                    `).join('')}
                  </div>
                </section>
              `).join('')}
            </div>
          </article>
        `;
        }).join('');
      })()}
    </div>
  `;
  recommendationComparePanel.querySelector('[data-compare-clear="1"]')?.addEventListener('click', onClear);
  recommendationComparePanel.querySelectorAll('[data-compare-remove]').forEach((button) => {
    button.addEventListener('click', () => {
      const code = button.dataset.compareRemove || '';
      if (code) onToggle(code, false);
    });
  });
  if (compareCodes.length) {
    recommendationMobileCompareCollapsed = false;
    applyRecommendationMobileSectionState();
  }
}

function buildRecommendationDigest(fund) {
  const parts = [];
  const reason = compactReasonText(fund['入选原因']);
  const risk = compactRiskText(fund['风险提示']);
  const confidence = String(fund['项目质量信心'] || '').trim();
  const validation = fundValidationContract(fund);
  const validationStatus = normalizeRecommendationStatus(validation.rawStatus || validation.displayStatus);
  const stability = normalizeRecommendationStabilityLabel(fund['长期稳定性标签']);
  if (reason) parts.push(`入选 ${reason}`);
  if (confidence) parts.push(`信心 ${confidence}`);
  if (stability) parts.push(stability);
  if (validationStatus) parts.push(`${validation.sourceLabel} ${validationStatus}`);
  if (risk) parts.push(`关注 ${risk}`);
  return parts.join(' | ');
}

function summarizeGroupNarrative(group, groupBrief = null) {
  const detailLoaded = Array.isArray(group?.funds);
  const funds = detailLoaded ? group.funds : [];
  if (!detailLoaded) {
    return { headline: '明细尚未加载', metrics: [], note: '摘要已加载，打开完整明细后查看基金。' };
  }
  if (!funds.length) {
    return {
      headline: '暂无基金',
      metrics: [],
      note: '',
    };
  }
  if (groupBrief) {
    const metrics = [];
    if (Number.isFinite(Number(groupBrief.style_count))) metrics.push(`风格 ${groupBrief.style_count}`);
    if (Number.isFinite(Number(groupBrief.high_confidence_count))) metrics.push(`高信心 ${groupBrief.high_confidence_count}`);
    if (Number.isFinite(Number(groupBrief.strict_match_count))) metrics.push(`严格匹配 ${groupBrief.strict_match_count}`);
    if (Number.isFinite(Number(groupBrief.long_term_strong_count))) metrics.push(`长期强 ${groupBrief.long_term_strong_count}`);
    if (Number.isFinite(Number(groupBrief.confirmed_holding_count))) metrics.push(`已确认 ${groupBrief.confirmed_holding_count}`);
    if (Number(groupBrief.no_signal_holding_count || 0) > 0) metrics.push(`未见持有期限制 ${groupBrief.no_signal_holding_count}`);
    const unknownHoldingCount = Number(groupBrief.unknown_holding_count || 0)
      + Number(groupBrief.name_unparsed_holding_count || 0)
      + Number(groupBrief.generic_placeholder_holding_count || 0);
    if (Number.isFinite(unknownHoldingCount) && unknownHoldingCount > 0) metrics.push(`待确认 ${unknownHoldingCount}`);
    const riskTags = Array.isArray(groupBrief.top_risk_tags) ? groupBrief.top_risk_tags : [];
    const riskText = riskTags
      .map((item) => compactUiText(item.label, 10))
      .filter(Boolean)
      .slice(0, 2)
      .join('；');
    return {
      headline: shortRecommendationGroupHint(group),
      metrics,
      note: riskText || '无明显风险',
    };
  }
  const styleCount = new Set(funds.map((item) => item['债基风格标签']).filter(Boolean)).size;
  const highConfidenceCount = funds.filter((item) => item['项目质量信心'] === '高信心').length;
  const strictMatchCount = funds.filter((item) => fundValidationContract(item).rawStatus === 'strict_match').length;
  const longTermStrongCount = funds.filter((item) => item['长期稳定性标签'] === '长期稳定性强').length;
  const riskFlags = funds
    .map((item) => compactRiskText(item['风险提示']))
    .filter(Boolean);
  const riskPreview = [...new Set(riskFlags)].slice(0, 3);
  return {
    headline: shortRecommendationGroupHint(group),
    metrics: [
      `本组 ${funds.length} 只`,
      `风格 ${styleCount}`,
      `高信心 ${highConfidenceCount}`,
      `严格匹配 ${strictMatchCount}`,
      `长期强 ${longTermStrongCount}`,
    ],
    note: riskPreview.length ? riskPreview.join('；') : '无明显风险',
  };
}

function renderRecommendationGroupNarrative(group, groupBriefMap = {}) {
  if (!recommendationGroupNarrative) return;
  if (!group) {
    recommendationGroupNarrative.innerHTML = '';
    return;
  }
  const narrative = summarizeGroupNarrative(group, groupBriefMap?.[group.name] || null);
  recommendationGroupNarrative.innerHTML = `
    <div class="subtle-panel">
      <strong>分组</strong>
      <span>${escapeHtml(narrative.headline)}</span>
    </div>
    <div class="recommendation-group-metric-list">
      ${narrative.metrics.map((item) => `<span class="table-pill pill-style">${escapeHtml(item)}</span>`).join('')}
    </div>
    <div class="info-banner recommendation-group-note">
      <strong>关注</strong>
      <span>${escapeHtml(narrative.note)}</span>
    </div>
  `;
}

function renderRecommendationGroupSwitch(groups, activeName, onChange) {
  if (!recommendationGroupSwitch) return;
  recommendationGroupSwitch.innerHTML = groups.map((group) => `
    <button
      type="button"
      class="${group.name === activeName ? 'is-active' : ''}"
      data-group="${escapeHtml(group.name)}"
      aria-pressed="${group.name === activeName ? 'true' : 'false'}"
    >${escapeHtml(group.name)}</button>
  `).join('');

  recommendationGroupSwitch.querySelectorAll('button').forEach((button) => {
    button.addEventListener('click', () => {
      const nextGroup = button.dataset.group || '';
      if (nextGroup && nextGroup !== activeName) onChange(nextGroup);
    });
  });
}

function buildRecommendationRows(funds) {
  return funds.map((fund, index) => ({
    对比: fund.__compareToggle || '',
    序号: index + 1,
    基金代码: fund['基金代码'] || '',
    基金名称: fund['基金名称'] || '',
    收藏: fund.__favoriteToggle || '',
    '出现次数&出现频率(%)': fund['出现次数&出现频率(%)'] || '',
    数据质量提示: buildRecommendationQualityHintText(fund),
    实时估值: fund['实时估值'] || '',
    实时估值时间: fund['实时估值时间'] || '',
    实时估算净值: fund['实时估算净值'] || '',
    __valuation_state: fund.__valuation_state || '',
    __valuation_status_label: fund.__valuation_status_label || '',
    __valuation_reason: fund.__valuation_reason || '',
    '1年-年化收益(%)': fund['1年-年化收益(%)'] ?? '',
    '平台成立以来收益年化(%)': fund['平台成立以来收益年化(%)'] ?? '',
    '累计净值比年化(%)': fund['累计净值比年化(%)'] ?? '',
    成立以来年度收益明细: Array.isArray(fund['成立以来年度收益明细']) ? fund['成立以来年度收益明细'] : [],
    __style_metric_references: fund.__style_metric_references || {},
    '近1月(%)': fund['近1月(%)'] ?? '',
    '近3月(%)': fund['近3月(%)'] ?? '',
    '1年-夏普比率': fund['1年-夏普比率'] ?? '',
    '1年-最大回撤(%)': fund['1年-最大回撤(%)'] ?? '',
    '1年-波动率': fund['1年-波动率'] ?? '',
    最新净值日期: fund['最新净值日期'] || '',
    '基金规模(亿)': fund['基金规模(亿)'] || '',
    正式榜分组: fund['正式榜分组'] || '',
    债基风格标签: fund['债基风格标签'] || '',
    推荐解读: buildRecommendationDigest(fund),
    基金公司: fund['基金公司'] || '',
    投资优先池得分: fund['投资优先池得分'] || '',
    正式投资评分: fund['正式投资评分'] || '',
    项目质量信心: fund['项目质量信心'] || '',
    出现次数: fund['出现次数'] ?? '',
    '出现频率(%)': fund['出现频率(%)'] ?? '',
    '费率合计(%)': recommendationFeeDisplayValue(fund),
    ...copyFundValidationFields(fund),
    长期稳定性标签: fund['长期稳定性标签'] || '',
    建议权重: fund['建议权重'] || '',
    入选原因: fund['入选原因'] || '',
    风险提示: fund['风险提示'] || '',
  }));
}

function sortRecommendationRows(rows, sortState) {
  if (!Array.isArray(rows) || !rows.length) return [];
  if (!sortState?.column || !sortState?.direction) return [...rows];
  const direction = sortState.direction === 'desc' ? -1 : 1;
  return [...rows].sort((left, right) => {
    const leftValue = sortState.column === '出现次数&出现频率(%)'
      ? left['出现次数'] ?? left['出现次数&出现频率(%)']
      : left[sortState.column];
    const rightValue = sortState.column === '出现次数&出现频率(%)'
      ? right['出现次数'] ?? right['出现次数&出现频率(%)']
      : right[sortState.column];
    return compareSortableValues(leftValue, rightValue, direction);
  });
}

function enrichRecommendationRowsWithValuation(funds, valuations, valuationState) {
  return funds.map((fund) => {
    const code = String(fund['基金代码'] || '').trim();
    const valuation = (valuations || {})[code] || {};
    let text = '';
    let uiState = valuationState === VALUATION_STATE_LOADING ? 'loading' : '';
    let uiStatusLabel = valuationState === VALUATION_STATE_LOADING ? '请求中' : '';
    let uiReason = '';
    if (valuationState === VALUATION_STATE_READY && valuation.available) {
      text = String(valuation.estimated_change_pct || '').trim();
      uiState = 'ready';
      uiStatusLabel = '已返回';
    } else if (valuationState === VALUATION_STATE_READY && (valuation.reason === 'valuation_not_provided' || valuation.reason === 'upstream_missing')) {
      text = '暂无估值';
      uiState = 'missing';
      uiStatusLabel = valuation.reason === 'valuation_not_provided' ? '该基金当前未提供盘中估值' : '上游未返回该基金';
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
      ...fund,
      实时估值: text,
      实时估值时间: valuation.valuation_date || '',
      实时估算净值: valuation.estimated_nav || '',
      __valuation_state: uiState,
      __valuation_status_label: uiStatusLabel,
      __valuation_reason: uiReason,
    };
  });
}

function renderRecommendationValuationBanner(message, options = {}) {
  if (!recommendationValuationBanner) return;
  const { loading = false, failed = false, retryable = false, onRetry = null } = options;
  const classes = [
    'info-banner',
    'recommendation-valuation-banner',
    loading ? 'is-loading' : '',
    failed ? 'is-failed' : '',
  ].filter(Boolean).join(' ');
  recommendationValuationBanner.className = classes;
  recommendationValuationBanner.innerHTML = `
    <strong>实时估值</strong>
    <span>${escapeHtml(message)}</span>
    ${retryable ? '<button type="button" class="secondary recommendation-valuation-retry">重试</button>' : ''}
  `;
  if (retryable) {
    recommendationValuationBanner.querySelector('.recommendation-valuation-retry')?.addEventListener('click', () => {
      if (typeof onRetry === 'function') onRetry();
    });
  }
}

function buildRecommendationSnapshotComparisonLabel(row) {
  const snapshotDate = String(row?.['最新净值日期'] || '').trim();
  return snapshotDate ? `正式快照（净值日期 ${snapshotDate}）` : '正式快照';
}

function renderRecommendationSignals(genericHoldingWatch = {}, lowVolPositiveRatioWatch = {}, positiveRatioSemantics = {}) {
  if (!recommendationsSignals) return;

  const styleSummary = Array.isArray(lowVolPositiveRatioWatch?.style_summary)
    ? lowVolPositiveRatioWatch.style_summary
    : [];
  const styleText = styleSummary
    .slice(0, 4)
    .map((item) => {
      const style = compactUiText(item?.['债基风格标签'] || '-', 6);
      const alertCount = formatNumber(item?.['自然月正收益差>=15pct样本数']);
      return `${style} ${alertCount}`;
    })
    .join(' / ');

  const summaryText = [
    `持有期待补 ${formatNumber(genericHoldingWatch?.count)}`,
    lowVolPositiveRatioWatch?.available ? `低波观察 ${formatNumber(lowVolPositiveRatioWatch?.focus_sample_count)}` : '',
    lowVolPositiveRatioWatch?.available ? `门槛命中 ${formatNumber(lowVolPositiveRatioWatch?.positive_ratio_gate_hit_count)}` : '',
    lowVolPositiveRatioWatch?.available ? `高差样本 ${formatNumber(lowVolPositiveRatioWatch?.positive_ratio_gap_alert_count)}` : '',
  ].filter(Boolean).join(' / ');

  recommendationsSignals.innerHTML = `
    <strong>${lowVolPositiveRatioWatch?.available ? '低波观察' : '口径说明'}</strong>
    <span>${escapeHtml(summaryText || '份额合并后展示')}</span>
    <span>${escapeHtml((positiveRatioSemantics?.definition ? `positive_ratio=${positiveRatioSemantics.definition}；` : '') + (styleText || '份额合并后展示'))}</span>
  `;
}

function defaultRecommendationSortState() {
  return {
    column: DEFAULT_RECOMMENDATION_SORT_STATE.column,
    direction: DEFAULT_RECOMMENDATION_SORT_STATE.direction,
  };
}

function renderRecommendationContent(
  groups,
  activeName,
  valuations,
  valuationState,
  sortState,
  filterState,
  compareCodes,
  favoriteCodeSet,
  focusCode,
  expandedCodes,
  onFavoriteSave,
  onCompareToggle,
  onToggleExpand,
  onToggleExpandAll,
  onSort,
  groupBriefMap = {},
  viewMode = FUND_STYLE_VIEW_MODES.mixed,
  densityMode = FUND_TABLE_DENSITY_MODES.core,
) {
  const currentGroup = groups.find((item) => item.name === activeName) || groups[0] || null;
  if (!currentGroup) {
    recommendationGroupStats.innerHTML = '';
    renderRecommendationGroupNarrative(null);
    recommendationCards.innerHTML = '<div class="empty-state-inline">暂无推荐</div>';
    return { funds: [], tableRows: [], tableColumns: [], groupName: '', totalFunds: 0 };
  }

  const funds = Array.isArray(currentGroup.funds) ? currentGroup.funds : [];

  recommendationGroupStats.innerHTML = [
    buildStatCard('当前分组', currentGroup.name || '-', shortRecommendationGroupHint(currentGroup)),
    buildStatCard('基金数量', formatNumber(funds.length), `${formatNumber(currentGroup.count || funds.length)} 只原始分组样本`),
    buildStatCard('覆盖风格数', formatNumber(new Set((currentGroup.funds || []).map((item) => item['债基风格标签']).filter(Boolean)).size), '风格覆盖'),
  ].join('');
  renderRecommendationGroupNarrative(currentGroup, groupBriefMap);

  if (!funds.length) {
    recommendationCards.innerHTML = renderRecommendationEmptyState(groups, currentGroup, filterState, currentGroup.count || 0);
    return { funds: [], tableRows: [], tableColumns: [], groupName: currentGroup.name || '', totalFunds: 0 };
  }
  const enrichedFunds = enrichRecommendationRowsWithValuation(funds, valuations, valuationState).map((fund) => {
    const code = String(fund['基金代码'] || '').trim();
    const checked = compareCodes.includes(code);
    const disabled = !checked && compareCodes.length >= 4;
    return {
      ...fund,
      __compareToggle: renderRecommendationCompareToggle(code, checked, disabled),
      __favoriteToggle: renderRecommendationFavoriteToggle(code, favoriteCodeSet.has(code)),
    };
  });
  const tableRows = buildRecommendationRows(enrichedFunds).map((row) => {
    const code = normalizeRecommendationCode(row['基金代码'] || '');
    return {
      ...row,
      详情: '',
      __expandKey: code,
      __expanded: expandedCodes.has(code),
    };
  });
  const fundLookup = new Map(enrichedFunds.map((fund) => [normalizeRecommendationCode(fund['基金代码'] || ''), fund]));
  const activeCoreColumns = densityMode === FUND_TABLE_DENSITY_MODES.expanded
    ? RECOMMENDATION_EXPANDED_TABLE_COLUMNS
    : RECOMMENDATION_CORE_TABLE_COLUMNS;
  const buildTableOptions = (tableRowsInput) => {
    const headerReferenceMap = buildRecommendationHeaderReferenceMap(tableRowsInput);
    return {
      coreColumns: activeCoreColumns,
      detailGroups: RECOMMENDATION_DETAIL_GROUPS,
      tableClass: 'data-table recommendation-table recommendation-compact-table compact-expand-table',
      tableAttributes: {
        'data-density-mode': densityMode,
      },
      wrapClass: 'recommendation-table-wrap recommendation-compact-wrap compact-expand-wrap',
      toolbarClass: 'favorite-table-toolbar compact-table-toolbar recommendation-table-toolbar',
      toolbarTitle: densityMode === FUND_TABLE_DENSITY_MODES.expanded
        ? (viewMode === FUND_STYLE_VIEW_MODES.grouped ? '按风格分组扩展视图' : '扩展混合视图')
        : (viewMode === FUND_STYLE_VIEW_MODES.grouped ? '按风格分组视图' : '高密度混合视图'),
      toolbarHint: densityMode === FUND_TABLE_DENSITY_MODES.expanded
        ? '主表直接展开更多收益、规模和出现频次字段，适合深度比对。'
        : (viewMode === FUND_STYLE_VIEW_MODES.grouped
          ? FUND_COPYWRITING.sameStyleAnnualizedCompareNote
          : '主表仅保留核心决策列，标签、风险和推荐说明按行展开查看。'),
      headerContentRenderers: {
        '正式投资评分': () => buildRecommendationHeaderReferenceMarkup('正式投资评分', headerReferenceMap, { allowMixedRange: false }),
        '1年-年化收益(%)': () => buildRecommendationHeaderReferenceMarkup('1年-年化收益(%)', headerReferenceMap, { allowMixedRange: false }),
        '平台成立以来收益年化(%)': () => buildRecommendationHeaderReferenceMarkup('平台成立以来收益年化(%)', headerReferenceMap, { allowMixedRange: false }),
        '近3月(%)': () => buildRecommendationHeaderReferenceMarkup('近3月(%)', headerReferenceMap, { allowMixedRange: false }),
        '近1月(%)': () => buildRecommendationHeaderReferenceMarkup('近1月(%)', headerReferenceMap, { allowMixedRange: false }),
        '1年-夏普比率': () => buildRecommendationHeaderReferenceMarkup('1年-夏普比率', headerReferenceMap, { allowMixedRange: false }),
        '1年-最大回撤(%)': () => buildRecommendationHeaderReferenceMarkup('1年-最大回撤(%)', headerReferenceMap, { allowMixedRange: false }),
        '1年-波动率': () => buildRecommendationHeaderReferenceMarkup('1年-波动率', headerReferenceMap, { allowMixedRange: false }),
      },
      mobileCardColumns: RECOMMENDATION_MOBILE_CARD_COLUMNS,
      renderCards: true,
      sortableColumns: SORTABLE_RECOMMENDATION_COLUMNS.filter((column) => activeCoreColumns.includes(column)),
      initialSortDirections: RECOMMENDATION_INITIAL_SORT_DIRECTIONS,
      sortState,
      sortAccessors: {
        '出现次数&出现频率(%)': (row) => row['出现次数'] ?? '',
      },
      rowDataResolver: (row) => fundLookup.get(normalizeRecommendationCode(row['基金代码'] || '')) || row,
      rowClassName: (row) => {
        const classes = [];
        const code = normalizeRecommendationCode(row['基金代码'] || '');
        if (compareCodes.includes(code)) classes.push('is-compare-selected');
        if (focusCode && code === focusCode) classes.push('is-focus-target');
        return classes.join(' ');
      },
      rowAttributes: (row) => ({
        'data-fund-code': normalizeRecommendationCode(row['基金代码'] || ''),
      }),
      cellRenderers: {
        对比: (value) => value,
        收藏: (value) => value,
        债基风格标签: (value, row) => renderStyleBenchmarkTooltipCell(value, row),
        正式投资评分: (value, row) => renderFormalScoreTooltipCell(value, row, {
          code: normalizeRecommendationCode(row['基金代码'] || ''),
        }),
        '1年-年化收益(%)': (value, row) => renderBenchmarkAwareReturnTooltipCell(value, row, {
          field: '1年-年化收益(%)',
          title: `1年年化收益 ${formatSignedPercentOrDash(value)}`,
          snapshotLabel: buildRecommendationSnapshotComparisonLabel(row),
          description: '用于衡量基金近一年收益效率，适合与同风格统一数据基准直接比较。',
          referenceFields: ['1年-年化收益(%)', '1年-夏普比率', '1年-最大回撤(%)', '1年-波动率'],
        }),
        '近1月(%)': (value, row) => renderBenchmarkAwareReturnTooltipCell(value, row, {
          field: '近1月(%)',
          title: `近1月涨跌 ${formatSignedPercentOrDash(value)}`,
          snapshotLabel: buildRecommendationSnapshotComparisonLabel(row),
          description: '该列用于观察最近一个月相对同风格真实样本基准的强弱，避免只看绝对涨跌幅。',
          referenceFields: ['近1月(%)', '1年-波动率'],
        }),
        '近3月(%)': (value, row) => renderBenchmarkAwareReturnTooltipCell(value, row, {
          field: '近3月(%)',
          title: `近3月涨跌 ${formatSignedPercentOrDash(value)}`,
          snapshotLabel: buildRecommendationSnapshotComparisonLabel(row),
          description: '该列用于观察最近三个月相对同风格真实样本基准的强弱，适合判断短中期收益延续性。',
          referenceFields: ['近3月(%)', '1年-年化收益(%)', '1年-夏普比率', '1年-最大回撤(%)', '1年-波动率'],
        }),
        实时估值: (value, row) => renderRealtimeValuationTooltipCell(value, row, {
          columnLabel: '实时估值',
          snapshotComparisonLabel: buildRecommendationSnapshotComparisonLabel(row),
          valuationState: row.__valuation_state,
          valuationStatusLabel: row.__valuation_status_label,
          valuationReason: row.__valuation_reason,
        }),
        '平台成立以来收益年化(%)': (value, row) => renderSinceInceptionAnnualizedTooltipCell(value, row),
        数据质量提示: (value) => renderRecommendationQualityHint(value),
        详情: (_, row) => renderRecommendationExpandToggle(row),
      },
      detailValueFormatter: (column, value) => {
        if (column === '推荐解读') return renderRecommendationQualityHint(value) === value ? escapeHtml(String(value || '')) : renderCell(column, value);
        return renderCell(column, value);
      },
      rowKey: (row) => normalizeRecommendationCode(row['基金代码'] || ''),
      expandedRows: expandedCodes,
      onToggleExpand,
      onToggleExpandAll,
      onSort,
    };
  };
  let tableResult = { rows: [], columns: [] };
  if (viewMode === FUND_STYLE_VIEW_MODES.grouped) {
    renderFundStyleGroupedTables(recommendationCards, tableRows, {
      renderTable: (container, styleRows) => {
        tableResult = renderCompactExpandableTable(
          container,
          styleRows,
          {
            ...buildTableOptions(styleRows),
            showToolbar: false,
          },
        );
      },
    });
  } else {
    tableResult = renderCompactExpandableTable(
      recommendationCards,
      tableRows,
      buildTableOptions(tableRows),
    );
  }
  recommendationCards.querySelectorAll('input[data-compare-code]').forEach((input) => {
    input.addEventListener('change', () => {
      const code = input.dataset.compareCode || '';
      if (!code) return;
      onCompareToggle(code, input.checked);
    });
  });
  recommendationCards.querySelectorAll('button[data-favorite-code]').forEach((button) => {
    button.addEventListener('click', async () => {
      const code = button.dataset.favoriteCode || '';
      if (!code) return;
      if (favoriteCodeSet.has(code)) {
        window.location.href = buildFavoritePageUrl(code);
        return;
      }
      button.disabled = true;
      try {
        await onFavoriteSave(code);
      } catch (error) {
        renderRecommendationActionBanner(error?.message || `收藏 ${code} 失败`, 'info-banner-error');
      } finally {
        button.disabled = false;
      }
    });
  });
  return {
    funds: enrichedFunds,
    tableRows: tableResult?.rows || [],
    tableColumns: tableResult?.columns || [],
    groupName: currentGroup.name || '',
    totalFunds: funds.length,
  };
}

function isRecommendationSummaryPayload(payload) {
  return String(payload?.payload_scope?.mode || '').trim() === 'summary';
}

function shouldLoadRecommendationDetail(options = {}) {
  const queryDetail = String(recommendationQueryParams.get('detail') || '').trim().toLowerCase();
  return Boolean(options?.detail) || Boolean(RECOMMENDATION_FOCUS_CODE) || queryDetail === '1' || queryDetail === 'true';
}

function recommendationDetailCacheKey(summaryPayload = {}, groupName = '') {
  const runId = String(summaryPayload?.run_id || 'latest').trim() || 'latest';
  return `${runId}::${String(groupName || '').trim() || 'all'}`;
}

async function loadRecommendationDetailPayloadForGroup(summaryPayload = {}, groupName = '') {
  const selectedGroupName = String(groupName || '').trim();
  const cacheKey = recommendationDetailCacheKey(summaryPayload, selectedGroupName);
  if (recommendationDetailPayloadCache.has(cacheKey)) {
    return recommendationDetailPayloadCache.get(cacheKey);
  }
  const detailPayload = await loadRecommendationsLatestDetail(
    selectedGroupName ? { group: selectedGroupName } : {},
  );
  recommendationDetailPayloadCache.set(cacheKey, detailPayload);
  return detailPayload;
}

function bindRecommendationSummaryDetailAction(payload = {}) {
  const button = recommendationCards?.querySelector('[data-rec-load-detail="1"]');
  if (!button) return;
  button.onclick = async () => {
    if (button.disabled) return;
    const groups = Array.isArray(payload?.portfolio_suggestions?.groups)
      ? payload.portfolio_suggestions.groups
      : [];
    const initialGroup = initialRecommendationGroupName(groups, RECOMMENDATION_FOCUS_CODE);
    button.disabled = true;
    button.textContent = '加载中';
    try {
      await initRecommendationsPage({ detail: true, group: initialGroup });
    } finally {
      if (button.isConnected) {
        button.disabled = false;
        button.textContent = '完整明细';
      }
    }
  };
}

function renderRecommendationSummaryShell(payload = {}) {
  applyPayloadDataContract(payload);
  const portfolio = payload?.portfolio_suggestions || {};
  const reportTiming = payload?.report_timing_summary || {};
  const groups = Array.isArray(portfolio.groups) ? portfolio.groups : [];
  const counts = payload?.counts || {};
  const portfolioDisplayScope = payload?.portfolio_display_scope || {};
  const publishLayerSemantics = payload?.publish_layer_semantics_summary || {};
  const trackerSummary = payload?.tracker_summary || {};
  const genericHoldingWatch = payload?.generic_holding_placeholder_watch || {};
  const lowVolPositiveRatioWatch = payload?.low_vol_positive_ratio_shadow_watch || {};
  const positiveRatioSemantics = payload?.positive_ratio_semantics || {};
  const compressionAudit = portfolio?.compression_audit || {};
  const targetPolicy = portfolio?.target_policy || {};
  const qualityGate = portfolio?.quality_gate || {};
  const displayCount = firstPresentValue(counts.portfolio_fund_count, portfolio.fund_count);
  const sourceCandidateCount = firstPresentValue(portfolio.source_candidate_count, portfolio.candidate_count);
  const candidateCount = firstPresentValue(portfolio.candidate_count);
  const targetCount = firstPresentValue(portfolio.target_count, portfolio.fund_count);

  renderRecommendationHeroMeta(
    payload,
    displayCount,
    reportTiming,
    counts.portfolio_group_count ?? groups.length,
  );
  recommendationsMeta.textContent = recommendationVersionMetaText(payload, [
    `推荐摘要 ${formatNumber(displayCount)} 只`,
  ]);
  recommendationsStats.innerHTML = [
    buildStatCard('正式主榜', formatNumber(counts.final_count), '完整正式推荐名单'),
    buildStatCard('推荐展示池', formatNumber(displayCount), '精选后的页面展示'),
    buildStatCard('推荐分组', formatNumber(counts.portfolio_group_count ?? groups.length), '纯债 / 增强债'),
    buildStatCard('正式快照', recommendationSnapshotText(payload) || '-', '发布净值日期'),
  ].join('');
  const noteParts = [
    `正式主榜 ${formatNumber(counts.final_count)} 只，页面精选展示 ${formatNumber(displayCount)} 只`,
    `候选链路 ${formatNumber(sourceCandidateCount)} → ${formatNumber(candidateCount)} → ${formatNumber(targetCount)} 只`,
    qualityGate?.available ? `质量门 ${escapeHtml(String(qualityGate.selected_step_label || ''))}` : '',
  ].filter(Boolean).join('；');
  recommendationsNarrative.innerHTML = `
    <strong>筛选摘要</strong>
    <span>${escapeHtml(noteParts || '暂无摘要')}</span>
  `;
  renderRecommendationSignals(genericHoldingWatch, lowVolPositiveRatioWatch, positiveRatioSemantics);
  renderRecommendationChangePanel(trackerSummary);
  renderRecommendationGroupSwitch(groups, groups[0]?.name || '', () => {});
  const filteredExcel = payload?.artifacts?.filtered_excel || '';
  if (recommendationDownloadBtn) {
    recommendationDownloadBtn.href = filteredExcel ? fileUrl(filteredExcel, true) : '#';
    recommendationDownloadBtn.classList.toggle('is-disabled', !filteredExcel);
  }
  recommendationGroupStats.innerHTML = groups.length
    ? [
        buildStatCard('当前分组', groups[0]?.name || '-', shortRecommendationGroupHint(groups[0] || {})),
        buildStatCard('基金数量', formatNumber(groups[0]?.count), '明细加载中'),
        buildStatCard('覆盖风格数', '-', '明细加载中'),
      ].join('')
    : '';
  renderRecommendationGroupNarrative(groups[0] || null, payload?.portfolio_group_brief || {});
  recommendationCards.innerHTML = `
    <div class="empty-state-inline recommendation-empty-state">
      <strong>推荐摘要已加载</strong>
      <span>${escapeHtml(`摘要预览 ${formatNumber(displayCount)} 只；完整明细会加载行级基准、筛选和展开内容。`)}</span>
      <div class="recommendation-empty-state-actions">
        <button type="button" class="secondary" data-rec-load-detail="1">完整明细</button>
      </div>
    </div>
  `;
  recommendationFilterBar.innerHTML = '';
  recommendationViewBar.innerHTML = '';
  recommendationStyleThresholdSummary.innerHTML = '';
  recommendationComparePanel.innerHTML = '';
  recommendationListHints.innerHTML = '';
  if (recommendationExportBtn) recommendationExportBtn.disabled = true;
}

async function initRecommendationsPage(options = {}) {
  rerenderRecommendationsForViewport = null;
  const loadDetail = shouldLoadRecommendationDetail(options);
  void loadBootstrapSummary().then((payload) => {
    renderRunBadge(recommendationsHeaderRunBadge, getBootstrapWorkflow(payload));
    renderHealthBadge(recommendationsHeaderHealthBadge, getBootstrapHealth(payload));
  }).catch(() => {
    renderUnavailableBadge(recommendationsHeaderRunBadge, '运行状态', '接口暂不可用');
    renderUnavailableBadge(recommendationsHeaderHealthBadge, '数据源状态', '接口暂不可用');
  });
  const [recommendationsResult, favoriteHistoryResult] = await Promise.allSettled([
    loadRecommendationsLatest(),
    loadDetail ? loadFavoriteFundHistory() : Promise.resolve({ records: [] }),
  ]);

  if (recommendationsResult.status !== 'fulfilled') {
    recommendationsHeroMeta.innerHTML = buildStatCard('当前主发布版本', '加载失败', '稍后重试');
    renderRetryableEmptyState(recommendationCards, {
      title: '推荐列表加载失败',
      message: recommendationsResult.reason?.message || '请稍后重试',
      actionLabel: '重新加载',
      onRetry: () => initRecommendationsPage(options),
    });
    return;
  }

  let payload = recommendationsResult.value || {};
  applyPayloadDataContract(payload);
  renderFormalScoreContractInto('recommendationFormalScoreContract', payload?.formal_score_contract);
  if (isRecommendationSummaryPayload(payload)) {
    renderRecommendationSummaryShell(payload);
    if (!loadDetail) {
      applyRecommendationMobileSectionState();
      bindRecommendationSummaryDetailAction(payload);
      renderSummaryHintChips(recommendationListHints, [
        `摘要预览 ${formatNumber(firstPresentValue((payload?.counts || {}).portfolio_fund_count, (payload?.portfolio_suggestions || {}).fund_count))} 只`,
        '完整明细含行级基准',
        '核心/扩展视图加载后可切换',
      ]);
      renderRecommendationPublicationBanner(payload, '推荐摘要已加载，完整明细按需加载');
      return;
    }
    const summaryPayload = payload;
    const summaryGroups = Array.isArray(summaryPayload?.portfolio_suggestions?.groups)
      ? summaryPayload.portfolio_suggestions.groups
      : [];
    const explicitDetailGroup = String(
      options?.group || recommendationQueryParams.get(RECOMMENDATION_VIEW_PARAM_KEYS.group) || '',
    ).trim();
    const initialDetailGroup = explicitDetailGroup || (
      RECOMMENDATION_FOCUS_CODE
        ? ''
        : initialRecommendationGroupName(summaryGroups, RECOMMENDATION_FOCUS_CODE)
    );
    try {
      payload = await loadRecommendationDetailPayloadForGroup(summaryPayload, initialDetailGroup);
      applyPayloadDataContract(payload);
      renderFormalScoreContractInto('recommendationFormalScoreContract', payload?.formal_score_contract);
    } catch (error) {
      renderRetryableEmptyState(recommendationCards, {
        title: '推荐明细加载失败',
        message: error?.message || '请稍后重试',
        actionLabel: '重新加载',
        onRetry: () => initRecommendationsPage({ detail: true, group: initialDetailGroup }),
      });
      return;
    }
  }

  const portfolio = payload?.portfolio_suggestions || {};
  const reportTiming = payload?.report_timing_summary || {};
  const groupBriefMap = payload?.portfolio_group_brief || {};
  let groups = Array.isArray(portfolio.groups) ? portfolio.groups : [];
  const counts = payload?.counts || {};
  const portfolioDisplayScope = payload?.portfolio_display_scope || {};
  const publishLayerSemantics = payload?.publish_layer_semantics_summary || {};
  const trackerSummary = payload?.tracker_summary || {};
  const genericHoldingWatch = payload?.generic_holding_placeholder_watch || {};
  const lowVolPositiveRatioWatch = payload?.low_vol_positive_ratio_shadow_watch || {};
  const positiveRatioSemantics = payload?.positive_ratio_semantics || {};
  const compressionAudit = portfolio?.compression_audit || {};
  const targetPolicy = portfolio?.target_policy || {};
  const qualityGate = portfolio?.quality_gate || {};
  const styleThresholds = normalizeFundStyleThresholdEntries(payload?.style_thresholds);
  const globalStyleThreshold = payload?.global_style_threshold && typeof payload.global_style_threshold === 'object'
    ? { ...payload.global_style_threshold }
    : {};
  const missingStyleThresholds = Array.isArray(payload?.missing_style_thresholds)
    ? payload.missing_style_thresholds.map((item) => String(item || '').trim()).filter(Boolean)
    : [];
  let favoriteHistoryPayload = favoriteHistoryResult.status === 'fulfilled' ? (favoriteHistoryResult.value || {}) : { records: [] };
  let favoriteCodeSet = buildFavoriteCodeSet(favoriteHistoryPayload);
  const displayCount = firstPresentValue(counts.portfolio_fund_count, portfolio.fund_count);
  const sourceCandidateCount = firstPresentValue(portfolio.source_candidate_count, portfolio.candidate_count);
  const candidateCount = firstPresentValue(portfolio.candidate_count);
  const targetCount = firstPresentValue(portfolio.target_count, portfolio.fund_count);
  const initialFocusGroup = findRecommendationGroupNameByCode(groups, RECOMMENDATION_FOCUS_CODE);
  const initialGroup = initialRecommendationGroupName(
    groups,
    RECOMMENDATION_FOCUS_CODE,
    options?.group || payload?.payload_scope?.detail_group_name,
  );

  renderRecommendationHeroMeta(
    payload,
    displayCount,
    reportTiming,
    counts.portfolio_group_count ?? groups.length,
  );

  recommendationsMeta.textContent = recommendationVersionMetaText(payload, [
    `当前分组 ${formatNumber(displayCount)} 只`,
    `可见 ${formatNumber(displayCount)} 只`,
    `风格 ${formatNumber(new Set((groups[0]?.funds || []).map((item) => normalizeFundStyleLabel(item['债基风格标签'])).filter((style) => style && style !== '未标注风格')).size)} 个`,
  ]);
  recommendationsStats.innerHTML = [
    buildStatCard('正式主榜', formatNumber(counts.final_count), '完整正式推荐名单'),
    buildStatCard('推荐展示池', formatNumber(displayCount), '精选后的页面展示'),
    buildStatCard('推荐分组', formatNumber(counts.portfolio_group_count ?? groups.length), '纯债 / 增强债'),
    buildStatCard('正式快照', recommendationSnapshotText(payload) || '-', '发布净值日期'),
  ].join('');
  const noteParts = [
    `正式主榜 ${formatNumber(counts.final_count)} 只，页面精选展示 ${formatNumber(displayCount)} 只`,
    `候选链路 ${formatNumber(sourceCandidateCount)} → ${formatNumber(candidateCount)} → ${formatNumber(targetCount)} 只`,
    qualityGate?.available ? `质量门 ${escapeHtml(String(qualityGate.selected_step_label || ''))}` : '',
  ].filter(Boolean).join('；');
  recommendationsNarrative.innerHTML = `
    <strong>筛选摘要</strong>
    <span>${escapeHtml(noteParts || '暂无摘要')}</span>
  `;
  renderRecommendationSignals(genericHoldingWatch, lowVolPositiveRatioWatch, positiveRatioSemantics);
  renderRecommendationChangePanel(trackerSummary);

  const filteredExcel = payload?.artifacts?.filtered_excel || '';
  recommendationDownloadBtn.href = filteredExcel ? fileUrl(filteredExcel, true) : '#';
  recommendationDownloadBtn.classList.toggle('is-disabled', !filteredExcel);

  let activeGroup = initialGroup;
  let currentVisibleFunds = [];
  let valuationMap = {};
  let valuationState = VALUATION_STATE_IDLE;
  let valuationRequested = false;
  let sortState = recommendationSortStateFromUrl();
  let sortPreset = normalizeRecommendationSortPreset(sortState);
  let filterState = recommendationFilterStateFromUrl();
  let recommendationViewMode = recommendationViewModeFromUrl();
  let recommendationDensityMode = recommendationDensityModeFromUrl();
  let compareCodes = recommendationCompareCodesFromUrl();
  let expandedCodes = new Set();
  let focusBannerRendered = false;
  const applyDetailPayloadForGroup = (nextPayload) => {
    if (!nextPayload || typeof nextPayload !== 'object') return;
    applyPayloadDataContract(nextPayload);
    payload = nextPayload;
    renderFormalScoreContractInto('recommendationFormalScoreContract', nextPayload?.formal_score_contract);
    const nextGroups = Array.isArray(nextPayload?.portfolio_suggestions?.groups)
      ? nextPayload.portfolio_suggestions.groups
      : [];
    if (nextGroups.length) {
      groups = nextGroups;
    }
  };
  const selectRecommendationGroup = async (nextGroup) => {
    const normalizedGroup = String(nextGroup || '').trim();
    if (!normalizedGroup || normalizedGroup === activeGroup) return;
    activeGroup = normalizedGroup;
    valuationMap = {};
    valuationState = VALUATION_STATE_IDLE;
    sortState = defaultRecommendationSortState();
    sortPreset = defaultRecommendationSortPreset();
    filterState = defaultRecommendationFilterState();
    compareCodes = [];
    expandedCodes = new Set();
    const currentGroup = groups.find((item) => String(item?.name || '').trim() === activeGroup);
    if (!Array.isArray(currentGroup?.funds)) {
      recommendationCards.innerHTML = `
        <div class="empty-state-inline recommendation-empty-state">
          <strong>正在加载分组明细</strong>
          <span>${escapeHtml(activeGroup)} 明细按需读取中。</span>
        </div>
      `;
      try {
        applyDetailPayloadForGroup(await loadRecommendationDetailPayloadForGroup(payload, activeGroup));
      } catch (error) {
        renderRetryableEmptyState(recommendationCards, {
          title: '推荐分组明细加载失败',
          message: error?.message || '请稍后重试',
          actionLabel: '重新加载',
          onRetry: () => initRecommendationsPage({ detail: true, group: activeGroup }),
        });
        return;
      }
    }
    rerender();
    if (valuationRequested) refreshValuations();
  };
  const rerender = () => {
    renderRecommendationGroupSwitch(groups, activeGroup, (nextGroup) => {
      selectRecommendationGroup(nextGroup);
    });
    const currentGroup = groups.find((item) => item.name === activeGroup) || groups[0] || null;
    const groupFunds = Array.isArray(currentGroup?.funds) ? currentGroup.funds : [];
    renderRecommendationFilterBar(
      groupFunds,
      filterState,
      (key, value) => {
        filterState = {
          ...filterState,
          [key]: value,
        };
        compareCodes = compareCodes.filter((code) => groupFunds.some((item) => String(item['基金代码'] || '').trim() === code));
        rerender();
        if (valuationRequested) refreshValuations();
      },
      () => {
        filterState = defaultRecommendationFilterState();
        rerender();
        if (valuationRequested) refreshValuations();
      },
    );
    renderFundStyleViewBar(recommendationViewBar, {
      viewMode: recommendationViewMode,
      densityMode: recommendationDensityMode,
      rows: groupFunds,
      onChange: (nextMode) => {
        recommendationViewMode = nextMode;
        expandedCodes = new Set();
        rerender();
      },
      onDensityChange: (nextMode) => {
        recommendationDensityMode = nextMode;
        expandedCodes = new Set();
        rerender();
      },
      groupedNote: FUND_COPYWRITING.sameStyleAnnualizedCompareNote,
      mixedNote: '混合视图便于总览，但不同风格间的正式投资评分不宜直接横向比较。',
    });
    renderFundStyleThresholdSummary(recommendationStyleThresholdSummary, {
      rows: groupFunds,
      styleThresholds,
      globalStyleThreshold,
      missingStyleThresholds,
      viewMode: recommendationViewMode,
      description: '以下门槛来自当前最新 workflow，用于解释推荐页内不同风格基金的正式评分与入池标准。',
    });
    renderRecommendationSortPresetSwitch(sortPreset, (nextPreset) => {
      sortPreset = nextPreset;
      sortState = recommendationSortPresetSortState(nextPreset);
      rerender();
    });
    const filteredGroups = groups.map((group) => ({
      ...group,
      funds: group.name === activeGroup
        ? filterRecommendationFunds(groupFunds, filterState, favoriteCodeSet)
        : group.funds,
    }));
    const renderResult = renderRecommendationContent(
      filteredGroups,
      activeGroup,
      valuationMap,
      valuationState,
      sortState,
      filterState,
      compareCodes,
      favoriteCodeSet,
      RECOMMENDATION_FOCUS_CODE,
      expandedCodes,
      async (code) => {
        const saved = await saveFavoriteHistoryEntry({
          label: `推荐页收藏 ${code}`,
          codes: [code],
          entry_type: 'favorite',
        });
        favoriteHistoryPayload = saved || { records: [] };
        favoriteCodeSet = buildFavoriteCodeSet(favoriteHistoryPayload);
        renderRecommendationActionBanner(`已收藏 ${code}`, 'info-banner-success');
        rerender();
      },
      (code, checked) => {
        if (!code) return;
        if (checked) {
          if (compareCodes.includes(code) || compareCodes.length >= 4) return;
          compareCodes = [...compareCodes, code];
        } else {
          compareCodes = compareCodes.filter((item) => item !== code);
        }
        rerender();
      },
      (code) => {
        const normalized = normalizeRecommendationCode(code);
        if (!normalized) return;
        if (expandedCodes.has(normalized)) expandedCodes.delete(normalized);
        else expandedCodes.add(normalized);
        rerender();
      },
      (expanded, rows) => {
        expandedCodes = expanded
          ? new Set((Array.isArray(rows) ? rows : []).map((row) => normalizeRecommendationCode(row['基金代码'] || '')).filter(Boolean))
          : new Set();
        rerender();
      },
      (nextSortState) => {
        sortState = nextSortState;
        sortPreset = normalizeRecommendationSortPreset(nextSortState);
        rerender();
      },
      groupBriefMap,
      recommendationViewMode,
      recommendationDensityMode,
    );
    recommendationCards.querySelector('[data-rec-empty-action="clear-query"]')?.addEventListener('click', () => {
      filterState = {
        ...filterState,
        query: '',
      };
      rerender();
      if (valuationRequested) refreshValuations();
    });
    recommendationCards.querySelector('[data-rec-empty-action="reset-filters"]')?.addEventListener('click', () => {
      filterState = defaultRecommendationFilterState();
      rerender();
      if (valuationRequested) refreshValuations();
    });
    recommendationCards.querySelector('[data-rec-empty-action="next-group"]')?.addEventListener('click', () => {
      const nextGroup = String(recommendationCards.querySelector('[data-rec-empty-action="next-group"]')?.dataset.recNextGroup || '').trim();
      if (!nextGroup || nextGroup === activeGroup) return;
      selectRecommendationGroup(nextGroup);
    });
    currentVisibleFunds = renderResult.funds || [];
    if (RECOMMENDATION_FOCUS_CODE && currentVisibleFunds.some((item) => normalizeRecommendationCode(item['基金代码']) === RECOMMENDATION_FOCUS_CODE)) {
      window.requestAnimationFrame(() => {
        if (focusRecommendationTarget(RECOMMENDATION_FOCUS_CODE) && !focusBannerRendered) {
          renderRecommendationActionBanner(`已定位基金 ${RECOMMENDATION_FOCUS_CODE}`, 'info-banner-success');
          focusBannerRendered = true;
        }
      });
    } else if (RECOMMENDATION_FOCUS_CODE && !focusBannerRendered && initialFocusGroup) {
      renderRecommendationActionBanner(`基金 ${RECOMMENDATION_FOCUS_CODE} 当前未出现在筛选结果中`, 'info-banner-warning');
      focusBannerRendered = true;
    } else if (RECOMMENDATION_FOCUS_CODE && !focusBannerRendered && !initialFocusGroup) {
      renderRecommendationActionBanner(`基金 ${RECOMMENDATION_FOCUS_CODE} 当前不在推荐优先池`, 'info-banner-warning');
      focusBannerRendered = true;
    }
    compareCodes = compareCodes.filter((code) => currentVisibleFunds.some((item) => String(item['基金代码'] || '').trim() === code));
    syncRecommendationViewUrl({
      activeGroup,
      sortState,
      sortPreset,
      filterState,
      viewMode: recommendationViewMode,
      densityMode: recommendationDensityMode,
      compareCodes,
    });
    renderRecommendationComparePanel(
      currentVisibleFunds,
      compareCodes,
      (code, checked) => {
        if (checked) {
          if (compareCodes.includes(code) || compareCodes.length >= 4) return;
          compareCodes = [...compareCodes, code];
        } else {
          compareCodes = compareCodes.filter((item) => item !== code);
        }
        rerender();
      },
      () => {
        compareCodes = [];
        rerender();
      },
    );
    const visibleStyleCount = new Set(currentVisibleFunds.map((item) => normalizeFundStyleLabel(item['债基风格标签'])).filter((style) => style && style !== '未标注风格')).size;
    recommendationsMeta.textContent = recommendationVersionMetaText(payload, [
      `当前分组 ${formatNumber(groupFunds.length)} 只`,
      `可见 ${formatNumber(currentVisibleFunds.length)} 只`,
      `风格 ${formatNumber(visibleStyleCount)} 个`,
    ]);
    renderRecommendationListHintsPanel(
      currentVisibleFunds,
      groupFunds.length,
      sortPreset,
      favoriteCodeSet,
      filterState,
      recommendationViewMode,
      recommendationDensityMode,
      currentGroup?.name || '',
      '完整明细',
    );
    if (recommendationValuationBtn) {
      recommendationValuationBtn.disabled = valuationState === VALUATION_STATE_LOADING;
      recommendationValuationBtn.textContent = valuationState === VALUATION_STATE_LOADING ? '补估值中' : '补估值';
    }
    if (recommendationExportBtn) {
      recommendationExportBtn.disabled = !groups.length;
    }
    const sortText = sortState?.column
      ? `${sortState.column} ${sortState.direction === 'asc' ? '↑' : '↓'}`
      : '未排序';
    const viewLabel = recommendationViewMode === FUND_STYLE_VIEW_MODES.grouped ? '风格分组' : '混合总表';
    const densityLabel = recommendationDensityMode === FUND_TABLE_DENSITY_MODES.expanded ? '扩展视图' : '核心视图';
    recommendationsStats.innerHTML = [
      buildStatCard('当前可见', formatNumber(currentVisibleFunds.length), `${formatNumber(renderResult.totalFunds || 0)} 只分组候选`),
      buildStatCard('正式主榜', formatNumber(counts.final_count), '完整正式推荐名单'),
      buildStatCard('当前排序', sortText, recommendationSortPresetLabel(sortPreset)),
      buildStatCard('当前视图', `${viewLabel} / ${densityLabel}`, `覆盖风格 ${formatNumber(visibleStyleCount)} 个`),
    ].join('');
  };

  const refreshValuations = async () => {
    const codes = currentVisibleFunds.map((item) => item['基金代码']);
    if (!codes.length) {
      valuationState = VALUATION_STATE_IDLE;
      renderRecommendationValuationBanner('暂无可估值基金；主表收益仍按正式快照与统一数据基准比较。');
      return;
    }
    valuationState = VALUATION_STATE_LOADING;
    valuationMap = {};
    rerender();
    renderRecommendationValuationBanner(`估值中：${codes.length} 只；主表收益列继续按正式快照口径比较。`, { loading: true });
    try {
      const result = await loadTrackerValuations(codes);
      valuationMap = result?.valuations || {};
      valuationState = VALUATION_STATE_READY;
      rerender();
      const availableCount = Object.values(valuationMap).filter((item) => item?.available).length;
      renderRecommendationValuationBanner(
        formatTrackerValuationStatusMessage(result, availableCount, codes.length)
      );
    } catch (error) {
      valuationState = VALUATION_STATE_FAILED;
      valuationMap = {};
      rerender();
      renderRecommendationValuationBanner(
        error?.message ? `估值失败：${error.message}` : '估值失败',
        {
          failed: true,
          retryable: true,
          onRetry: async () => {
            valuationRequested = true;
            await refreshValuations();
          },
        },
      );
    }
  };

  if (recommendationValuationBtn) {
    recommendationValuationBtn.onclick = async () => {
      valuationRequested = true;
      await refreshValuations();
    };
  }

  if (recommendationExportBtn) {
    recommendationExportBtn.onclick = () => {
      window.location.href = '/api/recommendations/export';
    };
  }

  if (recommendationChangeToggleBtn) {
    recommendationChangeToggleBtn.onclick = () => {
      recommendationMobileChangeCollapsed = !recommendationMobileChangeCollapsed;
      applyRecommendationMobileSectionState();
    };
  }
  if (recommendationCompareToggleBtn) {
    recommendationCompareToggleBtn.onclick = () => {
      recommendationMobileCompareCollapsed = !recommendationMobileCompareCollapsed;
      applyRecommendationMobileSectionState();
    };
  }

  rerender();
  rerenderRecommendationsForViewport = rerender;
  applyRecommendationMobileSectionState();
  renderRecommendationValuationBanner('点击后补估值；主表收益列默认按正式快照与统一数据基准比较。');
  renderRecommendationPublicationBanner(payload, '可直接加入收藏');
}

initRecommendationsPage().catch((error) => {
  renderRecommendationActionBanner(error?.message || '推荐页加载失败', 'info-banner-error');
});
