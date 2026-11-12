function escapeHtml(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

const TRACKER_VALUATION_MAX_REQUEST_CODES = 50;
const API_INFO_CACHE_TTL_MS = 30000;

const FUND_DATA_VALIDATION_FIELDS = Object.freeze([
  '数据校验原始状态', '数据校验状态', '数据校验备注', '数据校验告警原因',
  '数据校验来源', '数据校验契约版本',
]);

function publicationValidationProviderLabel(source) {
  const value = String(source ?? '').trim();
  if (value === 'eastmoney_cross_endpoint') return '天天基金／东方财富跨接口校验（非独立供应商）';
  if (value === 'autostock') return 'AutoStock 校验';
  return value || '校验来源未提供';
}

function fundValidationContract(row = {}) {
  const record = row && typeof row === 'object' ? row : {};
  const neutral = FUND_DATA_VALIDATION_FIELDS.some((field) => Object.hasOwn(record, field));
  const source = neutral ? String(record['数据校验来源'] ?? '').trim() : 'autostock';
  return {
    neutral,
    source,
    sourceLabel: neutral ? publicationValidationProviderLabel(source) : 'AutoStock 历史校验',
    rawStatus: String(neutral ? record['数据校验原始状态'] ?? '' : record['AutoStock原始校验状态'] ?? '').trim(),
    displayStatus: String(neutral ? record['数据校验状态'] ?? '' : record['AutoStock校验状态'] ?? '').trim(),
    note: String(neutral ? record['数据校验备注'] ?? '' : record['AutoStock校验备注'] ?? '').trim(),
    warnings: String(neutral ? record['数据校验告警原因'] ?? '' : record['AutoStock校验告警原因'] ?? '').trim(),
    contractVersion: neutral ? String(record['数据校验契约版本'] ?? '').trim() : '',
  };
}

function copyFundValidationFields(row = {}) {
  const record = row && typeof row === 'object' ? row : {};
  const fields = fundValidationContract(record).neutral ? FUND_DATA_VALIDATION_FIELDS : [
    'AutoStock原始校验状态', 'AutoStock校验状态', 'AutoStock校验备注', 'AutoStock校验告警原因',
  ];
  return Object.fromEntries(fields.map((field) => [field, record[field] ?? null]));
}

function favoriteLiveSourceLabel(row = {}, metadata = {}) {
  const source = String(row?.__favorite_live_source ?? metadata?.source ?? '').trim();
  if (source === 'tiantian_fund_valuation_last') return '天天基金 FundValuationLast';
  if (source === 'autostock_batch' || source === 'autostock') return 'AutoStock';
  return source || '当前 API（来源未提供）';
}

function configuredAutoStockStatus(configured = {}) {
  if (configured.autostock_enabled === false) return '已禁用';
  if (configured.autostock_enabled === true) return '已启用';
  return '启用状态未提供';
}

function formatNavSnapshotComparison(freshness, effectiveSnapshot) {
  if (!freshness?.raw_latest_nav_date) return '原始净值日期缺失';
  if (!freshness?.effective_candidate_date) return '有效快照日期缺失';
  if (freshness.schema_version !== 2
      || freshness.effective_candidate_date !== effectiveSnapshot?.candidate_date) {
    return '日期尚未完成核对';
  }
  const labels = {
    aligned: '原始日期与有效快照一致',
    raw_ahead: '原始日期晚于有效快照',
    raw_behind: '原始日期早于有效快照，待核实',
    future_date_unverified: '含未来日期，待核实',
    unknown: '日期尚未完成核对',
  };
  return labels[freshness.raw_vs_effective] || '日期尚未完成核对';
}

function syncStickyTableOffset() {
  const header = document.querySelector('.site-header');
  if (!header) return;
  const height = Math.ceil(header.getBoundingClientRect().height || 0);
  document.documentElement.style.setProperty('--sticky-table-top', `${Math.max(height, 0)}px`);
}

function syncTableWrapStickyOffsets(root = document) {
  const header = document.querySelector('.site-header');
  if (!header) return;
  const wraps = typeof root?.querySelectorAll === 'function' ? root.querySelectorAll('.table-wrap') : [];
  if (!wraps.length) return;
  const headerHeight = Math.ceil(header.getBoundingClientRect().height || 0);
  const viewportHeight = Math.ceil(window.visualViewport?.height || window.innerHeight || 0);
  const limitToViewport = root === document;
  wraps.forEach((wrap) => {
    const rect = wrap.getBoundingClientRect();
    if (limitToViewport && viewportHeight > 0 && (rect.bottom < 0 || rect.top > viewportHeight)) {
      return;
    }
    const localTop = Math.max(headerHeight - rect.top, 0);
    wrap.style.setProperty('--sticky-table-local-top', `${Math.max(localTop, 0)}px`);
  });
}

function syncCompactExpandWrapOverflowState(root = document) {
  const wraps = typeof root?.querySelectorAll === 'function' ? root.querySelectorAll('.compact-expand-wrap') : [];
  if (!wraps.length) return;
  wraps.forEach((wrap) => {
    const hasHorizontalOverflow = (wrap.scrollWidth - wrap.clientWidth) > 1;
    const atScrollStart = (wrap.scrollLeft || 0) <= 4;
    wrap.dataset.horizontalOverflow = hasHorizontalOverflow ? 'true' : 'false';
    wrap.dataset.horizontalAtStart = hasHorizontalOverflow && atScrollStart ? 'true' : 'false';
    if (wrap.dataset.overflowStateBound === 'true') return;
    wrap.dataset.overflowStateBound = 'true';
    wrap.addEventListener('scroll', () => {
      const scrollable = (wrap.scrollWidth - wrap.clientWidth) > 1;
      const atStart = (wrap.scrollLeft || 0) <= 4;
      wrap.dataset.horizontalOverflow = scrollable ? 'true' : 'false';
      wrap.dataset.horizontalAtStart = scrollable && atStart ? 'true' : 'false';
    }, { passive: true });
  });
}

function requestAnimationFrameSafe(callback) {
  if (typeof window.requestAnimationFrame === 'function') {
    return window.requestAnimationFrame(callback);
  }
  return window.setTimeout(callback, 16);
}

function cancelAnimationFrameSafe(handle) {
  if (!handle) return;
  if (typeof window.cancelAnimationFrame === 'function') {
    window.cancelAnimationFrame(handle);
    return;
  }
  window.clearTimeout(handle);
}

const stickyTableLayoutState = {
  frame: 0,
  pendingRoots: new Set(),
  syncHeader: false,
};

function scheduleStickyTableLayout(root = document, { syncHeader = false } = {}) {
  if (root) {
    stickyTableLayoutState.pendingRoots.add(root);
  }
  if (syncHeader) {
    stickyTableLayoutState.syncHeader = true;
  }
  if (stickyTableLayoutState.frame) return;
  stickyTableLayoutState.frame = requestAnimationFrameSafe(() => {
    stickyTableLayoutState.frame = 0;
    const pendingRoots = [...stickyTableLayoutState.pendingRoots];
    stickyTableLayoutState.pendingRoots.clear();
    const shouldSyncHeader = stickyTableLayoutState.syncHeader;
    stickyTableLayoutState.syncHeader = false;
    if (shouldSyncHeader) {
      syncStickyTableOffset();
    }
    if (!pendingRoots.length) {
      pendingRoots.push(document);
    }
    pendingRoots.forEach((pendingRoot) => {
      syncTableWrapStickyOffsets(pendingRoot);
      syncCompactExpandWrapOverflowState(pendingRoot);
    });
  });
}

function syncStickyTableLayout(root = document) {
  syncStickyTableOffset();
  syncTableWrapStickyOffsets(root);
  syncCompactExpandWrapOverflowState(root);
}

window.addEventListener('resize', () => scheduleStickyTableLayout(document, { syncHeader: true }));
window.addEventListener('load', () => scheduleStickyTableLayout(document, { syncHeader: true }));
window.addEventListener('scroll', () => scheduleStickyTableLayout(), { passive: true });
syncStickyTableLayout();

const GLOBAL_FLOATING_TOOLTIP_ID = 'globalFloatingTooltip';
const floatingTooltipState = {
  layer: null,
  shell: null,
  source: null,
  trigger: null,
  hideTimer: 0,
  repositionFrame: 0,
  eventsBound: false,
  observer: null,
};

function clearFloatingTooltipHideTimer() {
  if (!floatingTooltipState.hideTimer) return;
  window.clearTimeout(floatingTooltipState.hideTimer);
  floatingTooltipState.hideTimer = 0;
}

function ensureFloatingTooltipLayer() {
  if (floatingTooltipState.layer?.isConnected && floatingTooltipState.shell?.isConnected) {
    return floatingTooltipState.layer;
  }
  const layer = document.createElement('div');
  layer.id = GLOBAL_FLOATING_TOOLTIP_ID;
  layer.className = 'floating-tooltip-layer';
  layer.setAttribute('role', 'tooltip');
  layer.hidden = true;

  const shell = document.createElement('div');
  shell.className = 'floating-tooltip-shell';
  layer.appendChild(shell);

  layer.addEventListener('mouseenter', clearFloatingTooltipHideTimer);
  layer.addEventListener('mouseleave', () => {
    scheduleFloatingTooltipHide();
  });

  document.body.appendChild(layer);
  floatingTooltipState.layer = layer;
  floatingTooltipState.shell = shell;
  return layer;
}

function requestFloatingTooltipPosition() {
  if (floatingTooltipState.repositionFrame) return;
  floatingTooltipState.repositionFrame = requestAnimationFrameSafe(() => {
    floatingTooltipState.repositionFrame = 0;
    positionFloatingTooltip();
  });
}

function refreshFloatingTooltipContentFromSource(source = floatingTooltipState.source) {
  if (!(source instanceof Element)) return;
  if (!floatingTooltipState.shell || floatingTooltipState.source !== source) {
    return;
  }
  const card = source.querySelector('.score-tooltip-card');
  if (!card) return;
  const floatingCard = document.createElement('div');
  floatingCard.className = `${card.className} is-floating-tooltip-card`;
  floatingCard.innerHTML = card.innerHTML;
  floatingCard.removeAttribute('id');
  floatingTooltipState.shell.replaceChildren(floatingCard);
}

function isFloatingTooltipVisible() {
  return Boolean(floatingTooltipState.layer && floatingTooltipState.layer.hidden === false);
}

function ensureFloatingTooltipObserver() {
  if (floatingTooltipState.observer || !document.body) return;
  floatingTooltipState.observer = new MutationObserver(() => {
    if (floatingTooltipState.source && !floatingTooltipState.source.isConnected) {
      hideFloatingTooltip();
    }
  });
  floatingTooltipState.observer.observe(document.body, {
    childList: true,
    subtree: true,
  });
}

function disconnectFloatingTooltipObserver() {
  if (!floatingTooltipState.observer) return;
  floatingTooltipState.observer.disconnect();
  floatingTooltipState.observer = null;
}

function positionFloatingTooltip() {
  const { trigger, source, layer } = floatingTooltipState;
  if (!trigger || !source || !layer || layer.hidden) return;
  if (!trigger.isConnected || !source.isConnected) {
    hideFloatingTooltip();
    return;
  }

  const rect = trigger.getBoundingClientRect();
  const viewportWidth = window.visualViewport?.width || window.innerWidth;
  const viewportHeight = window.visualViewport?.height || window.innerHeight;
  const margin = 24;
  const gap = 10;

  layer.style.left = '0px';
  layer.style.top = '0px';
  layer.style.visibility = 'hidden';
  layer.hidden = false;
  layer.classList.add('is-visible');

  const initialLayerRect = layer.getBoundingClientRect();
  const layoutWidth = layer.offsetWidth || initialLayerRect.width || 1;
  const scale = layoutWidth > 0 ? initialLayerRect.width / layoutWidth : 1;
  const floatingCard = layer.querySelector('.is-floating-tooltip-card, .score-tooltip-card');
  const nextWidth = Math.min(440, Math.max(240, (viewportWidth - margin * 2) / Math.max(scale, 1)));
  layer.style.width = `${Math.round(nextWidth)}px`;
  layer.style.maxWidth = `${Math.round(nextWidth)}px`;
  if (floatingCard) {
    const nextCardMaxHeight = Math.min(560, Math.max(180, (viewportHeight - margin * 2) / Math.max(scale, 1)));
    floatingCard.style.maxHeight = `${Math.round(nextCardMaxHeight)}px`;
  }

  const layerRect = layer.getBoundingClientRect();
  const width = layerRect.width;
  const height = layerRect.height;
  const spaceBelow = viewportHeight - rect.bottom - margin;
  const spaceAbove = rect.top - margin;
  const placeBelow = spaceBelow >= height + gap || spaceBelow >= spaceAbove;

  let left = rect.left + (rect.width / 2) - (width / 2);
  left = Math.max(margin, Math.min(left, viewportWidth - width - margin));

  let top = placeBelow ? rect.bottom + gap : rect.top - height - gap;
  top = Math.max(margin, Math.min(top, viewportHeight - height - margin));

  layer.style.left = `${Math.round(left / Math.max(scale, 1))}px`;
  layer.style.top = `${Math.round(top / Math.max(scale, 1))}px`;
  layer.style.visibility = 'visible';
  layer.dataset.placement = placeBelow ? 'bottom' : 'top';
}

function showFloatingTooltip(source) {
  if (!(source instanceof Element)) return;
  const trigger = source.querySelector('.score-tooltip-trigger');
  const card = source.querySelector('.score-tooltip-card');
  if (!trigger || !card) return;

  clearFloatingTooltipHideTimer();
  const layer = ensureFloatingTooltipLayer();
  const shell = floatingTooltipState.shell;
  if (!shell) return;
  const sameSourceVisible = floatingTooltipState.source === source && floatingTooltipState.trigger === trigger && isFloatingTooltipVisible();

  if (floatingTooltipState.trigger && floatingTooltipState.trigger !== trigger) {
    floatingTooltipState.trigger.setAttribute('aria-expanded', 'false');
    floatingTooltipState.trigger.removeAttribute('aria-describedby');
  }

  floatingTooltipState.source = source;
  floatingTooltipState.trigger = trigger;
  trigger.setAttribute('aria-expanded', 'true');
  trigger.setAttribute('aria-controls', GLOBAL_FLOATING_TOOLTIP_ID);
  trigger.setAttribute('aria-describedby', GLOBAL_FLOATING_TOOLTIP_ID);

  if (!sameSourceVisible) {
    refreshFloatingTooltipContentFromSource(source);
  } else {
    refreshFloatingTooltipContentFromSource(source);
  }

  layer.hidden = false;
  layer.classList.add('is-visible');
  ensureFloatingTooltipObserver();
  requestFloatingTooltipPosition();
}

function hideFloatingTooltip(options = {}) {
  clearFloatingTooltipHideTimer();
  if (floatingTooltipState.repositionFrame) {
    cancelAnimationFrameSafe(floatingTooltipState.repositionFrame);
    floatingTooltipState.repositionFrame = 0;
  }

  if (floatingTooltipState.trigger) {
    floatingTooltipState.trigger.setAttribute('aria-expanded', 'false');
    floatingTooltipState.trigger.removeAttribute('aria-describedby');
    if (options.restoreFocus && floatingTooltipState.trigger.isConnected) {
      floatingTooltipState.trigger.focus();
    }
  }

  floatingTooltipState.source = null;
  floatingTooltipState.trigger = null;
  disconnectFloatingTooltipObserver();

  if (floatingTooltipState.shell) {
    floatingTooltipState.shell.replaceChildren();
  }

  if (!floatingTooltipState.layer) return;
  floatingTooltipState.layer.classList.remove('is-visible');
  floatingTooltipState.layer.hidden = true;
  floatingTooltipState.layer.style.removeProperty('left');
  floatingTooltipState.layer.style.removeProperty('top');
  floatingTooltipState.layer.style.removeProperty('width');
  floatingTooltipState.layer.style.removeProperty('max-width');
  floatingTooltipState.layer.style.removeProperty('visibility');
  floatingTooltipState.layer.querySelectorAll('.is-floating-tooltip-card, .score-tooltip-card').forEach((card) => {
    card.style.removeProperty('max-height');
  });
  delete floatingTooltipState.layer.dataset.placement;
}

function scheduleFloatingTooltipHide(delay = 120) {
  clearFloatingTooltipHideTimer();
  floatingTooltipState.hideTimer = window.setTimeout(() => {
    floatingTooltipState.hideTimer = 0;
    hideFloatingTooltip();
  }, delay);
}

function bindFloatingTooltipEvents() {
  if (floatingTooltipState.eventsBound) return;
  floatingTooltipState.eventsBound = true;
  document.documentElement.classList.add('has-floating-tooltips');

  document.addEventListener('mouseover', (event) => {
    const source = event.target instanceof Element ? event.target.closest('.score-tooltip') : null;
    if (!source?.querySelector('.score-tooltip-card')) return;
    showFloatingTooltip(source);
  });

  document.addEventListener('mouseout', (event) => {
    const source = event.target instanceof Element ? event.target.closest('.score-tooltip') : null;
    if (!source?.querySelector('.score-tooltip-card')) return;
    const nextTarget = event.relatedTarget instanceof Element ? event.relatedTarget : null;
    if (nextTarget && (source.contains(nextTarget) || floatingTooltipState.layer?.contains(nextTarget))) {
      return;
    }
    scheduleFloatingTooltipHide();
  });

  document.addEventListener('focusin', (event) => {
    const source = event.target instanceof Element ? event.target.closest('.score-tooltip') : null;
    if (!source?.querySelector('.score-tooltip-card')) return;
    showFloatingTooltip(source);
  });

  document.addEventListener('focusout', (event) => {
    const source = event.target instanceof Element ? event.target.closest('.score-tooltip') : null;
    if (!source?.querySelector('.score-tooltip-card')) return;
    const nextTarget = event.relatedTarget instanceof Element ? event.relatedTarget : null;
    if (nextTarget && (source.contains(nextTarget) || floatingTooltipState.layer?.contains(nextTarget))) {
      return;
    }
    scheduleFloatingTooltipHide(0);
  });

  document.addEventListener('click', (event) => {
    const target = event.target instanceof Element ? event.target : null;
    const source = target?.closest('.score-tooltip');
    if (source?.querySelector('.score-tooltip-card')) {
      event.preventDefault();
      if (floatingTooltipState.source === source && floatingTooltipState.layer && !floatingTooltipState.layer.hidden) {
        hideFloatingTooltip();
      } else {
        showFloatingTooltip(source);
      }
      return;
    }
    if (floatingTooltipState.layer?.contains(target)) return;
    hideFloatingTooltip();
  });

  document.addEventListener('keydown', (event) => {
    if (event.key !== 'Escape' || floatingTooltipState.layer?.hidden !== false) return;
    hideFloatingTooltip({ restoreFocus: true });
  });

  window.addEventListener('resize', () => {
    if (isFloatingTooltipVisible()) {
      requestFloatingTooltipPosition();
    }
  });
  window.addEventListener('scroll', () => {
    if (isFloatingTooltipVisible()) {
      requestFloatingTooltipPosition();
    }
  }, true);
}

bindFloatingTooltipEvents();

function normalizeProjectPath(path) {
  let normalized = String(path || '')
    .replace(window.location.origin, '')
    .replace(/^\//, '')
    .trim();
  if (!normalized) return '';

  const relativeMatch = normalized.match(/(?:^|\/)(reports\/.+|backend\/.+|core\/.+|logs\/.+)$/);
  if (relativeMatch) {
    return relativeMatch[1];
  }
  return normalized;
}

function fileUrl(path, download = false) {
  const relative = normalizeProjectPath(path);
  return `/api/files/${relative}${download ? '?download=1' : ''}`;
}

function viewerUrl(path, sheet = '') {
  const params = new URLSearchParams({ path: normalizeProjectPath(path) });
  if (sheet) params.set('sheet', sheet);
  return `/excel-viewer?${params.toString()}`;
}

function cloneJsonPayload(payload) {
  if (typeof structuredClone === 'function') {
    return structuredClone(payload);
  }
  return JSON.parse(JSON.stringify(payload));
}

function formatDateTime(value) {
  if (!value) return '-';
  let timestamp = String(value).replace(' ', 'T');
  // Legacy API wall times were written in Shanghai; browser location must
  // not reinterpret them. Explicit UTC/offset timestamps retain their instant.
  if (/^\d{4}-\d{2}-\d{2}$/.test(timestamp)) timestamp += 'T00:00:00';
  if (!/(?:Z|[+-]\d{2}:?\d{2})$/i.test(timestamp)) timestamp += '+08:00';
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString('zh-CN', {
    timeZone: 'Asia/Shanghai',
    hourCycle: 'h23',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

function formatNumber(value) {
  if (value === null || value === undefined || value === '') return '-';
  const num = Number(value);
  if (!Number.isFinite(num)) return String(value);
  return num.toLocaleString('zh-CN');
}

function hasPresentValue(value) {
  return value !== null && value !== undefined && value !== '';
}

function firstPresentValue(...values) {
  return values.find((value) => hasPresentValue(value));
}

function numericValueOrNull(value) {
  if (value === null || value === undefined || value === '') return null;
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function sumPresentNumbers(...values) {
  const numbers = values.map(numericValueOrNull).filter((value) => value !== null);
  return numbers.length ? numbers.reduce((total, value) => total + value, 0) : null;
}

function formatPercent(value, digits = 2) {
  if (value === null || value === undefined || value === '') return '-';
  const num = Number(value);
  if (!Number.isFinite(num)) return String(value);
  return `${num.toFixed(digits)}%`;
}

function formatRatioPercent(value, digits = 2) {
  const ratio = numericValueOrNull(value);
  if (ratio === null) return '-';
  return formatPercent(ratio * 100, digits);
}

function formatSeconds(value, digits = 1) {
  if (value === null || value === undefined || value === '') return '-';
  const num = Number(value);
  if (!Number.isFinite(num)) return String(value);
  return `${num.toFixed(digits)}s`;
}

function compactUiText(value, maxLength = 15) {
  const text = String(value ?? '').trim();
  if (!text) return '';
  const chars = Array.from(text);
  if (chars.length <= maxLength) return text;
  return `${chars.slice(0, Math.max(1, maxLength - 1)).join('')}…`;
}

const STATUS_LABELS = {
  idle: '空闲',
  running: '运行中',
  started: '已启动',
  succeeded: '成功',
  success: '成功',
  skipped: '已跳过',
  skipped_normal: '正常跳过',
  skipped_blocked: '跳过需关注',
  degraded: '部分成功',
  failed: '失败',
  passed: '通过',
  error: '错误',
  accepted: '已受理',
  queued: '已排队',
  automatic: '自动闭环',
  automatic_recovering: '自动恢复中',
  automatic_with_warnings: '自动运行告警',
  attention_required: '需人工处理',
  unknown: '未知',
};

const FUND_SHARED_SORTABLE_COLUMNS = Object.freeze([
  '实时估值',
  '出现次数&出现频率(%)',
  '1年-年化收益(%)',
  '平台成立以来收益年化(%)',
  '近1周(%)',
  '近1月(%)',
  '近3月(%)',
  '近6月(%)',
  '1年-夏普比率',
  '1年-最大回撤(%)',
  '1年-波动率',
  '最新净值日期',
  '基金规模(亿)',
  '投资优先池得分',
  '正式投资评分',
  '费率合计(%)',
]);

const FUND_SHARED_INITIAL_SORT_DIRECTIONS = Object.freeze({
  '实时估值': 'desc',
  '出现次数&出现频率(%)': 'desc',
  '1年-年化收益(%)': 'desc',
  '平台成立以来收益年化(%)': 'desc',
  '近1周(%)': 'desc',
  '近1月(%)': 'desc',
  '近3月(%)': 'desc',
  '近6月(%)': 'desc',
  '1年-夏普比率': 'desc',
  '1年-最大回撤(%)': 'asc',
  '1年-波动率': 'asc',
  '最新净值日期': 'desc',
  '基金规模(亿)': 'desc',
  '投资优先池得分': 'desc',
  '正式投资评分': 'desc',
  '费率合计(%)': 'asc',
});

const FUND_SHARED_RISK_DETAIL_FIELDS = Object.freeze([
  '1年-夏普比率',
  '1年-最大回撤(%)',
  '1年-波动率',
  '费率合计(%)',
]);

const FUND_DECIMAL_RATIO_PERCENT_FIELDS = new Set([
  '1年-波动率',
  '最新波动率',
  '1年-最大回撤(%)',
  '最新最大回撤(%)',
  '1年-VaR95(%)',
  '1年-VaR99(%)',
  '最新VaR95(%)',
  '最新VaR99(%)',
]);

let activeMetricUnits = {};

function setMetricUnits(metricUnits) {
  activeMetricUnits = metricUnits && typeof metricUnits === 'object'
    ? Object.fromEntries(
      Object.entries(metricUnits).filter(([field, metadata]) => (
        String(field || '').trim() && metadata && typeof metadata === 'object'
      )),
    )
    : {};
  return activeMetricUnits;
}

function getMetricUnitMetadata(field) {
  return activeMetricUnits[String(field || '').trim()] || null;
}

function strategyMetricDisplayLabel(value) {
  const label = String(value ?? '');
  return label.includes('策略') ? label : label.replace(/夏普|索提诺|卡玛/gu, (name) => `策略${name}`);
}

function renderMetricColumnHeader(field) {
  const metadata = getMetricUnitMetadata(field);
  if (metadata?.raw_unit !== 'nav_observations') return escapeHtml(strategyMetricDisplayLabel(field));
  return `<span title="${escapeHtml(metadata.definition || '')}">${escapeHtml(metadata.label || field)}</span>`;
}

function normalizePayloadDataContract(payload = {}) {
  const sourceBasis = payload?.source_basis && typeof payload.source_basis === 'object'
    ? payload.source_basis
    : {};
  const cacheBasis = payload?.cache_basis && typeof payload.cache_basis === 'object'
    ? payload.cache_basis
    : {};
  const sourceContract = payload?.source_contract && typeof payload.source_contract === 'object'
    ? payload.source_contract
    : {};
  const source = String(sourceContract.source || sourceBasis.source || '').trim();
  const basisSource = String(sourceBasis.source || '').trim();
  const cacheSource = String(cacheBasis.source || '').trim();
  const basisDigest = String(sourceBasis.digest || '').trim();
  const cacheDigest = String(cacheBasis.digest || '').trim();
  const contractDigest = String(sourceContract.cache_basis_digest || '').trim();
  const sourceAligned = Boolean(source && basisSource && source === basisSource && (!cacheSource || cacheSource === basisSource));
  const cacheAligned = Boolean(
    basisDigest
    && cacheDigest
    && basisDigest === cacheDigest
    && (!contractDigest || contractDigest === basisDigest)
  );
  const missingValuePreserved = String(sourceContract.missing_value_policy || sourceBasis.missing_value_policy || '').trim()
    === 'preserve_missing_semantics';
  const available = Boolean(Object.keys(sourceBasis).length && Object.keys(cacheBasis).length && Object.keys(sourceContract).length);
  const aligned = Boolean(available && sourceAligned && cacheAligned && missingValuePreserved);
  const officialTruth = String(sourceContract.official_publication_truth || '').trim();
  let sourceLabel = '真实接口数据';
  if (officialTruth.includes('current_published')) sourceLabel = '当前正式发布';
  else if (officialTruth === 'publication_registry') sourceLabel = '发布登记';
  else if (officialTruth === 'false_preview_only') sourceLabel = '真实文件预览';
  else if (source.includes('favorite')) sourceLabel = '收藏评分查询';
  return {
    available,
    aligned,
    source,
    sourceLabel,
    missingValuePreserved,
    sourceBasis,
    cacheBasis,
    sourceContract,
    summary: !available
      ? '来源契约暂不可用'
      : aligned
        ? `来源 ${sourceLabel} · 缓存口径一致`
        : `来源 ${sourceLabel} · 契约待校验`,
  };
}

function applyPayloadDataContract(payload = {}) {
  setMetricUnits(payload?.metric_units);
  return normalizePayloadDataContract(payload);
}

function payloadSourceContractSummary(payload = {}) {
  return normalizePayloadDataContract(payload).summary;
}

const FUND_SIGNED_PERCENT_DISPLAY_FIELDS = new Set([
  '实时估值',
  '最新日增长率(%)',
  '近1周(%)',
  '近1月(%)',
  '近3月(%)',
  '近6月(%)',
  '1年-年化收益(%)',
  '平台成立以来收益年化(%)',
]);

const FUND_SHARED_LABEL_DETAIL_FIELDS = Object.freeze([
  '正式榜分组',
  '债基风格标签',
  '长期稳定性标签',
  '基金公司',
]);

const FUND_SHARED_TIME_DETAIL_FIELDS = Object.freeze([
  '实时估值',
  '实时估值时间',
  '实时估算净值',
  '最新单位净值',
  '最新净值日期',
]);

const FUND_DETAIL_SECTION_TITLES = Object.freeze({
  time: '净值与时间',
  label: '标签与归属',
  risk: '风险收益',
  subject: '主体信息',
  recommendation: '推荐信息',
  supplemental: '补充信息',
});

const FUND_COPYWRITING = Object.freeze({
  oneYearAnnualizedLabel: '1年年化',
  oneYearAnnualizedStrongHintPrefix: '1年年化较强',
  oneYearAnnualizedSteadyHintPrefix: '1年年化稳健',
  oneYearAnnualizedWeakHintPrefix: '1年年化偏弱',
  sinceInceptionAnnualizedLabel: '平台成立年化',
  sinceInceptionAnnualizedFullLabel: '平台成立以来收益年化(%)',
  sinceInceptionAnnualizedAvailableHint: '平台成立年化：仅显示可用',
  sinceInceptionAnnualizedMissingHint: '平台成立年化：仅显示待确认',
  sinceInceptionAnnualizedAvailableCountLabel: '平台成立年化可用',
  sinceInceptionAnnualizedPendingCountLabel: '平台成立年化待确认',
  sameStyleAnnualizedCompareNote: '建议优先在同风格内比较正式投资评分、平台成立年化与风险收益指标。',
});

const FUND_SHARED_CORE_BASE_COLUMNS = Object.freeze([
  '序号',
  '基金代码',
  '基金名称',
  '正式投资评分',
  '平台成立以来收益年化(%)',
  '1年-年化收益(%)',
  '近3月(%)',
  '近1周(%)',
  '近1月(%)',
  '实时估值',
]);

const FUND_SHARED_MOBILE_METRIC_COLUMNS = Object.freeze([
  '平台成立以来收益年化(%)',
  '1年-年化收益(%)',
  '近3月(%)',
  '近1周(%)',
  '近1月(%)',
  '实时估值',
]);

const FUND_STYLE_VIEW_MODES = Object.freeze({
  mixed: 'mixed',
  grouped: 'grouped',
});

const FUND_TABLE_DENSITY_MODES = Object.freeze({
  core: 'core',
  expanded: 'expanded',
});

const FUND_STYLE_THRESHOLD_FIELDS = Object.freeze([
  '债基风格标签',
  '样本数',
  '阈值来源',
  '综合加权得分下限',
  '近期得分下限',
  '长期得分下限',
  '1年年化收益下限',
  '1年夏普下限',
  '1年索提诺下限',
  '1年卡玛下限',
  '1年正收益率下限',
  '费率上限(%)',
  '1年最大回撤上限',
  '1年波动率上限',
  '规模下限(亿)',
]);

function buildFundSortableColumns(extraColumns = []) {
  return [...new Set([...FUND_SHARED_SORTABLE_COLUMNS, ...extraColumns])];
}

function buildFundInitialSortDirections(extraDirections = {}) {
  return {
    ...FUND_SHARED_INITIAL_SORT_DIRECTIONS,
    ...(extraDirections && typeof extraDirections === 'object' ? extraDirections : {}),
  };
}

function buildFundCoreColumns({
  leadingColumns = [],
  preScoreColumns = [],
  postScoreColumns = [],
  metricColumns = FUND_SHARED_CORE_BASE_COLUMNS.slice(4),
  trailingColumns = [],
  includeDetail = true,
} = {}) {
  const columns = [
    ...leadingColumns,
    '序号',
    '基金代码',
    '基金名称',
    ...preScoreColumns,
    '正式投资评分',
    ...postScoreColumns,
    ...(Array.isArray(metricColumns) ? metricColumns : FUND_SHARED_CORE_BASE_COLUMNS.slice(4)),
    ...trailingColumns,
  ];
  if (includeDetail) {
    columns.push('详情');
  }
  return [...new Set(columns)];
}

function buildFundMobileCardColumns({
  identityExtraColumns = [],
  postScoreColumns = [],
  metricColumns = FUND_SHARED_MOBILE_METRIC_COLUMNS,
  trailingColumns = [],
} = {}) {
  return [...new Set([
    '基金代码',
    '基金名称',
    ...identityExtraColumns,
    '正式投资评分',
    ...postScoreColumns,
    ...metricColumns,
    ...trailingColumns,
  ])];
}

function buildFundTimeDetailFields(extraFields = []) {
  return [...new Set([
    ...FUND_SHARED_TIME_DETAIL_FIELDS,
    ...(Array.isArray(extraFields) ? extraFields : []),
  ])];
}

function normalizeFundStyleLabel(value) {
  const text = String(value || '').trim();
  return text || '未标注风格';
}

function normalizeFundStyleThresholdEntries(entries) {
  return (Array.isArray(entries) ? entries : [])
    .filter((entry) => entry && typeof entry === 'object')
    .map((entry) => {
      const normalized = Object.fromEntries(
        FUND_STYLE_THRESHOLD_FIELDS.map((field) => [field, entry[field] ?? ''])
      );
      normalized['债基风格标签'] = String(normalized['债基风格标签'] || '').trim();
      normalized['阈值来源'] = String(normalized['阈值来源'] || '').trim();
      return normalized;
    })
    .filter((entry) => entry['债基风格标签']);
}

function buildFundStyleThresholdMap(entries) {
  const map = new Map();
  normalizeFundStyleThresholdEntries(entries).forEach((entry) => {
    map.set(String(entry['债基风格标签'] || '').trim(), entry);
  });
  return map;
}

function getVisibleFundStyleThresholds(rows, entries) {
  const styleMap = buildFundStyleThresholdMap(entries);
  const styleOrder = normalizeFundStyleThresholdEntries(entries)
    .map((entry) => String(entry['债基风格标签'] || '').trim())
    .filter(Boolean);
  const visibleStyleSet = new Set(
    (Array.isArray(rows) ? rows : [])
      .map((row) => normalizeFundStyleLabel(row?.['债基风格标签']))
      .filter((style) => style && style !== '未标注风格')
  );
  const orderedStyles = styleOrder.filter((style) => visibleStyleSet.has(style));
  const fallbackStyles = [...visibleStyleSet]
    .filter((style) => !orderedStyles.includes(style))
    .sort((left, right) => left.localeCompare(right, 'zh-CN'));
  return [...orderedStyles, ...fallbackStyles]
    .map((style) => styleMap.get(style))
    .filter(Boolean);
}

function formatFundStyleThresholdNumber(value, digits = 2, suffix = '') {
  if (value === null || value === undefined || value === '') return '-';
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return String(value);
  return `${numeric.toFixed(digits)}${suffix}`;
}

function buildFundStyleThresholdMetricRows(entry) {
  return [
    `评分门槛：综合≥${formatFundStyleThresholdNumber(entry['综合加权得分下限'], 2)} / 近期≥${formatFundStyleThresholdNumber(entry['近期得分下限'], 2)} / 长期≥${formatFundStyleThresholdNumber(entry['长期得分下限'], 2)}`,
    `收益风险：年化≥${formatFundStyleThresholdNumber(entry['1年年化收益下限'], 2, '%')} / 夏普≥${formatFundStyleThresholdNumber(entry['1年夏普下限'], 2)} / 索提诺≥${formatFundStyleThresholdNumber(entry['1年索提诺下限'], 2)} / 卡玛≥${formatFundStyleThresholdNumber(entry['1年卡玛下限'], 2)} / 正收益≥${formatFundStyleThresholdNumber(entry['1年正收益率下限'], 2, '%')}`,
    `约束上限：费率≤${formatFundStyleThresholdNumber(entry['费率上限(%)'], 2, '%')} / 回撤≤${formatFundStyleThresholdNumber(entry['1年最大回撤上限'], 4)} / 波动≤${formatFundStyleThresholdNumber(entry['1年波动率上限'], 4)} / 规模≥${formatFundStyleThresholdNumber(entry['规模下限(亿)'], 1)}亿`,
  ];
}

function renderFundStyleThresholdCard(entry, options = {}) {
  if (!entry || !String(entry['债基风格标签'] || '').trim()) return '';
  const style = String(entry['债基风格标签'] || '').trim();
  const source = String(entry['阈值来源'] || '').trim() || '未标注';
  const sampleSize = entry['样本数'];
  const sampleText = sampleSize === '' || sampleSize === null || sampleSize === undefined
    ? '样本数 -'
    : `样本数 ${formatNumber(sampleSize)}`;
  const rows = buildFundStyleThresholdMetricRows(entry);
  const compact = options.compact === true;
  return `
    <section class="favorite-threshold-card ${compact ? 'is-compact' : ''}">
      <div class="favorite-threshold-card-head">
        <div>
          <strong>${escapeHtml(style)}</strong>
          <span>${escapeHtml(source)}</span>
        </div>
        <div class="favorite-threshold-card-meta">
          <span>${escapeHtml(sampleText)}</span>
        </div>
      </div>
      <div class="favorite-threshold-card-body">
        ${rows.map((text) => `<p>${escapeHtml(text)}</p>`).join('')}
      </div>
    </section>
  `;
}

function renderFundStyleThresholdSummary(container, {
  rows,
  styleThresholds,
  globalStyleThreshold,
  missingStyleThresholds,
  viewMode = FUND_STYLE_VIEW_MODES.mixed,
  title = '风格动态门槛',
  description = '以下门槛来自当前最新 workflow，用于解释各风格正式投资评分与入榜标准。',
} = {}) {
  if (!container) return;
  const items = Array.isArray(rows) ? rows : [];
  const visibleThresholds = getVisibleFundStyleThresholds(items, styleThresholds);
  const globalThreshold = globalStyleThreshold && String(globalStyleThreshold['债基风格标签'] || '').trim()
    ? globalStyleThreshold
    : null;
  const missingStyles = (Array.isArray(missingStyleThresholds) ? missingStyleThresholds : [])
    .filter((style) => items.some((row) => normalizeFundStyleLabel(row?.['债基风格标签']) === String(style || '').trim()));
  if (!items.length || (!visibleThresholds.length && !globalThreshold && !missingStyles.length)) {
    container.innerHTML = '';
    return;
  }
  container.innerHTML = `
    <div class="favorite-threshold-overview-head">
      <strong>${escapeHtml(title)}</strong>
      <span>${escapeHtml(description)}</span>
    </div>
    ${viewMode === FUND_STYLE_VIEW_MODES.mixed ? `
      <div class="favorite-threshold-overview-grid">
        ${visibleThresholds.map((entry) => renderFundStyleThresholdCard(entry, { compact: true })).join('')}
      </div>
    ` : ''}
    ${missingStyles.length ? `<div class="info-banner info-banner-warning"><strong>提示</strong><span>以下风格暂无单独阈值摘要，将按全局基准理解：${escapeHtml(missingStyles.join('、'))}</span></div>` : ''}
    ${globalThreshold ? `<div class="favorite-threshold-global">${renderFundStyleThresholdCard(globalThreshold, { compact: true })}</div>` : ''}
  `;
}

function collectOrderedFundStyles(rows) {
  const items = Array.isArray(rows) ? rows : [];
  const styles = new Set();
  items.forEach((row) => {
    styles.add(normalizeFundStyleLabel(row?.['债基风格标签']));
  });
  return [...styles].sort((left, right) => left.localeCompare(right, 'zh-CN'));
}

function groupFundRowsByStyle(rows) {
  const items = Array.isArray(rows) ? rows : [];
  const map = new Map();
  items.forEach((row) => {
    const style = normalizeFundStyleLabel(row?.['债基风格标签']);
    if (!map.has(style)) map.set(style, []);
    map.get(style).push(row);
  });
  return map;
}

function renderFundStyleViewBar(container, {
  viewMode,
  densityMode = FUND_TABLE_DENSITY_MODES.core,
  compactnessMode = '',
  compactnessOptions = [],
  compactnessNotes = {},
  rows,
  onChange,
  onDensityChange,
  onCompactnessChange,
  groupedLabel = '按风格分组',
  mixedLabel = '全部混合',
  coreDensityLabel = '核心视图',
  expandedDensityLabel = '扩展视图',
  groupedNote = '建议优先在同风格内比较正式投资评分和风险收益指标。',
  mixedNote = '混合视图适合总览，但不同风格间的正式投资评分不宜直接横向比较。',
  coreDensityNote = '主表只保留核心决策列，更多字段按行展开查看。',
  expandedDensityNote = '主表直接展开更多收益、规模和出现频次字段，适合深度比对。',
} = {}) {
  if (!container) return;
  const items = Array.isArray(rows) ? rows : [];
  if (!items.length) {
    container.innerHTML = '';
    return;
  }
  const normalizedCompactnessOptions = Array.isArray(compactnessOptions)
    ? compactnessOptions
        .map((item) => ({
          value: String(item?.value || '').trim(),
          label: String(item?.label || '').trim(),
        }))
        .filter((item) => item.value && item.label)
    : [];
  const activeCompactness = normalizedCompactnessOptions.find((item) => item.value === compactnessMode) || null;
  const compactnessNote = activeCompactness
    ? String((compactnessNotes && compactnessNotes[activeCompactness.value]) || '').trim()
    : '';
  const styleCount = collectOrderedFundStyles(items)
    .filter((style) => style && style !== '未标注风格')
    .length;
  container.innerHTML = `
    <div class="favorite-view-toggle-group">
      <div class="favorite-view-toggle">
        <button
          type="button"
          class="favorite-view-toggle-btn ${viewMode === FUND_STYLE_VIEW_MODES.mixed ? 'is-active' : ''}"
          data-fund-style-view-mode="${FUND_STYLE_VIEW_MODES.mixed}"
          aria-pressed="${viewMode === FUND_STYLE_VIEW_MODES.mixed ? 'true' : 'false'}"
        >${escapeHtml(mixedLabel)}</button>
        <button
          type="button"
          class="favorite-view-toggle-btn ${viewMode === FUND_STYLE_VIEW_MODES.grouped ? 'is-active' : ''}"
          data-fund-style-view-mode="${FUND_STYLE_VIEW_MODES.grouped}"
          aria-pressed="${viewMode === FUND_STYLE_VIEW_MODES.grouped ? 'true' : 'false'}"
        >${escapeHtml(groupedLabel)}</button>
      </div>
      <div class="favorite-view-toggle">
        <button
          type="button"
          class="favorite-view-toggle-btn ${densityMode === FUND_TABLE_DENSITY_MODES.core ? 'is-active' : ''}"
          data-fund-table-density-mode="${FUND_TABLE_DENSITY_MODES.core}"
          aria-pressed="${densityMode === FUND_TABLE_DENSITY_MODES.core ? 'true' : 'false'}"
        >${escapeHtml(coreDensityLabel)}</button>
        <button
          type="button"
          class="favorite-view-toggle-btn ${densityMode === FUND_TABLE_DENSITY_MODES.expanded ? 'is-active' : ''}"
          data-fund-table-density-mode="${FUND_TABLE_DENSITY_MODES.expanded}"
          aria-pressed="${densityMode === FUND_TABLE_DENSITY_MODES.expanded ? 'true' : 'false'}"
        >${escapeHtml(expandedDensityLabel)}</button>
      </div>
      ${normalizedCompactnessOptions.length > 1 ? `
        <div class="favorite-view-toggle">
          ${normalizedCompactnessOptions.map((item) => `
            <button
              type="button"
              class="favorite-view-toggle-btn ${compactnessMode === item.value ? 'is-active' : ''}"
              data-fund-table-compactness-mode="${escapeHtml(item.value)}"
              aria-pressed="${compactnessMode === item.value ? 'true' : 'false'}"
            >${escapeHtml(item.label)}</button>
          `).join('')}
        </div>
      ` : ''}
    </div>
    <div class="favorite-view-note">
      <strong>${viewMode === FUND_STYLE_VIEW_MODES.grouped ? '当前按风格分组' : '当前为混合总表'} / ${densityMode === FUND_TABLE_DENSITY_MODES.expanded ? '扩展字段主表' : '核心字段主表'}${activeCompactness ? ` / ${activeCompactness.label}` : ''}</strong>
      <span>${escapeHtml(viewMode === FUND_STYLE_VIEW_MODES.grouped ? `共 ${formatNumber(styleCount)} 个风格，${groupedNote}` : mixedNote)}</span>
      <span>${escapeHtml(densityMode === FUND_TABLE_DENSITY_MODES.expanded ? expandedDensityNote : coreDensityNote)}</span>
      ${compactnessNote ? `<span>${escapeHtml(compactnessNote)}</span>` : ''}
    </div>
  `;
  container.querySelectorAll('[data-fund-style-view-mode]').forEach((button) => {
    button.addEventListener('click', () => {
      const nextMode = String(button.dataset.fundStyleViewMode || '').trim();
      if (!Object.values(FUND_STYLE_VIEW_MODES).includes(nextMode) || nextMode === viewMode) return;
      if (typeof onChange === 'function') onChange(nextMode);
    });
  });
  container.querySelectorAll('[data-fund-table-density-mode]').forEach((button) => {
    button.addEventListener('click', () => {
      const nextMode = String(button.dataset.fundTableDensityMode || '').trim();
      if (!Object.values(FUND_TABLE_DENSITY_MODES).includes(nextMode) || nextMode === densityMode) return;
      if (typeof onDensityChange === 'function') onDensityChange(nextMode);
    });
  });
  container.querySelectorAll('[data-fund-table-compactness-mode]').forEach((button) => {
    button.addEventListener('click', () => {
      const nextMode = String(button.dataset.fundTableCompactnessMode || '').trim();
      if (!normalizedCompactnessOptions.some((item) => item.value === nextMode) || nextMode === compactnessMode) return;
      if (typeof onCompactnessChange === 'function') onCompactnessChange(nextMode);
    });
  });
}

function renderFundStyleGroupedTables(container, rows, {
  renderTable,
  sectionClassName = '',
  showCountLabel = true,
  orderedStyles: orderedStylesInput = null,
  renderSectionSupplement = null,
} = {}) {
  if (!container) return;
  const items = Array.isArray(rows) ? rows : [];
  if (!items.length) {
    container.innerHTML = '<div class="empty-state-inline">暂无数据</div>';
    return;
  }
  const groupMap = groupFundRowsByStyle(items);
  const fallbackOrderedStyles = collectOrderedFundStyles(items);
  const preferredOrderedStyles = Array.isArray(orderedStylesInput)
    ? orderedStylesInput
        .map((style) => String(style || '').trim())
        .filter(Boolean)
    : [];
  const orderedStyles = [
    ...new Set([
      ...preferredOrderedStyles.filter((style) => groupMap.has(style)),
      ...fallbackOrderedStyles,
    ]),
  ];
  container.innerHTML = orderedStyles.map((style, index) => {
    const styleRows = groupMap.get(style) || [];
    const sectionSupplement = typeof renderSectionSupplement === 'function'
      ? renderSectionSupplement(styleRows, style)
      : '';
    return `
      <section class="favorite-style-section ${escapeHtml(sectionClassName)}" data-fund-style-section="${escapeHtml(style)}">
        <div class="favorite-style-section-head">
          <div class="favorite-style-section-title">
            <h3>${escapeHtml(style)}</h3>
            ${showCountLabel ? `<span>${escapeHtml(formatNumber(styleRows.length))} 只基金</span>` : ''}
          </div>
        </div>
        ${sectionSupplement}
        <div class="favorite-style-table" data-fund-style-table-index="${index}"></div>
      </section>
    `;
  }).join('');
  orderedStyles.forEach((style, index) => {
    const target = container.querySelector(`[data-fund-style-table-index="${index}"]`);
    if (!target || typeof renderTable !== 'function') return;
    renderTable(target, groupMap.get(style) || [], style);
  });
}

function renderSummaryHintChips(container, hints, options = {}) {
  if (!container) return;
  const items = (Array.isArray(hints) ? hints : [])
    .map((item) => String(item || '').trim())
    .filter(Boolean);
  if (!items.length) {
    container.innerHTML = '';
    return;
  }
  const itemClassName = String(options.itemClassName || 'recommendation-list-hint-chip').trim();
  container.innerHTML = items.map((text) => `
    <div class="active-filter-chip ${escapeHtml(itemClassName)}">
      <span>${escapeHtml(text)}</span>
    </div>
  `).join('');
}

function labelFromMap(value, map, fallback = '-') {
  const key = String(value ?? '').trim();
  if (!key) return fallback;
  return map[key] || key;
}

function formatWorkflowStatus(value) {
  return labelFromMap(value, STATUS_LABELS, '-');
}

function renderRunBadge(container, state) {
  if (!container) return;
  const status = String(state?.status || '').trim();
  const running = status === 'running';
  const failed = status === 'failed';
  const degraded = status === 'degraded';
  container.className = `status-badge ${failed ? 'status-badge-danger' : running || degraded ? 'status-badge-warning' : ''}`;
  container.innerHTML = `
    <span class="status-dot ${failed ? 'status-dot-danger' : running || degraded ? 'status-dot-warning' : ''}"></span>
    <span>${escapeHtml(formatWorkflowStatus(status))}</span>
  `;
}

function apiConfigurationMatchState(matches, configured = {}) {
  const keys = ['tiantian_base_url', 'autostock_base_url'];
  const validationKeys = ['publication_validation_provider', 'tiantian_valuation_primary_url',
    'tiantian_valuation_fallback_url', 'eastmoney_fund_info_url', 'autostock_enabled'];
  keys.push(...validationKeys.filter((key) => Object.hasOwn(configured || {}, key)));
  const values = keys.map((key) => (matches || {})[key]);
  if (values.some((value) => value === false)) return 'mismatch';
  return values.every((value) => value === true) ? 'consistent' : 'unknown';
}

function renderHealthBadge(container, health) {
  if (!container) return;
  const apiRuntime = health?.api_runtime && typeof health.api_runtime === 'object' ? health.api_runtime : null;
  if (!apiRuntime) {
    renderUnavailableBadge(container, '数据源配置', '未知');
    container.title = '配置证据暂不可用；未进行接口连通性探测';
    return;
  }
  const runtime = apiRuntime?.runtime || {};
  const configured = apiRuntime?.configured || {};
  const runtimeMatches = apiRuntime?.runtime_matches_config || {};
  const scheduler = apiRuntime?.scheduler || {};
  const schedulerMatches = scheduler.runtime_matches_config || {};
  const schedulerRuntime = scheduler.runtime || {};
  const tiantianBaseUrl = runtime.tiantian_base_url || '-';
  const autostockBaseUrl = runtime.autostock_base_url || '-';
  const matches = [runtimeMatches.tiantian_base_url, runtimeMatches.autostock_base_url];
  const validationKeys = ['publication_validation_provider', 'tiantian_valuation_primary_url',
    'tiantian_valuation_fallback_url', 'eastmoney_fund_info_url', 'autostock_enabled'];
  validationKeys.filter((key) => Object.hasOwn(configured, key)).forEach((key) => {
    matches.push(runtimeMatches[key]);
    if (scheduler.external_process === true) {
      matches.push(scheduler.runtime_evidence_available === true ? schedulerMatches[key] : null);
    }
  });
  if (scheduler.external_process === true) {
    matches.push(scheduler.runtime_evidence_available === true ? schedulerMatches.tiantian_base_url : null);
    matches.push(scheduler.runtime_evidence_available === true ? schedulerMatches.autostock_base_url : null);
  } else if (scheduler.external_process !== false) {
    matches.push(null);
  }
  const mismatch = matches.some((value) => value === false);
  const consistent = matches.every((value) => value === true);
  const label = mismatch ? '数据源配置待重载' : consistent ? '数据源配置一致' : '数据源配置未知';
  container.className = `status-badge ${consistent ? '' : 'status-badge-warning'}`;
  container.title = `仅核对运行配置，未进行连通性探测。Web Tiantian: ${tiantianBaseUrl} | AutoStock: ${autostockBaseUrl}`
    + `；正式校验 ${publicationValidationProviderLabel(runtime.publication_validation_provider)}；AutoStock 配置 ${configuredAutoStockStatus(configured)}`
    + (scheduler.external_process === true
      ? `；调度 Tiantian: ${schedulerRuntime.tiantian_base_url || '-'} | AutoStock: ${schedulerRuntime.autostock_base_url || '-'}；采样 ${schedulerRuntime.captured_at || '未知'}`
      : '');
  container.innerHTML = `
    <span class="status-dot ${consistent ? '' : 'status-dot-warning'}"></span>
    <span>${label}</span>
  `;
}

function renderUnavailableBadge(container, label, detail = '暂不可用') {
  if (!container) return;
  container.className = 'status-badge status-badge-warning';
  container.innerHTML = `
    <span class="status-dot status-dot-warning"></span>
    <span>${escapeHtml(label)}${detail ? `：${escapeHtml(detail)}` : ''}</span>
  `;
}

function buildStatCard(label, value, hint = '') {
  const compactHint = compactUiText(hint, 15);
  return `
    <div class="stat-card">
      <strong>${escapeHtml(label)}</strong>
      <span>${escapeHtml(value)}</span>
      ${compactHint ? `<small>${escapeHtml(compactHint)}</small>` : ''}
    </div>
  `;
}

function renderRetryableEmptyState(container, {
  title = '加载失败',
  message = '请稍后重试',
  actionLabel = '重试',
  onRetry = null,
} = {}) {
  if (!container) return;
  container.innerHTML = `
    <div class="empty-state-inline retryable-empty-state">
      <strong>${escapeHtml(title)}</strong>
      <span>${escapeHtml(message)}</span>
      <div class="retryable-empty-state-actions">
        <button type="button" class="secondary" data-empty-retry="1">${escapeHtml(actionLabel)}</button>
      </div>
    </div>
  `;
  const retryBtn = container.querySelector('[data-empty-retry="1"]');
  if (!retryBtn || typeof onRetry !== 'function') return;
  retryBtn.addEventListener('click', async () => {
    if (retryBtn.disabled) return;
    retryBtn.disabled = true;
    retryBtn.textContent = '重试中';
    try {
      await onRetry();
    } finally {
      if (retryBtn.isConnected) {
        retryBtn.disabled = false;
        retryBtn.textContent = actionLabel;
      }
    }
  });
}

function renderCell(column, value, row = null) {
  const text = String(value ?? '').trim();
  if (!text) {
    const status = row?.__risk_metric_unavailability?.[column];
    if (status?.label) {
      const explanation = [String(status.label), status.end_date ? `数据截至 ${status.end_date}` : ''].filter(Boolean).join('；');
      return `<span class="favorite-detail-value-empty" title="${escapeHtml(explanation)}" data-missing-reason="${escapeHtml(explanation)}">-</span>`;
    }
    return '<span class="favorite-detail-value-empty">-</span>';
  }
  if (column === '项目质量信心' || column === '项目质量等级') {
    const cls = text === '高信心' || text === '核心通过'
      ? 'pill-confidence-high'
      : text === '中信心' || text === '审慎通过'
        ? 'pill-confidence-mid'
        : 'pill-confidence-low';
    return `<span class="table-pill ${cls}" title="${escapeHtml(text)}">${escapeHtml(text)}</span>`;
  }
  if (column === '当前是否在榜') {
    const cls = text === '是' ? 'pill-yes' : 'pill-no';
    return `<span class="table-pill ${cls}" title="${escapeHtml(text)}">${escapeHtml(text)}</span>`;
  }
  if (column === '是否可申购') {
    const yesValues = ['True', 'true', '1', '1.0', '是', '可申购', '开放申购', '开放'];
    const noValues = ['False', 'false', '0', '0.0', '否', '不可申购', '暂停申购', '封闭'];
    const cls = yesValues.includes(text) ? 'pill-yes' : noValues.includes(text) ? 'pill-no' : 'pill-style';
    const label = yesValues.includes(text) ? '可申购' : noValues.includes(text) ? '不可申购' : text;
    return `<span class="table-pill ${cls}" title="${escapeHtml(label)}">${escapeHtml(label)}</span>`;
  }
  if (column === '债基风格标签') {
    return `<span class="table-pill pill-style" title="${escapeHtml(text)}">${escapeHtml(text)}</span>`;
  }
  if (column === '数据校验来源') {
    const label = publicationValidationProviderLabel(value);
    return `<span class="table-pill pill-style" title="${escapeHtml(label)}">${escapeHtml(label)}</span>`;
  }
  if (column === '正式榜分组' || column === '长期稳定性标签' || column === 'AutoStock原始校验状态' || column === '数据校验原始状态' || column === '数据校验状态' || column === '当前层级' || column === '建议动作') {
    return `<span class="table-pill pill-style" title="${escapeHtml(text)}">${escapeHtml(text)}</span>`;
  }
  if (column === '基金代码') {
    return `<span class="table-code">${escapeHtml(text)}</span>`;
  }
  if (column === '基金名称') {
    return `<span class="table-name">${escapeHtml(text)}</span>`;
  }
  if (column === '基金规模(亿)') {
    return `<span class="table-number" title="${escapeHtml(text)}">${escapeHtml(formatScoreMetricValue(value, 2))}</span>`;
  }
  if (column === '推荐解读') {
    return `<span class="table-note">${escapeHtml(text)}</span>`;
  }
  if (FUND_SIGNED_PERCENT_DISPLAY_FIELDS.has(column)) {
    return renderSignedPercentValue(text);
  }
  if (column === '实时估算净值' || column === '最新单位净值') {
    return `<span class="table-number">${escapeHtml(text)}</span>`;
  }
  if (isDecimalRatioPercentField(column)) {
    return `<span class="table-number">${escapeHtml(formatMetricDisplayValue(column, value))}</span>`;
  }
  if (/(评分|得分|收益|回撤|频率|规模|夏普|波动率|费率|权重|比率)/.test(column)) {
    return `<span class="table-number">${escapeHtml(text)}</span>`;
  }
  return escapeHtml(text);
}

function parseScoreMetricValue(value) {
  const text = String(value ?? '').replace(/,/gu, '').trim();
  if (!text) return null;
  const number = Number(text);
  return Number.isFinite(number) ? number : null;
}

function formatScoreMetricValue(value, digits = 2, suffix = '') {
  const numeric = parseScoreMetricValue(value);
  if (numeric === null) return '-';
  return `${numeric.toFixed(digits)}${suffix}`;
}

function isDecimalRatioPercentField(column) {
  const normalized = String(column || '').trim();
  const metadata = getMetricUnitMetadata(normalized);
  if (metadata) {
    return String(metadata.raw_unit || '').trim() === 'decimal_ratio'
      && String(metadata.display_unit || '').trim() === 'percent_point';
  }
  return FUND_DECIMAL_RATIO_PERCENT_FIELDS.has(normalized);
}

function parseMetricDisplayNumber(value) {
  let text = String(value ?? '').replace(/,/gu, '').trim();
  if (!text) return null;
  if (text.endsWith('%')) {
    text = text.slice(0, -1).trim();
  }
  const number = Number(text);
  return Number.isFinite(number) ? number : null;
}

function formatMetricDisplayValue(column, value, digits = null, suffix = '') {
  const text = String(value ?? '').trim();
  if (!text) return '-';
  const numeric = parseMetricDisplayNumber(value);
  if (numeric === null) return text;
  const metadata = getMetricUnitMetadata(column) || {};
  const alreadyPercentText = text.endsWith('%');
  const displayMultiplier = numericValueOrNull(metadata.display_multiplier);
  const displayNumber = isDecimalRatioPercentField(column) && !alreadyPercentText
    ? numeric * (displayMultiplier ?? 100)
    : numeric;
  const displayDigits = numericValueOrNull(digits) ?? numericValueOrNull(metadata.display_digits) ?? 2;
  const displaySuffix = suffix || String(metadata.display_suffix || '').trim() || (isDecimalRatioPercentField(column) ? '%' : '');
  return `${displayNumber.toFixed(Math.max(0, Math.floor(displayDigits)))}${displaySuffix}`;
}

function formatStyleMetricReferenceValue(field, value, suffix = '') {
  const text = String(value ?? '').trim();
  if (!text) return '';
  if (isDecimalRatioPercentField(field)) {
    return formatMetricDisplayValue(field, value);
  }
  return `${text}${suffix}`;
}

function getStyleMetricReferences(row) {
  const payload = row?.['__style_metric_references'];
  return payload && typeof payload === 'object' ? payload : {};
}

function getStyleMetricReferenceMetric(row, field) {
  const payload = getStyleMetricReferences(row);
  const metrics = payload?.metrics && typeof payload.metrics === 'object' ? payload.metrics : {};
  const item = metrics?.[field];
  return item && typeof item === 'object' ? item : {};
}

function buildStyleMetricRankBadgeMarkup(row, options = {}) {
  const targetField = String(options.field || '正式投资评分').trim() || '正式投资评分';
  const referenceMetric = getStyleMetricReferenceMetric(row, targetField);
  const statusLabel = String(referenceMetric?.status_label || '').trim();
  if (!statusLabel) return '';
  const prefix = String(options.prefix || '').trim();
  const compactText = String(options.compactText || '').trim()
    || `${prefix ? `${prefix} ` : ''}${statusLabel}`;
  const scopeLabel = String(referenceMetric?.reference_label || '').trim();
  const suffix = String(referenceMetric?.suffix || '').trim();
  const currentDisplay = formatStyleMetricReferenceValue(targetField, referenceMetric?.current, suffix);
  const baselineDisplay = formatStyleMetricReferenceValue(targetField, referenceMetric?.baseline, suffix);
  const titleParts = [
    scopeLabel,
    baselineDisplay ? `基准 ${baselineDisplay}` : '',
    currentDisplay ? `当前 ${currentDisplay}` : '',
    statusLabel,
  ].filter(Boolean);
  const badgeClass = [
    'favorite-style-rank-badge',
    statusLabel === '优于基准' ? 'is-strong' : '',
    statusLabel === '弱于基准' ? 'is-weak' : '',
    String(referenceMetric?.reference_source || '').trim() === 'tracker_median' ? 'is-fallback' : '',
  ].filter(Boolean).join(' ');
  return `<small class="${escapeHtml(badgeClass)}" title="${escapeHtml(titleParts.join(' · '))}">${escapeHtml(compactText)}</small>`;
}

function getStyleMetricReferenceTitle(field, referenceMetric = {}) {
  const normalizedField = String(field || '').trim();
  const lowerIsBetter = referenceMetric?.lower_is_better === true;
  if (normalizedField === '正式投资评分') return '正式评分参考线';
  if (normalizedField === '平台成立以来收益年化(%)') return '长期收益参考线';
  if (lowerIsBetter) return '风险参考线上限';
  return '收益参考线';
}

function getStyleMetricReferenceDescription(field, referenceMetric = {}) {
  const normalizedField = String(field || '').trim();
  const referenceSource = String(referenceMetric?.reference_source || '').trim();
  const lowerIsBetter = referenceMetric?.lower_is_better === true;
  if (normalizedField === '正式投资评分') {
    return '用于判断当前正式投资评分是否达到该风格的项目质量门槛。';
  }
  if (normalizedField === '平台成立以来收益年化(%)') {
    if (referenceSource === 'tracker_median') {
      return '当前没有独立风格门槛，暂以同类真实样本中位值作为长期收益参考线。';
    }
    return '用于观察该基金长期年化收益是否达到当前参考线。';
  }
  if (lowerIsBetter) {
    return '当前值越低越好，低于该上限代表风险控制更稳。';
  }
  return '当前值越高越好，高于该参考线代表表现更强。';
}

function buildVisibleStyleMetricHeaderReferenceMap(rows, fields = []) {
  const items = Array.isArray(rows) ? rows : [];
  const targetFields = (Array.isArray(fields) ? fields : [])
    .map((field) => String(field || '').trim())
    .filter(Boolean);
  const result = {};
  targetFields.forEach((field) => {
    const seen = new Map();
    items.forEach((row) => {
      const referenceMetric = getStyleMetricReferenceMetric(row, field);
      const baseline = String(referenceMetric?.baseline || '').trim();
      if (!baseline) return;
      const referenceLabel = String(referenceMetric?.reference_label || '').trim();
      const suffix = String(referenceMetric?.suffix || '').trim();
      const metricLabel = String(referenceMetric?.metric_label || field).trim() || field;
      const lowerIsBetter = referenceMetric?.lower_is_better === true;
      const style = normalizeFundStyleLabel(row?.['债基风格标签'] || getStyleMetricReferences(row)?.style_label || '');
      const key = `${style}::${baseline}::${suffix}::${referenceLabel}`;
      if (!seen.has(key)) {
        seen.set(key, {
          style,
          baseline,
          suffix,
          metricLabel,
          lowerIsBetter,
          referenceLabel,
          referenceSource: String(referenceMetric?.reference_source || '').trim(),
        });
      }
    });
    const entries = [...seen.values()];
    if (!entries.length) return;
    const numericEntries = entries
      .map((entry) => ({
        ...entry,
        numericBaseline: Number(String(entry.baseline || '').replace(/,/g, '').trim()),
      }))
      .filter((entry) => Number.isFinite(entry.numericBaseline));
    const sortedEntries = numericEntries.length === entries.length
      ? numericEntries.sort((left, right) => left.numericBaseline - right.numericBaseline)
      : entries;
    const first = sortedEntries[0];
    const last = sortedEntries[sortedEntries.length - 1];
    result[field] = {
      field,
      metricLabel: first.metricLabel,
      lowerIsBetter: first.lowerIsBetter,
      entryCount: sortedEntries.length,
      entries: sortedEntries,
      firstBaseline: `${first.baseline}${first.suffix || ''}`,
      lastBaseline: `${last.baseline}${last.suffix || ''}`,
    };
  });
  return result;
}

function getStyleMetricHeaderReferenceTitle(field, referenceSummary) {
  const normalizedField = String(field || '').trim();
  if (!referenceSummary || typeof referenceSummary !== 'object') return '';
  if (normalizedField === '正式投资评分') return '正式评分参考线';
  if (normalizedField === '平台成立以来收益年化(%)') return '长期收益参考线';
  if (normalizedField === '近1周(%)') return '近1周收益参考线';
  if (normalizedField === '近1月(%)') return '近1月收益参考线';
  if (normalizedField === '近3月(%)') return '近3月收益参考线';
  return referenceSummary.lowerIsBetter ? '风险参考线上限' : '收益参考线';
}

function formatStyleMetricHeaderReferenceSummary(referenceSummary, options = {}) {
  if (!referenceSummary || !Array.isArray(referenceSummary.entries) || !referenceSummary.entries.length) return '';
  const allowMixedRange = options.allowMixedRange === true;
  const entries = referenceSummary.entries;
  const firstValue = referenceSummary.firstBaseline || '';
  const lastValue = referenceSummary.lastBaseline || '';
  if (entries.length === 1 || firstValue === lastValue) {
    return referenceSummary.lowerIsBetter ? `<= ${firstValue}` : firstValue;
  }
  if (!allowMixedRange) return '按风格';
  return `${firstValue}~${lastValue}`;
}

function buildStyleMetricHeaderReferenceTitleText(field, referenceSummary, options = {}) {
  if (!referenceSummary || !Array.isArray(referenceSummary.entries) || !referenceSummary.entries.length) return '';
  const title = getStyleMetricHeaderReferenceTitle(field, referenceSummary);
  const allowMixedRange = options.allowMixedRange === true;
  const lines = [
    `${String(field || '').trim()} / ${title}`,
    referenceSummary.entryCount === 1
      ? '当前可见列表只包含一个参考口径，表头直接展示该真实参考值。'
      : allowMixedRange
        ? '当前可见列表包含多个风格，表头展示真实参考区间；以下为各风格参考值。'
        : '当前可见列表包含多个风格，第二行仅提示按风格；以下为各风格真实参考值。',
    ...referenceSummary.entries.map((entry) => {
      const parts = [
        entry.style,
        `${title} ${entry.baseline}${entry.suffix || ''}`,
        entry.referenceLabel || '',
      ].filter(Boolean);
      return parts.join(' | ');
    }),
  ].filter(Boolean);
  return lines.join('\n');
}

function buildStyleMetricHeaderReferenceMarkup(column, referenceSummaryMap = {}, options = {}) {
  const referenceSummary = referenceSummaryMap?.[column];
  const displayLabel = typeof options.labelResolver === 'function'
    ? String(options.labelResolver(column) || '').trim()
    : '';
  const label = displayLabel || String(column || '').trim();
  if (!referenceSummary) {
    return `
      <span class="table-header-stack" title="${escapeHtml(String(column || '').trim())}">
        <span class="table-header-title">${escapeHtml(label)}</span>
      </span>
    `;
  }
  const titleText = buildStyleMetricHeaderReferenceTitleText(column, referenceSummary, options);
  return `
    <span class="table-header-stack" title="${escapeHtml(titleText)}">
      <span class="table-header-title">${escapeHtml(label)}</span>
      <small class="table-header-subtitle">${escapeHtml(formatStyleMetricHeaderReferenceSummary(referenceSummary, options))}</small>
    </span>
  `;
}

function buildStyleMetricReferenceSummaryMarkup(row, metricFields = []) {
  const referencePayload = getStyleMetricReferences(row);
  const styleLabel = String(referencePayload?.style_label || row?.['债基风格标签'] || '').trim();
  const metrics = referencePayload?.metrics && typeof referencePayload.metrics === 'object'
    ? referencePayload.metrics
    : {};
  const fields = Array.isArray(metricFields) ? metricFields : [];
  const cards = fields.map((field) => {
    const item = metrics?.[field];
    if (!item || typeof item !== 'object') return '';
    const metricLabel = String(item.metric_label || field).trim() || field;
    const suffix = String(item.suffix || '').trim();
    const current = String(item.current || '').trim();
    const baseline = String(item.baseline || '').trim();
    const delta = String(item.baseline_delta || '').trim();
    const statusLabel = String(item.status_label || '').trim();
    const referenceLabel = String(item.reference_label || '').trim();
    const title = getStyleMetricReferenceTitle(field, item);
    const description = getStyleMetricReferenceDescription(field, item);
    const baselineLead = title.includes('上限') ? title : title;
    if (!current || !baseline || !statusLabel) return '';
    const currentDisplay = formatStyleMetricReferenceValue(field, current, suffix);
    const baselineDisplay = formatStyleMetricReferenceValue(field, baseline, suffix);
    const deltaDisplay = delta ? formatStyleMetricReferenceValue(field, delta, suffix) : '';
    return `
      <div class="favorite-style-benchmark-card">
        <strong>${escapeHtml(`${metricLabel} · ${title}`)}</strong>
        <span>${escapeHtml(`当前 ${currentDisplay} / ${baselineLead} ${baselineDisplay}`)}</span>
        <em>${escapeHtml(`${statusLabel}${deltaDisplay ? ` · 偏离 ${deltaDisplay}` : ''}`)}</em>
        ${description ? `<small>${escapeHtml(description)}</small>` : ''}
        ${referenceLabel ? `<small>${escapeHtml(referenceLabel)}</small>` : ''}
      </div>
    `;
  }).filter(Boolean).join('');
  if (!cards) return '';
  const hasFallbackMedian = fields.some((field) => String((metrics?.[field] || {}).reference_source || '').trim() === 'tracker_median');
  const lowSampleNotice = hasFallbackMedian
    ? `
      <p class="favorite-style-benchmark-note">
        部分指标当前缺少独立风格门槛，已回退为真实样本统计基准，建议同时结合风格阈值判断。
      </p>
    `
    : '';
  return `
    <div class="favorite-style-benchmark-block">
      <p><strong>统一数据基准${styleLabel ? `：${escapeHtml(styleLabel)}` : ''}</strong></p>
      ${lowSampleNotice}
      <div class="favorite-style-benchmark-grid">${cards}</div>
    </div>
  `;
}

function buildSinceInceptionAnnualizedTooltipMarkup(display, items, options = {}) {
  const validItems = Array.isArray(items) ? items : [];
  const emptyText = String(options.emptyText || '暂无完整年度收益明细').trim() || '暂无完整年度收益明细';
  const historyMarkup = validItems.length
    ? `
      <div class="favorite-annualized-tooltip-history">
        ${validItems.map((item) => {
          const year = String(item.year || '').trim();
          const annualReturn = String(item.annual_return || '').trim();
          const rank = Number(item.rank_in_category);
          const total = Number(item.category_count);
          const percentile = String(item.percentile || '').trim();
          const partial = Boolean(item.is_partial_year);
          const returnText = annualReturn ? `${annualReturn}%` : '未形成完整年度收益';
          const rankText = Number.isFinite(rank) && Number.isFinite(total) && total > 0
            ? `同类 ${formatNumber(rank)} / ${formatNumber(total)}`
            : '';
          const percentileText = percentile ? `前 ${percentile}%` : '';
          return `
            <div class="favorite-annualized-tooltip-item">
              <div class="favorite-annualized-tooltip-item-head">
                <strong>${escapeHtml(`${year}年`)}</strong>
                ${partial ? '<span class="favorite-annualized-tooltip-badge">成立当年</span>' : ''}
              </div>
              <div class="favorite-annualized-tooltip-item-body">
                <span>${escapeHtml(returnText)}</span>
                ${(rankText || percentileText) ? `<em>${escapeHtml([rankText, percentileText].filter(Boolean).join(' / '))}</em>` : ''}
              </div>
            </div>
          `;
        }).join('')}
      </div>
    `
    : `<div class="favorite-annualized-tooltip-empty">${escapeHtml(emptyText)}</div>`;
  return `
    <p><strong>平台成立以来收益年化 ${escapeHtml(display)}%</strong></p>
    <p>平台成立以来累计收益 SYL_LN 按成立日至公布净值日的自然日数年化：100×[(1+SYL_LN/100)^(365.25/天数)−1]。累计净值比年化另列，二者口径不同；平台数据尚待独立来源核验。</p>
    <p>累计净值比年化：${escapeHtml(String(options.accumNavRatio ?? '').trim() ? `${options.accumNavRatio}%` : '暂无数据')}。按首末累计净值比和实际区间天数计算，不代表分红再投资总回报。</p>
    ${historyMarkup}
  `;
}

function buildFavoritePeriodicReturnTooltipMarkup({
  title = '',
  description = '',
  records = [],
  totalVolatilityPct = '',
  volatilityLabel = '总波动率',
  emptyText = '暂无明细',
  benchmarkMarkup = '',
} = {}) {
  const normalizedTitle = String(title || '').trim() || '-';
  const normalizedDescription = String(description || '').trim();
  const normalizedVolatility = String(totalVolatilityPct || '').trim();
  const detailRecords = Array.isArray(records) ? records : [];
  const historyMarkup = detailRecords.length
    ? `
      <div class="favorite-annualized-tooltip-history favorite-periodic-tooltip-history">
        ${detailRecords.map((item) => `
          <div class="favorite-annualized-tooltip-item favorite-periodic-tooltip-item">
            <div class="favorite-annualized-tooltip-item-head">
              <strong>${escapeHtml(String(item?.label || '').trim() || '-')}</strong>
            </div>
            <div class="favorite-annualized-tooltip-item-body">
              <span>${escapeHtml(formatSignedPercentOrDash(item?.return_pct))}</span>
              <em>${escapeHtml([String(item?.period_start || '').trim(), String(item?.period_end || '').trim()].filter(Boolean).join(' ~ ') || '-')}</em>
            </div>
          </div>
        `).join('')}
      </div>
    `
    : `<div class="favorite-annualized-tooltip-empty">${escapeHtml(emptyText)}</div>`;
  const volatilityMarkup = normalizedVolatility
    ? `
      <div class="favorite-score-tooltip-metrics favorite-periodic-tooltip-metrics">
        <span>
          <strong>${escapeHtml(volatilityLabel)}</strong>
          <em>${escapeHtml(`${normalizedVolatility}%`)}</em>
        </span>
      </div>
    `
    : '';
  return `
    <p><strong>${escapeHtml(normalizedTitle)}</strong></p>
    ${normalizedDescription ? `<p>${escapeHtml(normalizedDescription)}</p>` : ''}
    ${volatilityMarkup}
    ${historyMarkup}
    ${String(benchmarkMarkup || '').trim()}
  `;
}

function buildSinceInceptionAnnualizedTooltipMarkupWithVolatility(display, items, annualVolatility = '', options = {}) {
  const baseMarkup = buildSinceInceptionAnnualizedTooltipMarkup(display, items, options);
  const normalizedVolatility = String(annualVolatility || '').trim();
  if (!normalizedVolatility) return baseMarkup;
  return baseMarkup.replace(
    '</p>',
    `；完整年度收益波动率 ${escapeHtml(normalizedVolatility)}%。</p>`
  );
}

function buildRealtimeValuationTooltipMarkup(row = {}, options = {}) {
  const columnLabel = String(options.columnLabel || '实时估值').trim() || '实时估值';
  const showEstimatedNav = options.showEstimatedNav === true;
  const showLatestNav = options.showLatestNav === true;
  const primaryDisplayField = String(options.primaryDisplayField || 'estimated_change_pct').trim();
  const snapshotComparisonLabel = String(options.snapshotComparisonLabel || '').trim();
  const valuationState = String(options.valuationState || row?.__valuation_state || '').trim();
  const valuationStatusLabel = String(options.valuationStatusLabel || row?.__valuation_status_label || '').trim();
  const valuationReason = String(options.valuationReason || row?.__valuation_reason || '').trim();
  const valuationText = String(row?.['实时估值'] ?? '').trim();
  const estimatedNavText = String(row?.['实时估算净值'] ?? '').trim();
  const latestNavText = String(row?.['最新单位净值'] ?? '').trim();
  const valuationTimeText = String(row?.['实时估值时间'] ?? '').trim();
  const navDateText = String(row?.['最新净值日期'] ?? '').trim();
  const changePctDisplay = valuationText ? formatSignedPercentText(valuationText) : '-';
  const primaryValue = primaryDisplayField === 'estimated_nav'
    ? (estimatedNavText || '-')
    : (valuationText ? changePctDisplay : (valuationStatusLabel || '-'));
  const primaryMetricLabel = primaryDisplayField === 'estimated_nav' ? '实时估算净值' : columnLabel;
  const timeDisplay = valuationTimeText ? formatDateTime(valuationTimeText) : '-';
  const navDateDisplay = navDateText || '-';
  const detailRows = [
    `<div class="favorite-score-tooltip-metrics"><span>${escapeHtml(primaryMetricLabel)}</span><strong>${escapeHtml(primaryValue)}</strong></div>`,
    `<div class="favorite-score-tooltip-metrics"><span>估算涨跌幅</span><strong>${escapeHtml(changePctDisplay)}</strong></div>`,
    `<div class="favorite-score-tooltip-metrics"><span>估值时间</span><strong>${escapeHtml(timeDisplay)}</strong></div>`,
  ];
  if (showEstimatedNav && primaryDisplayField !== 'estimated_nav') {
    detailRows.push(`<div class="favorite-score-tooltip-metrics"><span>实时估算净值</span><strong>${escapeHtml(estimatedNavText || '-')}</strong></div>`);
  }
  if (showLatestNav) {
    detailRows.push(`<div class="favorite-score-tooltip-metrics"><span>最新单位净值</span><strong>${escapeHtml(latestNavText || '-')}</strong></div>`);
    detailRows.push(`<div class="favorite-score-tooltip-metrics"><span>最新净值日期</span><strong>${escapeHtml(navDateDisplay)}</strong></div>`);
  }
  if (valuationStatusLabel) {
    detailRows.push(`<div class="favorite-score-tooltip-metrics"><span>估值状态</span><strong>${escapeHtml(valuationStatusLabel)}</strong></div>`);
  }
  const descriptionParts = [
    valuationState === 'not_requested'
      ? '当前尚未向天天基金请求盘中实时估值；页面不会用最新单位净值、数据库旧值或伪 0 代替实时估值。'
      : valuationState === 'missing'
        ? '天天基金当前未为该基金提供盘中估值，系统保留缺失语义，不用最新单位净值或伪 0 填充。'
      : valuationState === 'loading'
        ? '正在向天天基金请求盘中实时估值；正式收益列仍按快照口径展示。'
        : valuationState === 'unresolved'
          ? (valuationReason
            ? '本次已取得来源记录，但估值未通过字段、时间或净值基准校验；系统保留缺失语义。'
            : '已发起实时估值请求，但本次响应未返回该基金的估值记录；系统保留缺失语义。')
        : valuationState === 'failed'
          ? '本次实时估值请求失败，系统保留缺失语义，正式收益列仍按快照口径展示。'
          : valuationState === 'quota_unavailable'
            ? '当前实时估值请求受配额保护或配额不可用限制，系统保留缺失语义。'
            : primaryDisplayField === 'estimated_nav'
              ? '该列主值为天天基金返回的盘中实时估算净值；估算涨跌幅与估值时间作为同源辅助信息展示。'
              : showEstimatedNav || showLatestNav
                ? '该列主值为盘中估算涨跌幅，用于快速观察当日变化；净值类字段仅作为补充参考。'
                : '该列展示盘中估算涨跌幅，用于快速观察当日变化，不等同于基金单位净值本体。',
  ];
  if (valuationReason && valuationState !== 'ready') {
    descriptionParts.push(`上游状态：${valuationReason}。`);
  }
  if (snapshotComparisonLabel) {
    descriptionParts.push(`正式收益列与统一数据基准按${snapshotComparisonLabel}口径比较，实时估值不参与该比较。`);
  }
  const description = descriptionParts.join('');
  return `
    <p><strong>${escapeHtml(columnLabel)} ${escapeHtml(primaryValue)}</strong></p>
    <p>${escapeHtml(description)}</p>
    ${detailRows.join('')}
  `;
}

function renderRealtimeValuationTooltipCell(value, row, options = {}) {
  const display = String(value ?? '').trim();
  const fallbackText = String(options.emptyText ?? '').trim();
  const showEstimatedNav = options.showEstimatedNav === true;
  const showLatestNav = options.showLatestNav === true;
  const primaryDisplayField = String(options.primaryDisplayField || 'estimated_change_pct').trim();
  const explicitValuationState = String(options.valuationState || row?.__valuation_state || '').trim();
  const valuationStatusLabel = String(options.valuationStatusLabel || row?.__valuation_status_label || '').trim();
  const valuationReason = String(options.valuationReason || row?.__valuation_reason || '').trim();
  const estimatedNavText = String(row?.['实时估算净值'] ?? '').trim();
  const normalizedDisplay = display.replace(/,/g, '').replace(/%$/u, '').trim();
  const numericDisplay = Number(normalizedDisplay);
  const hasNumericDisplay = Number.isFinite(numericDisplay);
  const hasSupplement = Boolean(
    estimatedNavText
    || String(row?.['最新单位净值'] ?? '').trim()
    || String(row?.['实时估值时间'] ?? '').trim()
    || String(row?.['最新净值日期'] ?? '').trim()
  );
  const changePctText = display
    ? (hasNumericDisplay ? formatSignedPercentText(display) : display)
    : (fallbackText || valuationStatusLabel || '-');
  const buttonText = primaryDisplayField === 'estimated_nav' && estimatedNavText
    ? estimatedNavText
    : changePctText;
  const secondaryText = primaryDisplayField === 'estimated_nav'
    ? (estimatedNavText && display ? (hasNumericDisplay ? formatSignedPercentText(display) : display) : '')
    : (showEstimatedNav && estimatedNavText ? `估算净值 ${estimatedNavText}` : '');
  const statusText = !display && valuationStatusLabel && valuationStatusLabel !== buttonText
    ? valuationStatusLabel
    : '';
  const changeClass = display
    ? (hasNumericDisplay ? (numericDisplay > 0 ? 'num-pos' : numericDisplay < 0 ? 'num-neg' : 'num-zero') : '')
    : '';
  const primaryValueClass = primaryDisplayField === 'estimated_nav' ? '' : changeClass;
  const secondaryClass = primaryDisplayField === 'estimated_nav' ? changeClass : 'favorite-realtime-estimated-nav';
  if (!display && !hasSupplement && !explicitValuationState && !fallbackText) {
    return '<span class="favorite-detail-value-empty">-</span>';
  }
  const buttonClass = display
    ? `score-tooltip-trigger favorite-score-tooltip-trigger ${hasNumericDisplay ? (numericDisplay > 0 ? 'num-pos' : numericDisplay < 0 ? 'num-neg' : 'num-zero') : ''}`.trim()
    : 'score-tooltip-trigger favorite-score-tooltip-trigger';
  const stateClass = !display && explicitValuationState ? ` is-valuation-${explicitValuationState.replace(/[^a-z0-9_-]/gi, '-')}` : '';
  return `
    <div class="score-tooltip favorite-score-tooltip favorite-realtime-valuation-tooltip-anchor${escapeHtml(stateClass)}" tabindex="0">
      <button type="button" class="${escapeHtml(buttonClass)}" aria-controls="globalFloatingTooltip" aria-expanded="false">
        <span class="favorite-realtime-value-stack">
          <span class="favorite-realtime-nav-value ${escapeHtml(primaryValueClass)}">${escapeHtml(buttonText)}</span>
          ${statusText ? `<small class="favorite-realtime-status">${escapeHtml(statusText)}</small>` : ''}
          ${secondaryText ? `<small class="${escapeHtml(secondaryClass)}">${escapeHtml(secondaryText)}</small>` : ''}
        </span>
      </button>
      <div class="score-tooltip-card favorite-score-tooltip-card favorite-realtime-valuation-tooltip-card" role="tooltip">
        ${buildRealtimeValuationTooltipMarkup(row, {
          columnLabel: options.columnLabel || '实时估值',
          showEstimatedNav,
          showLatestNav,
          primaryDisplayField,
          snapshotComparisonLabel: options.snapshotComparisonLabel || '',
          valuationState: explicitValuationState,
          valuationStatusLabel,
          valuationReason,
        })}
      </div>
    </div>
  `;
}

function renderSinceInceptionAnnualizedTooltipCell(value, row) {
  const display = String(value ?? '').trim();
  if (!display) {
    return '<span class="favorite-detail-value-empty" title="平台成立以来收益或成立日期不足，暂无法计算；旧快照未区分口径时保持缺失">暂无数据</span>';
  }
  const code = String(row?.['基金代码'] || '').trim();
  const items = Array.isArray(row?.['成立以来年度收益明细']) ? row['成立以来年度收益明细'] : [];
  const validItems = items.filter((item) => item && String(item.year || '').trim());
  const annualVolatility = String(row?.['__favorite_since_inception_annual_return_volatility_pct'] || '').trim();
  const needsLazyDetail = Boolean(code && !validItems.length);
  const buttonMarkup = annualVolatility
    ? `
      <span class="favorite-tooltip-trigger-stack">
        <span>${escapeHtml(display)}</span>
        <small>年波动 ${escapeHtml(annualVolatility)}%</small>
      </span>
    `
    : escapeHtml(display);
  return `
    <div
      class="score-tooltip favorite-score-tooltip favorite-annualized-tooltip-anchor"
      tabindex="0"
      data-favorite-annual-detail-code="${escapeHtml(code)}"
      data-favorite-annual-display="${escapeHtml(display)}"
      data-favorite-annual-accum-nav="${escapeHtml(String(row?.['累计净值比年化(%)'] ?? ''))}"
      data-favorite-annual-volatility="${escapeHtml(annualVolatility)}"
      data-favorite-annual-detail-loaded="${needsLazyDetail ? 'false' : 'true'}"
    >
      <button type="button" class="score-tooltip-trigger favorite-score-tooltip-trigger" aria-controls="globalFloatingTooltip" aria-expanded="false">
        ${buttonMarkup}
      </button>
      <div class="score-tooltip-card favorite-score-tooltip-card favorite-annualized-tooltip-card" role="tooltip">
        ${buildSinceInceptionAnnualizedTooltipMarkupWithVolatility(display, validItems, annualVolatility, {
          emptyText: needsLazyDetail ? '正在加载真实年度收益明细…' : '暂无完整年度收益明细',
          accumNavRatio: row?.['累计净值比年化(%)'],
        })}
        ${buildStyleMetricReferenceSummaryMarkup(row, ['平台成立以来收益年化(%)', '1年-波动率'])}
      </div>
    </div>
  `;
}

function buildBenchmarkAwarePeriodicMetricDescription(field, display, options = {}) {
  const normalizedField = String(field || '').trim();
  const formattedDisplay = formatSignedPercentOrDash(display);
  const snapshotLabel = String(options.snapshotLabel || '正式快照').trim() || '正式快照';
  const referenceMetric = getStyleMetricReferenceMetric(options.row || {}, normalizedField);
  const statusLabel = String(referenceMetric?.status_label || '').trim();
  const baseline = String(referenceMetric?.baseline || '').trim();
  const suffix = String(referenceMetric?.suffix || '').trim();
  const referenceLabel = String(referenceMetric?.reference_label || '').trim();
  const parts = [`当前主表展示${snapshotLabel}值 ${formattedDisplay}。`];
  if (statusLabel && baseline) {
    parts.push(`同风格统一数据基准为 ${baseline}${suffix}，当前结果${statusLabel}。`);
  }
  if (referenceLabel) {
    parts.push(`参考线来源：${referenceLabel}。`);
  }
  const appendix = String(options.appendix || '').trim();
  if (appendix) {
    parts.push(appendix);
  }
  return parts.join('');
}

function renderBenchmarkAwareReturnTooltipCell(value, row, options = {}) {
  const display = String(value || '').trim();
  if (!display) {
    return '<span class="favorite-detail-value-empty">-</span>';
  }
  const field = String(options.field || '').trim();
  const title = String(options.title || '').trim() || `${field} ${formatSignedPercentText(display)}`;
  const description = buildBenchmarkAwarePeriodicMetricDescription(field, display, {
    row,
    snapshotLabel: options.snapshotLabel || '正式快照',
    appendix: options.description || '',
  });
  const metricFields = [field, ...(Array.isArray(options.referenceFields) ? options.referenceFields : [])]
    .map((item) => String(item || '').trim())
    .filter(Boolean);
  const benchmarkMarkup = buildStyleMetricReferenceSummaryMarkup(row, [...new Set(metricFields)]);
  return `
    <div class="score-tooltip favorite-score-tooltip favorite-periodic-tooltip-anchor" tabindex="0">
      <button type="button" class="score-tooltip-trigger favorite-score-tooltip-trigger" aria-controls="globalFloatingTooltip" aria-expanded="false">
        ${escapeHtml(formatSignedPercentText(display))}
      </button>
      <div class="score-tooltip-card favorite-score-tooltip-card favorite-periodic-tooltip-card" role="tooltip">
        <p><strong>${escapeHtml(title)}</strong></p>
        <p>${escapeHtml(description)}</p>
        ${benchmarkMarkup}
      </div>
    </div>
  `;
}

function renderRecommendationInfoDetailSection(row, helpers, options = {}) {
  const title = String(options.title || FUND_DETAIL_SECTION_TITLES.recommendation).trim() || FUND_DETAIL_SECTION_TITLES.recommendation;
  const includeDigest = options.includeDigest !== false;
  const includeReason = options.includeReason !== false;
  const includeRisk = options.includeRisk !== false;
  const items = [];

  if (includeDigest) {
    const text = String(row?.['推荐解读'] || '').trim();
    items.push({
      label: '推荐解读',
      content: text ? helpers.renderCell('推荐解读', row['推荐解读']) : '-',
    });
  }
  if (includeReason) {
    const text = String(row?.['入选原因'] || '').trim();
    items.push({
      label: '入选原因',
      content: text ? escapeHtml(text) : '-',
    });
  }
  if (includeRisk) {
    const text = String(row?.['风险提示'] || '').trim();
    items.push({
      label: '风险提示',
      content: text ? escapeHtml(text) : '-',
    });
  }

  return `
    <section class="favorite-detail-section favorite-detail-section-note">
      <h4>${escapeHtml(title)}</h4>
      <div class="favorite-detail-grid compact-note-grid">
        ${items.map((item) => `
          <div class="favorite-detail-item compact-note-card">
            <strong>${escapeHtml(item.label)}</strong>
            <span>${item.content}</span>
          </div>
        `).join('')}
      </div>
    </section>
  `;
}

function buildFormalScoreObservationTags(row) {
  const tags = [];
  const annual = parseScoreMetricValue(row?.['1年-年化收益(%)']);
  const sinceInception = parseScoreMetricValue(row?.['平台成立以来收益年化(%)']);
  const sharpe = parseScoreMetricValue(row?.['1年-夏普比率']);
  const drawdown = parseScoreMetricValue(row?.['1年-最大回撤(%)']);
  const volatility = parseScoreMetricValue(row?.['1年-波动率']);
  const fee = parseScoreMetricValue(row?.['费率合计(%)']);
  const scale = parseScoreMetricValue(row?.['基金规模(亿)']);
  const formalScore = parseScoreMetricValue(row?.['正式投资评分']);
  const confidence = String(row?.['项目质量信心'] || '').trim();
  const stability = String(row?.['长期稳定性标签'] || '').trim();
  const validation = fundValidationContract(row);
  const holding = String(row?.['持有期状态'] || '').trim();

  if (formalScore !== null) {
    if (formalScore >= 90) tags.push('正式评分处于高分段');
    else if (formalScore >= 85) tags.push('正式评分处于优势区间');
    else if (formalScore < 75) tags.push('正式评分低于优先区间');
  }
  if (annual !== null) {
    if (annual >= 6) tags.push(`${FUND_COPYWRITING.oneYearAnnualizedStrongHintPrefix} ${annual.toFixed(2)}%`);
    else if (annual >= 2) tags.push(`${FUND_COPYWRITING.oneYearAnnualizedSteadyHintPrefix} ${annual.toFixed(2)}%`);
    else if (annual < 1.5) tags.push(`${FUND_COPYWRITING.oneYearAnnualizedWeakHintPrefix} ${annual.toFixed(2)}%`);
  }
  if (sinceInception !== null) {
    if (sinceInception >= 5) tags.push(`平台成立年化较高 ${sinceInception.toFixed(2)}%`);
    else if (sinceInception >= 2.5) tags.push(`平台成立年化稳健 ${sinceInception.toFixed(2)}%`);
  }
  if (sharpe !== null) {
    if (sharpe >= 3) tags.push(`夏普较高 ${sharpe.toFixed(2)}`);
    else if (sharpe < 1.5) tags.push(`夏普偏低 ${sharpe.toFixed(2)}`);
  }
  if (drawdown !== null) {
    const drawdownDisplay = formatMetricDisplayValue('1年-最大回撤(%)', drawdown);
    if (drawdown <= 0.005) tags.push(`回撤控制优 ${drawdownDisplay}`);
    else if (drawdown >= 0.03) tags.push(`回撤偏大 ${drawdownDisplay}`);
  }
  if (volatility !== null) {
    const volatilityDisplay = formatMetricDisplayValue('1年-波动率', volatility);
    if (volatility <= 0.01) tags.push(`波动较低 ${volatilityDisplay}`);
    else if (volatility >= 0.04) tags.push(`波动偏高 ${volatilityDisplay}`);
  }
  if (fee !== null) {
    if (fee <= 0.3) tags.push(`费率较低 ${fee.toFixed(2)}%`);
    else if (fee >= 0.8) tags.push(`费率偏高 ${fee.toFixed(2)}%`);
  }
  if (scale !== null) {
    if (scale >= 20) tags.push(`规模较成熟 ${scale.toFixed(2)} 亿`);
    else if (scale < 2) tags.push(`规模偏小 ${scale.toFixed(2)} 亿`);
  }
  if (confidence) tags.push(confidence);
  if (stability) tags.push(stability);
  if (validation.rawStatus === 'strict_match') tags.push(`${validation.sourceLabel} 严格匹配`);
  else if (validation.rawStatus || validation.neutral) {
    tags.push(`${validation.sourceLabel} ${validation.displayStatus || validation.rawStatus || '状态未提供'}`);
  }
  if (holding === '已确认<=90天') tags.push('持有期已确认<=90天');
  else if (holding) tags.push(holding);

  return [...new Set(tags)].slice(0, 8);
}

function buildFormalScoreRationaleLines(row) {
  const lines = [];
  const risk = String(row?.['风险提示'] || '').trim();
  const digest = String(row?.['推荐解读'] || '').trim();
  const admissionReason = String(row?.['入选原因'] || '').trim();
  const occurrence = String(row?.['出现次数&出现频率(%)'] || '').trim();
  const group = String(row?.['正式榜分组'] || '').trim();
  const style = String(row?.['债基风格标签'] || '').trim();
  const validation = fundValidationContract(row);

  if (group || style) {
    lines.push(`定位：${[group, style].filter(Boolean).join(' / ') || '-'}`);
  }
  if (occurrence && occurrence !== '0 次 / -' && occurrence !== '- / -') {
    lines.push(`历史在榜：${occurrence}`);
  }
  if (admissionReason) {
    lines.push(`入选依据：${admissionReason}`);
  } else if (digest) {
    const compact = digest.split('|').map((part) => part.trim()).filter(Boolean).slice(0, 4).join(' | ');
    if (compact) lines.push(`机器摘要：${compact}`);
  }
  if (risk && risk !== '当前未见额外显式风险提示') {
    lines.push(`风险扣分项：${risk}`);
  } else if (risk) {
    lines.push(`风险提示：${risk}`);
  }
  if (validation.neutral || validation.rawStatus) {
    const status = validation.displayStatus || validation.rawStatus || '状态未提供';
    lines.push(`${validation.sourceLabel}：${status}${validation.contractVersion ? `；契约 ${validation.contractVersion}` : ''}`);
    if (validation.note) lines.push(`数据校验备注：${validation.note}`);
    if (validation.warnings && !validation.note.includes(validation.warnings)) {
      lines.push(`数据校验告警：${validation.warnings}`);
    }
  }
  if (!lines.length) {
    lines.push('当前行未提供更多可解释字段，建议结合收益、波动、回撤与费率综合判断。');
  }
  return lines.slice(0, 4);
}

let activeFormalScoreContract = null;

function normalizeFormalScoreContract(contract) {
  if (!contract || typeof contract !== 'object') return null;
  const schemaVersion = Number(contract.schema_version);
  const contractVersion = String(contract.contract_version || '').trim();
  const components = Array.isArray(contract.components) ? contract.components : [];
  if (!Number.isFinite(schemaVersion) || schemaVersion < 1 || !contractVersion || !components.length) {
    return null;
  }
  const normalizedComponents = components.map((component) => {
    const blendWeight = numericValueOrNull(component?.blend_weight);
    const metrics = Array.isArray(component?.metrics)
      ? component.metrics.map((metric) => ({
        field: String(metric?.field || '').trim(),
        label: String(metric?.label || metric?.field || '').trim(),
        weight_within_component: numericValueOrNull(metric?.weight_within_component),
        effective_weight: numericValueOrNull(metric?.effective_weight),
        direction: String(metric?.direction || '').trim(),
      })).filter((metric) => metric.field && metric.label && metric.weight_within_component !== null)
      : [];
    return {
      key: String(component?.key || '').trim(),
      label: String(component?.label || component?.key || '').trim(),
      blend_weight: blendWeight,
      metrics,
    };
  }).filter((component) => component.key && component.label && component.blend_weight !== null);
  if (!normalizedComponents.length) return null;
  return {
    ...contract,
    schema_version: schemaVersion,
    contract_version: contractVersion,
    components: normalizedComponents,
  };
}

function setFormalScoreContract(contract) {
  activeFormalScoreContract = normalizeFormalScoreContract(contract);
  return activeFormalScoreContract;
}

function formatFormalScoreContractWeight(value) {
  const weight = numericValueOrNull(value);
  if (weight === null) return '-';
  const percentage = weight * 100;
  return `${percentage.toFixed(Number.isInteger(percentage) ? 0 : 1)}%`;
}

function buildFormalScoreContractMarkup(contract = activeFormalScoreContract, options = {}) {
  const normalized = normalizeFormalScoreContract(contract);
  if (!normalized) {
    return '<p class="formal-score-contract-unavailable">评分权重契约暂不可用；当前不展示推测权重，请以正式评分和同风格门槛为准。</p>';
  }
  const includeMetrics = options.includeMetrics !== false;
  const componentTags = normalized.components.map((component) => (
    `<span>${escapeHtml(component.label)} ${escapeHtml(formatFormalScoreContractWeight(component.blend_weight))}</span>`
  )).join('');
  const metricLines = includeMetrics
    ? normalized.components.map((component) => {
      const metrics = component.metrics.map((metric) => (
        `${strategyMetricDisplayLabel(metric.label)} ${formatFormalScoreContractWeight(metric.weight_within_component)}`
      )).join(' / ');
      return metrics ? `<p><strong>${escapeHtml(component.label)}</strong>：${escapeHtml(metrics)}</p>` : '';
    }).join('')
    : '';
  const scopeLabel = String(normalized.comparison_scope_label || '').trim();
  const interpretation = String(normalized.interpretation || '').trim();
  return `
    <div class="formal-score-contract" data-contract-version="${escapeHtml(normalized.contract_version)}">
      <div class="score-tooltip-tags favorite-score-tooltip-tags">${componentTags}</div>
      ${metricLines ? `<div class="formal-score-contract-details">${metricLines}</div>` : ''}
      <details class="risk-strategy-definition"><summary>风险策略指标口径</summary>
        <p>策略夏普＝(年化收益−无风险利率)÷年化波动率；策略索提诺使用全样本下行半方差，无下行时沿用波动率×0.1的策略回退。二者按策略封顶。</p>
        <p>数据库卡玛＝年化收益÷最大回撤，无回撤为缺失；筛选策略卡玛扣除无风险利率，使用风格回撤下限并封顶。20个观测桶的统计称为观测桶收益。</p>
        <p>公式版本 strategy-risk-2026-10-10.1；每次计算记录实际年化因子、无风险利率和输入来源；旧记录参数未知。</p>
      </details>
      ${scopeLabel || interpretation ? `<p>${escapeHtml([scopeLabel, interpretation].filter(Boolean).join('；'))}</p>` : ''}
    </div>
  `;
}

function renderFormalScoreContractInto(target, contract, options = {}) {
  const element = typeof target === 'string' ? document.getElementById(target) : target;
  const normalized = setFormalScoreContract(contract);
  if (element) {
    element.innerHTML = buildFormalScoreContractMarkup(normalized, options);
  }
  return normalized;
}

function renderFormalScoreTooltipCell(value, row, options = {}) {
  if (!String(value ?? '').trim()) {
    return '<span class="favorite-detail-value-empty">-</span>';
  }
  const display = formatMetricDisplayValue('正式投资评分', value, 2);
  const code = String(options.code || row?.['基金代码'] || '').trim() || 'score-unknown';
  const observationTags = buildFormalScoreObservationTags(row);
  const rationaleLines = buildFormalScoreRationaleLines(row);
  const metricPairs = [
    [FUND_COPYWRITING.oneYearAnnualizedLabel, formatScoreMetricValue(row?.['1年-年化收益(%)'], 2, '%')],
    [FUND_COPYWRITING.sinceInceptionAnnualizedLabel, formatScoreMetricValue(row?.['平台成立以来收益年化(%)'], 2, '%')],
    ['策略夏普', formatScoreMetricValue(row?.['1年-夏普比率'], 2)],
    ['最大回撤', formatMetricDisplayValue('1年-最大回撤(%)', row?.['1年-最大回撤(%)'])],
    ['波动率', formatMetricDisplayValue('1年-波动率', row?.['1年-波动率'])],
    ['费率', formatScoreMetricValue(row?.['费率合计(%)'], 2, '%')],
    ['规模', formatScoreMetricValue(row?.['基金规模(亿)'], 2, ' 亿')],
  ].filter(([, metricValue]) => metricValue !== '-');
  const metricMarkup = metricPairs.length
    ? `
      <div class="favorite-score-tooltip-metrics">
        ${metricPairs.map(([label, metricValue]) => `
          <span><strong>${escapeHtml(label)}</strong><em>${escapeHtml(metricValue)}</em></span>
        `).join('')}
      </div>
    `
    : '';
  const rankBadgeMarkup = buildStyleMetricRankBadgeMarkup(row, { field: '正式投资评分', prefix: '总评' });
  const buttonMarkup = rankBadgeMarkup
    ? `
      <span class="favorite-tooltip-trigger-stack">
        <span>${escapeHtml(display)}</span>
        ${rankBadgeMarkup}
      </span>
    `
    : escapeHtml(display);
  return `
    <div class="score-tooltip favorite-score-tooltip" tabindex="0">
      <button type="button" class="score-tooltip-trigger favorite-score-tooltip-trigger" aria-controls="${GLOBAL_FLOATING_TOOLTIP_ID}" aria-expanded="false">
        ${buttonMarkup}
      </button>
      <div class="score-tooltip-card favorite-score-tooltip-card" role="tooltip">
        <p><strong>正式投资评分 ${escapeHtml(display)}</strong></p>
        <p>该分数反映基金在项目正式评分体系下的综合质量。下方统一数据基准用于说明它在同风格参考口径下相对风格门槛或真实样本参考线所处的位置。</p>
        ${buildFormalScoreContractMarkup(options.formalScoreContract || activeFormalScoreContract)}
        ${metricMarkup}
        <div class="favorite-score-tooltip-lines">
          ${rationaleLines.map((line) => `<p>${escapeHtml(line)}</p>`).join('')}
        </div>
        ${buildStyleMetricReferenceSummaryMarkup(row, ['正式投资评分', '1年-年化收益(%)', '1年-夏普比率', '1年-最大回撤(%)', '1年-波动率'])}
        ${observationTags.length ? `
          <div class="score-tooltip-tags favorite-score-tooltip-tags favorite-score-tooltip-tags-observation">
            ${observationTags.map((tag) => `<span>${escapeHtml(tag)}</span>`).join('')}
          </div>
        ` : ''}
      </div>
    </div>
  `;
}

function renderStyleBenchmarkTooltipCell(value, row) {
  const display = String(value || '').trim();
  if (!display) {
    return '<span class="favorite-detail-value-empty">-</span>';
  }
  const rankBadgeMarkup = buildStyleMetricRankBadgeMarkup(row, { field: '1年-年化收益(%)', prefix: '收益' });
  const benchmarkMarkup = buildStyleMetricReferenceSummaryMarkup(row, [
    '正式投资评分',
    '1年-年化收益(%)',
    '平台成立以来收益年化(%)',
    '1年-夏普比率',
    '1年-最大回撤(%)',
    '1年-波动率',
  ]);
  if (!benchmarkMarkup) {
    return `<span class="table-pill pill-style" title="${escapeHtml(display)}">${escapeHtml(display)}</span>`;
  }
  return `
    <div class="score-tooltip favorite-score-tooltip" tabindex="0">
      <button type="button" class="score-tooltip-trigger favorite-score-tooltip-trigger favorite-style-tooltip-trigger" aria-controls="${GLOBAL_FLOATING_TOOLTIP_ID}" aria-expanded="false">
        <span class="favorite-style-pill-stack">
          <span class="table-pill pill-style">${escapeHtml(display)}</span>
          ${rankBadgeMarkup}
        </span>
      </button>
      <div class="score-tooltip-card favorite-score-tooltip-card" role="tooltip">
        <p><strong>债基风格标签 ${escapeHtml(display)}</strong></p>
        <p>下方统一数据基准优先采用 workflow 风格门槛，缺失时回退到全量真实样本统计中位值，用于判断该基金的收益与风险是否达到当前风格参考线。</p>
        ${benchmarkMarkup}
      </div>
    </div>
  `;
}

function formatSignedPercentText(value) {
  const text = String(value ?? '').trim();
  if (!text) return '';
  const normalized = text.replace(/,/g, '').replace(/%$/u, '').trim();
  const num = Number(normalized);
  if (!Number.isFinite(num)) return String(value ?? '');
  const prefix = num > 0 ? '+' : '';
  return `${prefix}${num.toFixed(2)}%`;
}

function formatSignedPercentOrDash(value) {
  return formatSignedPercentText(value) || '-';
}

function renderSignedPercentValue(value) {
  const text = String(value ?? '').trim();
  if (!text) return '';
  const display = formatSignedPercentText(value);
  const normalized = text.replace(/,/g, '').replace(/%$/u, '').trim();
  const num = Number(normalized);
  if (!Number.isFinite(num)) return `<span class="table-number table-number-signed">${escapeHtml(display)}</span>`;
  const cls = num > 0 ? 'num-pos' : num < 0 ? 'num-neg' : 'num-zero';
  return `<span class="table-number table-number-signed ${cls}">${escapeHtml(display)}</span>`;
}

function parseSortableValue(value) {
  if (value === null || value === undefined) {
    return { empty: true, numeric: null, raw: '' };
  }
  if (typeof value === 'number' && Number.isFinite(value)) {
    return { empty: false, numeric: value, raw: String(value) };
  }
  if (typeof value === 'boolean') {
    return { empty: false, numeric: value ? 1 : 0, raw: value ? '1' : '0' };
  }
  const raw = String(value).trim();
  if (!raw) {
    return { empty: true, numeric: null, raw: '' };
  }
  const normalized = raw.replace(/,/g, '').replace(/%$/u, '').trim();
  const numeric = Number(normalized);
  return {
    empty: false,
    numeric: Number.isFinite(numeric) ? numeric : null,
    raw,
  };
}

function compareSortableValues(leftValue, rightValue, direction) {
  const left = parseSortableValue(leftValue);
  const right = parseSortableValue(rightValue);

  if (left.empty && right.empty) return 0;
  if (left.empty) return 1;
  if (right.empty) return -1;

  if (left.numeric !== null && right.numeric !== null) {
    if (left.numeric === right.numeric) return 0;
    return left.numeric > right.numeric ? direction : -direction;
  }

  return left.raw.localeCompare(right.raw, 'zh-CN', {
    numeric: true,
    sensitivity: 'base',
  }) * direction;
}

function renderSortIndicator(column, sortState) {
  const activeColumn = sortState?.column || '';
  const activeDirection = sortState?.direction || '';
  if (activeColumn !== column || !activeDirection) {
    return '<span class="sort-indicator">⇅</span>';
  }
  return `<span class="sort-indicator active">${activeDirection === 'desc' ? '↓' : '↑'}</span>`;
}

function nextTableSortDirection(column, sortState, initialSortDirections) {
  const defaultDirection = initialSortDirections?.[column] === 'asc' ? 'asc' : 'desc';
  if (sortState?.column !== column || !sortState?.direction) {
    return defaultDirection;
  }
  if (sortState.direction === defaultDirection) {
    return defaultDirection === 'desc' ? 'asc' : 'desc';
  }
  return null;
}

function isCompactTableViewport() {
  return window.innerWidth <= 720;
}

function buildAttributeMarkup(attributes) {
  if (!attributes || typeof attributes !== 'object') return '';
  return Object.entries(attributes)
    .map(([key, value]) => {
      const name = String(key || '').trim();
      if (!name) return '';
      return ` ${escapeHtml(name)}="${escapeHtml(String(value ?? ''))}"`;
    })
    .join('');
}

function resolveTableHeaderContentRenderer(headerContentRenderers, column) {
  const renderer = headerContentRenderers && typeof headerContentRenderers === 'object'
    ? headerContentRenderers[column]
    : null;
  if (typeof renderer === 'function') {
    return String(renderer(column) || '').trim();
  }
  if (typeof renderer === 'string') {
    return renderer.trim();
  }
  return '';
}

function resolveTableCellRenderer(cellRenderers, column, value, row) {
  return typeof cellRenderers?.[column] === 'function'
    ? cellRenderers[column](value, row, column)
    : renderCell(column, value, row);
}

function renderCompactTableSortHeader(column, sortState, sortableColumns, headerContentRenderers = null) {
  const customHeaderContent = resolveTableHeaderContentRenderer(headerContentRenderers, column);
  const headerContent = customHeaderContent || renderMetricColumnHeader(column);
  if (!sortableColumns.has(column)) {
    return `<th data-column="${escapeHtml(column)}">${headerContent}</th>`;
  }
  return `
    <th data-column="${escapeHtml(column)}">
      <button class="table-sort-btn" type="button" data-sort-column="${escapeHtml(column)}">
        <span class="table-sort-btn-label">${headerContent}</span>
        ${renderSortIndicator(column, sortState)}
      </button>
    </th>
  `;
}

function renderCompactExpandableTable(container, rows, options = {}) {
  if (!container) return { rows: [], columns: [], visibleColumns: [], sortedRows: [] };

  const items = Array.isArray(rows) ? rows : [];
  if (!items.length) {
    container.innerHTML = '<div class="empty-state-inline">暂无数据</div>';
    return { rows: [], columns: [], visibleColumns: [], sortedRows: [] };
  }

  const detailColumnLabel = options.detailColumnLabel || '详情';
  const rowHasColumn = (row, column) => Boolean(
    row
    && typeof row === 'object'
    && Object.prototype.hasOwnProperty.call(row, column)
  );
  const columnExistsInRows = (column) => (
    column === detailColumnLabel
    || items.some((row) => rowHasColumn(row, column))
  );
  const coreColumns = (options.coreColumns || []).filter((column) => columnExistsInRows(column));
  const mobileCardColumns = Array.isArray(options.mobileCardColumns) && options.mobileCardColumns.length
    ? options.mobileCardColumns.filter((column) => columnExistsInRows(column))
    : coreColumns.filter((column) => column !== detailColumnLabel);
  const detailGroups = Array.isArray(options.detailGroups) ? options.detailGroups : [];
  const rowDataResolver = typeof options.rowDataResolver === 'function'
    ? options.rowDataResolver
    : (row) => row;
  const sortState = options.sortState || { column: null, direction: null };
  const sortableColumns = new Set((options.sortableColumns || []).filter((column) => coreColumns.includes(column)));
  const sortAccessors = options.sortAccessors || {};
  const initialSortDirections = options.initialSortDirections || {};
  const tableClass = options.tableClass || 'data-table compact-table';
  const tableAttributes = options.tableAttributes || null;
  const wrapClass = options.wrapClass ? `table-wrap ${options.wrapClass}` : 'table-wrap';
  const toolbarClass = options.toolbarClass || 'compact-table-toolbar';
  const containerClass = options.containerClass || '';
  const toolbarContainer = options.toolbarContainer || null;
  const rowClassName = typeof options.rowClassName === 'function' ? options.rowClassName : null;
  const rowAttributes = typeof options.rowAttributes === 'function' ? options.rowAttributes : null;
  const cellClassName = typeof options.cellClassName === 'function' ? options.cellClassName : null;
  const cellRenderers = options.cellRenderers || {};
  const headerContentRenderers = options.headerContentRenderers || {};
  const detailValueFormatter = typeof options.detailValueFormatter === 'function'
    ? options.detailValueFormatter
    : (column, value, row) => resolveTableCellRenderer(cellRenderers, column, value, row);
  const rowKey = typeof options.rowKey === 'function'
    ? options.rowKey
    : (row) => String(row?.['基金代码'] || row?.id || '');
  const expandedRows = options.expandedRows instanceof Set ? options.expandedRows : new Set();
  const detailEmptyText = options.detailEmptyText || '暂无更多明细';
  const detailExpandLabel = options.detailExpandLabel || '展开';
  const detailCollapseLabel = options.detailCollapseLabel || '收起';
  const renderTableMarkup = !isCompactTableViewport() || options.forceDesktopTable === true;
  const renderCardMarkup = options.renderCards === 'always'
    ? true
    : options.renderCards === false
      ? false
      : isCompactTableViewport();
  const showExpandAllButton = renderTableMarkup && typeof options.onToggleExpandAll === 'function';
  const renderExpandCell = typeof options.renderExpandCell === 'function'
    ? options.renderExpandCell
    : (row) => {
      const key = rowKey(row);
      const expanded = key ? expandedRows.has(key) : false;
      return `
        <button
          type="button"
          class="favorite-expand-toggle ${expanded ? 'is-expanded' : ''}"
          data-compact-expand-key="${escapeHtml(key)}"
          aria-expanded="${expanded ? 'true' : 'false'}"
        >
          ${expanded ? detailCollapseLabel : detailExpandLabel}
        </button>
      `;
    };

  let sortedRows = items;
  if (sortableColumns.has(sortState.column) && (sortState.direction === 'asc' || sortState.direction === 'desc')) {
    const direction = sortState.direction === 'desc' ? -1 : 1;
    sortedRows = [...items].sort((left, right) => {
      const leftValue = typeof sortAccessors[sortState.column] === 'function'
        ? sortAccessors[sortState.column](left)
        : left[sortState.column];
      const rightValue = typeof sortAccessors[sortState.column] === 'function'
        ? sortAccessors[sortState.column](right)
        : right[sortState.column];
      return compareSortableValues(leftValue, rightValue, direction);
    });
  }

  const availableColumns = coreColumns.includes(detailColumnLabel)
    ? coreColumns
    : [...coreColumns];
  const header = renderTableMarkup ? availableColumns
    .map((column) => renderCompactTableSortHeader(column, sortState, sortableColumns, headerContentRenderers))
    .join('') : '';

  const renderDetailField = (row, field) => {
    const rowData = rowDataResolver(row);
    const content = detailValueFormatter(field, rowData[field], rowData, row);
    return `
      <div class="favorite-detail-item">
        <strong>${renderMetricColumnHeader(field)}</strong>
        <span>${content || '<span class="favorite-detail-value-empty">-</span>'}</span>
      </div>
    `;
  };

  const renderDetailSection = (row, section) => {
    const rowData = rowDataResolver(row);
    const fields = (section?.fields || []).filter((field) => field in rowData);
    if (!fields.length) return '';
    if (section.render) {
      return section.render(rowData, {
        escapeHtml,
        renderCell: (column, value) => detailValueFormatter(column, value, rowData, row),
        renderFields: (fieldNames) => fieldNames
          .filter((field) => field in rowData)
          .map((field) => renderDetailField(row, field))
          .join(''),
      });
    }
    return `
      <section class="favorite-detail-section">
        <h4>${escapeHtml(section.title || '')}</h4>
        <div class="favorite-detail-grid">
          ${fields.map((field) => renderDetailField(row, field)).join('')}
        </div>
      </section>
    `;
  };

  const buildDetailRowMarkup = (row) => {
    const key = rowKey(row);
    const expanded = key ? expandedRows.has(key) : false;
    if (!expanded) return '';
    const colspan = Math.max(availableColumns.length, 1);
    const sections = detailGroups
      .map((section) => renderDetailSection(row, section))
      .filter(Boolean)
      .join('');
    return `
      <tr class="favorite-detail-row ${expanded ? 'is-open' : ''}" data-compact-detail-key="${escapeHtml(key)}">
        <td class="favorite-detail-cell" colspan="${colspan}">
          <div class="favorite-detail-panel ${expanded ? 'is-open' : ''}">
            ${sections || `<div class="favorite-detail-empty">${escapeHtml(detailEmptyText)}</div>`}
          </div>
        </td>
      </tr>
    `;
  };

  const body = renderTableMarkup ? sortedRows.map((row) => {
    const key = rowKey(row);
    const attributes = rowAttributes ? rowAttributes(row) : null;
    const classes = rowClassName ? String(rowClassName(row) || '') : '';
    const cells = availableColumns.map((column) => {
      const content = column === detailColumnLabel
        ? renderExpandCell(row)
        : resolveTableCellRenderer(cellRenderers, column, row[column], row);
      const cellClass = cellClassName ? String(cellClassName(column, row[column], row) || '') : '';
      return `<td class="${escapeHtml(cellClass)}" data-column="${escapeHtml(column)}">${content}</td>`;
    }).join('');
    return `
      <tr class="${escapeHtml(classes)}"${buildAttributeMarkup(attributes)} data-compact-row-key="${escapeHtml(key)}">
        ${cells}
      </tr>
      ${buildDetailRowMarkup(row)}
    `;
  }).join('') : '';

  const expandedCount = sortedRows.reduce((count, row) => count + (expandedRows.has(rowKey(row)) ? 1 : 0), 0);
  const allExpanded = sortedRows.length > 0 && expandedCount === sortedRows.length;
  const toolbarMarkup = options.showToolbar === false
    ? ''
    : `
      <div class="${escapeHtml(toolbarClass)}">
        <div class="favorite-table-toolbar-copy">
          <strong>${escapeHtml(options.toolbarTitle || '高密度视图')}</strong>
          <span>${escapeHtml(options.toolbarHint || '默认只展示核心决策列，更多字段按行展开查看')}</span>
        </div>
        ${(showExpandAllButton || options.toolbarActionsMarkup) ? `
          <div class="favorite-table-toolbar-actions">
            ${options.toolbarActionsMarkup || ''}
            ${showExpandAllButton ? `<button type="button" class="secondary" data-compact-expand-all="1">${allExpanded ? '全部收起' : '全部展开'}</button>` : ''}
          </div>
        ` : ''}
      </div>
    `;
  const cardRowClassName = typeof options.cardRowClassName === 'function' ? options.cardRowClassName : rowClassName;

  if (toolbarContainer) {
    toolbarContainer.innerHTML = toolbarMarkup;
  }

  container.innerHTML = `
    ${toolbarContainer ? '' : toolbarMarkup}
    ${renderTableMarkup ? `
      <div class="${wrapClass}">
        <table class="${escapeHtml(tableClass)}"${buildAttributeMarkup(tableAttributes)}>
          <thead>
            <tr>${header}</tr>
          </thead>
          <tbody>${body}</tbody>
        </table>
      </div>
    ` : ''}
    ${renderCardMarkup ? `
      <div class="data-card-list ${escapeHtml(containerClass)}">
        ${sortedRows.map((row, index) => `
          <article class="data-card ${escapeHtml(cardRowClassName ? String(cardRowClassName(row) || '') : '')}"${buildAttributeMarkup(rowAttributes ? rowAttributes(row) : null)}>
            ${mobileCardColumns.map((column) => `
              <div class="data-card-item" data-column="${escapeHtml(column)}">
                <strong>${escapeHtml(column)}</strong>
                <span>${resolveTableCellRenderer(cellRenderers, column, row[column], row)}</span>
              </div>
            `).join('')}
            ${detailGroups.length ? `
              <details class="compact-disclosure data-card-details" data-compact-detail-index="${index}">
                <summary>查看详情</summary>
              </details>
            ` : ''}
          </article>
        `).join('')}
      </div>
    ` : ''}
  `;

  if (renderCardMarkup && detailGroups.length) {
    container.querySelectorAll('[data-compact-detail-index]').forEach((detail) => {
      detail.addEventListener('toggle', () => {
        if (!detail.open || detail.dataset.compactDetailRendered === 'true') return;
        const row = sortedRows[Number(detail.dataset.compactDetailIndex)];
        if (!row) return;
        detail.insertAdjacentHTML('beforeend', detailGroups.map((section) => renderDetailSection(row, section)).join(''));
        detail.dataset.compactDetailRendered = 'true';
        if (typeof options.onDetailRendered === 'function') options.onDetailRendered(detail, row);
      });
    });
  }

  scheduleStickyTableLayout(container);

  if (typeof options.onSort === 'function' && sortableColumns.size) {
    container.querySelectorAll('.table-sort-btn').forEach((button) => {
      button.addEventListener('click', () => {
        const column = button.dataset.sortColumn || '';
        const nextDirection = nextTableSortDirection(column, sortState, initialSortDirections);
        options.onSort({
          column: nextDirection ? column : null,
          direction: nextDirection,
        });
      });
    });
  }

  const toolbarRoot = toolbarContainer || container;
  if (typeof options.onToggleExpand === 'function') {
    container.querySelectorAll('[data-compact-expand-key]').forEach((button) => {
      button.addEventListener('click', () => {
        options.onToggleExpand(button.dataset.compactExpandKey || '');
      });
    });
  }

  if (showExpandAllButton) {
    toolbarRoot.querySelector('[data-compact-expand-all="1"]')?.addEventListener('click', () => {
      options.onToggleExpandAll(!allExpanded, sortedRows);
    });
  }

  return {
    rows: sortedRows,
    columns: availableColumns,
    visibleColumns: availableColumns,
    sortedRows,
  };
}

function renderSimpleTable(container, rows, preferredColumns, options = {}) {
  if (!container) return;
  if (!Array.isArray(rows) || !rows.length) {
    container.innerHTML = '<div class="empty-state-inline">暂无数据</div>';
    return { rows: [], columns: [] };
  }

  const available = preferredColumns.filter((column) => column in rows[0]);
  const mobileCardColumns = Array.isArray(options.mobileCardColumns) && options.mobileCardColumns.length
    ? options.mobileCardColumns.filter((column) => column in rows[0])
    : available;
  const responsiveMode = options.responsiveMode || 'both';
  const wrapClass = options.wrapClass ? `table-wrap ${options.wrapClass}` : 'table-wrap';
  const tableClass = options.tableClass || 'data-table tracker-table';
  const sortState = options.sortState || { column: null, direction: null };
  const sortableColumns = new Set((options.sortableColumns || []).filter((column) => available.includes(column)));
  const sortAccessors = options.sortAccessors || {};
  const initialSortDirections = options.initialSortDirections || {};
  const rowClassName = typeof options.rowClassName === 'function' ? options.rowClassName : null;
  const rowAttributes = typeof options.rowAttributes === 'function' ? options.rowAttributes : null;
  const cellRenderers = options.cellRenderers || {};
  const headerContentRenderers = options.headerContentRenderers || {};
  const renderTableCell = (column, value, row) => (
    resolveTableCellRenderer(cellRenderers, column, value, row)
  );

  let sortedRows = rows;
  if (sortableColumns.has(sortState.column) && (sortState.direction === 'asc' || sortState.direction === 'desc')) {
    const direction = sortState.direction === 'desc' ? -1 : 1;
    sortedRows = [...rows].sort((left, right) => {
      const leftValue = typeof sortAccessors[sortState.column] === 'function'
        ? sortAccessors[sortState.column](left)
        : left[sortState.column];
      const rightValue = typeof sortAccessors[sortState.column] === 'function'
        ? sortAccessors[sortState.column](right)
        : right[sortState.column];
      return compareSortableValues(leftValue, rightValue, direction);
    });
  }

  const header = available.map((column) => {
    const customHeaderContent = resolveTableHeaderContentRenderer(headerContentRenderers, column);
    const headerContent = customHeaderContent || renderMetricColumnHeader(column);
    if (!sortableColumns.has(column)) {
      return `<th data-column="${escapeHtml(column)}">${headerContent}</th>`;
    }
    return `
      <th data-column="${escapeHtml(column)}">
        <button class="table-sort-btn" type="button" data-sort-column="${escapeHtml(column)}">
          <span class="table-sort-btn-label">${headerContent}</span>
          ${renderSortIndicator(column, sortState)}
        </button>
      </th>
    `;
  }).join('');

  const body = sortedRows
    .map(
      (row) => `
        <tr class="${escapeHtml(rowClassName ? String(rowClassName(row) || '') : '')}"${buildAttributeMarkup(rowAttributes ? rowAttributes(row) : null)}>
          ${available.map((column) => `<td data-column="${escapeHtml(column)}">${renderTableCell(column, row[column], row)}</td>`).join('')}
        </tr>
      `
    )
    .join('');

  const renderTableMarkup = responsiveMode !== 'adaptive' || !isCompactTableViewport();
  const renderCardMarkup = responsiveMode === 'both' || (responsiveMode === 'adaptive' && isCompactTableViewport());

  container.innerHTML = `
    ${renderTableMarkup ? `
      <div class="${wrapClass}">
        <table class="${tableClass}">
          <thead>
            <tr>${header}</tr>
          </thead>
          <tbody>${body}</tbody>
        </table>
      </div>
    ` : ''}
    ${renderCardMarkup ? `
      <div class="data-card-list">
        ${sortedRows
          .map(
            (row) => `
              <article class="data-card ${escapeHtml(rowClassName ? String(rowClassName(row) || '') : '')}"${buildAttributeMarkup(rowAttributes ? rowAttributes(row) : null)}>
                ${mobileCardColumns
                  .map(
                    (column) => `
                      <div class="data-card-item">
                        <strong>${escapeHtml(column)}</strong>
                        <span>${renderTableCell(column, row[column], row)}</span>
                      </div>
                    `
                  )
                  .join('')}
              </article>
            `
          )
          .join('')}
      </div>
    ` : ''}
  `;
  scheduleStickyTableLayout(container);

  if (typeof options.onSort === 'function' && sortableColumns.size) {
    container.querySelectorAll('.table-sort-btn').forEach((button) => {
      button.addEventListener('click', () => {
        const column = button.dataset.sortColumn || '';
        const nextDirection = nextTableSortDirection(column, sortState, initialSortDirections);
        options.onSort({
          column: nextDirection ? column : null,
          direction: nextDirection,
        });
      });
    });
  }

  return {
    rows: sortedRows,
    columns: available,
  };
}

async function fetchJson(url, options) {
  const requestOptions = options && typeof options === 'object' ? { ...options } : {};
  const method = String(requestOptions.method || 'GET').trim().toUpperCase() || 'GET';
  const cacheTtlMs = Number(requestOptions.cacheTtlMs);
  const timeoutMs = Number(requestOptions.timeoutMs);
  const dedupe = requestOptions.dedupe !== false;
  const cacheKeyOverride = String(requestOptions.cacheKey || '').trim();
  delete requestOptions.cacheTtlMs;
  delete requestOptions.timeoutMs;
  delete requestOptions.dedupe;
  delete requestOptions.cacheKey;

  const normalizedBody = typeof requestOptions.body === 'string' ? requestOptions.body : '';
  const requestKey = cacheKeyOverride || [method, url, normalizedBody].join('::');
  const enableShortCache = method === 'GET' && Number.isFinite(cacheTtlMs) && cacheTtlMs > 0;
  const effectiveTimeoutMs = Number.isFinite(timeoutMs) && timeoutMs > 0
    ? timeoutMs
    : method === 'GET'
      ? 12000
      : 20000;

  if (!fetchJson._inflight) fetchJson._inflight = new Map();
  if (!fetchJson._responseCache) fetchJson._responseCache = new Map();

  const responseCache = fetchJson._responseCache;
  const inflight = fetchJson._inflight;

  if (enableShortCache) {
    const cached = responseCache.get(requestKey);
    if (cached && (Date.now() - cached.storedAt) <= cacheTtlMs) {
      return cloneJsonPayload(cached.payload);
    }
    if (cached) {
      responseCache.delete(requestKey);
    }
  }

  if (dedupe && inflight.has(requestKey)) {
    return inflight.get(requestKey).then((payload) => cloneJsonPayload(payload));
  }

  const controller = new AbortController();
  const timeoutHandle = window.setTimeout(() => {
    controller.abort(new DOMException(`请求超时: ${url}`, 'AbortError'));
  }, effectiveTimeoutMs);

  let detachAbortForwarder = null;
  const mergedSignal = (() => {
    const externalSignal = requestOptions.signal;
    if (!externalSignal) return controller.signal;
    if (externalSignal.aborted) {
      controller.abort(externalSignal.reason);
      return controller.signal;
    }
    const forwardAbort = () => controller.abort(externalSignal.reason);
    externalSignal.addEventListener('abort', forwardAbort, { once: true });
    detachAbortForwarder = () => {
      externalSignal.removeEventListener('abort', forwardAbort);
    };
    return controller.signal;
  })();

  const requestPromise = (async () => {
    let res;
    try {
      res = await fetch(url, {
        ...requestOptions,
        method,
        signal: mergedSignal,
      });
    } catch (error) {
      if (error?.name === 'AbortError') {
        throw new Error(`请求超时或已取消: ${url}`);
      }
      throw error;
    } finally {
      window.clearTimeout(timeoutHandle);
    }

    const contentType = String(res.headers.get('content-type') || '').toLowerCase();
    let data = null;
    let rawText = '';
    if (contentType.includes('application/json')) {
      try {
        data = await res.json();
      } catch (error) {
        throw new Error(`接口返回了无效 JSON: ${url}`);
      }
    } else {
      rawText = await res.text();
      if (rawText) {
        try {
          data = JSON.parse(rawText);
        } catch (_error) {
          data = null;
        }
      }
    }

    if (!res.ok) {
      const message = data?.error
        || data?.message
        || (rawText ? rawText.slice(0, 160) : '')
        || `请求失败: ${url}`;
      throw new Error(message);
    }

    if (data === null || typeof data !== 'object') {
      const preview = rawText ? rawText.slice(0, 160) : '响应内容为空';
      throw new Error(`接口未返回 JSON: ${url}${preview ? ` / ${preview}` : ''}`);
    }

    if (enableShortCache) {
      responseCache.set(requestKey, {
        storedAt: Date.now(),
        payload: cloneJsonPayload(data),
      });
    }

    return data;
  })();

  if (dedupe) {
    inflight.set(requestKey, requestPromise);
  }

  try {
    const payload = await requestPromise;
    return cloneJsonPayload(payload);
  } finally {
    detachAbortForwarder?.();
    if (dedupe) {
      inflight.delete(requestKey);
    }
  }
}

async function loadRecommendationsLatest() {
  return fetchJson('/api/recommendations/latest', { cacheTtlMs: 15000 });
}

async function loadRecommendationsLatestDetail(options = {}) {
  const params = new URLSearchParams();
  const group = String(options?.group || '').trim();
  if (group) {
    params.set('group', group);
  }
  const suffix = params.toString();
  const url = suffix ? `/api/recommendations/latest/detail?${suffix}` : '/api/recommendations/latest/detail';
  return fetchJson(url, { cacheTtlMs: 15000 });
}

async function loadDashboard(options = {}) {
  const sampleLimit = options && Object.prototype.hasOwnProperty.call(options, 'sampleLimit')
    ? Number(options.sampleLimit)
    : null;
  const full = Boolean(options?.full);
  const params = new URLSearchParams();
  if (Number.isFinite(sampleLimit) && sampleLimit >= 0) {
    params.set('sample_limit', String(Math.floor(sampleLimit)));
  }
  if (full) {
    params.set('full', '1');
  }
  const suffix = params.toString();
  const url = suffix ? `/api/dashboard/latest?${suffix}` : '/api/dashboard/latest';
  return fetchJson(url, {
    cacheTtlMs: 15000,
    timeoutMs: 30000,
    cacheKey: `dashboard-latest::sample_limit=${Number.isFinite(sampleLimit) ? Math.floor(sampleLimit) : 'default'}::full=${full ? 1 : 0}`,
  });
}

async function loadDashboardDetail() {
  return fetchJson('/api/dashboard/latest/detail', { cacheTtlMs: 15000 });
}

async function loadDashboardReportsDetail() {
  return fetchJson('/api/dashboard/latest/reports', { cacheTtlMs: 15000 });
}

async function loadDashboardTaskSummaryDetail() {
  return fetchJson('/api/dashboard/latest/task-summary', { cacheTtlMs: 15000 });
}

async function loadDashboardOverview() {
  return fetchJson('/api/dashboard/overview', {
    cacheTtlMs: 15000,
    timeoutMs: 30000,
  });
}

async function loadTrackerCurrent(options = {}) {
  const full = Boolean(options?.full);
  const page = Number(options?.page);
  const pageSize = Number(options?.pageSize ?? options?.page_size);
  const group = String(options?.group || '').trim();
  const params = new URLSearchParams();
  if (full) {
    params.set('full', '1');
    params.set('row_aliases', '0');
  }
  if (Number.isFinite(page) && page > 0) {
    params.set('page', String(Math.floor(page)));
  }
  if (Number.isFinite(pageSize) && pageSize > 0) {
    params.set('page_size', String(Math.floor(pageSize)));
  }
  if (group) {
    params.set('group', group);
  }
  const suffix = params.toString();
  const url = suffix ? `/api/tracker/current?${suffix}` : '/api/tracker/current';
  return fetchJson(url, {
    cacheTtlMs: 15000,
    cacheKey: [
      `tracker-current::full=${full ? 1 : 0}`,
      `page=${Number.isFinite(page) && page > 0 ? Math.floor(page) : 'default'}`,
      `page_size=${Number.isFinite(pageSize) && pageSize > 0 ? Math.floor(pageSize) : 'default'}`,
      `group=${group || 'all'}`,
    ].join('::'),
  });
}

async function loadFavoriteFundHistory() {
  return fetchJson('/api/favorites/history', { cacheTtlMs: 8000 });
}

async function queryFavoriteFundScores(payload) {
  return fetchJson('/api/favorites/score', {
    method: 'POST',
    // A full query can include 300 funds plus recommendations, cold report
    // reads and sequential upstream batches. Keep its wait budget bounded
    // without changing the default timeout for other POST requests.
    timeoutMs: 120000,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ response_mode: 'compact', ...(payload || {}) }),
  });
}

async function loadFavoriteMetricDetails(codes, options = {}) {
  const normalized = Array.isArray(codes)
    ? [...new Set(codes.map((item) => String(item || '').trim()).filter(Boolean))]
    : [];
  if (!normalized.length) {
    return { requested_count: 0, returned_count: 0, details: {} };
  }
  return fetchJson('/api/favorites/metric-details', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ codes: normalized }),
    // The page owns the bounded detail cache. Separate inflight requests from
    // different score queries so an older response cannot satisfy a new query.
    cacheKey: `favorite-metric-details::${String(options.sourceVersion || '')}::${normalized.join(',')}`,
  });
}

async function loadTrackerValuations(codes, options = {}) {
  const normalized = Array.isArray(codes)
    ? [...new Set(codes.map((item) => String(item || '').trim()).filter(Boolean))]
    : [];
  if (!normalized.length) {
    return { requested_count: 0, returned_count: 0, valuations: {} };
  }
  const batches = [];
  for (let index = 0; index < normalized.length; index += TRACKER_VALUATION_MAX_REQUEST_CODES) {
    batches.push(normalized.slice(index, index + TRACKER_VALUATION_MAX_REQUEST_CODES));
  }
  const payloads = [];
  for (const batch of batches) {
    const params = new URLSearchParams({ codes: batch.join(',') });
    try {
      payloads.push(await fetchJson(`/api/tracker/valuations?${params.toString()}`, {
        cacheTtlMs: 5000,
        timeoutMs: 30000,
        cacheKey: `tracker-valuations::${batch.join(',')}`,
      }));
    } catch (error) {
      if (options.continueOnError !== true) throw error;
      payloads.push({
        status: 'upstream_failed',
        source: 'tiantian_fund_valuation_last',
        requested_count: batch.length,
        returned_count: 0,
        cached_count: 0,
        uncached_count: batch.length,
        available_count: 0,
        missing_count: batch.length,
        upstream_requested_count: batch.length,
        upstream_returned_count: 0,
        skipped_uncached_count: 0,
        batch_count: 1,
        failed_batch_count: 1,
        rate_limit_wait_seconds: 0,
        quota_exhausted: false,
        quota_unavailable: false,
        upstream_error: error?.message || String(error),
        valuations: Object.fromEntries(batch.map((code) => [code, {
          available: false,
          reason: 'upstream_failed',
          message: error?.message || String(error),
          source: 'tiantian_fund_valuation_last',
        }])),
      });
    }
  }
  if (payloads.length === 1) {
    return payloads[0];
  }
  const merged = payloads.reduce((merged, payload) => {
    const valuations = payload?.valuations && typeof payload.valuations === 'object'
      ? payload.valuations
      : {};
    const maxWait = Math.max(Number(merged.rate_limit_wait_seconds) || 0, Number(payload?.rate_limit_wait_seconds) || 0);
    const status = merged.status === 'ok' && payload?.status === 'ok'
      ? 'ok'
      : (payload?.status === 'quota_degraded' || merged.status === 'quota_degraded' ? 'quota_degraded' : 'partial');
    return {
      ...merged,
      status,
      source: payload?.source || merged.source || 'tiantian_fund_valuation_last',
      requested_count: (Number(merged.requested_count) || 0) + (Number(payload?.requested_count) || 0),
      returned_count: (Number(merged.returned_count) || 0) + (Number(payload?.returned_count) || 0),
      cached_count: (Number(merged.cached_count) || 0) + (Number(payload?.cached_count) || 0),
      uncached_count: (Number(merged.uncached_count) || 0) + (Number(payload?.uncached_count) || 0),
      available_count: (Number(merged.available_count) || 0) + (Number(payload?.available_count) || 0),
      missing_count: (Number(merged.missing_count) || 0) + (Number(payload?.missing_count) || 0),
      upstream_requested_count: (Number(merged.upstream_requested_count) || 0) + (Number(payload?.upstream_requested_count) || 0),
      upstream_returned_count: (Number(merged.upstream_returned_count) || 0) + (Number(payload?.upstream_returned_count) || 0),
      skipped_uncached_count: (Number(merged.skipped_uncached_count) || 0) + (Number(payload?.skipped_uncached_count) || 0),
      batch_count: (Number(merged.batch_count) || 0) + (Number(payload?.batch_count) || 0),
      failed_batch_count: (Number(merged.failed_batch_count) || 0) + (Number(payload?.failed_batch_count) || 0),
      rate_limit_wait_seconds: maxWait,
      quota_exhausted: Boolean(merged.quota_exhausted || payload?.quota_exhausted),
      quota_unavailable: Boolean(merged.quota_unavailable || payload?.quota_unavailable),
      quota_recent_call_count: payload?.quota_recent_call_count ?? merged.quota_recent_call_count,
      quota_remaining: payload?.quota_remaining ?? merged.quota_remaining,
      quota_reserved_for_automation: Math.max(Number(merged.quota_reserved_for_automation) || 0, Number(payload?.quota_reserved_for_automation) || 0),
      max_calls_per_hour: payload?.max_calls_per_hour ?? merged.max_calls_per_hour,
      cache_ttl_seconds: payload?.cache_ttl_seconds ?? merged.cache_ttl_seconds,
      missing_cache_ttl_seconds: payload?.missing_cache_ttl_seconds ?? merged.missing_cache_ttl_seconds,
      max_request_codes: Math.min(
        Number(merged.max_request_codes) || TRACKER_VALUATION_MAX_REQUEST_CODES,
        Number(payload?.max_request_codes) || TRACKER_VALUATION_MAX_REQUEST_CODES,
      ),
      effective_batch_size: payload?.effective_batch_size ?? merged.effective_batch_size,
      upstream_error: payload?.upstream_error || merged.upstream_error,
      state_read_error: payload?.state_read_error || merged.state_read_error,
      valuations: {
        ...(merged.valuations || {}),
        ...valuations,
      },
    };
  }, {
    status: 'ok',
    source: 'tiantian_fund_valuation_last',
    requested_count: 0,
    returned_count: 0,
    cached_count: 0,
    uncached_count: 0,
    available_count: 0,
    missing_count: 0,
    upstream_requested_count: 0,
    upstream_returned_count: 0,
    skipped_uncached_count: 0,
    batch_count: 0,
    rate_limit_wait_seconds: 0,
    quota_exhausted: false,
    quota_unavailable: false,
    quota_reserved_for_automation: 0,
    max_request_codes: TRACKER_VALUATION_MAX_REQUEST_CODES,
    failed_batch_count: 0,
    valuations: {},
  });
  if (merged.failed_batch_count > 0) {
    merged.status = merged.failed_batch_count < payloads.length ? 'partial' : 'upstream_failed';
  }
  return merged;
}

function formatTrackerValuationStatusMessage(result, availableCount, totalCount, options = {}) {
  const suffix = String(options.suffix || '实时估值仅作盘中辅助观察').trim();
  const parts = [];
  const safeTotal = Number.isFinite(Number(totalCount)) ? Number(totalCount) : 0;
  const safeAvailable = Number.isFinite(Number(availableCount)) ? Number(availableCount) : 0;
  const missingCount = Number(result?.missing_count) || 0;
  const cachedCount = Number(result?.cached_count) || 0;
  const upstreamRequestedCount = Number(result?.upstream_requested_count) || 0;
  const skippedCount = Number(result?.skipped_uncached_count) || 0;

  parts.push(missingCount || skippedCount ? `已补 ${formatNumber(safeAvailable)} / ${formatNumber(safeTotal)}` : `已补 ${formatNumber(safeAvailable)} 只`);
  if (cachedCount > 0) parts.push(`缓存 ${formatNumber(cachedCount)}`);
  if (upstreamRequestedCount > 0) parts.push(`新取 ${formatNumber(upstreamRequestedCount)}`);
  if (skippedCount > 0) parts.push(`未请求 ${formatNumber(skippedCount)}`);
  if (result?.quota_exhausted) {
    const waitSeconds = Number(result?.rate_limit_wait_seconds) || 0;
    parts.push(waitSeconds > 0 ? `配额耗尽，约 ${Math.ceil(waitSeconds / 60)} 分钟后恢复` : '配额耗尽');
  } else if (result?.quota_unavailable) {
    parts.push('已保留调度配额');
  } else if (result?.status === 'upstream_failed') {
    parts.push('实时估值接口请求失败');
  } else if (result?.status && result.status !== 'ok') {
    parts.push('部分返回');
  }
  const valuationContract = normalizePayloadDataContract(result || {});
  if (valuationContract.available) parts.push(valuationContract.summary);
  if (suffix) parts.push(suffix);
  return parts.join('；');
}

async function loadStatus() {
  return fetchJson('/api/status', { cacheTtlMs: 5000 });
}

async function loadStatusSummary() {
  return fetchJson('/api/status/summary', { cacheTtlMs: 5000 });
}

async function loadHealth() {
  return fetchJson('/api/health/summary', { cacheTtlMs: 5000 });
}

async function loadBootstrapSummary() {
  return fetchJson('/api/bootstrap/summary', { cacheTtlMs: 5000 });
}

function getBootstrapHealth(payload) {
  return payload?.health || {};
}

function getBootstrapWorkflow(payload) {
  return payload?.workflow || payload?.status_summary || {};
}

async function loadSchedulerStatus() {
  // Cold governance summaries can exceed the generic 12-second GET budget.
  return fetchJson('/api/scheduler/status', { cacheTtlMs: 8000, timeoutMs: 30000 });
}

async function loadApiInfo() {
  return fetchJson('/api/info', {
    cacheTtlMs: API_INFO_CACHE_TTL_MS,
    cacheKey: 'api-info::endpoint-contracts',
  });
}

async function loadApiSettings() {
  return fetchJson('/api/settings/apis', {
    cacheTtlMs: 0,
    dedupe: false,
    cache: 'no-store',
    headers: { 'Cache-Control': 'no-cache' },
  });
}

async function saveApiSettings(payload) {
  return fetchJson('/api/settings/apis', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache' },
    body: JSON.stringify(payload || {}),
    cache: 'no-store',
    dedupe: false,
    timeoutMs: 20000,
  });
}

async function probeApiSettings(payload) {
  return fetchJson('/api/settings/apis/probe', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache' },
    body: JSON.stringify(payload || {}),
    cache: 'no-store',
    dedupe: false,
    timeoutMs: 15000,
  });
}

function getEndpointContract(apiInfo, endpoint) {
  const target = String(endpoint || '').trim();
  if (!target) return null;
  const contracts = Array.isArray(apiInfo?.endpoint_contracts) ? apiInfo.endpoint_contracts : [];
  for (const contract of contracts) {
    if (String(contract?.endpoint || '').trim() === target) {
      return { ...contract };
    }
    const explicitModes = Array.isArray(contract?.explicit_heavy_modes) ? contract.explicit_heavy_modes : [];
    for (const mode of explicitModes) {
      if (String(mode?.endpoint || '').trim() === target) {
        return {
          ...mode,
          method: contract.method,
          default_heavy: true,
          parent_endpoint: contract.endpoint,
          default_scope: 'explicit_heavy',
        };
      }
    }
  }
  return null;
}

async function loadEndpointContract(endpoint) {
  const apiInfo = await loadApiInfo();
  return getEndpointContract(apiInfo, endpoint);
}

async function fetchExplicitHeavyJson(endpoint, options = {}) {
  await loadEndpointContract(endpoint).catch(() => null);
  return fetchJson(endpoint, options);
}

async function loadSchedulerStatusDetail(options = {}) {
  const params = new URLSearchParams();
  const section = String(options?.section || '').trim();
  if (section) {
    params.set('section', section);
  }
  if (options?.detail) {
    params.set('detail', '1');
  }
  if (options?.full) {
    params.set('full', '1');
  }
  if (options?.sampleLimit !== undefined && options?.sampleLimit !== null) {
    params.set('sample_limit', String(options.sampleLimit));
  }
  const suffix = params.toString();
  const url = suffix ? `/api/scheduler/status/detail?${suffix}` : '/api/scheduler/status/detail';
  const explicitHeavy = options?.full || section === 'full_audit';
  const loader = explicitHeavy ? fetchExplicitHeavyJson : fetchJson;
  return loader(url, {
    cacheTtlMs: 12000,
    timeoutMs: explicitHeavy ? undefined : 30000,
    cacheKey: [
      'scheduler-status-detail',
      `section=${section || 'default'}`,
      `detail=${options?.detail ? 1 : 0}`,
      `full=${options?.full ? 1 : 0}`,
      `sample=${options?.sampleLimit ?? 'default'}`,
    ].join('::'),
  });
}

async function requestSchedulerManualRun(payload) {
  return fetchJson('/api/scheduler/manual-run', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
}

async function loadPublication(options = {}) {
  if (options?.full) {
    return loadPublicationFull(options);
  }
  return loadPublicationSummary();
}

async function loadPublicationFull(_options = {}) {
  return fetchExplicitHeavyJson('/api/publication?full=1', {
    cacheTtlMs: 10000,
    cacheKey: 'publication::explicit-full=1',
  });
}

async function loadPublicationSummary() {
  return fetchJson('/api/publication/summary', { cacheTtlMs: 10000 });
}

async function startWorkflow(payload) {
  return fetchJson('/api/run', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
}
