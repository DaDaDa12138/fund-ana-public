const statusMeta = document.getElementById('statusMeta');
const workflowNarrative = document.getElementById('workflowNarrative');
const logsEl = document.getElementById('logs');
const schedulerStats = document.getElementById('schedulerStats');
const schedulerSecondaryStats = document.getElementById('schedulerSecondaryStats');
const schedulerNarrative = document.getElementById('schedulerNarrative');
const automationClosureStats = document.getElementById('automationClosureStats');
const automationClosureCards = document.getElementById('automationClosureCards');
const automationClosureNarrative = document.getElementById('automationClosureNarrative');
const automationLayerStats = document.getElementById('automationLayerStats');
const automationLayerNarrative = document.getElementById('automationLayerNarrative');
const regressionSmokeStats = document.getElementById('regressionSmokeStats');
const regressionSmokeNarrative = document.getElementById('regressionSmokeNarrative');
const regressionSmokeArtifacts = document.getElementById('regressionSmokeArtifacts');
const longTermGovernanceStats = document.getElementById('longTermGovernanceStats');
const longTermGovernanceNarrative = document.getElementById('longTermGovernanceNarrative');
const longTermGovernanceQueue = document.getElementById('longTermGovernanceQueue');
const longTermGovernanceSamples = document.getElementById('longTermGovernanceSamples');
const integrityAuditStats = document.getElementById('integrityAuditStats');
const integrityAuditNarrative = document.getElementById('integrityAuditNarrative');
const exclusionStats = document.getElementById('exclusionStats');
const exclusionNarrative = document.getElementById('exclusionNarrative');
const exclusionSamples = document.getElementById('exclusionSamples');
const lowVolPositiveRatioStats = document.getElementById('lowVolPositiveRatioStats');
const lowVolPositiveRatioNarrative = document.getElementById('lowVolPositiveRatioNarrative');
const lowVolPositiveRatioSamples = document.getElementById('lowVolPositiveRatioSamples');
const operationsHeaderRunBadge = document.getElementById('operationsHeaderRunBadge');
const operationsHeaderHealthBadge = document.getElementById('operationsHeaderHealthBadge');
const operationsHeroMeta = document.getElementById('operationsHeroMeta');
const operationsQuickNav = document.getElementById('operationsQuickNav');
const operationsRunSummary = document.getElementById('operationsRunSummary');
const automationModeBanner = document.getElementById('automationModeBanner');
const runActionFeedback = document.getElementById('runActionFeedback');
const manualRunFeedback = document.getElementById('manualRunFeedback');
const manualRunHistory = document.getElementById('manualRunHistory');
const manualRunHistoryToggleBtn = document.getElementById('manualRunHistoryToggleBtn');
const workflowLogsToggleBtn = document.getElementById('workflowLogsToggleBtn');
const manualDailyBtn = document.getElementById('manualDailyBtn');
const manualIntradayBtn = document.getElementById('manualIntradayBtn');
const manualWeeklyBtn = document.getElementById('manualWeeklyBtn');
const startBtn = document.getElementById('startBtn');
const manualRunSection = document.getElementById('manualRunSection');
const runOptionGrid = document.getElementById('runOptionGrid');
const regressionSmokeArtifactsToggleBtn = document.getElementById('regressionSmokeArtifactsToggleBtn');
const longTermGovernanceSamplesToggleBtn = document.getElementById('longTermGovernanceSamplesToggleBtn');
const exclusionSamplesToggleBtn = document.getElementById('exclusionSamplesToggleBtn');
const lowVolPositiveRatioSamplesToggleBtn = document.getElementById('lowVolPositiveRatioSamplesToggleBtn');
const schedulerSecondaryToggleBtn = document.getElementById('schedulerSecondaryToggleBtn');
const operationsSchedulerDetail = window.OperationsSchedulerDetail;
const operationsRegressionSmoke = window.OperationsRegressionSmoke;
const operationsWorkflowStatus = window.OperationsWorkflowStatus;
const operationsSchedulerStatus = window.OperationsSchedulerStatus;
const operationsAutomationLayers = window.OperationsAutomationLayers;
const operationsGovernanceSummary = window.OperationsGovernanceSummary;
const operationsDashboardWatch = window.OperationsDashboardWatch;

if (!operationsSchedulerDetail) {
  throw new Error('Operations scheduler detail helpers are not loaded');
}

if (!operationsRegressionSmoke) {
  throw new Error('Operations regression smoke helpers are not loaded');
}

if (!operationsWorkflowStatus) {
  throw new Error('Operations workflow status helpers are not loaded');
}

if (!operationsSchedulerStatus) {
  throw new Error('Operations scheduler status helpers are not loaded');
}

if (!operationsAutomationLayers) {
  throw new Error('Operations automation layer helpers are not loaded');
}

if (!operationsGovernanceSummary) {
  throw new Error('Operations governance summary helpers are not loaded');
}

if (!operationsDashboardWatch) {
  throw new Error('Operations dashboard watch helpers are not loaded');
}

const {
  OPERATIONS_DETAIL_SECTION_SCHEDULER_SECTIONS,
  normalizeOperationsSchedulerSections,
  hasOperationsSchedulerSectionData,
  mergeOperationsSchedulerDetailPayloads,
  loadOperationsSchedulerDetailSections,
} = operationsSchedulerDetail;

let polling = null;
let lastKnownWorkflowStatus = '';
let manualOperationsEnabled = false;
let cachedOperationsDashboard = null;
let cachedSchedulerSummaryState = null;
let cachedSchedulerStatusDetail = null;
let cachedWorkflowState = null;
let cachedHealthState = null;
let cachedOperationsDashboardSampleLimit = null;
let lastWorkflowDetailRefreshAt = 0;
let lastSchedulerDetailRefreshAt = 0;
let lastOperationsHeavyRefreshAt = 0;
let operationsHeavyRefreshPromise = null;
let operationsLazySectionObserver = null;
let operationsWorkflowDetailRequested = false;
let operationsSchedulerDetailRequested = false;
let operationsDashboardDetailRequested = false;
const operationsRequestedSchedulerSections = new Set();

const OPERATIONS_LIGHT_POLL_MS = 3000;
const OPERATIONS_HEAVY_REFRESH_INTERVAL_MS = 30000;
const OPERATIONS_WORKFLOW_DETAIL_REFRESH_INTERVAL_MS = 5000;
const OPERATIONS_DASHBOARD_SUMMARY_SAMPLE_LIMIT = 0;
const OPERATIONS_DASHBOARD_EXPANDED_SAMPLE_LIMIT = 8;
const OPERATIONS_DETAIL_SECTION_CONFIG = Object.freeze({
  workflowLogs: {
    container: logsEl,
    button: workflowLogsToggleBtn,
    collapsedText: '展开运行日志',
    expandedText: '收起运行日志',
  },
  schedulerSecondaryStats: {
    container: schedulerSecondaryStats,
    button: schedulerSecondaryToggleBtn,
    collapsedText: '展开扩展指标',
    expandedText: '收起扩展指标',
  },
  manualRunHistory: {
    container: manualRunHistory,
    button: manualRunHistoryToggleBtn,
    collapsedText: '展开补跑记录',
    expandedText: '收起补跑记录',
  },
  regressionSmokeArtifacts: {
    container: regressionSmokeArtifacts,
    button: regressionSmokeArtifactsToggleBtn,
    collapsedText: '展开 Smoke 产物',
    expandedText: '收起 Smoke 产物',
  },
  longTermGovernanceSamples: {
    container: longTermGovernanceSamples,
    button: longTermGovernanceSamplesToggleBtn,
    collapsedText: '展开治理样本',
    expandedText: '收起治理样本',
  },
  exclusionSamples: {
    container: exclusionSamples,
    button: exclusionSamplesToggleBtn,
    collapsedText: '展开诊断样本',
    expandedText: '收起诊断样本',
  },
  lowVolPositiveRatioSamples: {
    container: lowVolPositiveRatioSamples,
    button: lowVolPositiveRatioSamplesToggleBtn,
    collapsedText: '展开观察样本',
    expandedText: '收起观察样本',
  },
});

const OPERATIONS_DETAIL_SECTION_RESOURCE_HINTS = Object.freeze({
  workflowLogs: 'workflow',
  manualRunHistory: 'scheduler',
  schedulerSecondaryStats: 'scheduler',
  regressionSmokeArtifacts: 'scheduler',
  longTermGovernanceQueue: 'scheduler',
  longTermGovernanceSamples: 'dashboard',
  exclusionSamples: 'dashboard',
  lowVolPositiveRatioSamples: 'dashboard',
});

const operationsDetailCollapsedState = {
  workflowLogs: true,
  schedulerSecondaryStats: true,
  manualRunHistory: true,
  regressionSmokeArtifacts: true,
  longTermGovernanceQueue: true,
  longTermGovernanceSamples: true,
  exclusionSamples: true,
  lowVolPositiveRatioSamples: true,
};

const OPERATIONS_DASHBOARD_LAZY_TARGETS = [
  automationLayerStats,
  longTermGovernanceStats,
  longTermGovernanceQueue,
  integrityAuditStats,
  exclusionStats,
  lowVolPositiveRatioStats,
].filter(Boolean);

function renderOperationsHeroSkeleton() {
  if (!operationsHeroMeta || operationsHeroMeta.childElementCount > 0) return;
  operationsHeroMeta.innerHTML = [
    buildStatCard('调度配置', '加载中', '等待调度状态'),
    buildStatCard('调度进程', '加载中', '等待调度状态'),
    buildStatCard('自动化状态', '加载中', '等待自动化摘要'),
    buildStatCard('当前有效快照', '加载中', '等待快照摘要'),
    buildStatCard('原始最新净值日期', '加载中', '等待快照摘要'),
    buildStatCard('最近日常调度', '加载中', '等待最近任务状态'),
  ].join('');
}

function hasWorkflowDetailData() {
  return Array.isArray(cachedWorkflowState?.logs) || Array.isArray(cachedWorkflowState?.logs_tail);
}

function hasSchedulerDetailData() {
  return Boolean(
    cachedSchedulerStatusDetail
    && (
      Object.prototype.hasOwnProperty.call(cachedSchedulerStatusDetail, 'state_schema_version')
      || Object.prototype.hasOwnProperty.call(cachedSchedulerStatusDetail, 'jobs')
      || Object.prototype.hasOwnProperty.call(cachedSchedulerStatusDetail, 'manual_run_history')
      || Array.isArray(cachedSchedulerStatusDetail.loaded_sections)
    )
  );
}

function getExpandedOperationsSchedulerSections() {
  const sections = [];
  Object.entries(OPERATIONS_DETAIL_SECTION_SCHEDULER_SECTIONS).forEach(([key, sectionList]) => {
    if (operationsDetailCollapsedState[key] === false) {
      sections.push(...sectionList);
    }
  });
  return normalizeOperationsSchedulerSections(sections);
}

function getRequestedOperationsSchedulerSections() {
  return normalizeOperationsSchedulerSections([
    ...operationsRequestedSchedulerSections,
    ...getExpandedOperationsSchedulerSections(),
  ]);
}

function shouldRefreshWorkflowDetail(forceHeavy = false) {
  if (forceHeavy) return true;
  if (!hasWorkflowDetailData()) return true;
  return (Date.now() - lastWorkflowDetailRefreshAt) >= OPERATIONS_WORKFLOW_DETAIL_REFRESH_INTERVAL_MS;
}

function shouldRefreshSchedulerDetail(forceHeavy = false, sections = []) {
  const requestedSections = normalizeOperationsSchedulerSections(sections);
  if (!requestedSections.length) return false;
  if (forceHeavy) return true;
  if (!hasSchedulerDetailData()) return true;
  if (requestedSections.some((section) => !hasOperationsSchedulerSectionData(cachedSchedulerStatusDetail, section))) return true;
  return (Date.now() - lastSchedulerDetailRefreshAt) >= OPERATIONS_HEAVY_REFRESH_INTERVAL_MS;
}

function hasSchedulerSectionData(section) {
  return hasOperationsSchedulerSectionData(cachedSchedulerStatusDetail, section);
}

function shouldRefreshDashboardDetail(forceHeavy = false) {
  if (forceHeavy) return true;
  if (!cachedOperationsDashboard) return true;
  if ((cachedOperationsDashboardSampleLimit ?? -1) < getOperationsDashboardSampleLimit()) return true;
  return (Date.now() - lastOperationsHeavyRefreshAt) >= OPERATIONS_HEAVY_REFRESH_INTERVAL_MS;
}

function getOperationsDashboardSampleLimit() {
  const sampleSectionExpanded = (
    operationsDetailCollapsedState.longTermGovernanceSamples === false
    || operationsDetailCollapsedState.exclusionSamples === false
    || operationsDetailCollapsedState.lowVolPositiveRatioSamples === false
  );
  return sampleSectionExpanded
    ? OPERATIONS_DASHBOARD_EXPANDED_SAMPLE_LIMIT
    : OPERATIONS_DASHBOARD_SUMMARY_SAMPLE_LIMIT;
}

function getOperationsSchedulerState() {
  return buildOperationsSchedulerState(
    cachedSchedulerSummaryState,
    cachedSchedulerStatusDetail,
  );
}

function disconnectOperationsLazySectionObserver() {
  if (!operationsLazySectionObserver) return;
  operationsLazySectionObserver.disconnect();
  operationsLazySectionObserver = null;
}

function requestOperationsHeavyResource(resource, { force = false, schedulerSections = [] } = {}) {
  if (resource === 'workflow') {
    operationsWorkflowDetailRequested = true;
  } else if (resource === 'scheduler') {
    operationsSchedulerDetailRequested = true;
    const sections = normalizeOperationsSchedulerSections(
      schedulerSections.length ? schedulerSections : getExpandedOperationsSchedulerSections(),
    );
    sections.forEach((section) => operationsRequestedSchedulerSections.add(section));
  } else if (resource === 'dashboard') {
    operationsDashboardDetailRequested = true;
    disconnectOperationsLazySectionObserver();
  }
  if (document.visibilityState === 'hidden') return;
  refreshOperationsHeavy(force).catch(() => {});
}

function renderOperationsDetailRetryState(container, {
  title = '加载失败',
  message = '请稍后重试',
  resource = '',
  schedulerSections = [],
} = {}) {
  if (!container || !resource) return;
  renderRetryableEmptyState(container, {
    title,
    message,
    actionLabel: '重新加载',
    onRetry: () => requestOperationsHeavyResource(resource, { force: true, schedulerSections }),
  });
}

function renderOperationsInlineState(container, {
  noun = '数据',
  loading = false,
} = {}) {
  if (!container) return;
  const text = loading ? `${noun}补充加载中` : `暂无${noun}`;
  container.innerHTML = `<div class="empty-state-inline">${escapeHtml(text)}</div>`;
}

function renderSchedulerDetailFailureStates(message) {
  const detailMessage = message || '调度详情暂不可用';
  if (manualRunHistory && !hasSchedulerDetailData()) {
    renderOperationsDetailRetryState(manualRunHistory, {
      title: '补跑记录加载失败',
      message: detailMessage,
      resource: 'scheduler',
      schedulerSections: OPERATIONS_DETAIL_SECTION_SCHEDULER_SECTIONS.manualRunHistory,
    });
  }
  if (regressionSmokeArtifacts && !hasSchedulerDetailData()) {
    const smokeSummary = cachedSchedulerSummaryState?.maintenance_summary?.regression_smoke_summary || {};
    const smokeTask = cachedSchedulerSummaryState?.weekly_run?.child_tasks?.regression_smoke || {};
    const hasSmokeArtifacts = Boolean(
      smokeSummary?.summary_path
      || smokeSummary?.artifact_dir
      || smokeTask?.summary_path
      || smokeTask?.artifact_dir
    );
    if (!hasSmokeArtifacts) {
      renderOperationsDetailRetryState(regressionSmokeArtifacts, {
        title: 'Smoke 产物加载失败',
        message: detailMessage,
        resource: 'scheduler',
        schedulerSections: OPERATIONS_DETAIL_SECTION_SCHEDULER_SECTIONS.regressionSmokeArtifacts,
      });
    }
  }
  if (
    longTermGovernanceQueue
    && !hasOperationsSchedulerSectionData(cachedSchedulerStatusDetail, 'long_term_governance')
  ) {
    renderOperationsDetailRetryState(longTermGovernanceQueue, {
      title: '治理队列加载失败',
      message: detailMessage,
      resource: 'scheduler',
      schedulerSections: OPERATIONS_DETAIL_SECTION_SCHEDULER_SECTIONS.longTermGovernanceQueue,
    });
  }
}

function renderDashboardDetailFailureStates(message) {
  const detailMessage = message || '诊断明细暂不可用';
  const dashboard = cachedOperationsDashboard || {};
  if (longTermGovernanceSamples && !dashboard.long_term_governance_summary) {
    renderOperationsDetailRetryState(longTermGovernanceSamples, {
      title: '治理样本加载失败',
      message: detailMessage,
      resource: 'dashboard',
    });
  }
  if (exclusionSamples && !dashboard.selection_exclusion_summary) {
    renderOperationsDetailRetryState(exclusionSamples, {
      title: '未入榜样本加载失败',
      message: detailMessage,
      resource: 'dashboard',
    });
  }
  if (lowVolPositiveRatioSamples && !dashboard.low_vol_positive_ratio_shadow_watch) {
    renderOperationsDetailRetryState(lowVolPositiveRatioSamples, {
      title: '低波观察样本加载失败',
      message: detailMessage,
      resource: 'dashboard',
    });
  }
}

function ensureOperationsLazySectionObserver() {
  if (operationsDashboardDetailRequested) return;
  if (!OPERATIONS_DASHBOARD_LAZY_TARGETS.length) return;
  if (typeof window.IntersectionObserver !== 'function') {
    requestOperationsHeavyResource('dashboard');
    return;
  }
  if (operationsLazySectionObserver) return;
  operationsLazySectionObserver = new window.IntersectionObserver((entries) => {
    const visibleEntries = entries.filter((entry) => entry.isIntersecting || entry.intersectionRatio > 0);
    const shouldLoad = visibleEntries.length > 0;
    if (!shouldLoad) return;
    const longTermVisible = visibleEntries.some((entry) => (
      entry.target === longTermGovernanceStats || entry.target === longTermGovernanceQueue
    ));
    if (longTermVisible) {
      requestOperationsHeavyResource('scheduler', {
        schedulerSections: OPERATIONS_DETAIL_SECTION_SCHEDULER_SECTIONS.longTermGovernanceQueue,
      });
    }
    requestOperationsHeavyResource('dashboard');
  }, {
    rootMargin: '240px 0px',
    threshold: 0.01,
  });
  OPERATIONS_DASHBOARD_LAZY_TARGETS.forEach((element) => {
    operationsLazySectionObserver.observe(element);
  });
}

function applyOperationsDetailSectionState(key) {
  const config = OPERATIONS_DETAIL_SECTION_CONFIG[key];
  if (!config?.container) return;
  const collapsed = operationsDetailCollapsedState[key] !== false;
  config.container.classList.toggle('is-collapsed', collapsed);
  if (config.button) {
    config.button.textContent = collapsed ? config.collapsedText : config.expandedText;
    config.button.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
  }
}

function bindOperationsDetailSectionToggle(key) {
  const config = OPERATIONS_DETAIL_SECTION_CONFIG[key];
  if (!config?.button) return;
  config.button.addEventListener('click', () => {
    operationsDetailCollapsedState[key] = !operationsDetailCollapsedState[key];
    applyOperationsDetailSectionState(key);
    if (operationsDetailCollapsedState[key] === false) {
      const resource = OPERATIONS_DETAIL_SECTION_RESOURCE_HINTS[key];
      if (resource) {
        requestOperationsHeavyResource(resource, {
          schedulerSections: OPERATIONS_DETAIL_SECTION_SCHEDULER_SECTIONS[key] || [],
        });
      }
    }
  });
  applyOperationsDetailSectionState(key);
}

function expandOperationsSectionForAnchor(anchorId) {
  const anchorTarget = document.getElementById(anchorId);
  const disclosure = anchorTarget?.matches('details')
    ? anchorTarget
    : anchorTarget?.closest('details');
  if (disclosure) disclosure.open = true;

  if (anchorId === 'workflowStatus') {
    operationsDetailCollapsedState.workflowLogs = false;
    applyOperationsDetailSectionState('workflowLogs');
    requestOperationsHeavyResource('workflow');
    return;
  }
  if (anchorId === 'schedulerStatusCard') {
    operationsDetailCollapsedState.schedulerSecondaryStats = false;
    applyOperationsDetailSectionState('schedulerSecondaryStats');
    requestOperationsHeavyResource('scheduler', {
      schedulerSections: OPERATIONS_DETAIL_SECTION_SCHEDULER_SECTIONS.schedulerSecondaryStats,
    });
    return;
  }
  if (anchorId === 'regressionSmokeSection') {
    operationsDetailCollapsedState.regressionSmokeArtifacts = false;
    applyOperationsDetailSectionState('regressionSmokeArtifacts');
    requestOperationsHeavyResource('scheduler', {
      schedulerSections: OPERATIONS_DETAIL_SECTION_SCHEDULER_SECTIONS.regressionSmokeArtifacts,
    });
    return;
  }
  if (anchorId === 'longTermGovernanceSection') {
    operationsDetailCollapsedState.longTermGovernanceSamples = false;
    applyOperationsDetailSectionState('longTermGovernanceSamples');
    requestOperationsHeavyResource('scheduler', {
      schedulerSections: OPERATIONS_DETAIL_SECTION_SCHEDULER_SECTIONS.longTermGovernanceQueue,
    });
    requestOperationsHeavyResource('dashboard');
    return;
  }
  if (anchorId === 'exclusionSection') {
    operationsDetailCollapsedState.exclusionSamples = false;
    applyOperationsDetailSectionState('exclusionSamples');
    requestOperationsHeavyResource('dashboard');
    return;
  }
  if (anchorId === 'lowVolPositiveRatioSection') {
    operationsDetailCollapsedState.lowVolPositiveRatioSamples = false;
    applyOperationsDetailSectionState('lowVolPositiveRatioSamples');
    requestOperationsHeavyResource('dashboard');
    return;
  }
  if (anchorId === 'manualRunSection') {
    operationsDetailCollapsedState.manualRunHistory = false;
    applyOperationsDetailSectionState('manualRunHistory');
    requestOperationsHeavyResource('scheduler', {
      schedulerSections: OPERATIONS_DETAIL_SECTION_SCHEDULER_SECTIONS.manualRunHistory,
    });
  }
}

function renderOperationsQuickNav() {
  if (!operationsQuickNav) return;
  const items = [
    { label: '执行入口', hint: '运行模式', anchor: 'runControls' },
    { label: '调试补跑', hint: '补跑记录', anchor: 'manualRunSection' },
    { label: '工作流状态', hint: '日志与状态', anchor: 'workflowStatus' },
    { label: '调度状态', hint: '扩展指标', anchor: 'schedulerStatusCard' },
    { label: '自动化闭环', hint: '最近闭环', anchor: 'automationClosureSection' },
    { label: '自动化分层', hint: '版本分层', anchor: 'automationLayerSection' },
    { label: '完整性审计', hint: '一致性检查', anchor: 'integrityAuditSection' },
    { label: 'Smoke 自检', hint: '非阻断自检', anchor: 'regressionSmokeSection' },
    { label: '长周期治理', hint: '历史覆盖', anchor: 'longTermGovernanceSection' },
    { label: '未入榜诊断', hint: '主因样本', anchor: 'exclusionSection' },
    { label: '低波观察', hint: '正收益口径差', anchor: 'lowVolPositiveRatioSection' },
  ];
  operationsQuickNav.innerHTML = items.map((item) => `
    <a class="active-filter-chip recommendation-list-hint-chip" href="#${escapeHtml(item.anchor)}" data-operations-anchor="${escapeHtml(item.anchor)}">
      <span>${escapeHtml(item.label)}</span>
      <span>${escapeHtml(item.hint)}</span>
    </a>
  `).join('');
  operationsQuickNav.querySelectorAll('[data-operations-anchor]').forEach((element) => {
    element.addEventListener('click', (event) => {
      const anchorId = String(element.dataset.operationsAnchor || '').trim();
      if (!anchorId) return;
      event.preventDefault();
      expandOperationsSectionForAnchor(anchorId);
      const target = document.getElementById(anchorId);
      if (!target) return;
      window.history.replaceState(null, '', `#${anchorId}`);
      target.scrollIntoView({ behavior: 'smooth', block: 'start' });
    });
  });
}

function renderOperationsRunSummary(state, schedulerState) {
  if (!operationsRunSummary) return;
  const workflow = state || {};
  const overview = schedulerState?.scheduler_overview || schedulerState || {};
  const autonomy = schedulerState?.automation_autonomy || {};
  const schedulerRuns = getSchedulerRunMap(schedulerState);
  const latestRun = schedulerRuns.scheduled_daily_bond_refresh || {};
  const manualQueue = autonomy?.manual_run_queue_summary || {};
  const hints = [
    `工作流 ${formatWorkflowStatus(workflow.status || 'unknown')}`,
    `调度 ${formatSchedulerRuntime(overview?.enabled, overview?.scheduler_running)}`,
    `自动化 ${formatAutomationAutonomyState(autonomy?.status || 'unknown')}`,
    `手动排队 ${formatNumber(manualQueue.pending_request_count)}`,
    `最近日更 ${formatScheduledRunStatus(latestRun)}`,
  ];
  renderSummaryHintChips(operationsRunSummary, hints);
}

function formatAutomationAutonomyState(value) {
  return formatWorkflowStatus(value || 'unknown');
}

function automationAutonomyHint(payload) {
  const autonomy = payload || {};
  const manualQueue = autonomy?.manual_run_queue_summary || {};
  const currentTaskLabel = formatOperationsTaskLabel(autonomy?.current_task_name);
  const nextAction = (autonomy.warning_items || []).find(item => item.next_automatic_action_at);
  return [
    autonomy?.summary || '',
    nextAction ? `下次自动处理 ${formatDateTime(nextAction.next_automatic_action_at)}` : '',
    currentTaskLabel ? `当前任务 ${currentTaskLabel}` : '',
    Number(manualQueue?.pending_request_count || 0) > 0 ? `手动排队 ${formatNumber(manualQueue.pending_request_count)} 个` : '',
  ].filter(Boolean).join('；') || '当前无额外说明';
}

function formatCoverageRatioPercent(value) {
  return formatRatioPercent(value);
}

function resolveRiskCoverageSemantics(source) {
  const payload = source || {};
  return {
    ratio: numericValueOrNull(
      payload.risk_1y_effective_coverage_ratio_vs_candidate
      ?? payload.risk_1y_coverage_ratio_vs_candidate
      ?? payload.coverage_ratio_vs_candidate
    ),
    publishMinimum: numericValueOrNull(
      payload.risk_1y_publish_minimum_coverage_ratio
      ?? payload.publish_minimum_coverage_ratio
      ?? payload.minimum_coverage_ratio
    ),
    observationLine: numericValueOrNull(
      payload.risk_1y_observation_coverage_ratio
      ?? payload.observation_coverage_ratio
    ),
    gateLabel: payload.risk_1y_coverage_gate_label || payload.coverage_gate_label || '',
    gateStatus: payload.risk_1y_coverage_gate_status || payload.coverage_gate_status || '',
    gateExplanation: payload.risk_1y_coverage_gate_explanation || payload.coverage_gate_explanation || '',
    observationLineMet: Boolean(
      payload.risk_1y_coverage_observation_line_met
      ?? payload.coverage_observation_line_met
    ),
  };
}

function formatOperationsTaskLabel(taskName) {
  const key = String(taskName || '').trim();
  if (!key) return '';
  const labels = {
    scheduled_daily_bond_refresh: '日更刷新',
    scheduled_intraday_bond_refresh: '盘中补刷新',
    scheduled_weekly_bond_maintenance: '周维护',
    scheduled_snapshot_watchdog_refresh: '快照看护',
  };
  return labels[key] || compactUiText(key.replace(/_/gu, ' '), 18);
}

function getSchedulerRunMap(state) {
  const explicitRuns = state?.latest_runs || state?.last_runs;
  if (explicitRuns && Object.keys(explicitRuns).length) {
    return explicitRuns;
  }
  return {
    scheduled_daily_bond_refresh: state?.daily_run || {},
    scheduled_intraday_bond_refresh: state?.intraday_run || {},
    scheduled_weekly_bond_maintenance: state?.weekly_run || {},
    scheduled_snapshot_watchdog_refresh: state?.snapshot_watchdog_run || {},
  };
}

function setBannerState(element, state = 'neutral') {
  if (!element) return;
  element.classList.remove('info-banner-success', 'info-banner-error', 'info-banner-warning');
  if (state === 'success') {
    element.classList.add('info-banner-success');
  } else if (state === 'error') {
    element.classList.add('info-banner-error');
  } else if (state === 'warning') {
    element.classList.add('info-banner-warning');
  }
}

function setRunActionFeedback(title, message, state = 'neutral') {
  if (!runActionFeedback) return;
  setBannerState(runActionFeedback, state);
  runActionFeedback.innerHTML = `
    <strong>${escapeHtml(title || '反馈')}</strong>
    <span>${escapeHtml(message || '暂无')}</span>
  `;
}

function setElementDisabled(element, disabled) {
  if (!element) return;
  element.disabled = Boolean(disabled);
}

function renderAutomationMode(health) {
  manualOperationsEnabled = Boolean(health?.manual_operations_enabled);

  setElementDisabled(startBtn, !manualOperationsEnabled);
  setElementDisabled(manualDailyBtn, !manualOperationsEnabled);
  setElementDisabled(manualIntradayBtn, !manualOperationsEnabled);
  setElementDisabled(manualWeeklyBtn, !manualOperationsEnabled);
  if (startBtn) {
    startBtn.hidden = !manualOperationsEnabled;
  }
  if (runOptionGrid) {
    runOptionGrid.hidden = !manualOperationsEnabled;
  }

  if (manualRunSection) {
    manualRunSection.hidden = !manualOperationsEnabled;
  }
  if (!manualOperationsEnabled && runActionFeedback) {
    setBannerState(runActionFeedback, 'success');
    runActionFeedback.innerHTML = `
      <strong>自动模式</strong>
      <span>手动入口关闭</span>
    `;
  }
}

function renderAutomationAutonomy(autonomy, health) {
  if (!automationModeBanner) return;
  const payload = autonomy || {};
  const bannerState = payload?.status === 'attention_required'
    ? 'error'
    : payload?.status === 'automatic_recovering' || payload?.status === 'automatic_with_warnings' || Boolean(health?.manual_operations_enabled)
      ? 'warning'
      : 'success';
  setBannerState(automationModeBanner, bannerState);
  automationModeBanner.innerHTML = `
    <strong>${escapeHtml(formatAutomationAutonomyState(payload?.status || (manualOperationsEnabled ? 'automatic_with_warnings' : 'automatic')))}</strong>
    <span>${escapeHtml(automationAutonomyHint(payload))}</span>
  `;
}

function clearPolling() {
  if (!polling) return;
  window.clearTimeout(polling);
  polling = null;
}

function scheduleTick(delay = 3000) {
  if (polling || document.visibilityState === 'hidden') return;
  polling = window.setTimeout(async () => {
    polling = null;
    await tick();
  }, delay);
}

function boolValue(id) {
  return document.getElementById(id).checked;
}

function setStatus(state) {
  operationsWorkflowStatus.renderOperationsWorkflowStatus(state, {
    statusElement: statusMeta,
    narrativeElement: workflowNarrative,
    logsElement: logsEl,
  });
}

function renderScheduler(state, audit) {
  operationsSchedulerStatus.renderOperationsSchedulerStatus(state, audit, {
    elements: {
      schedulerStats,
      schedulerSecondaryStats,
      schedulerNarrative,
      automationClosureStats,
      automationClosureCards,
      automationClosureNarrative,
    },
    getSchedulerRunMap,
    resolveRiskCoverageSemantics,
    formatCoverageRatioPercent,
  });
}

function renderAutomationLayers(layers, publicationStatus = {}) {
  operationsAutomationLayers.renderOperationsAutomationLayers(layers, publicationStatus, {
    statsElement: automationLayerStats,
    narrativeElement: automationLayerNarrative,
  });
}

function renderRegressionSmoke(summary, smokeTask) {
  operationsRegressionSmoke.renderOperationsRegressionSmoke(summary, smokeTask, {
    statsElement: regressionSmokeStats,
    narrativeElement: regressionSmokeNarrative,
    artifactsElement: regressionSmokeArtifacts,
    detailPending: operationsRequestedSchedulerSections.has('maintenance') && !hasSchedulerSectionData('maintenance'),
  });
}

function formatGovernanceQueueSource(value) {
  const key = String(value || '').trim();
  const labels = {
    'publication_registry.automation_layers.latest_successful_run': '最新成功 run',
    'publication_registry.latest_successful_run': '登记最新成功 run',
    latest_report_file: 'latest.json',
  };
  return labels[key] || key || '-';
}

function renderCurrentFinalSelectionGovernanceQueue(queue) {
  if (!longTermGovernanceQueue) return;
  const payload = queue && typeof queue === 'object' ? queue : {};
  if (!Object.keys(payload).length) {
    longTermGovernanceQueue.innerHTML = `
      <section class="operations-stat-section">
        <div class="operations-stat-section-heading">
          <strong>当前最终精选治理队列</strong>
          <span>等待调度分区详情加载</span>
        </div>
      </section>
    `;
    return;
  }
  const actionCounts = payload.codes_by_action_counts && typeof payload.codes_by_action_counts === 'object'
    ? payload.codes_by_action_counts
    : {};
  const actionText = Object.entries(actionCounts)
    .map(([action, count]) => `${action}: ${formatNumber(count)}`)
    .join(' / ');
  const statusText = payload.available ? '可用' : '不可用';
  const cards = [
    buildStatCard('队列状态', statusText, payload.reason || 'ok'),
    buildStatCard('目标 run', payload.target_report_run_id || payload.run_id || '-', formatGovernanceQueueSource(payload.target_report_run_source)),
    buildStatCard('候选代码', formatNumber(payload.candidate_code_total), `队列行 ${formatNumber(payload.queue_total)}`),
  ];
  longTermGovernanceQueue.innerHTML = `
    <section class="operations-stat-section">
      <div class="operations-stat-section-heading">
        <strong>当前最终精选治理队列</strong>
        <span>${escapeHtml(actionText || '暂无动作计数')}；仅用于自动治理参考，不代表正式发布名单。</span>
      </div>
      <div class="stats-grid compact">${cards.join('')}</div>
    </section>
  `;
}

function renderLongTermGovernance(summary) {
  operationsGovernanceSummary.renderOperationsLongTermGovernance(summary, {
    elements: {
      statsElement: longTermGovernanceStats,
      narrativeElement: longTermGovernanceNarrative,
      samplesElement: longTermGovernanceSamples,
    },
    renderInlineState: renderOperationsInlineState,
  });
}

function renderIntegrityAudits(publicationIntegrity, artifactConsistency) {
  operationsDashboardWatch.renderOperationsIntegrityAudits(publicationIntegrity, artifactConsistency, {
    elements: {
      statsElement: integrityAuditStats,
      narrativeElement: integrityAuditNarrative,
    },
  });
}

function renderExclusionSummary(summary) {
  operationsDashboardWatch.renderOperationsExclusionSummary(summary, {
    elements: {
      statsElement: exclusionStats,
      narrativeElement: exclusionNarrative,
      samplesElement: exclusionSamples,
    },
    renderInlineState: renderOperationsInlineState,
  });
}

function renderLowVolPositiveRatioWatch(watch) {
  operationsDashboardWatch.renderOperationsLowVolPositiveRatioWatch(watch, {
    elements: {
      statsElement: lowVolPositiveRatioStats,
      narrativeElement: lowVolPositiveRatioNarrative,
      samplesElement: lowVolPositiveRatioSamples,
    },
    renderInlineState: renderOperationsInlineState,
  });
}

function renderManualRunPanel(schedulerState) {
  const history = Array.isArray(schedulerState?.manual_run_history) ? schedulerState.manual_run_history : [];
  const pendingQueue = Array.isArray(schedulerState?.pending_manual_run_requests) ? schedulerState.pending_manual_run_requests : [];
  const historyCount = Number(schedulerState?.manual_run_history_count || history.length || 0);
  const pendingQueueCount = Number(schedulerState?.pending_manual_run_request_count || pendingQueue.length || 0);
  const autonomy = schedulerState?.automation_autonomy || {};
  const queueSummary = autonomy?.manual_run_queue_summary || {};
  const latest = history[0] || null;
  if (manualRunFeedback) {
    if (Number(queueSummary?.pending_request_count || 0) > 0) {
      setBannerState(manualRunFeedback, 'warning');
      manualRunFeedback.innerHTML = `
        <strong>手动排队</strong>
        <span>${escapeHtml(`待执行 ${formatNumber(queueSummary.pending_request_count)} 个 / ${queueSummary.summary || '等待调度器领取'}`)}</span>
      `;
    } else if (!latest) {
      if ((historyCount > 0 || pendingQueueCount > 0) && !hasSchedulerSectionData('manual_history')) {
        setBannerState(manualRunFeedback, 'warning');
        manualRunFeedback.innerHTML = `
          <strong>补跑</strong>
          <span>${escapeHtml(`历史 ${formatNumber(historyCount)} 条 / 排队 ${formatNumber(pendingQueueCount)} 条，明细补充中`)}</span>
        `;
      } else {
        setBannerState(manualRunFeedback);
        manualRunFeedback.innerHTML = '暂无补跑';
      }
    } else {
      const reason = latest.error ? formatReasonCode(latest.error) : formatReasonCode(latest?.result?.warning_reason || '');
      const feedbackState = latest.status === 'failed'
        ? 'error'
        : latest.status === 'success' || latest.status === 'succeeded'
          ? 'success'
          : latest.status === 'running' || latest.status === 'started'
            ? 'warning'
            : 'neutral';
      setBannerState(manualRunFeedback, feedbackState);
      manualRunFeedback.innerHTML = `
        <strong>补跑</strong>
        <span>${escapeHtml(latest.job_id || '-')} / ${escapeHtml(formatWorkflowStatus(latest.status || 'unknown'))}${reason && reason !== '-' ? ` / ${escapeHtml(reason)}` : ''}</span>
      `;
    }
  }
  if (!manualRunHistory) return;
  if (!history.length && !pendingQueue.length) {
    if ((historyCount > 0 || pendingQueueCount > 0) && !hasSchedulerSectionData('manual_history')) {
      renderOperationsInlineState(manualRunHistory, {
        noun: '补跑记录',
        loading: true,
      });
      return;
    }
    renderOperationsInlineState(manualRunHistory, { noun: '补跑记录' });
    return;
  }
  const pendingCards = pendingQueue.slice(0, 3).map((item) => `
    <article class="data-card">
      <div class="data-card-item">
        <strong>来源</strong>
        <span>手动补跑排队</span>
      </div>
      <div class="data-card-item">
        <strong>任务</strong>
        <span>${escapeHtml(item.job_id || '-')}</span>
      </div>
      <div class="data-card-item">
        <strong>请求时间</strong>
        <span>${escapeHtml(formatDateTime(item.requested_at))}</span>
      </div>
      <div class="data-card-item">
        <strong>请求方</strong>
        <span>${escapeHtml(item.requested_by || '-')}</span>
      </div>
      <div class="data-card-item">
        <strong>状态</strong>
        <span>等待调度器领取</span>
      </div>
      <div class="data-card-item">
        <strong>说明</strong>
        <span>手动压测或补跑请求，不代表生产自动化中断。</span>
      </div>
    </article>
  `).join('');
  const historyCards = history.slice(0, 6).map((item) => `
    <article class="data-card">
      <div class="data-card-item">
        <strong>任务</strong>
        <span>${escapeHtml(item.job_id || '-')}</span>
      </div>
      <div class="data-card-item">
        <strong>状态</strong>
        <span>${escapeHtml(formatWorkflowStatus(item.status || 'unknown'))}</span>
      </div>
      <div class="data-card-item">
        <strong>请求时间</strong>
        <span>${escapeHtml(formatDateTime(item.requested_at))}</span>
      </div>
      <div class="data-card-item">
        <strong>开始时间</strong>
        <span>${escapeHtml(formatDateTime(item.started_at))}</span>
      </div>
      <div class="data-card-item">
        <strong>结束时间</strong>
        <span>${escapeHtml(formatDateTime(item.finished_at))}</span>
      </div>
      <div class="data-card-item">
        <strong>说明</strong>
        <span>${escapeHtml(formatReasonCode(item.error || item?.result?.warning_reason || ''))}</span>
      </div>
    </article>
  `).join('');
  manualRunHistory.innerHTML = `${pendingCards}${historyCards}`;
}

async function triggerManualRun(jobId) {
  if (!manualOperationsEnabled) {
    if (manualRunFeedback) {
      setBannerState(manualRunFeedback, 'success');
      manualRunFeedback.innerHTML = `
        <strong>自动模式</strong>
        <span>补跑入口关闭</span>
      `;
    }
    return;
  }
  try {
    if (manualRunFeedback) {
      setBannerState(manualRunFeedback, 'warning');
      manualRunFeedback.innerHTML = `
        <strong>补跑中</strong>
        <span>${escapeHtml(jobId)}</span>
      `;
    }
    const result = await requestSchedulerManualRun({ job_id: jobId });
    if (manualRunFeedback) {
      setBannerState(manualRunFeedback, 'success');
      manualRunFeedback.innerHTML = `
        <strong>已提交</strong>
        <span>${escapeHtml(result.job_id || jobId)} / ${escapeHtml(formatWorkflowStatus(result.status || 'accepted'))}</span>
      `;
    }
    await tick();
  } catch (error) {
    if (manualRunFeedback) {
      setBannerState(manualRunFeedback, 'error');
      manualRunFeedback.innerHTML = `
        <strong>补跑失败</strong>
        <span>${escapeHtml(error?.message || '调度补跑失败')}</span>
      `;
    }
  }
}

function renderOperationsHero(state, schedulerState, health) {
  const overview = schedulerState?.scheduler_overview || schedulerState || {};
  const autonomy = schedulerState?.automation_autonomy || {};
  const effective = overview?.effective_nav_snapshot || {};
  const navFreshness = overview?.nav_freshness || {};
  const currentTask = overview?.current_task || {};
  const persistedDailyRun = getSchedulerRunMap(schedulerState).scheduled_daily_bond_refresh || {};
  const latestRun = persistedDailyRun.status
    ? persistedDailyRun
    : currentTask?.task_name === 'scheduled_daily_bond_refresh'
      ? { status: 'running', started_at: currentTask.started_at }
      : {};
  const detailPending = !effective.candidate_date && !navFreshness.raw_latest_nav_date;
  const effectiveSnapshotText = effective.candidate_date || (detailPending ? '补充中' : '-');
  const rawLatestNavText = navFreshness.raw_latest_nav_date || (detailPending ? '补充中' : '-');
  if (state) {
    renderRunBadge(operationsHeaderRunBadge, state);
  } else {
    renderUnavailableBadge(operationsHeaderRunBadge, '运行状态', '接口暂不可用');
  }
  if (health) {
    renderHealthBadge(operationsHeaderHealthBadge, health);
  } else {
    renderUnavailableBadge(operationsHeaderHealthBadge, '数据源状态', '接口暂不可用');
  }
  operationsHeroMeta.innerHTML = [
    buildStatCard('调度配置', formatSchedulerConfig(overview?.enabled), '每日增量更新'),
    buildStatCard('调度进程', formatSchedulerRuntime(overview?.enabled, overview?.scheduler_running), '后台调度器状态'),
    buildStatCard('自动化状态', formatAutomationAutonomyState(autonomy?.status || 'unknown'), automationAutonomyHint(autonomy)),
    buildStatCard('当前有效快照', effectiveSnapshotText, detailPending ? '等待调度详情补充' : '数据库最新有效净值'),
    buildStatCard('原始最新净值日期', rawLatestNavText, detailPending ? '等待调度详情补充' : formatNavSnapshotComparison(navFreshness, effective)),
    buildStatCard('最近日常调度', formatScheduledRunStatus(latestRun), formatScheduledRunHint(latestRun)),
  ].join('');
  renderOperationsRunSummary(state, schedulerState);
}

function buildOperationsSchedulerState(summaryState, detailState) {
  const summary = summaryState || {};
  const detail = detailState || {};
  const merged = { ...detail, ...summary };
  for (const key of [
    'automation_closure_digest',
    'scheduler_chain_audit',
    'daily_run',
    'intraday_run',
    'weekly_run',
    'snapshot_watchdog_run',
    'maintenance_summary',
    'manual_run_history',
    'pending_manual_run_requests',
    'jobs',
    'task_closures',
    'child_task_idle_streaks',
    'last_runs',
    'latest_runs',
    'task_run_artifacts',
    'schedule_stability_summary',
    'current_final_selection_governance_queue',
  ]) {
    if (detail[key] !== undefined) {
      merged[key] = detail[key];
    }
  }
  return merged;
}

function renderOperationsDashboardFallbacks(dashboardResult) {
  const reasonMessage = dashboardResult?.reason?.message || '滚动到该区域后自动补充诊断摘要';
  if (!cachedOperationsDashboard?.automation_layers) {
    automationLayerStats.innerHTML = buildStatCard('自动化分层', '加载中', '等待重诊断摘要');
    automationLayerNarrative.innerHTML = `
      <strong>分层</strong>
      <span>${escapeHtml(reasonMessage)}</span>
    `;
  }
  if (!cachedOperationsDashboard?.selection_exclusion_summary) {
    exclusionStats.innerHTML = buildStatCard('未入榜诊断', '加载中', '等待诊断摘要');
    exclusionNarrative.innerHTML = `
      <strong>诊断</strong>
      <span>${escapeHtml(reasonMessage)}</span>
    `;
    renderOperationsInlineState(exclusionSamples, {
      noun: '未入榜样本',
      loading: true,
    });
  }
  if (!cachedOperationsDashboard?.long_term_governance_summary) {
    longTermGovernanceStats.innerHTML = buildStatCard('长周期治理', '加载中', '等待治理摘要');
    longTermGovernanceNarrative.innerHTML = `
      <strong>治理</strong>
      <span>${escapeHtml(reasonMessage)}</span>
    `;
    renderOperationsInlineState(longTermGovernanceSamples, {
      noun: '治理样本',
      loading: true,
    });
  }
  if (longTermGovernanceQueue && !getOperationsSchedulerState()?.current_final_selection_governance_queue) {
    renderCurrentFinalSelectionGovernanceQueue({});
  }
  if (!cachedOperationsDashboard?.publication_integrity && !cachedOperationsDashboard?.artifact_consistency) {
    integrityAuditStats.innerHTML = buildStatCard('完整性审计', '加载中', '等待一致性检查摘要');
    integrityAuditNarrative.innerHTML = `
      <strong>审计</strong>
      <span>${escapeHtml(reasonMessage)}</span>
    `;
  }
  if (!cachedOperationsDashboard?.low_vol_positive_ratio_shadow_watch) {
    lowVolPositiveRatioStats.innerHTML = buildStatCard('低波观察', '加载中', '等待观察摘要');
    lowVolPositiveRatioNarrative.innerHTML = `
      <strong>观察</strong>
      <span>${escapeHtml(reasonMessage)}</span>
    `;
    renderOperationsInlineState(lowVolPositiveRatioSamples, {
      noun: '低波观察样本',
      loading: true,
    });
  }
}

async function refreshOperationsHeavy(forceHeavy = false) {
  if (operationsHeavyRefreshPromise) {
    // A new section or larger sample can be requested while this batch runs.
    // Re-evaluate demand after it settles, including when the batch failed.
    await operationsHeavyRefreshPromise.catch(() => {});
    return refreshOperationsHeavy(forceHeavy);
  }
  const requestedSchedulerSections = getRequestedOperationsSchedulerSections();
  const shouldLoadWorkflow = (
    forceHeavy
    || operationsWorkflowDetailRequested
    || operationsDetailCollapsedState.workflowLogs === false
  ) && shouldRefreshWorkflowDetail(forceHeavy);
  const shouldLoadScheduler = (
    forceHeavy
    || operationsSchedulerDetailRequested
    || requestedSchedulerSections.length > 0
  ) && shouldRefreshSchedulerDetail(forceHeavy, requestedSchedulerSections);
  const shouldLoadDashboard = (
    forceHeavy
    || operationsDashboardDetailRequested
  ) && shouldRefreshDashboardDetail(forceHeavy);
  if (!shouldLoadWorkflow && !shouldLoadScheduler && !shouldLoadDashboard) {
    return Promise.resolve();
  }
  const requestPromise = (async () => {
    const requests = [];
    let loadedDashboardSampleLimit = null;
    if (shouldLoadWorkflow) {
      requests.push({ kind: 'workflow', promise: loadStatus() });
    }
    if (shouldLoadScheduler) {
      requests.push({
        kind: 'scheduler',
        sections: requestedSchedulerSections,
        promise: loadOperationsSchedulerDetailSections(requestedSchedulerSections),
      });
    }
    if (shouldLoadDashboard) {
      requests.push({
        kind: 'dashboard',
        sampleLimit: getOperationsDashboardSampleLimit(),
        promise: loadDashboard({ sampleLimit: getOperationsDashboardSampleLimit() }),
      });
    }
    const results = await Promise.allSettled(requests.map((item) => item.promise));
    let workflowUpdated = false;
    let schedulerUpdated = false;
    let dashboardUpdated = false;
    const failures = {};
    results.forEach((result, index) => {
      const kind = requests[index]?.kind || '';
      if (result.status !== 'fulfilled') {
        if (kind) failures[kind] = result.reason;
        return;
      }
      if (kind === 'workflow') {
        cachedWorkflowState = result.value;
        lastWorkflowDetailRefreshAt = Date.now();
        workflowUpdated = true;
        return;
      }
      if (kind === 'scheduler') {
        cachedSchedulerStatusDetail = mergeOperationsSchedulerDetailPayloads(
          cachedSchedulerStatusDetail,
          result.value,
        );
        lastSchedulerDetailRefreshAt = Date.now();
        schedulerUpdated = true;
        return;
      }
      if (kind === 'dashboard') {
        cachedOperationsDashboard = result.value;
        cachedOperationsDashboardSampleLimit = Number.isFinite(requests[index]?.sampleLimit)
          ? requests[index].sampleLimit
          : null;
        loadedDashboardSampleLimit = cachedOperationsDashboardSampleLimit;
        lastOperationsHeavyRefreshAt = Date.now();
        dashboardUpdated = true;
      }
    });

    if (workflowUpdated) {
      setStatus(cachedWorkflowState || {});
    }

    if (workflowUpdated || schedulerUpdated) {
      const schedulerState = getOperationsSchedulerState();
      const state = cachedWorkflowState || { status: lastKnownWorkflowStatus || 'unknown' };
      renderOperationsHero(state, schedulerState, cachedHealthState);
      renderAutomationAutonomy(schedulerState?.automation_autonomy || {}, cachedHealthState);
      renderScheduler(schedulerState, {});
      renderManualRunPanel(schedulerState);
      renderCurrentFinalSelectionGovernanceQueue(
        schedulerState?.current_final_selection_governance_queue || {}
      );
      renderRegressionSmoke(
        schedulerState?.maintenance_summary?.regression_smoke_summary || {},
        schedulerState?.weekly_run?.child_tasks?.regression_smoke || {},
      );
    }

    if (dashboardUpdated) {
      if (cachedOperationsDashboard?.automation_layers) {
        renderAutomationLayers(
          cachedOperationsDashboard.automation_layers || {},
          cachedOperationsDashboard.publication_status_summary || cachedOperationsDashboard.publication_summary?.status_summary || {},
        );
      }
      if (cachedOperationsDashboard?.long_term_governance_summary) {
        renderLongTermGovernance(cachedOperationsDashboard.long_term_governance_summary || {});
      }
      if (cachedOperationsDashboard?.publication_integrity || cachedOperationsDashboard?.artifact_consistency) {
        renderIntegrityAudits(
          cachedOperationsDashboard.publication_integrity || {},
          cachedOperationsDashboard.artifact_consistency || {},
        );
      }
      if (cachedOperationsDashboard?.selection_exclusion_summary) {
        renderExclusionSummary(cachedOperationsDashboard.selection_exclusion_summary);
      }
      if (cachedOperationsDashboard?.low_vol_positive_ratio_shadow_watch) {
        renderLowVolPositiveRatioWatch(cachedOperationsDashboard.low_vol_positive_ratio_shadow_watch || {});
      }
      renderOperationsDashboardFallbacks();
    }

    if (failures.scheduler) {
      renderSchedulerDetailFailureStates(failures.scheduler?.message || '调度详情暂不可用');
    }

    if (failures.dashboard) {
      renderOperationsDashboardFallbacks({ reason: failures.dashboard });
      renderDashboardDetailFailureStates(failures.dashboard?.message || '诊断明细暂不可用');
    }

    const state = cachedWorkflowState || { status: lastKnownWorkflowStatus || 'unknown' };
    if (state.status === 'running' && !forceHeavy && shouldLoadWorkflow) {
      scheduleTick(OPERATIONS_LIGHT_POLL_MS);
    }
    if (
      dashboardUpdated
      && Number.isFinite(loadedDashboardSampleLimit)
      && loadedDashboardSampleLimit < getOperationsDashboardSampleLimit()
      && document.visibilityState !== 'hidden'
    ) {
      window.setTimeout(() => {
        refreshOperationsHeavy().catch(() => {});
      }, 0);
    }
  })();
  operationsHeavyRefreshPromise = requestPromise.finally(() => {
    operationsHeavyRefreshPromise = null;
  });
  return operationsHeavyRefreshPromise;
}

async function refreshOperations(options = {}) {
  const forceHeavy = Boolean(options.forceHeavy);
  const requests = [
    loadBootstrapSummary(),
    loadSchedulerStatus(),
  ];
  const results = await Promise.allSettled(requests);
  const [bootstrapResult, schedulerResult] = results;
  const bootstrap = bootstrapResult.status === 'fulfilled' ? bootstrapResult.value : null;
  const stateSummary = bootstrap ? getBootstrapWorkflow(bootstrap) : null;
  const schedulerSummaryState = schedulerResult.status === 'fulfilled' ? schedulerResult.value : null;
  const health = bootstrap ? getBootstrapHealth(bootstrap) : null;
  if (health) {
    cachedHealthState = health;
  }
  if (schedulerSummaryState) {
    cachedSchedulerSummaryState = schedulerSummaryState;
  }
  if (stateSummary) {
    cachedWorkflowState = {
      ...(cachedWorkflowState || {}),
      ...stateSummary,
    };
  }
  const state = cachedWorkflowState || stateSummary;
  const schedulerState = getOperationsSchedulerState();

  const resolvedHealth = cachedHealthState || health;
  renderOperationsHero(state, schedulerState, resolvedHealth);
  renderAutomationMode(resolvedHealth);
  renderAutomationAutonomy(schedulerState?.automation_autonomy || {}, resolvedHealth);

  if (state) {
    setStatus(state);
  } else {
    operationsWorkflowStatus.renderOperationsWorkflowStatusFailure(bootstrapResult.reason, {
      statusElement: statusMeta,
      narrativeElement: workflowNarrative,
      logsElement: logsEl,
      onRetry: () => tick({ forceHeavy: true }),
    });
  }

  if (schedulerSummaryState) {
    renderScheduler(schedulerState, {});
    renderManualRunPanel(schedulerState);
    renderCurrentFinalSelectionGovernanceQueue(
      schedulerState?.current_final_selection_governance_queue || {}
    );
  } else {
    renderRetryableEmptyState(schedulerStats, {
      title: '调度状态加载失败',
      message: schedulerResult.reason?.message || '调度接口暂不可用',
      actionLabel: '重新加载',
      onRetry: () => tick({ forceHeavy: true }),
    });
    if (schedulerSecondaryStats) schedulerSecondaryStats.innerHTML = '';
    schedulerNarrative.innerHTML = `
      <strong>调度</strong>
      <span>${escapeHtml(schedulerResult.reason?.message || '暂不可用')}</span>
    `;
    if (manualRunFeedback) {
      setBannerState(manualRunFeedback, 'error');
      manualRunFeedback.innerHTML = '<strong>补跑</strong><span>暂不可用</span>';
    }
    if (manualRunHistory) {
      manualRunHistory.innerHTML = '';
    }
  }

  renderRegressionSmoke(
    schedulerState?.maintenance_summary?.regression_smoke_summary || {},
    schedulerState?.weekly_run?.child_tasks?.regression_smoke || {},
  );
  renderOperationsDashboardFallbacks();

  refreshOperationsHeavy(forceHeavy).catch(() => {});

  const workflowState = state || { status: 'unknown' };
  lastKnownWorkflowStatus = workflowState.status || 'unknown';
  return workflowState;
}

async function tick(options = {}) {
  clearPolling();
  if (document.visibilityState === 'hidden') {
    return;
  }
  const state = await refreshOperations(options);
  if (state.status === 'running') {
    scheduleTick(OPERATIONS_LIGHT_POLL_MS);
  }
}

document.getElementById('startBtn').addEventListener('click', async () => {
  if (!manualOperationsEnabled) {
    setRunActionFeedback('自动模式', '手动入口关闭', 'success');
    return;
  }
  try {
    setRunActionFeedback('执行中', '请求已提交', 'warning');
    await startWorkflow({
      run_collection: boolValue('run_collection'),
      run_screening: boolValue('run_screening'),
      reuse_existing_outputs: boolValue('reuse_existing_outputs'),
    });
    setRunActionFeedback('已提交', '任务已启动', 'success');
    await tick({ forceHeavy: true });
  } catch (error) {
    setRunActionFeedback('执行失败', error?.message || '执行失败', 'error');
  }
});

if (manualDailyBtn) {
  manualDailyBtn.addEventListener('click', async () => {
    await triggerManualRun('daily_refresh');
  });
}

if (manualIntradayBtn) {
  manualIntradayBtn.addEventListener('click', async () => {
    await triggerManualRun('intraday_refresh');
  });
}

if (manualWeeklyBtn) {
  manualWeeklyBtn.addEventListener('click', async () => {
    await triggerManualRun('weekly_maintenance');
  });
}

Object.keys(OPERATIONS_DETAIL_SECTION_CONFIG).forEach((key) => {
  bindOperationsDetailSectionToggle(key);
});

document.getElementById('automationClosureSection')?.addEventListener('toggle', (event) => {
  if (event.currentTarget.open) {
    requestOperationsHeavyResource('scheduler', {
      schedulerSections: ['task_closures', 'maintenance'],
    });
  }
});

document.getElementById('refreshBtn').addEventListener('click', async () => {
  await tick({ forceHeavy: true });
});

document.addEventListener('visibilitychange', async () => {
  if (document.visibilityState === 'hidden') {
    clearPolling();
    return;
  }
  await tick();
});

window.addEventListener('beforeunload', clearPolling);
window.addEventListener('beforeunload', disconnectOperationsLazySectionObserver);
window.addEventListener('pagehide', () => {
  clearPolling();
  disconnectOperationsLazySectionObserver();
});

renderOperationsHeroSkeleton();
renderOperationsQuickNav();
if (operationsDetailCollapsedState.workflowLogs === false) {
  operationsWorkflowDetailRequested = true;
}
ensureOperationsLazySectionObserver();
tick();

window.addEventListener('load', () => {
  const anchorId = String(window.location.hash || '').replace(/^#/u, '').trim();
  if (!anchorId) return;
  expandOperationsSectionForAnchor(anchorId);
  const target = document.getElementById(anchorId);
  target?.scrollIntoView({ behavior: 'auto', block: 'start' });
});
