const taskSummaryHeaderRunBadge = document.getElementById('taskSummaryHeaderRunBadge');
const taskSummaryHeaderHealthBadge = document.getElementById('taskSummaryHeaderHealthBadge');
const taskSummaryHeroMeta = document.getElementById('taskSummaryHeroMeta');
const taskSummaryMeta = document.getElementById('taskSummaryMeta');
const taskSummaryStats = document.getElementById('taskSummaryStats');
const taskSummaryNarrative = document.getElementById('taskSummaryNarrative');
const taskSummaryQuickNav = document.getElementById('taskSummaryQuickNav');
const taskStageStats = document.getElementById('taskStageStats');
const taskStageTable = document.getElementById('taskStageTable');
const taskAuditToggleBtn = document.getElementById('taskAuditToggleBtn');
const taskAuditNarrative = document.getElementById('taskAuditNarrative');
const taskAuditTable = document.getElementById('taskAuditTable');
const taskReasonCoreStats = document.getElementById('taskReasonCoreStats');
const taskReasonExtendedToggleBtn = document.getElementById('taskReasonExtendedToggleBtn');
const taskReasonStats = document.getElementById('taskReasonStats');
const taskReasonNarrative = document.getElementById('taskReasonNarrative');

const TASK_STAGE_SORTABLE_COLUMNS = Object.freeze([
  '阶段',
  '范围',
  '输入样本数',
  '通过数',
  '淘汰数',
  '淘汰率(%)',
]);

const TASK_STAGE_INITIAL_SORT_DIRECTIONS = Object.freeze({
  '输入样本数': 'desc',
  '通过数': 'desc',
  '淘汰数': 'desc',
  '淘汰率(%)': 'desc',
});

const taskSummarySectionConfig = Object.freeze({
  audit: {
    containers: [taskAuditNarrative, taskAuditTable],
    button: taskAuditToggleBtn,
    collapsedText: '展开固定阈值审计',
    expandedText: '收起固定阈值审计',
    defaultCollapsed: window.innerWidth <= 720,
  },
  reasonExtended: {
    containers: [taskReasonStats],
    button: taskReasonExtendedToggleBtn,
    collapsedText: '展开扩展诊断',
    expandedText: '收起扩展诊断',
    defaultCollapsed: true,
  },
});

const taskSummarySectionCollapsedState = Object.fromEntries(
  Object.entries(taskSummarySectionConfig).map(([key, config]) => [key, Boolean(config.defaultCollapsed)])
);

function applyTaskSummarySectionState(key) {
  const config = taskSummarySectionConfig[key];
  if (!config) return;
  const collapsed = taskSummarySectionCollapsedState[key] !== false;
  (config.containers || []).forEach((container) => {
    if (!container) return;
    container.classList.toggle('is-collapsed', collapsed);
  });
  if (config.button) {
    config.button.textContent = collapsed ? config.collapsedText : config.expandedText;
    config.button.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
  }
}

function bindTaskSummarySectionToggle(key) {
  const config = taskSummarySectionConfig[key];
  if (!config?.button) return;
  config.button.addEventListener('click', () => {
    taskSummarySectionCollapsedState[key] = !taskSummarySectionCollapsedState[key];
    applyTaskSummarySectionState(key);
  });
  applyTaskSummarySectionState(key);
}

function expandTaskSummarySectionForAnchor(anchorId) {
  if (anchorId === 'taskAuditSection') {
    taskSummarySectionCollapsedState.audit = false;
    applyTaskSummarySectionState('audit');
    return;
  }
  if (anchorId === 'taskReasonExtendedSection') {
    taskSummarySectionCollapsedState.reasonExtended = false;
    applyTaskSummarySectionState('reasonExtended');
  }
}

function renderTaskSummaryQuickNav() {
  if (!taskSummaryQuickNav) return;
  const items = [
    { label: '主链阶段', href: '#taskStageCard', hint: '阶段压缩' },
    { label: '固定阈值审计', href: '#taskAuditSection', hint: '审计分支' },
    { label: '主原因摘要', href: '#taskReasonCard', hint: '主原因' },
    { label: '扩展诊断', href: '#taskReasonExtendedSection', hint: '责任指标' },
  ];
  taskSummaryQuickNav.innerHTML = items.map((item) => `
    <a class="active-filter-chip recommendation-list-hint-chip" href="${escapeHtml(item.href)}" data-task-summary-anchor="${escapeHtml(item.href.slice(1))}">
      <span>${escapeHtml(item.label)}</span>
      <span>${escapeHtml(item.hint)}</span>
    </a>
  `).join('');
  taskSummaryQuickNav.querySelectorAll('[data-task-summary-anchor]').forEach((element) => {
    element.addEventListener('click', (event) => {
      const anchorId = String(element.dataset.taskSummaryAnchor || '').trim();
      if (!anchorId) return;
      event.preventDefault();
      expandTaskSummarySectionForAnchor(anchorId);
      const target = document.getElementById(anchorId);
      if (!target) return;
      window.history.replaceState(null, '', `#${anchorId}`);
      target.scrollIntoView({ behavior: 'smooth', block: 'start' });
    });
  });
}

function formatCoverageRatioPercent(value) {
  return formatRatioPercent(value);
}

function taskSummaryScopeText(contract) {
  const label = String(contract?.scope_label || '').trim();
  const boundary = String(contract?.truth_boundary || '').trim();
  return [label, boundary].filter(Boolean).join('：');
}

function firstReasonText(stage) {
  const topReasons = Array.isArray(stage?.top_reasons) ? stage.top_reasons : [];
  if (!topReasons.length) return '暂无';
  return `${topReasons[0].display_reason || topReasons[0].reason}（${formatNumber(topReasons[0].count)}只）`;
}

function leadReasonText(stage) {
  const leadReason = stage?.lead_reason || stage?.bottleneck_reason || null;
  if (leadReason && (leadReason.display_reason || leadReason.reason)) {
    return `${leadReason.display_reason || leadReason.reason}（${formatNumber(leadReason.count)}只）`;
  }
  return firstReasonText(stage);
}

function stageSummaryCard(stage, hint) {
  return buildStatCard(
    stage?.stage_label || '-',
    `${formatNumber(stage?.passed_count || 0)} / ${formatNumber(stage?.input_count || 0)}`,
    hint || `淘汰 ${formatNumber(stage?.removed_count || 0)} 只，淘汰率 ${formatPercent(stage?.removed_rate_pct)}`
  );
}

function stageTableRows(rows) {
  return rows.map((item) => ({
    阶段: item.stage_label,
    范围: item.scope,
    输入样本数: item.input_count,
    通过数: item.passed_count,
    淘汰数: item.removed_count,
    '淘汰率(%)': item.removed_rate_pct,
    主原因1: firstReasonText(item),
    主原因2: Array.isArray(item.top_reasons) && item.top_reasons[1]
      ? `${item.top_reasons[1].display_reason || item.top_reasons[1].reason}（${formatNumber(item.top_reasons[1].count)}只）`
      : '暂无',
    说明: item.note || '',
  }));
}

function renderTaskStageTable(container, rows, tableClass, options = {}) {
  if (!container) return;
  const items = Array.isArray(rows) ? rows : [];
  if (!items.length) {
    container.innerHTML = '<div class="empty-state-inline">暂无数据</div>';
    return;
  }
  renderCompactExpandableTable(container, items, {
    coreColumns: ['阶段', '范围', '输入样本数', '通过数', '淘汰数', '淘汰率(%)', '主原因1', '主原因2', '详情'],
    detailGroups: [
      {
        title: '阶段说明',
        fields: ['说明'],
      },
    ],
    tableClass: `data-table task-summary-table compact-expand-table ${tableClass || ''}`.trim(),
    wrapClass: 'task-summary-table-wrap compact-expand-wrap',
    toolbarClass: 'favorite-table-toolbar compact-table-toolbar',
    toolbarTitle: '阶段紧凑视图',
    toolbarHint: '主表保留阶段核心指标，详细说明按行展开查看',
    showToolbar: options.showToolbar !== false,
    renderCards: true,
    mobileCardColumns: ['阶段', '范围', '通过数', '淘汰数', '淘汰率(%)', '主原因1'],
    sortableColumns: TASK_STAGE_SORTABLE_COLUMNS,
    initialSortDirections: TASK_STAGE_INITIAL_SORT_DIRECTIONS,
    sortState: renderTaskStageTable.sortState?.get(container) || { column: null, direction: null },
    rowKey: (row) => `${row['阶段'] || ''}::${row['范围'] || ''}`,
    expandedRows: renderTaskStageTable.expandedRows?.get(container) || new Set(),
    cellRenderers: {
      详情: (_, row) => {
        const key = `${row['阶段'] || ''}::${row['范围'] || ''}`;
        const expanded = (renderTaskStageTable.expandedRows?.get(container) || new Set()).has(key);
        return `
          <button
            type="button"
            class="favorite-expand-toggle ${expanded ? 'is-expanded' : ''}"
            data-compact-expand-key="${escapeHtml(key)}"
            aria-expanded="${expanded ? 'true' : 'false'}"
          >
            ${expanded ? '收起' : '展开'}
          </button>
        `;
      },
    },
    onSort: (nextSortState) => {
      const map = renderTaskStageTable.sortState || new WeakMap();
      map.set(container, nextSortState);
      renderTaskStageTable.sortState = map;
      renderTaskStageTable(container, items, tableClass, options);
    },
    onToggleExpand: (key) => {
      const map = renderTaskStageTable.expandedRows || new WeakMap();
      const next = new Set(map.get(container) || new Set());
      if (next.has(key)) next.delete(key);
      else next.add(key);
      map.set(container, next);
      renderTaskStageTable.expandedRows = map;
      renderTaskStageTable(container, items, tableClass, options);
    },
    onToggleExpandAll: (expanded, visibleRows) => {
      const map = renderTaskStageTable.expandedRows || new WeakMap();
      map.set(
        container,
        expanded
          ? new Set((Array.isArray(visibleRows) ? visibleRows : []).map((row) => `${row['阶段'] || ''}::${row['范围'] || ''}`).filter(Boolean))
          : new Set(),
      );
      renderTaskStageTable.expandedRows = map;
      renderTaskStageTable(container, items, tableClass, options);
    },
  });
}

async function initTaskSummaryPage() {
  void loadBootstrapSummary().then((payload) => {
    renderRunBadge(taskSummaryHeaderRunBadge, getBootstrapWorkflow(payload));
    renderHealthBadge(taskSummaryHeaderHealthBadge, getBootstrapHealth(payload));
  }).catch(() => {
    renderUnavailableBadge(taskSummaryHeaderRunBadge, '运行状态', '接口暂不可用');
    renderUnavailableBadge(taskSummaryHeaderHealthBadge, '数据源状态', '接口暂不可用');
  });
  const [dashboardResult] = await Promise.allSettled([
    loadDashboardTaskSummaryDetail(),
  ]);

  if (dashboardResult.status !== 'fulfilled') {
    taskSummaryHeroMeta.innerHTML = buildStatCard('当前主发布版本', '加载失败', '稍后重试');
    renderRetryableEmptyState(taskStageTable, {
      title: '阶段概览加载失败',
      message: dashboardResult.reason?.message || '请稍后重试',
      actionLabel: '重新加载',
      onRetry: initTaskSummaryPage,
    });
    taskAuditNarrative.innerHTML = '<strong>审计</strong><span>暂不可用</span>';
    renderRetryableEmptyState(taskAuditTable, {
      title: '固定阈值审计加载失败',
      message: dashboardResult.reason?.message || '请稍后重试',
      actionLabel: '重新加载',
      onRetry: initTaskSummaryPage,
    });
    return;
  }

  const dashboard = dashboardResult.value || {};
  const scopeContract = dashboard?.scope_contract || {};
  const scopeText = taskSummaryScopeText(scopeContract);
  const publishFunnel = dashboard?.publish_funnel_summary || {};
  const stageAudit = dashboard?.publish_stage_audit || {};
  const publishReasonSummary = dashboard?.publish_reason_summary || {};
  const longTermGovernance = dashboard?.long_term_governance_summary || {};
  const portfolioSummary = dashboard?.portfolio_suggestions || {};
  const displayPoolCount = portfolioSummary?.fund_count || (Array.isArray(portfolioSummary?.funds) ? portfolioSummary.funds.length : 0);
  const focusMetricSummary = Array.isArray(publishReasonSummary?.focus_metric_summary) ? publishReasonSummary.focus_metric_summary : [];
  const focusMetricMap = Object.fromEntries(focusMetricSummary.map((item) => [item.metric_key, item]));
  const hasPublishReasonSummary = Boolean(publishReasonSummary?.available);
  const stageRows = Array.isArray(stageAudit?.stage_rows) ? stageAudit.stage_rows : [];
  const mainChainKeys = Array.isArray(stageAudit?.main_chain_stage_keys) ? stageAudit.main_chain_stage_keys : ['base_gate', 'dynamic_baseline', 'dynamic_post_review', 'final_selection'];
  const auditStageKeys = Array.isArray(stageAudit?.audit_stage_keys) ? stageAudit.audit_stage_keys : ['fixed_baseline', 'fixed_post_review'];
  const mainChainRows = hasPublishReasonSummary
    ? (Array.isArray(publishReasonSummary?.main_chain) ? publishReasonSummary.main_chain : [])
    : stageRows.filter((item) => mainChainKeys.includes(item?.stage_key));
  const auditStageRows = hasPublishReasonSummary
    ? (Array.isArray(publishReasonSummary?.audit_branch) ? publishReasonSummary.audit_branch : [])
    : stageRows.filter((item) => auditStageKeys.includes(item?.stage_key));

  const baseGateStage = mainChainRows.find((item) => item?.stage_key === 'base_gate') || stageRows.find((item) => item?.stage_key === 'base_gate') || {};
  const dynamicBaselineStage = mainChainRows.find((item) => item?.stage_key === 'dynamic_baseline') || stageRows.find((item) => item?.stage_key === 'dynamic_baseline') || {};
  const dynamicPostReviewStage = mainChainRows.find((item) => item?.stage_key === 'dynamic_post_review') || stageRows.find((item) => item?.stage_key === 'dynamic_post_review') || {};
  const finalSelectionStage = mainChainRows.find((item) => item?.stage_key === 'final_selection') || stageRows.find((item) => item?.stage_key === 'final_selection') || {};
  const fixedBaselineStage = auditStageRows.find((item) => item?.stage_key === 'fixed_baseline') || stageRows.find((item) => item?.stage_key === 'fixed_baseline') || {};
  const primaryBottleneckStage = publishReasonSummary?.primary_bottleneck || dynamicBaselineStage || {};
  const publishHeadlineHighlights = Array.isArray(publishReasonSummary?.headline_highlights)
    ? publishReasonSummary.headline_highlights.filter(Boolean)
    : [];
  const publishNarrativeSummary = publishReasonSummary?.narrative_summary || {};
  const exclusionSummary = dashboard?.selection_exclusion_summary || {};
  const topReasonGroup = Array.isArray(exclusionSummary?.reason_group_preview) && exclusionSummary.reason_group_preview.length
    ? `${exclusionSummary.reason_group_preview[0].主原因分组}（${formatNumber(exclusionSummary.reason_group_preview[0].数量)}只）`
    : '暂无';
  const topQualityFactorGroup = Array.isArray(exclusionSummary?.quality_factor_group_preview) && exclusionSummary.quality_factor_group_preview.length
    ? `${exclusionSummary.quality_factor_group_preview[0].质量因子组}（${formatNumber(exclusionSummary.quality_factor_group_preview[0].数量)}只）`
    : '暂无';
  const topQualityFactorDetail = Array.isArray(exclusionSummary?.quality_factor_detail_preview) && exclusionSummary.quality_factor_detail_preview.length
    ? `${exclusionSummary.quality_factor_detail_preview[0].质量细分因子}（${formatNumber(exclusionSummary.quality_factor_detail_preview[0].命中样本数)}只）`
    : '暂无';
  const topQualityOverlapCombo = Array.isArray(exclusionSummary?.quality_overlap_combo_preview) && exclusionSummary.quality_overlap_combo_preview.length
    ? `${formatNumber(exclusionSummary.quality_overlap_combo_preview[0].样本数)}只 / ${formatNumber(exclusionSummary.quality_overlap_combo_preview[0].因子数)}因子`
    : '暂无';
  const topHighScoreFrictionReason = Array.isArray(exclusionSummary?.high_score_friction_reason_preview) && exclusionSummary.high_score_friction_reason_preview.length
    ? `${exclusionSummary.high_score_friction_reason_preview[0].主排除原因}（${formatNumber(exclusionSummary.high_score_friction_reason_preview[0].样本数)}只）`
    : '暂无';
  const publishStatusTop = Array.isArray(exclusionSummary?.status_split_preview)
    ? exclusionSummary.status_split_preview.find((item) => item?.状态维度 === '发布结论状态')
    : null;
  const officialPublishCount = firstPresentValue(publishFunnel.official_publish_count, dashboard?.counts?.final_count);

  taskSummaryHeroMeta.innerHTML = [
    buildStatCard('当前主发布版本', dashboard?.run_id || '-', '任务摘要读取的最新运行批次'),
    buildStatCard('有效快照', dashboard?.scheduler_overview?.latest_screened_snapshot?.candidate_date || dashboard?.tracker_summary?.latest_nav_snapshot_date || '-', '当前发布口径'),
    buildStatCard('正式主榜', formatNumber(officialPublishCount), '完整正式推荐名单'),
    buildStatCard('展示池', formatNumber(displayPoolCount), '推荐页压缩展示'),
  ].join('');

  taskSummaryMeta.textContent = [
    `版本 ${dashboard?.run_id || '-'}`,
    scopeContract?.scope_label || '任务摘要快照',
    `正式主榜 ${formatNumber(publishFunnel.official_publish_count ?? dashboard?.counts?.final_count)}`,
    `展示池 ${formatNumber(displayPoolCount)}`,
  ].filter(Boolean).join(' / ');

  taskSummaryStats.innerHTML = [
    buildStatCard('项目治理候选池', formatNumber(publishFunnel.governance_candidate_pool_count), '最新任务入口样本'),
    buildStatCard('基础门禁通过', formatNumber(baseGateStage?.passed_count), firstReasonText(baseGateStage)),
    buildStatCard('动态阈值基准通过', formatNumber(publishFunnel.raw_dynamic_baseline_pass_count ?? dynamicBaselineStage?.passed_count), firstReasonText(dynamicBaselineStage)),
    buildStatCard('自动发布门禁通过', formatNumber(dynamicPostReviewStage?.passed_count), firstReasonText(dynamicPostReviewStage)),
    buildStatCard('最终正式主榜', formatNumber(finalSelectionStage?.passed_count ?? publishFunnel.official_publish_count), firstReasonText(finalSelectionStage)),
    buildStatCard('长周期就绪率', formatCoverageRatioPercent(longTermGovernance?.full_feature_ready_coverage_ratio), longTermGovernance?.blocking_reason || '当前无明显阻塞'),
    buildStatCard('长周期治理样本', formatNumber(longTermGovernance?.neutralized_fund_count), `历史缺口 ${formatNumber((longTermGovernance?.annual_history_missing_total ?? 0) + (longTermGovernance?.rank_history_missing_total ?? 0))} / 停更 ${formatNumber(longTermGovernance?.full_feature_stale_fund_count)}`),
  ].join('');

  taskSummaryNarrative.innerHTML = `
    <strong>摘要</strong>
    <span>${escapeHtml(
      hasPublishReasonSummary
        ? [
            publishNarrativeSummary?.overview,
            publishNarrativeSummary?.main_chain,
            scopeText,
            `展示池 ${formatNumber(displayPoolCount)} 只，来自正式主榜压缩展示`,
            longTermGovernance?.headline,
            publishHeadlineHighlights[2],
            publishHeadlineHighlights[3],
          ].filter(Boolean).join('；')
        : [
            `候选 ${formatNumber(publishFunnel.governance_candidate_pool_count)} 只`,
            `主榜 ${formatNumber(publishFunnel.official_publish_count)} 只`,
            `动态 ${formatNumber(publishFunnel.raw_dynamic_baseline_pass_count ?? dynamicBaselineStage?.passed_count)} 只`,
            `固定 ${formatNumber(fixedBaselineStage?.passed_count)} 只`,
            scopeText,
            `长周期全特征就绪率 ${formatCoverageRatioPercent(longTermGovernance?.full_feature_ready_coverage_ratio)}`,
          ].join('；')
    )}</span>
  `;

  taskStageStats.innerHTML = [
    stageSummaryCard(baseGateStage),
    stageSummaryCard(dynamicBaselineStage),
    stageSummaryCard(dynamicPostReviewStage),
    stageSummaryCard(finalSelectionStage),
  ].join('');

  taskStageTable.classList.add('task-summary-table-shell');
  renderTaskStageTable(
    taskStageTable,
    stageTableRows(mainChainRows).map((row) => ({ ...row, 详情: '' })),
    'task-summary-main-table',
    { showToolbar: true },
  );

  taskAuditNarrative.innerHTML = `
    <strong>审计</strong>
    <span>${escapeHtml(
      hasPublishReasonSummary && auditStageRows.length
        ? `${fixedBaselineStage?.stage_label || '固定阈值基准'}保留 ${formatNumber(fixedBaselineStage?.passed_count)} 只；首要原因为 ${leadReasonText(fixedBaselineStage)}`
        : `固定阈值 ${formatNumber(fixedBaselineStage?.passed_count)} 只`
    )}</span>
  `;

  if (auditStageRows.length) {
    taskAuditTable.classList.add('task-summary-table-shell');
    renderTaskStageTable(
      taskAuditTable,
      stageTableRows(auditStageRows).map((row) => ({ ...row, 详情: '' })),
      'task-summary-audit-table',
      { showToolbar: false },
    );
    taskAuditToggleBtn?.removeAttribute('hidden');
  } else {
    taskAuditTable.innerHTML = '<div class="empty-state-inline">暂无审计阶段</div>';
    taskSummarySectionCollapsedState.audit = false;
    applyTaskSummarySectionState('audit');
    taskAuditToggleBtn?.setAttribute('hidden', 'hidden');
  }

  taskReasonCoreStats.innerHTML = [
    buildStatCard('主瓶颈阶段', primaryBottleneckStage?.stage_label || '-', leadReasonText(primaryBottleneckStage)),
    buildStatCard('基础门禁首要原因', leadReasonText(baseGateStage), `淘汰 ${formatNumber(baseGateStage?.removed_count)} 只`),
    buildStatCard('动态阈值首要原因', leadReasonText(dynamicBaselineStage), `淘汰 ${formatNumber(dynamicBaselineStage?.removed_count)} 只`),
    buildStatCard('自动发布门禁首要原因', leadReasonText(dynamicPostReviewStage), `淘汰 ${formatNumber(dynamicPostReviewStage?.removed_count)} 只`),
    buildStatCard('最终精选首要原因', leadReasonText(finalSelectionStage), `淘汰 ${formatNumber(finalSelectionStage?.removed_count)} 只`),
  ].join('');

  taskReasonStats.innerHTML = [
    buildStatCard('收益与评分主责拦截', formatNumber((focusMetricMap.annual_return?.intercepted_count ?? 0) + (focusMetricMap.score?.intercepted_count ?? 0)), `年化 ${formatNumber(focusMetricMap.annual_return?.intercepted_count)} / 评分 ${formatNumber(focusMetricMap.score?.intercepted_count)}`),
    buildStatCard('风险与经理门禁拦截', formatNumber((focusMetricMap.sharpe?.intercepted_count ?? 0) + (focusMetricMap.positive_ratio?.intercepted_count ?? 0) + (focusMetricMap.manager_stability?.intercepted_count ?? 0)), `夏普 ${formatNumber(focusMetricMap.sharpe?.intercepted_count)} / 正收益 ${formatNumber(focusMetricMap.positive_ratio?.intercepted_count)} / 经理 ${formatNumber(focusMetricMap.manager_stability?.intercepted_count)}`),
    buildStatCard('未入榜主原因分组', topReasonGroup, '按结构性/投资性/数据/质量/自动发布门禁归并'),
    buildStatCard('高分摩擦样本', formatNumber(exclusionSummary?.high_score_friction_count), topHighScoreFrictionReason),
    buildStatCard('未入榜主发布状态', publishStatusTop ? `${publishStatusTop.状态值}（${formatNumber(publishStatusTop.数量)}只）` : '暂无', '来自标准筛选审计状态拆分'),
    buildStatCard('质量拦截首要因子', topQualityFactorGroup, '仅拆解质量筛选拦截内部主导因素'),
  ].join('');

  taskReasonNarrative.innerHTML = `
    <strong>主因</strong>
    <span>${escapeHtml([
      hasPublishReasonSummary && primaryBottleneckStage?.stage_label
        ? `主瓶颈 ${primaryBottleneckStage.stage_label}`
        : '',
      hasPublishReasonSummary && primaryBottleneckStage?.explanation
        ? primaryBottleneckStage.explanation
        : '',
      finalSelectionStage?.explanation || '',
      publishReasonSummary?.responsibility_chain_headline || '',
      focusMetricMap.annual_return ? `年化 ${formatNumber(focusMetricMap.annual_return.intercepted_count)} 只` : '',
      focusMetricMap.score ? `评分 ${formatNumber(focusMetricMap.score.intercepted_count)} 只` : '',
      focusMetricMap.positive_ratio ? `正收益 ${formatNumber(focusMetricMap.positive_ratio.intercepted_count)} 只` : '',
      focusMetricMap.manager_stability ? `经理 ${formatNumber(focusMetricMap.manager_stability.intercepted_count)} 只` : '',
      longTermGovernance?.headline || '',
      `分组 ${topReasonGroup}`,
      `摩擦 ${topHighScoreFrictionReason}`,
      publishStatusTop ? `状态 ${publishStatusTop.状态值}` : '',
    ].filter(Boolean).join('；') || '暂无摘要')}</span>
  `;
}

Object.keys(taskSummarySectionConfig).forEach((key) => {
  bindTaskSummarySectionToggle(key);
});

renderTaskSummaryQuickNav();
initTaskSummaryPage();

window.addEventListener('load', () => {
  const anchorId = String(window.location.hash || '').replace(/^#/u, '').trim();
  if (!anchorId) return;
  expandTaskSummarySectionForAnchor(anchorId);
  const target = document.getElementById(anchorId);
  target?.scrollIntoView({ behavior: 'auto', block: 'start' });
});
