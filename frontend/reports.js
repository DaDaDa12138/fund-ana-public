const reportsMeta = document.getElementById('reportsMeta');
const reportsStats = document.getElementById('reportsStats');
const primaryFiles = document.getElementById('primaryFiles');
const publicationTable = document.getElementById('publicationTable');
const publicationFilterBar = document.getElementById('publicationFilterBar');
const publicationListHints = document.getElementById('publicationListHints');
const reportsHeaderRunBadge = document.getElementById('reportsHeaderRunBadge');
const reportsHeaderHealthBadge = document.getElementById('reportsHeaderHealthBadge');
const reportsHeroMeta = document.getElementById('reportsHeroMeta');
const reportsFunnelNarrative = document.getElementById('reportsFunnelNarrative');
const reportsSummaryHints = document.getElementById('reportsSummaryHints');
const reportsReasonStats = document.getElementById('reportsReasonStats');
const reportsReasonNarrative = document.getElementById('reportsReasonNarrative');
const reportsReasonHints = document.getElementById('reportsReasonHints');
const publicationSummaryBanner = document.getElementById('publicationSummaryBanner');

const REPORTS_PUBLICATION_SORTABLE_COLUMNS = Object.freeze([
  '运行批次',
  '发布状态',
  '有效净值日期',
  '覆盖基金数',
  '覆盖率',
]);

const REPORTS_PUBLICATION_INITIAL_SORT_DIRECTIONS = Object.freeze({
  运行批次: 'desc',
  '有效净值日期': 'desc',
  '覆盖基金数': 'desc',
  覆盖率: 'desc',
});

const DEFAULT_PUBLICATION_FILTER_STATE = Object.freeze({
  status: 'all',
});

const REPORTS_VIEW_PARAM_KEYS = Object.freeze({
  publicationStatus: 'publication_status',
});

function defaultPublicationFilterState() {
  return {
    status: DEFAULT_PUBLICATION_FILTER_STATE.status,
  };
}

function publicationFilterStateFromUrl() {
  const params = new URLSearchParams(window.location.search);
  let status = String(params.get(REPORTS_VIEW_PARAM_KEYS.publicationStatus) || '').trim();
  if (status === 'publish_hold') status = 'publication_issue';
  return {
    status: ['all', 'published', 'publication_issue', 'snapshot'].includes(status)
      ? status
      : DEFAULT_PUBLICATION_FILTER_STATE.status,
  };
}

function publicationRecordStatus(item) {
  if (item?.published) {
    return { key: 'published', label: '当前主发布' };
  }
  if (item?.publishable === false) {
    return { key: 'publication_issue', label: '不可发布诊断版本' };
  }
  return { key: 'snapshot', label: '可发布历史版本' };
}

function publicationSemanticLabel(value) {
  const rawValue = value && typeof value === 'object'
    ? (value.label || value.status_label || value.status || value.semantic || value.key || value.code || '')
    : value;
  const normalized = String(rawValue || '').trim();
  const labels = {
    formal_publication: '正式发布',
    publishable_snapshot: '可发布快照',
    diagnostic_only: '仅诊断',
    current_published: '当前正式发布',
    publishable_not_current: '可发布但非当前',
    not_publishable: '未通过发布门禁',
  };
  return labels[normalized] || normalized || '-';
}

function publicationBlockerSummary(item) {
  const blockers = Array.isArray(item?.publish_blockers) ? item.publish_blockers : [];
  if (!blockers.length) return '-';
  return blockers.map((blocker) => (
    String(blocker?.label || blocker?.detail || blocker?.message || blocker?.code || '').trim()
  )).filter(Boolean).slice(0, 3).join('；') || '-';
}

function syncPublicationViewUrl(filterState) {
  const params = new URLSearchParams(window.location.search);
  if (filterState?.status && filterState.status !== 'all') {
    params.set(REPORTS_VIEW_PARAM_KEYS.publicationStatus, filterState.status);
  } else {
    params.delete(REPORTS_VIEW_PARAM_KEYS.publicationStatus);
  }
  const nextQuery = params.toString();
  const nextUrl = `${window.location.pathname}${nextQuery ? `?${nextQuery}` : ''}`;
  window.history.replaceState(null, '', nextUrl);
}

function renderPublicationFilterBar(filterState, onChange, onReset) {
  if (!publicationFilterBar) return;
  publicationFilterBar.innerHTML = `
    <div class="recommendation-filter-grid">
      <label class="recommendation-filter-field">
        <span>版本状态</span>
        <select data-publication-filter-id="status">
          <option value="all" ${filterState.status === 'all' ? 'selected' : ''}>全部</option>
          <option value="published" ${filterState.status === 'published' ? 'selected' : ''}>仅当前主发布</option>
          <option value="publication_issue" ${filterState.status === 'publication_issue' ? 'selected' : ''}>仅不可发布诊断</option>
          <option value="snapshot" ${filterState.status === 'snapshot' ? 'selected' : ''}>仅可发布历史版本</option>
        </select>
      </label>
    </div>
    <div class="recommendation-filter-actions">
      <button type="button" class="secondary" data-publication-filter-reset="1">重置筛选</button>
    </div>
  `;
  publicationFilterBar.querySelectorAll('[data-publication-filter-id]').forEach((element) => {
    element.addEventListener('change', () => {
      const id = element.dataset.publicationFilterId || '';
      if (!id) return;
      onChange(id, element.value);
    });
  });
  publicationFilterBar.querySelector('[data-publication-filter-reset="1"]')?.addEventListener('click', onReset);
}

function filterPublicationRows(rows, filterState) {
  const items = Array.isArray(rows) ? rows : [];
  const status = String(filterState?.status || 'all').trim();
  if (status === 'all') return items;
  return items.filter((row) => String(row.__publication_status_key || '').trim() === status);
}

function renderPublicationListHints(rows, totalRows, filterState) {
  if (!publicationListHints) return;
  const items = Array.isArray(rows) ? rows : [];
  const total = Number(totalRows) || 0;
  if (!items.length && !total) {
    publicationListHints.innerHTML = '';
    return;
  }
  const statusCounts = {
    published: items.filter((item) => item.__publication_status_key === 'published').length,
    publication_issue: items.filter((item) => item.__publication_status_key === 'publication_issue').length,
    snapshot: items.filter((item) => item.__publication_status_key === 'snapshot').length,
  };
  const hints = [
    `符合筛选 ${formatNumber(items.length)} / ${formatNumber(total)} 条`,
    `当前主发布 ${formatNumber(statusCounts.published)} 条`,
    `不可发布诊断 ${formatNumber(statusCounts.publication_issue)} 条`,
    `可发布历史版本 ${formatNumber(statusCounts.snapshot)} 条`,
  ];
  if (filterState?.status && filterState.status !== 'all') {
    const labelMap = {
      published: '当前主发布',
      publication_issue: '不可发布诊断',
      snapshot: '可发布历史版本',
    };
    hints.unshift(`已筛选 ${labelMap[filterState.status] || filterState.status}`);
  }
  renderSummaryHintChips(publicationListHints, hints);
}

function formatCoverageRatioPercent(value) {
  return formatRatioPercent(value);
}

function reportsScopeText(contract) {
  const label = String(contract?.scope_label || '').trim();
  const boundary = String(contract?.truth_boundary || '').trim();
  return [label, boundary].filter(Boolean).join('：');
}

function formatSummaryPreview(item, labelKey, countKey, valueFormatter = formatNumber) {
  if (!item || !labelKey || !countKey) return '暂无';
  const label = String(item[labelKey] ?? '').trim();
  const count = item[countKey];
  if (!label) return '暂无';
  return `${label}（${valueFormatter(count)}）`;
}

function formatReasonSample(value) {
  return String(value || '')
    .split(/\s*\|\s*/u)
    .map((item) => item.trim())
    .filter(Boolean)
    .slice(0, 4)
    .join(' | ');
}

function resolveDashboardFile(payload, key) {
  return payload?.files?.[key] || payload?.artifacts?.[key] || payload?.[key] || '';
}

function fileCard(label, filePath, sheet = '') {
  if (!filePath) {
    return `<div class="link-card"><strong>${escapeHtml(label)}</strong><span>暂无文件</span></div>`;
  }
  return `
    <div class="link-card">
      <strong>${escapeHtml(label)}</strong>
      <span>${escapeHtml(filePath.split('/').pop())}</span>
      <div class="link-card-actions">
        <a class="preview-link" href="${viewerUrl(filePath, sheet)}" target="_blank" rel="noreferrer">预览</a>
        <a class="download-link" href="${fileUrl(filePath, true)}" target="_blank" rel="noreferrer">下载</a>
      </div>
    </div>
  `;
}

function renderPublicationTable(rows) {
  const items = Array.isArray(rows) ? rows.map(row => ({ ...row, 历史数据: row['运行批次'] })) : [];
  if (!items.length) {
    publicationTable.innerHTML = '<div class="empty-state-inline">暂无版本记录</div>';
    return;
  }
  const pageSize = 10;
  const pageCount = Math.ceil(items.length / pageSize);
  const page = Math.min(Math.max(renderPublicationTable.page || 1, 1), pageCount);
  renderPublicationTable.page = page;
  const sortState = renderPublicationTable.sortState || { column: null, direction: null };
  const sortedItems = [...items];
  if (REPORTS_PUBLICATION_SORTABLE_COLUMNS.includes(sortState.column) && sortState.direction) {
    const direction = sortState.direction === 'desc' ? -1 : 1;
    sortedItems.sort((left, right) => compareSortableValues(left[sortState.column], right[sortState.column], direction));
  }
  const pageRows = sortedItems.slice((page - 1) * pageSize, page * pageSize);
  renderCompactExpandableTable(publicationTable, pageRows, {
    coreColumns: [
      '运行批次',
      '分组键',
      '发布状态',
      '有效净值日期',
      '覆盖基金数',
      '覆盖率',
      '详情',
    ],
    detailGroups: [
      {
        title: '补充信息',
        fields: ['覆盖基金数', '覆盖率', '运行语义', '发布口径', '阻断原因', '被替代批次', '历史数据'],
      },
    ],
    tableClass: 'data-table compact-expand-table report-compact-table',
    wrapClass: 'compact-expand-wrap report-compact-wrap',
    toolbarClass: 'favorite-table-toolbar compact-table-toolbar',
    toolbarTitle: '版本记录视图',
    toolbarHint: '主表保留版本核心信息，被替代批次按行展开查看',
    renderCards: true,
    mobileCardColumns: ['运行批次', '发布状态', '有效净值日期'],
    sortableColumns: REPORTS_PUBLICATION_SORTABLE_COLUMNS,
    initialSortDirections: REPORTS_PUBLICATION_INITIAL_SORT_DIRECTIONS,
    sortState,
    rowKey: (row) => String(row['运行批次'] || '').trim(),
    expandedRows: renderPublicationTable.expandedRows || new Set(),
    cellRenderers: {
      历史数据: (_, row) => {
        const info = row.__history_storage;
        if (info && info.status !== 'available') return info.status === 'not_imported' ? '尚未导入历史账本' : '历史服务暂不可用';
        if (info) {
          const labels = { formal: '正式榜', tracker: '当期跟踪', forward_validation: '当期前瞻验证' };
          return `<a href="${escapeHtml(info.view_url)}" target="_blank" rel="noreferrer">历史记录</a> ` +
            Object.entries(info.views || {}).map(([key, item]) => `<a href="${escapeHtml(item.export_url)}">${escapeHtml(labels[key] || key)}（${escapeHtml(item.original_status)}）</a>`).join(' ');
        }
        const run = encodeURIComponent(String(row['运行批次'] || ''));
        return `<a href="/history?run_id=${run}" target="_blank" rel="noreferrer">历史记录</a>
          <a href="/api/reports/history/${run}/export?view=formal&format=xlsx">重生成正式榜</a>
          <a href="/api/reports/history/${run}/export?view=tracker&format=xlsx">当期跟踪</a>
          <a href="/api/reports/history/${run}/export?view=forward_validation&format=xlsx">当期前瞻验证</a>`;
      },
      详情: (_, row) => {
        const key = String(row['运行批次'] || '').trim();
        const expanded = (renderPublicationTable.expandedRows || new Set()).has(key);
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
      renderPublicationTable.sortState = nextSortState;
      renderPublicationTable.page = 1;
      renderPublicationTable(items);
    },
    onToggleExpand: (key) => {
      const next = new Set(renderPublicationTable.expandedRows || new Set());
      if (next.has(key)) next.delete(key);
      else next.add(key);
      renderPublicationTable.expandedRows = next;
      renderPublicationTable(items);
    },
    onToggleExpandAll: (expanded, visibleRows) => {
      renderPublicationTable.expandedRows = expanded
        ? new Set((Array.isArray(visibleRows) ? visibleRows : []).map((row) => String(row['运行批次'] || '').trim()).filter(Boolean))
        : new Set();
      renderPublicationTable(items);
    },
  });
  const pager = document.createElement('nav');
  pager.className = 'publication-pagination';
  pager.setAttribute('aria-label', '版本记录分页');
  pager.innerHTML = `
    <span>${formatNumber((page - 1) * pageSize + 1)}–${formatNumber((page - 1) * pageSize + pageRows.length)} / ${formatNumber(items.length)} 条</span>
    <div class="favorite-table-toolbar-actions">
      <button type="button" class="secondary" data-publication-page="${page - 1}"${page === 1 ? ' disabled' : ''}>上一页</button>
      <button type="button" class="secondary" data-publication-page="${page + 1}"${page === pageCount ? ' disabled' : ''}>下一页</button>
    </div>
  `;
  pager.querySelectorAll('[data-publication-page]').forEach((button) => {
    button.addEventListener('click', () => {
      renderPublicationTable.page = Number(button.dataset.publicationPage);
      renderPublicationTable(items);
      publicationTable.scrollIntoView({ block: 'start' });
    });
  });
  publicationTable.appendChild(pager);
}

async function initReportsPage() {
  void loadBootstrapSummary().then((payload) => {
    renderRunBadge(reportsHeaderRunBadge, getBootstrapWorkflow(payload));
    renderHealthBadge(reportsHeaderHealthBadge, getBootstrapHealth(payload));
  }).catch(() => {
    renderUnavailableBadge(reportsHeaderRunBadge, '运行状态', '接口暂不可用');
    renderUnavailableBadge(reportsHeaderHealthBadge, '数据源状态', '接口暂不可用');
  });
  const [dashboardResult, publicationResult] = await Promise.allSettled([
    loadDashboardReportsDetail(),
    loadPublicationSummary(),
  ]);

  if (dashboardResult.status !== 'fulfilled') {
    reportsHeroMeta.innerHTML = buildStatCard('当前主发布版本', '加载失败', '稍后重试');
    renderRetryableEmptyState(publicationTable, {
      title: '报告中心加载失败',
      message: dashboardResult.reason?.message || '请稍后重试',
      actionLabel: '重新加载',
      onRetry: initReportsPage,
    });
    return;
  }

  const dashboard = dashboardResult.value;
  applyPayloadDataContract(dashboard);
  const scopeContract = dashboard?.scope_contract || {};
  const scopeText = reportsScopeText(scopeContract);
  const publication = publicationResult.status === 'fulfilled' ? publicationResult.value : {};
  const publicationSourceSummary = payloadSourceContractSummary(publication);
  const publishFunnel = dashboard?.publish_funnel_summary || {};
  const exclusionSummary = dashboard?.selection_exclusion_summary || {};
  const publishReasonSummary = dashboard?.publish_reason_summary || {};
  const longTermGovernance = dashboard?.long_term_governance_summary || {};
  const positiveRatioSemantics = dashboard?.positive_ratio_semantics || {};
  const lowVolPositiveRatioWatch = dashboard?.low_vol_positive_ratio_shadow_watch || {};
  const portfolioSummary = dashboard?.portfolio_suggestions || {};
  const displayPoolCount = portfolioSummary?.fund_count ?? (Array.isArray(portfolioSummary?.funds) ? portfolioSummary.funds.length : null);
  const focusMetricSummary = Array.isArray(publishReasonSummary?.focus_metric_summary) ? publishReasonSummary.focus_metric_summary : [];
  const focusMetricMap = Object.fromEntries(focusMetricSummary.map((item) => [item.metric_key, item]));
  const canonicalRuns = publication?.canonical_runs || [];
  let publicationFilterState = publicationFilterStateFromUrl();
  const currentPublishedRunId = publication?.current_published_run_id || publication?.latest_canonical_run_id || '';
  const currentPublished = canonicalRuns.find((item) => item?.run_id === currentPublishedRunId) || canonicalRuns[0] || {};
  const latest = currentPublished;
  const latestSnapshot = latest?.snapshot || {};
  const topReason = Array.isArray(exclusionSummary?.top_reasons) && exclusionSummary.top_reasons.length
    ? `${exclusionSummary.top_reasons[0].reason}（${formatNumber(exclusionSummary.top_reasons[0].count)}只）`
    : '暂无';
  const topReasonGroup = Array.isArray(exclusionSummary?.reason_group_preview) ? exclusionSummary.reason_group_preview[0] : null;
  const topQualityFactorGroup = Array.isArray(exclusionSummary?.quality_factor_group_preview) ? exclusionSummary.quality_factor_group_preview[0] : null;
  const topQualityFactorDetail = Array.isArray(exclusionSummary?.quality_factor_detail_preview) ? exclusionSummary.quality_factor_detail_preview[0] : null;
  const topQualityOverlap = Array.isArray(exclusionSummary?.quality_overlap_combo_preview) ? exclusionSummary.quality_overlap_combo_preview[0] : null;
  const topFrictionReason = Array.isArray(exclusionSummary?.high_score_friction_reason_preview) ? exclusionSummary.high_score_friction_reason_preview[0] : null;
  const topStatus = Array.isArray(exclusionSummary?.status_split_preview) ? exclusionSummary.status_split_preview[0] : null;
  const highScoreCandidateTotal = exclusionSummary?.high_score_candidate_total;
  const highScoreFrictionCount = exclusionSummary?.high_score_friction_count;
  const frictionShareText = highScoreCandidateTotal > 0
    ? `${((highScoreFrictionCount / highScoreCandidateTotal) * 100).toFixed(2)}%`
    : '-';
  const officialPublishCount = firstPresentValue(publishFunnel.official_publish_count, dashboard?.counts?.final_count);
  const dynamicReferenceCount = firstPresentValue(
    publishFunnel.dynamic_reference_export_count,
    publishFunnel.dynamic_reference_count,
    dashboard?.counts?.dynamic_final_count,
  );
  reportsHeroMeta.innerHTML = [
    buildStatCard('当前主发布版本', latest.run_id || dashboard?.run_id || '-', '当前默认读取版本'),
    buildStatCard('有效快照', latestSnapshot.candidate_date || dashboard?.tracker_summary?.latest_nav_snapshot_date || '-', '当前主发布对应快照'),
    buildStatCard('正式主榜', formatNumber(officialPublishCount), '完整正式推荐名单'),
  ].join('');
  reportsMeta.textContent = [
    `版本 ${latest.run_id || dashboard?.run_id || '-'}`,
    `快照 ${latestSnapshot.candidate_date || dashboard?.tracker_summary?.latest_nav_snapshot_date || '-'}`,
    `主榜 ${formatNumber(publishFunnel.official_publish_count ?? dashboard?.counts?.final_count)}`,
    payloadSourceContractSummary(dashboard),
  ].filter(Boolean).join(' / ');

  reportsStats.innerHTML = [
    buildStatCard('正式主榜', formatNumber(officialPublishCount), '完整结果展示与下载'),
    buildStatCard('展示池', formatNumber(displayPoolCount), '推荐页压缩展示'),
    buildStatCard('动态阈值参考榜', formatNumber(dynamicReferenceCount), '对照导出'),
    buildStatCard('当前在榜跟踪', formatNumber(dashboard?.counts?.project_tracker_current_active_count), '跟踪在榜'),
    buildStatCard('长周期就绪率', formatCoverageRatioPercent(longTermGovernance?.full_feature_ready_coverage_ratio), longTermGovernance?.blocking_reason || '当前无明显阻塞'),
    buildStatCard('长周期治理样本', formatNumber(longTermGovernance?.neutralized_fund_count), `本地重试 ${formatNumber(longTermGovernance?.local_retry_pool_count)} / 停更 ${formatNumber(longTermGovernance?.full_feature_stale_fund_count)}`),
    buildStatCard('归档替代版本数', publication?.archived_superseded_run_count ?? '-', '已归档替代版本'),
  ].join('');

  reportsFunnelNarrative.innerHTML = `
    <strong>口径</strong>
    <span>${escapeHtml([
      `正式主榜 ${formatNumber(publishFunnel.official_publish_count ?? dashboard?.counts?.final_count)} 只`,
      `展示池 ${formatNumber(displayPoolCount)} 只`,
      '展示池来自正式主榜压缩展示，不代表只筛出这部分基金',
      scopeText,
      longTermGovernance?.headline || '',
    ].filter(Boolean).join('；'))}</span>
  `;
  renderSummaryHintChips(reportsSummaryHints, [
    `主榜 ${formatNumber(publishFunnel.official_publish_count ?? dashboard?.counts?.final_count)} 只`,
    `展示池 ${formatNumber(displayPoolCount)} 只`,
    `动态参考榜 ${formatNumber(publishFunnel.dynamic_reference_export_count ?? publishFunnel.dynamic_reference_count ?? dashboard?.counts?.dynamic_final_count)} 只`,
    `跟踪在榜 ${formatNumber(dashboard?.counts?.project_tracker_current_active_count)} 只`,
  ]);

  primaryFiles.innerHTML = [
    fileCard('跟踪总表', resolveDashboardFile(dashboard, 'tracker_file') || resolveDashboardFile(dashboard, 'project_tracker_excel'), '当前在榜'),
    fileCard('正式主榜', resolveDashboardFile(dashboard, 'filtered_excel')),
    fileCard('动态阈值参考榜', resolveDashboardFile(dashboard, 'dynamic_filtered_excel')),
    fileCard('分析报告', resolveDashboardFile(dashboard, 'report_markdown')),
  ].join('');

  reportsReasonStats.innerHTML = [
    buildStatCard('高收益未入榜总账', formatNumber(exclusionSummary?.high_score_candidate_total), topReason),
    buildStatCard('主拦截分组', formatSummaryPreview(topReasonGroup, '主原因分组', '数量'), topReasonGroup ? `占比 ${formatPercent(topReasonGroup['占未入榜样本比例(%)'])}` : '暂无'),
    buildStatCard('收益与评分主责拦截', formatNumber(sumPresentNumbers(focusMetricMap.annual_return?.intercepted_count, focusMetricMap.score?.intercepted_count)), `年化 ${formatNumber(focusMetricMap.annual_return?.intercepted_count)} / 评分 ${formatNumber(focusMetricMap.score?.intercepted_count)}`),
    buildStatCard('风险与经理门禁拦截', formatNumber(sumPresentNumbers(focusMetricMap.sharpe?.intercepted_count, focusMetricMap.positive_ratio?.intercepted_count, focusMetricMap.manager_stability?.intercepted_count)), `夏普 ${formatNumber(focusMetricMap.sharpe?.intercepted_count)} / 正收益 ${formatNumber(focusMetricMap.positive_ratio?.intercepted_count)} / 经理 ${formatNumber(focusMetricMap.manager_stability?.intercepted_count)}`),
    buildStatCard('高分摩擦样本', formatNumber(highScoreFrictionCount), highScoreCandidateTotal ? `占比 ${frictionShareText}` : '暂无'),
    buildStatCard('主质量因子组', formatSummaryPreview(topQualityFactorGroup, '质量因子组', '数量'), topQualityFactorGroup ? `占比 ${formatPercent(topQualityFactorGroup['占质量拦截样本比例(%)'])}` : '暂无'),
    buildStatCard('主发布结论状态', formatSummaryPreview(topStatus, '状态值', '数量'), topStatus ? `${compactUiText(topStatus.状态维度 || '状态', 6)} ${formatPercent(topStatus['占未入榜样本比例(%)'])}` : '暂无'),
  ].join('');

  reportsReasonNarrative.innerHTML = `
    <strong>诊断</strong>
    <span>${escapeHtml([
      publishReasonSummary?.responsibility_chain_headline || '',
      positiveRatioSemantics?.headline || '',
      lowVolPositiveRatioWatch?.explanation || '',
      longTermGovernance?.explanation || '',
      `主因 ${formatSummaryPreview(topReasonGroup, '主原因分组', '数量')}`,
      `因子 ${formatSummaryPreview(topQualityFactorGroup, '质量因子组', '数量')}`,
      `摩擦 ${formatSummaryPreview(topFrictionReason, '主排除原因', '样本数')}`,
      `细项 ${formatSummaryPreview(topQualityFactorDetail, '质量细分因子', '命中样本数')}`,
      topQualityOverlap ? `重叠 ${formatNumber(topQualityOverlap.样本数)}只` : '',
    ].filter(Boolean).join('；') || '暂无摘要')}</span>
  `;
  renderSummaryHintChips(reportsReasonHints, [
    `主因 ${formatSummaryPreview(topReasonGroup, '主原因分组', '数量')}`,
    `主因子 ${formatSummaryPreview(topQualityFactorGroup, '质量因子组', '数量')}`,
    `摩擦 ${formatSummaryPreview(topFrictionReason, '主排除原因', '样本数')}`,
    `主发布状态 ${formatSummaryPreview(topStatus, '状态值', '数量')}`,
  ]);

  if (!publicationResult || publicationResult.status !== 'fulfilled') {
    publicationTable.innerHTML = '<div class="empty-state-inline">版本记录暂不可用</div>';
    return;
  }

  const rows = canonicalRuns.map((item) => {
    const status = publicationRecordStatus(item);
    return {
      运行批次: item.run_id,
      分组键: item.group_key,
      发布状态: status.label,
      有效净值日期: item.snapshot?.candidate_date || '-',
      覆盖基金数: item.snapshot?.candidate_fund_count ?? '-',
      覆盖率: formatCoverageRatioPercent(item.snapshot?.coverage_ratio_vs_total),
      运行语义: publicationSemanticLabel(item.run_semantics),
      发布口径: publicationSemanticLabel(item.publication_status),
      阻断原因: publicationBlockerSummary(item),
      被替代批次: (item.superseded_run_ids || []).join(', ') || '-',
      __publication_status_key: status.key,
      __history_storage: item.history_storage,
      详情: '',
    };
  });
  const applyPublicationRender = () => {
    renderPublicationTable.page = 1;
    syncPublicationViewUrl(publicationFilterState);
    renderPublicationFilterBar(
      publicationFilterState,
      (key, value) => {
        publicationFilterState = {
          ...publicationFilterState,
          [key]: value,
        };
        applyPublicationRender();
      },
      () => {
        publicationFilterState = defaultPublicationFilterState();
        applyPublicationRender();
      },
    );
    const filteredRows = filterPublicationRows(rows, publicationFilterState);
    if (publicationSummaryBanner) {
      const selectedLabelMap = {
        all: '全部版本',
        published: '当前主发布',
        publication_issue: '不可发布诊断',
        snapshot: '可发布历史版本',
      };
      publicationSummaryBanner.innerHTML = `
        <strong>版本记录</strong>
        <span>${escapeHtml(`当前查看 ${selectedLabelMap[publicationFilterState.status] || publicationFilterState.status}；匹配 ${formatNumber(filteredRows.length)} / ${formatNumber(rows.length)} 条；${publicationSourceSummary}`)}</span>
      `;
    }
    renderPublicationListHints(filteredRows, rows.length, publicationFilterState);
    renderPublicationTable(filteredRows);
  };
  applyPublicationRender();
}

initReportsPage();
