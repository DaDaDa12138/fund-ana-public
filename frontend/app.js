const overviewStats = document.getElementById('overviewStats');
const integrityStats = document.getElementById('integrityStats');
const gateRuleList = document.getElementById('gateRuleList');
const heroMeta = document.getElementById('heroMeta');
const snapshotStatusBanner = document.getElementById('snapshotStatusBanner');
const headerRunBadge = document.getElementById('headerRunBadge');
const headerHealthBadge = document.getElementById('headerHealthBadge');
const trackerNarrative = document.getElementById('trackerNarrative');
const overviewSummaryHints = document.getElementById('overviewSummaryHints');
const schedulerOverviewStats = document.getElementById('schedulerOverviewStats');
const schedulerOverviewNarrative = document.getElementById('schedulerOverviewNarrative');
const schedulerSummaryHints = document.getElementById('schedulerSummaryHints');
const schedulerClosureStats = document.getElementById('schedulerClosureStats');
const schedulerClosureCards = document.getElementById('schedulerClosureCards');
const schedulerClosureNarrative = document.getElementById('schedulerClosureNarrative');
const schedulerSmokeStats = document.getElementById('schedulerSmokeStats');
const schedulerSmokeNarrative = document.getElementById('schedulerSmokeNarrative');
const closureSummaryHints = document.getElementById('closureSummaryHints');
const publicationNarrative = document.getElementById('publicationNarrative');
const publicationSummaryHints = document.getElementById('publicationSummaryHints');
const latestFileLinks = document.getElementById('latestFileLinks');
const apiSettingsOverviewStats = document.getElementById('apiSettingsOverviewStats');
const apiSettingsOverviewBanner = document.getElementById('apiSettingsOverviewBanner');
const refreshApiSettingsOverviewBtn = document.getElementById('refreshApiSettingsOverviewBtn');
const databaseDetailStats = document.getElementById('databaseDetailStats');
const schedulerTaskStats = document.getElementById('schedulerTaskStats');
const ruleCardList = document.getElementById('ruleCardList');
const overviewIntegrityToggleBtn = document.getElementById('overviewIntegrityToggleBtn');
const overviewDatabaseToggleBtn = document.getElementById('overviewDatabaseToggleBtn');
const overviewSchedulerToggleBtn = document.getElementById('overviewSchedulerToggleBtn');

const overviewSectionConfig = Object.freeze({
  integrity: {
    container: integrityStats,
    button: overviewIntegrityToggleBtn,
    collapsedText: '展开完整性诊断',
    expandedText: '收起完整性诊断',
    defaultCollapsed: true,
  },
  database: {
    container: databaseDetailStats,
    button: overviewDatabaseToggleBtn,
    collapsedText: '展开数据库细项',
    expandedText: '收起数据库细项',
    defaultCollapsed: true,
  },
  scheduler: {
    container: schedulerTaskStats,
    button: overviewSchedulerToggleBtn,
    collapsedText: '展开调度细项',
    expandedText: '收起调度细项',
    defaultCollapsed: true,
  },
});

const overviewSectionCollapsedState = Object.fromEntries(
  Object.entries(overviewSectionConfig).map(([key, config]) => [key, Boolean(config.defaultCollapsed)])
);

function applyOverviewSectionState(key) {
  const config = overviewSectionConfig[key];
  if (!config?.container) return;
  const collapsed = overviewSectionCollapsedState[key] !== false;
  config.container.classList.toggle('is-collapsed', collapsed);
  if (config.button) {
    config.button.textContent = collapsed ? config.collapsedText : config.expandedText;
    config.button.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
  }
}

function bindOverviewSectionToggle(key) {
  const config = overviewSectionConfig[key];
  if (!config?.button) return;
  config.button.addEventListener('click', () => {
    overviewSectionCollapsedState[key] = !overviewSectionCollapsedState[key];
    applyOverviewSectionState(key);
  });
  applyOverviewSectionState(key);
}

function resolveDashboardFile(payload, key) {
  return payload?.files?.[key] || payload?.artifacts?.[key] || payload?.[key] || '';
}

function renderLinkCard(label, filePath, sheet = '') {
  if (!filePath) {
    return `<div class="link-card"><strong>${escapeHtml(label)}</strong><span>暂无文件</span></div>`;
  }
  return `
    <div class="link-card">
      <strong title="${escapeHtml(filePath.split('/').pop())}">${escapeHtml(label)}</strong>
      <div class="link-card-actions">
        <a class="preview-link" href="${viewerUrl(filePath, sheet)}" target="_blank" rel="noreferrer">预览</a>
        <a class="download-link" href="${fileUrl(filePath, true)}" target="_blank" rel="noreferrer">下载</a>
      </div>
    </div>
  `;
}

function renderPageCard(label, href, description) {
  return `
    <div class="link-card">
      <strong>${escapeHtml(label)}</strong>
      <span>${escapeHtml(description)}</span>
      <div class="link-card-actions">
        <a class="preview-link" href="${escapeHtml(href)}">进入</a>
      </div>
    </div>
  `;
}

function renderApiSettingsOverview(payload) {
  if (!apiSettingsOverviewStats || !apiSettingsOverviewBanner) return;
  const apiRuntime = payload?.configured || payload?.runtime ? payload : {};
  const configured = apiRuntime?.configured || {};
  const runtime = apiRuntime?.runtime || {};
  const matches = apiRuntime?.runtime_matches_config || {};
  const env = apiRuntime?.environment_overrides || {};
  const scheduler = apiRuntime?.scheduler || {};
  const frontend = apiRuntime?.frontend || {};
  const schedulerRuntime = scheduler?.runtime || {};
  const schedulerMatches = scheduler?.runtime_matches_config || {};
  const settingsAvailable = Boolean(apiRuntime?.configured || apiRuntime?.runtime);
  const runtimeMatchState = apiConfigurationMatchState(matches, configured);
  const runtimeMatchText = runtimeMatchState === 'consistent' ? '已同步'
    : runtimeMatchState === 'mismatch' ? '待重载' : '未核验';
  const environmentOverride = Boolean(env.tiantian_override_active);
  const schedulerRestartRequired = Boolean(scheduler.restart_required_after_change);
  const schedulerEvidenceAvailable = scheduler.runtime_evidence_available === true;
  const schedulerMatchState = schedulerEvidenceAvailable
    ? apiConfigurationMatchState(schedulerMatches, configured) : 'unknown';
  const schedulerMatchText = schedulerMatchState === 'consistent' ? '已同步'
    : schedulerMatchState === 'mismatch' ? '待重启' : schedulerEvidenceAvailable ? '未核验' : '未上报';

  if (!settingsAvailable) {
    apiSettingsOverviewStats.innerHTML = [
      buildStatCard('Tiantian 当前运行', '-', '接口设置暂不可用'),
      buildStatCard('Tiantian 配置文件', '-', 'config/project.yaml'),
      buildStatCard('正式发布校验', '-', '当前来源暂不可用'),
      buildStatCard('AutoStock 状态', '-', '启用状态暂不可用'),
      buildStatCard('当前来源', '/api/settings/apis', '实时配置接口 / no-store'),
      buildStatCard('配置入口', '接口设置', '/api-settings'),
      buildStatCard('前端配置', '不可用', '等待设置接口'),
    ].join('');
    apiSettingsOverviewBanner.className = 'info-banner info-banner-warning';
    apiSettingsOverviewBanner.innerHTML = `
      <strong>接口状态</strong>
      <span>接口设置实时配置暂不可用，首页不会用历史报告 data_sources 代替当前 API 地址。</span>
      <small>请进入接口设置页或检查 /api/settings/apis。</small>
    `;
    return;
  }

  apiSettingsOverviewStats.innerHTML = [
    buildStatCard('Tiantian 当前运行', runtime.tiantian_base_url || '-', runtimeMatchText),
    buildStatCard('Tiantian 配置文件', configured.tiantian_base_url || '-', 'config/project.yaml'),
    buildStatCard('调度当前运行', schedulerRuntime.tiantian_base_url || '-', schedulerMatchText),
    buildStatCard('正式发布校验', publicationValidationProviderLabel(configured.publication_validation_provider), '当前配置；校验结果随每次正式运行记录'),
    buildStatCard('净值与收益主接口', configured.tiantian_valuation_primary_url || '-', 'FundValuationLast'),
    buildStatCard('主接口备用域名', configured.tiantian_valuation_fallback_url || '-', 'FundValuationLast 同供应商备用入口'),
    buildStatCard('净值日期复核接口', configured.eastmoney_fund_info_url || '-', 'FundMNFInfo；未提供区间收益'),
    buildStatCard('AutoStock 状态', configuredAutoStockStatus(configured), configured.autostock_base_url || '原接口地址未提供'),
    buildStatCard('当前来源', apiRuntime?.frontend?.homepage_api_source || '/api/settings/apis', '实时配置接口 / no-store'),
    buildStatCard('读取时间', formatDateTime(apiRuntime.fetched_at), 'no-store 响应'),
    buildStatCard('配置入口', '接口设置', apiRuntime?.frontend?.settings_page || '/api-settings'),
    buildStatCard('前端配置', frontend.editable_from_frontend ? '已启用' : '未启用', frontend.save_endpoint || '/api/settings/apis'),
    buildStatCard('调度读取', schedulerRestartRequired ? '保存后需重启' : schedulerMatchText, scheduler.host_mode || '-'),
  ].join('');

  const bannerKind = environmentOverride || runtimeMatchState !== 'consistent' || schedulerMatchState !== 'consistent' ? 'warning' : 'success';
  const bannerText = environmentOverride
    ? `TIANTIAN_API_URL 环境变量正在覆盖配置：${env.TIANTIAN_API_URL || '-'}`
    : runtimeMatchState === 'mismatch'
      ? '配置文件已变化，当前运行进程尚未完全同步。'
      : runtimeMatchState === 'unknown'
        ? 'Web 运行配置的核对证据不完整，尚不能确认已同步。'
      : schedulerMatchState === 'mismatch'
        ? 'Web 已读取当前配置；调度容器仍在旧接口地址，需重启或重新部署后生效。'
      : schedulerMatchState === 'unknown'
        ? 'Web 已读取当前配置；调度运行配置的核对证据不完整，需等待心跳后复核。'
      : schedulerRestartRequired
        ? 'Web 与调度运行配置已对齐；保存新的接口配置后需重启调度容器。'
        : 'Web 与调度运行配置已对齐。';
  apiSettingsOverviewBanner.className = `info-banner info-banner-${bannerKind}`;
  apiSettingsOverviewBanner.innerHTML = `
    <strong>接口状态</strong>
    <span>${escapeHtml(bannerText)}</span>
    <small>当前页面数据来自 ${escapeHtml(apiRuntime?.frontend?.homepage_api_source || '/api/settings/apis')}；可通过 ${escapeHtml(frontend.settings_page || '/api-settings')} 保存配置，不会读取历史报告 data_sources、旧健康摘要或浏览器本地缓存作为当前 API 地址。</small>
  `;
}

function setApiSettingsOverviewBusy(busy) {
  if (!refreshApiSettingsOverviewBtn) return;
  if (busy) {
    refreshApiSettingsOverviewBtn.dataset.originalText = refreshApiSettingsOverviewBtn.textContent || '';
    refreshApiSettingsOverviewBtn.textContent = '刷新中';
    refreshApiSettingsOverviewBtn.disabled = true;
    return;
  }
  refreshApiSettingsOverviewBtn.disabled = false;
  refreshApiSettingsOverviewBtn.textContent = refreshApiSettingsOverviewBtn.dataset.originalText || '刷新';
}

async function refreshApiSettingsOverview() {
  setApiSettingsOverviewBusy(true);
  try {
    const payload = await loadApiSettings();
    renderApiSettingsOverview(payload);
    renderHealthBadge(headerHealthBadge, { api_runtime: payload });
  } catch (_error) {
    renderApiSettingsOverview(null);
    renderUnavailableBadge(headerHealthBadge, '数据源状态', '接口暂不可用');
  } finally {
    setApiSettingsOverviewBusy(false);
  }
}

function renderTrackerNarrative(summary) {
  const narrative = summary?.narrative_summary || {};
  const latestChanges = narrative.latest_changes || summary?.latest_changes || '暂无正式发布名单变化摘要。';
  const riskHint = narrative.risk_hint || '成员稳定性以正式主榜唯一快照为准，不复用内部研究主榜口径。';
  return `
    <strong>名单变化</strong>
    <span>${escapeHtml(latestChanges)}</span>
    <small>${escapeHtml(riskHint)}</small>
  `;
}

function renderPublicationNarrative(summary) {
  if (!summary || summary.available === false) {
    return `
      <strong>发布</strong>
      <span>暂无发布状态摘要</span>
    `;
  }
  return `
    <strong>${escapeHtml(summary.status_label || '发布状态')}</strong>
    <span>${escapeHtml(summary.headline || summary.detail || '-')}</span>
    ${summary.detail ? `<small>${escapeHtml(summary.detail)}</small>` : ''}
  `;
}

function renderDashboard(payload, publication) {
  const trackerSummary = payload?.tracker_summary || {};
  const publicationStatus = payload?.publication_status_summary || payload?.publication_summary?.status_summary || {};
  const counts = payload?.counts || {};
  const reportTiming = payload?.report_timing_summary || {};
  const timingStages = reportTiming?.stages || {};
  const databaseOverview = payload?.database_overview || {};
  const databaseIntegrity = payload?.database_integrity || {};
  const bondUniverseSnapshot = payload?.bond_universe_snapshot || {};
  const publishFunnel = payload?.publish_funnel_summary || {};
  const schedulerOverview = payload?.scheduler_overview || {};
  const automationClosureDigest = schedulerOverview?.automation_closure_digest || {};
  const closureSummary = automationClosureDigest?.summary || {};
  const coverageSummary = payload?.coverage_summary || {};
  const effectiveNav = schedulerOverview?.effective_nav_snapshot || payload?.database_freshness?.effective_nav_snapshot || {};
  const latestScreenedSnapshot = schedulerOverview?.latest_screened_snapshot || {};
  const currentTask = schedulerOverview?.current_task || {};
  const dailyRun = schedulerOverview?.daily_run?.status
    ? schedulerOverview?.daily_run
    : currentTask?.task_name === 'scheduled_daily_bond_refresh'
      ? { status: 'running', started_at: currentTask.started_at }
      : {};
  const intradayRun = schedulerOverview?.intraday_run?.status
    ? schedulerOverview?.intraday_run
    : currentTask?.task_name === 'scheduled_intraday_bond_refresh'
      ? { status: 'running', started_at: currentTask.started_at }
      : {};
  const dailyJob = schedulerOverview?.daily_job || {};
  const intradayJob = schedulerOverview?.intraday_job || {};
  const weeklyJob = schedulerOverview?.weekly_job || {};
  const maintenanceSummary = schedulerOverview?.maintenance_summary || {};
  const weeklyRun = schedulerOverview?.weekly_run || {};
  const regressionSmokeSummary = payload?.maintenance_summary?.regression_smoke_summary || maintenanceSummary?.regression_smoke_summary || {};
  const regressionSmokeTask = payload?.weekly_run?.child_tasks?.regression_smoke || weeklyRun?.child_tasks?.regression_smoke || {};
  const navFreshness = schedulerOverview?.nav_freshness || {};
  const currentPublishedRecord = publicationStatus?.current_published || {};
  const currentPublishedRunId = publicationStatus?.current_published_run_id
    || publication?.current_published_run_id
    || payload?.run_id
    || '-';
  const latestCanonicalRunId = publicationStatus?.latest_canonical_run_id
    || publication?.latest_canonical_run_id
    || payload?.run_id
    || '-';
  const currentPublishedSnapshotDate = publicationStatus?.current_effective_nav_date
    || currentPublishedRecord?.effective_nav_date
    || currentPublishedRecord?.snapshot?.candidate_date
    || '-';
  const latestDatabaseSnapshotDate = publicationStatus?.latest_database_snapshot_date
    || databaseOverview?.latest_nav_business_date
    || effectiveNav?.candidate_date
    || '-';
  const databaseSnapshotAheadOfPublication = Boolean(
    publicationStatus?.database_snapshot_ahead_of_publication
    || (
      latestDatabaseSnapshotDate !== '-'
      && currentPublishedSnapshotDate !== '-'
      && latestDatabaseSnapshotDate > currentPublishedSnapshotDate
    )
  );
  const replayPrecisionLabel = publicationStatus.replay_precision_status_label || '-';
  const replayPrecisionHint = publicationStatus.replay_precision_detail || '运行历史输入精确性摘要';
  const dailyRunStatus = formatScheduledRunStatus(dailyRun);
  const intradayRunStatus = formatScheduledRunStatus(intradayRun);
  const schedulerError = formatSchedulerError(schedulerOverview.last_error, schedulerOverview.enabled);
  const dailyRunReason = formatScheduledRunHint(dailyRun);
  const intradayRunReason = formatScheduledRunHint(intradayRun);
  const runStatusHint = formatScheduledRunHint(dailyRun);
  const schedulerRuntime = formatSchedulerRuntime(schedulerOverview.enabled, schedulerOverview.scheduler_running);
  const screenedUniverseCount = firstPresentValue(
    publishFunnel.screenable_universe_count,
    coverageSummary.screenable_universe_count,
    databaseOverview.effective_nav_fund_count,
  );
  const mainListCount = firstPresentValue(publishFunnel.official_publish_count, counts.final_count);
  const dynamicReferenceCount = publishFunnel.dynamic_reference_export_count ?? publishFunnel.dynamic_reference_count ?? counts.dynamic_final_count;
  const riskCoverageSemantics = databaseOverview?.risk_coverage_semantics || {};
  const riskCoverageRatio = riskCoverageSemantics?.coverage_ratio_vs_candidate ?? databaseOverview?.risk_1y_coverage_ratio_vs_candidate;
  const riskCoveragePublishMinimum = riskCoverageSemantics?.publish_minimum_coverage_ratio ?? databaseOverview?.risk_1y_publish_minimum_coverage_ratio;
  const riskCoverageObservationLine = riskCoverageSemantics?.observation_coverage_ratio ?? databaseOverview?.risk_1y_observation_coverage_ratio;
  const riskCoverageGateLabel = riskCoverageSemantics?.coverage_gate_label
    || databaseOverview?.risk_1y_coverage_gate_label
    || '风险覆盖';
  const riskCoverageGateExplanation = riskCoverageSemantics?.coverage_gate_explanation
    || databaseOverview?.risk_1y_coverage_gate_explanation
    || '风险覆盖语义摘要';

  overviewStats.innerHTML = [
    buildStatCard('正式在榜', formatNumber(mainListCount), '完整名单见跟踪页'),
    buildStatCard('纯债稳健', formatNumber(counts.stable_bond_final_count), '正式榜分组'),
    buildStatCard('增强债', formatNumber(counts.enhanced_bond_final_count), '正式榜分组'),
    buildStatCard('净值日期', currentPublishedSnapshotDate, '当前发布快照'),
  ].join('');

  integrityStats.innerHTML = [
    buildStatCard('原始最新净值日期', navFreshness.raw_latest_nav_date || '-', '原始入库最大净值日期'),
    buildStatCard('正式可筛选宇宙', formatNumber(screenedUniverseCount), '通过最新净值、风险和持有期硬门槛'),
    buildStatCard('数据库文件更新', formatDateTime(databaseOverview.database_file_updated_at), 'SQLite 文件最后修改时间'),
    buildStatCard('净值业务日期', databaseOverview.latest_nav_business_date || '-', '当前有效净值快照日期'),
    buildStatCard('风险业务日期', databaseOverview.risk_1y_business_date || '-', '1年风险指标 ready 覆盖日期'),
    buildStatCard('原始风险日期', databaseOverview.risk_1y_raw_business_date || '-', 'fund_risk_metrics 原始最大 end_date'),
    buildStatCard('净值表写入时间', formatDateTime(databaseOverview.nav_data_updated_at), 'fund_returns.last_updated_at'),
    buildStatCard('风险计算时间', formatDateTime(databaseOverview.risk_data_updated_at), 'fund_risk_metrics.calculated_at'),
    buildStatCard('待修复净值基金', formatNumber(databaseIntegrity.candidate_total), '仅作为数据维护提示，不影响首页入口判断'),
  ].join('');

  databaseDetailStats.innerHTML = [
    buildStatCard('纯债稳健榜', formatNumber(counts.stable_bond_final_count), '正式主榜中的稳健分组'),
    buildStatCard('增强债弹性榜', formatNumber(counts.enhanced_bond_final_count), '正式主榜中的弹性分组'),
    buildStatCard('当前在榜跟踪', formatNumber(counts.project_tracker_current_active_count), '当前仍在榜的完整跟踪样本数'),
    buildStatCard('已发布快照', currentPublishedSnapshotDate, '当前主发布版本对应快照'),
    buildStatCard('风险观察线', formatRatioPercent(riskCoverageObservationLine, 2), riskCoverageGateExplanation),
    buildStatCard('慢阶段', timingStages.materialize_summary_payload ? 'summary_payload' : '-', timingStages.materialize_summary_payload ? formatSeconds(timingStages.materialize_summary_payload, 1) : '最新报表阶段耗时'),
    buildStatCard('下一次日更', dailyJob.next_run_time ? formatDateTime(dailyJob.next_run_time) : '-', '下次执行'),
    buildStatCard('下一次盘中补刷', intradayJob.next_run_time ? formatDateTime(intradayJob.next_run_time) : '-', '盘中补取最新净值机会'),
  ].join('');

  heroMeta.innerHTML = [
    buildStatCard('当前主发布版本', currentPublishedRunId, '正式发布指针'),
    buildStatCard('已发布净值日', currentPublishedSnapshotDate, '正式发布快照'),
    buildStatCard('正式主榜', formatNumber(mainListCount), '当前发布数量'),
    buildStatCard('自动化', schedulerRuntime, '后台调度状态'),
  ].join('');

  gateRuleList.innerHTML = [
    '推荐：优先池',
    '跟踪：在榜留痕',
    '文件：主发布产物',
  ].map((item) => `<li>${escapeHtml(item)}</li>`).join('');
  ruleCardList.innerHTML = [
    {
      title: '健康',
      detail: '版本 / 快照 / 调度',
    },
    {
      title: '推荐',
      detail: '优先池名单',
    },
    {
      title: '文件',
      detail: '主发布文件',
    },
    {
      title: '运行',
      detail: '任务与日志',
    },
  ].map((item) => `
    <article class="rule-card">
      <strong>${escapeHtml(item.title)}</strong>
      <span>${escapeHtml(item.detail)}</span>
    </article>
  `).join('');

  trackerNarrative.innerHTML = renderTrackerNarrative(trackerSummary);
  renderSummaryHintChips(overviewSummaryHints, [
    `主发布 ${currentPublishedRunId}`,
    latestCanonicalRunId !== currentPublishedRunId ? `最新规范运行 ${latestCanonicalRunId}` : '',
    `正式主榜 ${formatNumber(mainListCount)} 只`,
    `动态参考榜 ${formatNumber(dynamicReferenceCount)} 只`,
    `当前快照 ${effectiveNav.candidate_date || databaseOverview.latest_nav_business_date || '-'}`,
  ]);

  schedulerOverviewStats.innerHTML = [
    buildStatCard('调度配置', formatSchedulerConfig(schedulerOverview.enabled), 'config.project.yaml -> scheduler.enabled'),
    buildStatCard('调度进程', schedulerRuntime, '当前后台调度进程状态'),
    buildStatCard('最近日常调度', dailyRunStatus, runStatusHint || '最近一次日常调度结果'),
    buildStatCard('下一次日更', dailyJob.next_run_time ? formatDateTime(dailyJob.next_run_time) : '-', 'daily_refresh'),
    buildStatCard('最近盘中补刷新', intradayRunStatus, intradayRunReason || '最近一次盘中补刷新结果'),
    buildStatCard('最近周维护', formatScheduledRunStatus(weeklyRun), weeklyRun.warning_reason ? formatReasonCode(weeklyRun.warning_reason) : '最近一次周维护结果'),
    buildStatCard('最近心跳', formatDateTime(schedulerOverview.last_heartbeat_at), '调度器最近状态上报'),
    buildStatCard('最近错误', schedulerError, '无则表示没有残留错误'),
  ].join('');

  schedulerTaskStats.innerHTML = [
    buildStatCard(
      '自动托管模式',
      formatSchedulerHostMode(schedulerOverview.host_mode),
      schedulerOverview.automation_mode || 'automatic / attention_required / unknown',
    ),
    buildStatCard('启动补跑待执行', formatNumber((schedulerOverview.startup_catch_up_due_jobs || []).length), schedulerOverview.startup_catch_up_pending ? '启动后仍有待补跑任务' : '当前无待补跑任务'),
    buildStatCard('筛选积压判定', schedulerOverview.screening_backlog_due ? '存在' : '无', schedulerOverview.screening_backlog_reason ? formatReasonCode(schedulerOverview.screening_backlog_reason) : '已与当前主发布版本对齐'),
    buildStatCard('当前执行任务', currentTask?.task_name || '-', currentTask?.started_at ? `开始于 ${formatDateTime(currentTask.started_at)}` : '当前无运行中任务'),
    buildStatCard('日更触发筛选', dailyRun.status ? (dailyRun.screening_triggered ? '是' : '否') : '-', dailyRun.no_action_category ? formatSchedulerNoActionCategory(dailyRun.no_action_category) : (dailyRun.action_category ? formatSchedulerActionCategory(dailyRun.action_category) : (dailyRun.screening_reason ? formatReasonCode(dailyRun.screening_reason) : ''))),
    buildStatCard('盘中补刷触发筛选', intradayRun.status ? (intradayRun.screening_triggered ? '是' : '否') : '-', intradayRun.no_action_category ? formatSchedulerNoActionCategory(intradayRun.no_action_category) : (intradayRun.action_category ? formatSchedulerActionCategory(intradayRun.action_category) : (intradayRun.screening_reason ? formatReasonCode(intradayRun.screening_reason) : ''))),
    buildStatCard('周维护触发筛选', weeklyRun.status ? (weeklyRun.screening_triggered ? '是' : '否') : '-', weeklyRun.no_action_category ? formatSchedulerNoActionCategory(weeklyRun.no_action_category) : (weeklyRun.action_category ? formatSchedulerActionCategory(weeklyRun.action_category) : (weeklyRun.warning_reason ? formatReasonCode(weeklyRun.warning_reason) : ''))),
    buildStatCard('周维护净值插入', formatNumber(maintenanceSummary.nav_records_inserted), '最近一次周维护新增净值记录'),
    buildStatCard('周维护净值失败', formatNumber(maintenanceSummary.nav_failed_total), '最近一次周维护失败基金数'),
    buildStatCard('长期年收益保存成功', formatNumber(maintenanceSummary.long_term_annual_success_total), '最近周维护保存成功的基金数'),
    buildStatCard('长期排名保存成功', formatNumber(maintenanceSummary.long_term_rank_success_total), '保存成功数不代表数据发生变更'),
    buildStatCard('当前有效快照', effectiveNav.candidate_date || '-', effectiveNav.ready ? '已就绪' : formatReasonCode(effectiveNav.readiness_reason || '')),
    buildStatCard('原始净值日期', navFreshness.raw_latest_nav_date || '-', formatNavSnapshotComparison(navFreshness, effectiveNav)),
    buildStatCard('最新筛选快照', latestScreenedSnapshot.candidate_date || '-', '调度器最近完成筛选的快照，不等同于正式发布'),
  ].join('');

  schedulerOverviewNarrative.innerHTML = `
    <strong>调度</strong>
    <span>${escapeHtml([
      `日更 ${dailyRunStatus}`,
      `盘中 ${intradayRunStatus}`,
      `周维 ${formatScheduledRunStatus(weeklyRun)}`,
      dailyRun.finished_at ? `最近完成 ${formatDateTime(dailyRun.finished_at)}` : '',
      schedulerOverview.next_run_at ? `下次任务 ${formatDateTime(schedulerOverview.next_run_at)}${schedulerOverview.next_run_label ? ` (${schedulerOverview.next_run_label})` : ''}` : '',
      dailyJob.next_run_time ? `下次日更 ${formatDateTime(dailyJob.next_run_time)}` : '',
      intradayJob.next_run_time ? `下次盘中 ${formatDateTime(intradayJob.next_run_time)}` : '',
      weeklyJob.next_run_time ? `下次周维 ${formatDateTime(weeklyJob.next_run_time)}` : '',
    ].filter(Boolean).join('；') || '日更 / 盘中 / 周维')}</span>
  `;
  renderSummaryHintChips(schedulerSummaryHints, [
    `调度 ${schedulerRuntime}`,
    `日更 ${dailyRunStatus}`,
    `盘中 ${intradayRunStatus}`,
    `周维 ${formatScheduledRunStatus(weeklyRun)}`,
    `异常 ${formatNumber(schedulerOverview.issue_count || 0) || '0'} 项`,
  ]);

  schedulerClosureStats.innerHTML = [
    buildStatCard('最近闭环任务数', formatNumber(closureSummary.task_count || 0), '固定覆盖日更、盘中补刷、周维护和快照看门狗'),
    buildStatCard('本轮触发重筛', formatNumber(closureSummary.screening_triggered_total || 0), '最近一次闭环中实际触发正式筛选的任务数'),
    buildStatCard('本轮重建报告', formatNumber(closureSummary.report_refresh_triggered_total || 0), '未重筛但进行了发布层重建'),
    buildStatCard('存在待复核任务', formatNumber(closureSummary.attention_total || 0), '含快照推进未重筛、上游受限或任务降级'),
  ].join('');

  schedulerClosureCards.innerHTML = renderAutomationClosureCards(automationClosureDigest, { detailed: false });
  const closureAlerts = Array.isArray(automationClosureDigest?.alerts) ? automationClosureDigest.alerts : [];
  schedulerClosureNarrative.innerHTML = `
    <strong>闭环</strong>
    <span>${escapeHtml([
      closureAlerts.length ? `告警 ${String(closureAlerts.length)} 条` : '无新增告警',
      `上游受限 ${formatNumber(closureSummary.upstream_limited_child_task_total || 0)} 个`,
      `快照推进 ${formatNumber(closureSummary.snapshot_changed_total || 0)} 次`,
    ].join('；'))}</span>
  `;
  renderSummaryHintChips(closureSummaryHints, [
    `闭环任务 ${formatNumber(closureSummary.task_count || 0)}`,
    `触发重筛 ${formatNumber(closureSummary.screening_triggered_total || 0)}`,
    `重建报告 ${formatNumber(closureSummary.report_refresh_triggered_total || 0)}`,
    `待复核 ${formatNumber(closureSummary.attention_total || 0)}`,
  ]);

  if (Object.keys(regressionSmokeSummary).length || Object.keys(regressionSmokeTask).length) {
    const smokeOverallStatus = regressionSmokeSummary?.overall_status || regressionSmokeTask?.overall_status || regressionSmokeSummary?.status || regressionSmokeTask?.status || 'unknown';
    const smokePassedTotal = regressionSmokeSummary?.passed_group_total || regressionSmokeTask?.changed_total || 0;
    const smokeFailedTotal = regressionSmokeSummary?.failed_group_total || 0;
    const smokeGroupTotal = (Array.isArray(regressionSmokeSummary?.groups) ? regressionSmokeSummary.groups.length : 0) || regressionSmokeTask?.candidate_total || 0;
    const smokeSource = regressionSmokeSummary?.source || regressionSmokeTask?.source || '';
    const smokeIssueLabel = regressionSmokeSummary?.issue_classification_label
      || regressionSmokeTask?.issue_classification_label
      || regressionSmokeSummary?.issue_classification_status
      || regressionSmokeTask?.issue_classification_status
      || '';
    schedulerSmokeStats.innerHTML = [
      buildStatCard('最近 Smoke 自检', formatWorkflowStatus(smokeOverallStatus), smokeSource === 'latest_artifact_fallback' ? '由最新产物回填' : '来自最近周维护'),
      buildStatCard('通过分组', formatNumber(smokePassedTotal), `共 ${formatNumber(smokeGroupTotal)} 组`),
      buildStatCard('失败分组', formatNumber(smokeFailedTotal), smokeFailedTotal ? '需进一步复核' : '当前无失败分组'),
      buildStatCard('失败分类', smokeIssueLabel || '-', smokeFailedTotal ? '来自 smoke 日志分类' : '当前无失败分类'),
      buildStatCard('生成时间', formatDateTime(regressionSmokeSummary?.generated_at || regressionSmokeTask?.generated_at), '最近一次 smoke 结果'),
    ].join('');
    schedulerSmokeNarrative.innerHTML = `
      <strong>自检</strong>
      <span>${escapeHtml([
        smokeOverallStatus ? `总体 ${formatWorkflowStatus(smokeOverallStatus)}` : '',
        smokeIssueLabel ? `分类 ${smokeIssueLabel}` : '',
        smokeGroupTotal ? `分组 ${formatNumber(smokeGroupTotal)} 个` : '',
        smokeSource === 'latest_artifact_fallback' ? '当前为运行态回填口径' : '',
      ].filter(Boolean).join('；') || '暂无 smoke 摘要')}</span>
    `;
  } else {
    schedulerSmokeStats.innerHTML = buildStatCard('最近 Smoke 自检', '暂无', '最近一次周维护尚未写入');
    schedulerSmokeNarrative.innerHTML = `
      <strong>自检</strong>
      <span>暂无最近一次 smoke 结果</span>
    `;
  }

  const isNewSnapshot = Boolean(trackerSummary.latest_run_is_new_snapshot);
  if (databaseSnapshotAheadOfPublication) {
    // Keep the publication backlog signal as the final render: the generic
    // effective/latest-screened snapshot status must not hide a newer DB
    // snapshot waiting for the scheduler to publish it.
    snapshotStatusBanner.className = 'snapshot-status-banner is-new';
    const publicationDependency = schedulerOverview.publication_dependency;
    const publicationHint = publicationDependency?.code
      ? publicationDependency.message || formatReasonCode(publicationDependency.code)
      : '等待筛选及发布门禁通过。';
    snapshotStatusBanner.innerHTML = `
      <strong>${publicationDependency?.code ? '正式发布受阻' : '有待发布快照'}</strong>
      <span>已发布 ${escapeHtml(currentPublishedSnapshotDate)} / 数据库最新 ${escapeHtml(latestDatabaseSnapshotDate)}；${escapeHtml(publicationHint)}</span>
    `;
  } else {
    snapshotStatusBanner.className = `snapshot-status-banner ${isNewSnapshot ? 'is-new' : 'is-duplicate'}`;
    snapshotStatusBanner.innerHTML = `
      <strong>${effectiveNav.ready ? '快照就绪' : '快照未就绪'}</strong>
      <span>净值 ${escapeHtml(effectiveNav.candidate_date || '-')} · 已分析 ${escapeHtml(latestScreenedSnapshot.candidate_date || '-')}</span>
    `;
  }

  publicationNarrative.innerHTML = renderPublicationNarrative(publicationStatus);
  renderSummaryHintChips(publicationSummaryHints, [
    `主发布 ${publicationStatus.status_label || '-'}`,
    `正式主榜文件 ${formatNumber(mainListCount)} 只`,
    `跟踪文件 ${formatNumber(counts.project_tracker_current_active_count)} 只`,
    `快照 ${effectiveNav.candidate_date || '-'}`,
  ]);

  latestFileLinks.innerHTML = [
    renderLinkCard('跟踪总表', resolveDashboardFile(payload, 'tracker_file') || resolveDashboardFile(payload, 'project_tracker_excel'), '当前在榜'),
    renderLinkCard('正式主榜', resolveDashboardFile(payload, 'filtered_excel')),
    renderLinkCard('分析报告', resolveDashboardFile(payload, 'report_markdown')),
  ].join('');
}

async function init() {
  // A newly published run invalidates the overview sidecar. Let that rebuild
  // finish before ancillary summaries compete for the same publication data.
  try {
    const dashboardPayload = await loadDashboardOverview();
    renderDashboard(dashboardPayload, {});
  } catch (error) {
    renderRetryableEmptyState(overviewStats, {
      title: '首页数据加载失败',
      message: error?.message || '请检查接口状态后重试',
      actionLabel: '重新加载',
      onRetry: init,
    });
  }

  const [bootstrapResult, apiSettingsResult] = await Promise.allSettled([
    loadBootstrapSummary(),
    loadApiSettings(),
  ]);

  if (apiSettingsResult.status === 'fulfilled') {
    renderHealthBadge(headerHealthBadge, { api_runtime: apiSettingsResult.value });
  } else {
    renderUnavailableBadge(headerHealthBadge, '数据源状态', '接口设置暂不可用');
  }

  if (bootstrapResult.status === 'fulfilled') {
    renderRunBadge(headerRunBadge, getBootstrapWorkflow(bootstrapResult.value));
  } else {
    renderUnavailableBadge(headerRunBadge, '运行状态', '接口暂不可用');
  }

  if (apiSettingsResult.status === 'fulfilled') {
    renderApiSettingsOverview(apiSettingsResult.value);
  } else {
    renderApiSettingsOverview(null);
  }

}

Object.keys(overviewSectionConfig).forEach((key) => {
  bindOverviewSectionToggle(key);
});

refreshApiSettingsOverviewBtn?.addEventListener('click', refreshApiSettingsOverview);

init();
