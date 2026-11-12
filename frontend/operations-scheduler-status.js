(function () {
  function renderTaskTiming(task) {
    const timing = task?.task_timing;
    if (timing?.schema_version !== 1) return '';
    const seconds = (value) => typeof value === 'number' && Number.isFinite(value) && value >= 0
      ? `${value.toFixed(1)} 秒` : '未知';
    const phases = timing.phases || {};
    const labels = { business: '业务处理', archive_observation: '历史巡检', storage_observation: '容量采样', finalization: '审计与收尾' };
    const rows = Object.entries(labels).filter(([key]) => phases[key]).map(([key, label]) =>
      `<li>${escapeHtml(label)}：${escapeHtml(seconds(phases[key].elapsed_seconds))}</li>`).join('');
    return `<details class="compact-disclosure task-timing"><summary>用时 ${escapeHtml(seconds(timing.elapsed_seconds))}</summary>
      ${task.business_finished_at ? `<p>业务结束：${escapeHtml(formatDateTime(task.business_finished_at))}</p>` : ''}
      <ul>${rows}</ul></details>`;
  }

  function renderHistoricalCollectionCounts(correction) {
    if (!correction || correction.status === 'absent') return '';
    if (!correction.verified) {
      return '<details class="compact-disclosure historical-collection-counts"><summary>历史采集计数待核验</summary><p>原始证据无法通过核验，暂不显示纠正计数。</p></details>';
    }
    const count = (value) => Number.isInteger(value) && value >= 0 ? formatNumber(value) : '未知';
    const describe = (counts) => `目标 ${count(counts?.target_count)} · 成功 ${count(counts?.success_count)} · 失败 ${count(counts?.failed_count)} · 跳过 ${count(counts?.skipped_count)}`;
    const stages = Array.isArray(correction.stages) ? correction.stages : [];
    return `<details class="compact-disclosure historical-collection-counts">
      <summary>历史采集计数核验</summary>
      <ul>${stages.map((stage) => `<li><strong>${escapeHtml(stage.label)}（${escapeHtml(stage.unit)}）</strong>
        <p>采集器：${escapeHtml(describe(stage.collector_outcomes))}</p>
        ${stage.outcome_labels_match === false ? `<p>执行进度：${escapeHtml(describe(stage.execution_outcomes))}</p>` : ''}
      </li>`).join('')}</ul>
      <p>各阶段分别计数；跨阶段去重总数未知。历史采集问题仍保留原状态。</p>
      <a class="preview-link" href="/api/scheduler/tasks/${encodeURIComponent(correction.task_id)}/collection-counts" target="_blank" rel="noreferrer">查看核验依据</a>
    </details>`;
  }

  function renderHistoricalCollectionOutcomes(payload) {
    const records = Array.isArray(payload?.records) ? payload.records : [];
    if (!records.length) return '';
    const count = (value) => Number.isInteger(value) && value >= 0 ? formatNumber(value) : '未知';
    const recordsHtml = records.map((record) => {
      const link = `/api/scheduler/tasks/${encodeURIComponent(record.task_id)}/collection-outcome`;
      if (record.verified !== true) {
        return `<article><strong>任务 ${escapeHtml(String(record.task_id))}：历史采集复核失效</strong>
          <p>原始证据未通过核验，暂不显示推导状态和计数。</p>
          <a class="preview-link" href="${link}" target="_blank" rel="noreferrer">查看核验结果</a></article>`;
      }
      const phases = Array.isArray(record.phases) ? record.phases : [];
      const status = formatWorkflowStatus(record.derived?.status);
      const reason = record.derived?.warning_reason === 'period_return_date_lag_quarantined'
        ? '收益数据日期滞后，相关响应已隔离' : formatReasonCode(record.derived?.warning_reason);
      return `<article data-historical-task-id="${escapeHtml(String(record.task_id))}">
        <strong>任务 ${escapeHtml(String(record.task_id))} · ${escapeHtml(formatDateTime(record.task_identity?.started_at))}</strong>
        <p>原记录：${escapeHtml(formatWorkflowStatus(record.recorded?.status))}；采集复核：${escapeHtml(status)}。${escapeHtml(reason)}</p>
        <ul>${phases.map((phase, index) => `<li>第 ${index + 1} 段（${escapeHtml(phase.scope === 'parent' ? '初始采集' : '工作流内采集')}）：
          目标 ${escapeHtml(count(phase.target_count))}，保存 ${escapeHtml(count(phase.saved_fund_count))}，
          失败 ${escapeHtml(count(phase.failed_count))}，当时待补 ${escapeHtml(count(phase.pending_count))}。</li>`).join('')}</ul>
        <p>原记录待补 ${escapeHtml(count(record.recorded?.pending_count))}，复核末段待补 ${escapeHtml(count(record.derived?.pending_count))}；各段可能涉及相同基金，不累计为唯一基金数。</p>
        <a class="preview-link" href="${link}" target="_blank" rel="noreferrer">查看核验依据</a>
      </article>`;
    }).join('');
    return `<details class="compact-disclosure historical-collection-outcomes">
      <summary>历史采集复核 · ${escapeHtml(String(records.length))} 项</summary>
      ${recordsHtml}<p>以上解释当时的任务结果，原始记录保留；待补数量不代表当前队列。</p>
      ${payload.truncated ? `<p>当前显示最近 ${escapeHtml(String(records.length))} 项，共 ${escapeHtml(count(payload.available_count))} 项。</p>` : ''}
    </details>`;
  }

  function renderTaskRunArtifacts(artifacts, elements = {}) {
    const automationClosureCards = elements.automationClosureCards;
    if (!automationClosureCards) return;
    const payload = artifacts || {};
    const tasks = Array.isArray(payload.ordered_tasks) ? payload.ordered_tasks : [];
    if (!tasks.length) return;
    const cards = tasks.slice(0, 4).map((task) => {
      const logPath = task.task_log_file_project_path || task.task_log_file_relative || task.task_log_file || '';
      const auditPath = task.task_run_audit_json_project_path || task.task_run_audit_json_relative || task.task_run_audit_json || '';
      const links = [
        logPath ? `<a class="preview-link" href="${fileUrl(logPath)}" target="_blank" rel="noreferrer">任务日志</a>` : '',
        auditPath ? `<a class="preview-link" href="${fileUrl(auditPath)}" target="_blank" rel="noreferrer">审计 JSON</a>` : '',
      ].filter(Boolean).join('');
      const timeLabel = task.task_timing ? (task.finished_at ? '结束' : '收尾中') : '记录';
      return `
        <article class="automation-closure-card" data-task-identity="${escapeHtml(`${task.task_key || task.task_name}:${task.started_at || ''}`)}">
          <div class="automation-closure-head">
            <div>
              <strong>${escapeHtml(task.task_label || task.task_key || '-')}</strong>
              <span>${escapeHtml(formatScheduledRunStatus(task))}</span>
            </div>
            <div class="automation-closure-meta">
              <span>${escapeHtml(timeLabel)} ${escapeHtml(formatDateTime(task.finished_at || task.started_at))}</span>
            </div>
          </div>
          <div class="automation-closure-body">
            <p>${escapeHtml(task.workflow_run_id ? `关联运行：${task.workflow_run_id}` : '最近真实任务产物')}</p>
            <div class="automation-artifact-row">${links || '<span class="automation-closure-inline">暂无链接</span>'}</div>
            <p>${escapeHtml(compactUiText(normalizeProjectPath(logPath || auditPath), 54) || '-')}</p>
            ${renderTaskTiming(task)}
          </div>
        </article>
      `;
    }).join('');
    automationClosureCards.insertAdjacentHTML(
      'afterbegin',
      `<div class="automation-artifact-section">${cards}</div>`,
    );
  }

  function renderOperationsSchedulerStatus(state, audit, options = {}) {
    const elements = options.elements || {};
    const getSchedulerRunMap = options.getSchedulerRunMap;
    const resolveRiskCoverageSemantics = options.resolveRiskCoverageSemantics;
    const formatCoverageRatioPercent = options.formatCoverageRatioPercent;
    const {
      schedulerStats,
      schedulerSecondaryStats,
      schedulerNarrative,
      automationClosureStats,
      automationClosureCards,
      automationClosureNarrative,
    } = elements;
    if (!schedulerStats || !schedulerNarrative || !automationClosureStats || !automationClosureCards || !automationClosureNarrative) {
      return;
    }
    if (typeof getSchedulerRunMap !== 'function' || typeof resolveRiskCoverageSemantics !== 'function' || typeof formatCoverageRatioPercent !== 'function') {
      throw new Error('Operations scheduler status helpers are incomplete');
    }

    const overview = state?.scheduler_overview || state || {};
    const effective = overview?.effective_nav_snapshot || {};
    const navFreshness = overview?.nav_freshness || {};
    const latestScreened = overview?.latest_screened_snapshot || {};
    const currentTask = overview?.current_task || {};
    const chainAudit = state?.scheduler_chain_audit || audit || {};
    const closureDigest = overview?.automation_closure_digest || state?.automation_closure_digest || {};
    const closureSummary = closureDigest?.summary || {};
    const openIssues = Array.isArray(chainAudit.open_issues) ? chainAudit.open_issues : [];
    const firstIssue = openIssues[0] || {};
    const auditRuns = chainAudit.runs || {};
    const schedulerRuns = getSchedulerRunMap(state);
    const historicalOutcomes = state?.historical_collection_outcomes || overview?.historical_collection_outcomes;
    const historicalCount = Array.isArray(historicalOutcomes?.records) ? historicalOutcomes.records.length : 0;
    const persistedDailyRun = schedulerRuns.scheduled_daily_bond_refresh || {};
    const persistedIntradayRun = schedulerRuns.scheduled_intraday_bond_refresh || {};
    const persistedWeeklyRun = schedulerRuns.scheduled_weekly_bond_maintenance || {};
    const latestRun = persistedDailyRun.status
      ? persistedDailyRun
      : currentTask?.task_name === 'scheduled_daily_bond_refresh'
        ? { status: 'running', started_at: currentTask.started_at }
        : {};
    const intradayRun = persistedIntradayRun.status
      ? persistedIntradayRun
      : currentTask?.task_name === 'scheduled_intraday_bond_refresh'
        ? { status: 'running', started_at: currentTask.started_at }
        : {};
    const auditWeeklyRun = auditRuns.weekly_maintenance || {};
    const weeklyRun = persistedWeeklyRun.status
      ? {
          ...persistedWeeklyRun,
          status: auditWeeklyRun.current_recovery_in_progress || persistedWeeklyRun.current_recovery_in_progress
            ? 'running'
            : auditWeeklyRun.suppressed_historical_false_positive
              ? 'success'
              : (auditWeeklyRun.status || persistedWeeklyRun.status),
          run_semantic_status: auditWeeklyRun.current_recovery_in_progress || persistedWeeklyRun.current_recovery_in_progress
            ? 'running'
            : (persistedWeeklyRun.run_semantic_status || auditWeeklyRun.run_semantic_status),
          warning_reason: auditWeeklyRun.suppressed_historical_false_positive
            ? 'historical_false_positive_suppressed'
            : auditWeeklyRun.current_recovery_in_progress || persistedWeeklyRun.current_recovery_in_progress
              ? 'historical_issue_recovering_automatically'
              : (auditWeeklyRun.warning_reason || persistedWeeklyRun.warning_reason),
          degraded_stage: auditWeeklyRun.suppressed_historical_false_positive
            ? ''
            : auditWeeklyRun.current_recovery_in_progress || persistedWeeklyRun.current_recovery_in_progress
              ? ''
              : (auditWeeklyRun.degraded_stage || persistedWeeklyRun.degraded_stage),
        }
      : currentTask?.task_name === 'scheduled_weekly_bond_maintenance'
        ? { status: 'running', started_at: currentTask.started_at }
        : {};
    const latestRunStatus = formatScheduledRunStatus(latestRun);
    const latestRunHint = latestRun.no_action_category
      ? `${formatScheduledRunHint(latestRun)} / ${formatSchedulerNoActionCategory(latestRun.no_action_category)}`
      : latestRun.action_category
        ? `${formatScheduledRunHint(latestRun)} / ${formatSchedulerActionCategory(latestRun.action_category)}`
        : formatScheduledRunHint(latestRun);
    const intradayRunStatus = formatScheduledRunStatus(intradayRun);
    const intradayRunHint = intradayRun.no_action_category
      ? `${formatScheduledRunHint(intradayRun)} / ${formatSchedulerNoActionCategory(intradayRun.no_action_category)}`
      : intradayRun.action_category
        ? `${formatScheduledRunHint(intradayRun)} / ${formatSchedulerActionCategory(intradayRun.action_category)}`
        : formatScheduledRunHint(intradayRun);
    const weeklyRunStatus = formatScheduledRunStatus(weeklyRun);
    const weeklyRunHint = weeklyRun.no_action_category
      ? `${formatScheduledRunHint(weeklyRun)} / ${formatSchedulerNoActionCategory(weeklyRun.no_action_category)}`
      : weeklyRun.action_category
        ? `${formatScheduledRunHint(weeklyRun)} / ${formatSchedulerActionCategory(weeklyRun.action_category)}`
        : formatScheduledRunHint(weeklyRun);
    const schedulerRuntime = formatSchedulerRuntime(overview?.enabled, overview?.scheduler_running);
    const riskCoverage = resolveRiskCoverageSemantics(effective);
    schedulerStats.innerHTML = [
      buildStatCard('调度配置', formatSchedulerConfig(overview?.enabled), '配置项 scheduler.enabled'),
      buildStatCard('调度进程', schedulerRuntime, '后台调度器进程状态'),
      buildStatCard('最近日常调度', latestRunStatus, compactUiText(latestRunHint || '最近一次日常调度结果', 18)),
      buildStatCard('当前有效快照', effective.candidate_date || '-', '数据库当前有效净值日期'),
      buildStatCard(
        '风险覆盖',
        riskCoverage.ratio === null ? '-' : formatCoverageRatioPercent(riskCoverage.ratio),
        riskCoverage.gateLabel || '发布门槛与观察线',
      ),
    ].join('');

    if (schedulerSecondaryStats) {
      schedulerSecondaryStats.innerHTML = [
        buildStatCard('最近盘中补刷新', intradayRunStatus, compactUiText(intradayRunHint || '最近一次盘中补刷新结果', 18)),
        buildStatCard('最近周维护', weeklyRunStatus, compactUiText(weeklyRunHint || '最近一次周维护结果', 18)),
        buildStatCard(
          '启动补跑待处理',
          chainAudit.startup_catch_up_pending ? '是' : '否',
          chainAudit.startup_catch_up_pending
            ? compactUiText((chainAudit.startup_catch_up_due_jobs || []).join(', '), 18)
            : '当前无待补跑任务'
        ),
        buildStatCard(
          '待补筛快照',
          chainAudit.screening_backlog_due ? '是' : '否',
          chainAudit.screening_backlog_due
            ? compactUiText(chainAudit.screening_backlog_reason || '数据库有效快照已领先发布版本', 18)
            : '当前无待补筛快照'
        ),
        buildStatCard(
          '风险发布门槛',
          riskCoverage.publishMinimum === null ? '-' : formatCoverageRatioPercent(riskCoverage.publishMinimum),
          riskCoverage.gateStatus || 'publish_gate',
        ),
        buildStatCard(
          '风险观察线',
          riskCoverage.observationLine === null ? '-' : formatCoverageRatioPercent(riskCoverage.observationLine),
          riskCoverage.gateExplanation || 'operational_watchline',
        ),
      ].join('');
    }

    schedulerNarrative.innerHTML = `
      <strong>调度</strong>
      <span>${escapeHtml([
        `日更 ${latestRunStatus}`,
        `盘中 ${intradayRunStatus}`,
        `周维 ${weeklyRunStatus}`,
        `原始净值 ${navFreshness.raw_latest_nav_date || '-'}`,
        `覆盖 ${formatNumber(effective.candidate_fund_count)} 只`,
        `发布快照 ${latestScreened.candidate_date || '-'}`,
        overview.publication_dependency?.code ? formatReasonCode(overview.publication_dependency.code) : '',
        chainAudit.issue_count ? `异常 ${chainAudit.issue_count} 项` : historicalCount ? `历史采集差异 ${historicalCount} 项` : '无异常',
        firstIssue.label ? `首项 ${firstIssue.label}` : '',
      ].filter(Boolean).join('；'))}</span>
    `;

    automationClosureStats.innerHTML = [
      buildStatCard('最近闭环任务数', formatNumber(closureSummary.task_count), '固定覆盖四类自动化任务'),
      buildStatCard('最近触发重筛', formatNumber(closureSummary.screening_triggered_total), '真正进入正式筛选的任务数'),
      buildStatCard('最近重建报告', formatNumber(closureSummary.report_refresh_triggered_total), '仅维护后触发发布层重建'),
      buildStatCard('存在待复核任务', formatNumber(closureSummary.attention_total), '含快照推进未重筛、上游受限、任务降级'),
      buildStatCard('子任务上游受限', formatNumber(closureSummary.upstream_limited_child_task_total), '维护链路中的上游受限子任务数'),
      buildStatCard('任务内快照推进', formatNumber(closureSummary.snapshot_changed_total), '触发前后有效快照发生变化的任务数'),
    ].join('');
    const correction = persistedWeeklyRun.historical_collection_counts;
    const correctionKey = correction ? `${correction.task_id || ''}:${correction.verified ? correction.record_sha256 : correction.status}` : '';
    const previousCounts = automationClosureCards.querySelector('.historical-collection-counts');
    const previousFocus = previousCounts?.contains(document.activeElement) ? document.activeElement : null;
    const previousTimings = new Map([...automationClosureCards.querySelectorAll('.task-timing')].map((detail) => [
      detail.closest('[data-task-identity]')?.dataset.taskIdentity,
      { open: detail.open, focused: document.activeElement === detail.querySelector('summary') },
    ]));
    const outcomeKey = (historicalOutcomes?.records || []).map((record) =>
      `${record.task_id}:${record.verified ? record.record_sha256 : record.status}`).join('|');
    const previousOutcomes = automationClosureCards.querySelector('.historical-collection-outcomes');
    const previousOutcomeFocus = previousOutcomes?.contains(document.activeElement) ? document.activeElement : null;
    automationClosureCards.innerHTML = renderAutomationClosureCards(closureDigest, { detailed: true });
    if (previousCounts && correctionKey && previousCounts.dataset.correctionKey === correctionKey) {
      automationClosureCards.appendChild(previousCounts);
      previousFocus?.focus({ preventScroll: true });
    } else {
      automationClosureCards.insertAdjacentHTML('beforeend', renderHistoricalCollectionCounts(correction));
      const currentCounts = automationClosureCards.querySelector('.historical-collection-counts');
      if (currentCounts) currentCounts.dataset.correctionKey = correctionKey;
    }
    if (previousOutcomes && outcomeKey && previousOutcomes.dataset.outcomeKey === outcomeKey) {
      automationClosureCards.appendChild(previousOutcomes);
      previousOutcomeFocus?.focus({ preventScroll: true });
    } else {
      automationClosureCards.insertAdjacentHTML('beforeend', renderHistoricalCollectionOutcomes(historicalOutcomes));
      const outcomes = automationClosureCards.querySelector('.historical-collection-outcomes');
      if (outcomes) outcomes.dataset.outcomeKey = outcomeKey;
    }
    const closureAlerts = Array.isArray(closureDigest?.alerts) ? closureDigest.alerts : [];
    automationClosureNarrative.innerHTML = `
      <strong>闭环</strong>
      <span>${closureAlerts.length ? `告警 ${escapeHtml(String(closureAlerts.length))} 条` : '无新增告警'}</span>
    `;
    renderTaskRunArtifacts(state?.task_run_artifacts || {}, { automationClosureCards });
    automationClosureCards.querySelectorAll('.task-timing').forEach((detail) => {
      const previous = previousTimings.get(detail.closest('[data-task-identity]')?.dataset.taskIdentity);
      if (!previous) return;
      detail.open = previous.open;
      if (previous.focused) detail.querySelector('summary')?.focus({ preventScroll: true });
    });
  }

  window.OperationsSchedulerStatus = Object.freeze({
    renderOperationsSchedulerStatus,
    renderHistoricalCollectionCounts,
    renderHistoricalCollectionOutcomes,
    renderTaskTiming,
  });
}());
