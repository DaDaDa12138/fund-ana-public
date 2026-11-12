const STATUS_HINTS = {
  idle: '无运行任务',
  running: '任务进行中',
  started: '任务已发起',
  succeeded: '任务已完成',
  success: '任务已完成',
  skipped: '本次已跳过',
  skipped_normal: '自动闭环正常跳过',
  skipped_blocked: '跳过原因需关注',
  degraded: '部分成功',
  failed: '任务失败',
  passed: '检查已通过',
  error: '执行异常',
  accepted: '请求已受理',
  queued: '请求已排队',
  automatic: '自动闭环正常',
  automatic_recovering: '自动恢复或补跑中',
  automatic_with_warnings: '自动运行但有告警',
  attention_required: '需人工处理',
  unknown: '状态未知',
};

const REASON_LABELS = {
  scheduler_process_missing: '调度未运行',
  workflow_process_missing: '任务已结束',
  workflow_busy: '已有任务',
  workflow_busy_after_collection: '采集后有任务',
  effective_nav_snapshot_advanced_without_collection: '快照已推进',
  effective_nav_snapshot_advanced: '快照已推进',
  effective_nav_date_advanced: '净值已推进',
  effective_nav_snapshot_already_screened: '快照已发布',
  effective_nav_date_already_screened: '净值已发布',
  no_previous_screened_snapshot: '首轮筛选',
  no_new_inputs: '无新增输入',
  effective_nav_not_ready: '快照未就绪',
  database_missing: '数据库缺失',
  risk_effective_snapshot_lagging: '风险快照滞后',
  nav_incremental_partial_failure: '净值部分失败',
  period_return_date_lag_quarantined: '收益日期滞后，响应已隔离',
  manager_collection_failed: '经理采集失败',
  manager_collection_retry_cooldown: '经理名单等待自动重试',
  company_collection_failed: '公司资料刷新失败',
  company_collection_retry_cooldown: '公司资料等待自动重试',
  company_collection_summary_invalid: '公司采集统计待核验',
  tiantian_payload_invalid: '上游返回异常',
  independent_source_unavailable: '独立数据源不可用，正式发布暂停',
  historical_false_positive_suppressed: '历史误报已抑制',
  historical_issue_recovering_automatically: '历史失败自动恢复中',
  ready: '数据已就绪',
};

const SCHEDULER_ACTION_CATEGORY_LABELS = {
  screening_triggered: '已重筛',
  report_refresh_triggered: '已重建',
  data_maintenance_changed_only: '仅维护',
  scan_only_no_material_change: '仅巡检',
  none: '无动作',
};

const SCHEDULER_NO_ACTION_CATEGORY_LABELS = {
  already_screened: '快照已处理',
  no_new_nav: '无新净值',
  maintenance_only: '仅维护',
  backlog_empty: '无积压',
  idle_by_design: '按策略空闲',
  no_action_after_scan: '扫描无变更',
  retry_window_pending: '等待重试窗口',
  upstream_limited: '上游受限',
  automation_guard: '保护跳过',
  disabled_by_policy: '策略禁用',
  disabled_by_config: '配置禁用',
  other: '其他原因',
};

const PHASE_LABELS = {
  waiting: '等待中',
  initializing: '初始化',
  'running-core-workflow': '执行核心筛选',
  'building-report': '生成报告',
  completed: '已完成',
  failed: '失败',
};

function workflowStatusHint(value) {
  return labelFromMap(value, STATUS_HINTS, '');
}

function formatReasonCode(value) {
  return labelFromMap(value, REASON_LABELS, '-');
}

function formatSchedulerError(value, enabled = true) {
  if (value) return formatReasonCode(value);
  if (!enabled) return '调度未启用';
  return '无';
}

function formatSchedulerConfig(enabled) {
  if (enabled === null || enabled === undefined || enabled === '') return '未知';
  return enabled ? '已启用' : '未启用';
}

function formatSchedulerRuntime(enabled, running) {
  if (running === null || running === undefined || running === '') {
    return enabled === false ? '未启动' : '未知';
  }
  if (running) return '运行中';
  return enabled ? '未运行' : '未启动';
}

function formatSchedulerHostMode(value) {
  return labelFromMap(
    value,
    {
      external_process: '外部调度进程',
      embedded_web: 'Web 内嵌调度',
    },
    value || '-',
  );
}

function normalizeScheduledRunSemanticStatus(runState) {
  const status = String(runState?.status || '').trim().toLowerCase();
  const semantic = String(runState?.run_semantic_status || '').trim().toLowerCase();
  if (!semantic) return '';
  if (semantic === 'skipped_normal' && status === 'skipped') return semantic;
  if (semantic === 'skipped_blocked' && status === 'skipped') return semantic;
  if (['success', 'running', 'degraded', 'failed'].includes(semantic)) return semantic;
  return '';
}

function formatScheduledRunStatus(runState) {
  if (!runState || (!runState.status && !runState.run_semantic_status)) return '尚无记录';
  const correction = runState.historical_collection_outcome;
  if (correction && correction.status !== 'absent') {
    const recorded = formatWorkflowStatus(runState.status);
    return correction.verified === true
      ? `采集复核${formatWorkflowStatus(correction.derived?.status)}（原记${recorded}）`
      : `原记${recorded} · 复核失效`;
  }
  const semantic = normalizeScheduledRunSemanticStatus(runState);
  return semantic ? formatWorkflowStatus(semantic) : formatWorkflowStatus(runState.status);
}

function formatScheduledRunHint(runState) {
  if (!runState || (!runState.status && !runState.run_semantic_status)) return '暂无记录';
  const correction = runState.historical_collection_outcome;
  if (correction && correction.status !== 'absent') {
    return correction.verified === true
      ? `历史采集复核：${formatReasonCode(correction.derived?.warning_reason)}`
      : '历史证据未通过核验，原记录保留';
  }
  const semantic = normalizeScheduledRunSemanticStatus(runState);
  const semanticHint = semantic ? workflowStatusHint(semantic) : '';
  const reason = runState.screening_reason || runState.skip_reason || runState.warning_reason || '';
  if (reason) {
    const reasonLabel = formatReasonCode(reason);
    return semantic === 'skipped_blocked' && semanticHint ? `${semanticHint}：${reasonLabel}` : reasonLabel;
  }
  return semanticHint || workflowStatusHint(runState.status);
}

function formatSchedulerActionCategory(value) {
  return labelFromMap(value, SCHEDULER_ACTION_CATEGORY_LABELS, '-');
}

function formatSchedulerNoActionCategory(value) {
  return labelFromMap(value, SCHEDULER_NO_ACTION_CATEGORY_LABELS, '-');
}

function formatWorkflowPhase(value) {
  return labelFromMap(value, PHASE_LABELS, '-');
}

function formatSchedulerTaskOutcome(task) {
  if (!task) return '-';
  if (task.no_action_category) return formatSchedulerNoActionCategory(task.no_action_category);
  if (task.action_category) return formatSchedulerActionCategory(task.action_category);
  return formatScheduledRunHint(task);
}

function summarizeSchedulerSnapshot(snapshot) {
  const payload = snapshot || {};
  const candidateDate = payload.candidate_date || '-';
  const fundCount = payload.candidate_fund_count;
  const readiness = payload.ready === true
    ? '就绪'
    : payload.ready === false
      ? formatReasonCode(payload.readiness_reason || '')
      : '';
  const parts = [candidateDate];
  if (fundCount || fundCount === 0) {
    parts.push(`${formatNumber(fundCount)}只`);
  }
  if (readiness) {
    parts.push(readiness);
  }
  return parts.filter(Boolean).join(' / ');
}

function renderSchedulerMaintenanceChanges(items) {
  const rows = Array.isArray(items) ? items : [];
  if (!rows.length) {
    return '<span class="automation-closure-inline">无变更</span>';
  }
  return rows.map((item) => `
    <span class="automation-chip">
      ${escapeHtml(item.label || item.key || '-')}：${escapeHtml(formatNumber(item.value))}
    </span>
  `).join('');
}

function renderSchedulerChildTaskSummary(digest) {
  const payload = digest || {};
  const items = Array.isArray(payload.items) ? payload.items : [];
  if (!items.length) {
    return '<div class="automation-closure-subtasks-empty">无子任务</div>';
  }
  return items.map((item) => {
    const meta = [
      item.no_action_category ? formatSchedulerNoActionCategory(item.no_action_category) : '',
      item.changed_total > 0 ? `变更 ${formatNumber(item.changed_total)}` : '',
      item.processed_total > 0 ? `处理 ${formatNumber(item.processed_total)}` : '',
      item.candidate_total > 0 ? `候选 ${formatNumber(item.candidate_total)}` : '',
      item.skip_reason ? formatReasonCode(item.skip_reason) : '',
    ].filter(Boolean).join(' / ');
    return `
      <div class="automation-closure-subtask ${item.attention_needed ? 'is-attention' : ''}">
        <strong>${escapeHtml(item.task_label || item.task_key || '-')}</strong>
        <span>${escapeHtml(meta || '已完成')}</span>
      </div>
    `;
  }).join('');
}

function renderSchedulerTaskArtifactLinks(task) {
  const payload = task || {};
  const logPath = payload.task_log_file_project_path || payload.task_log_file_relative || payload.task_log_file || '';
  const auditPath = payload.task_run_audit_json_project_path || payload.task_run_audit_json_relative || payload.task_run_audit_json || '';
  const links = [
    logPath ? { label: '任务日志', path: logPath } : null,
    auditPath ? { label: '审计 JSON', path: auditPath } : null,
  ].filter(Boolean);
  if (!links.length) return '';
  return `
    <div class="automation-artifact-row">
      ${links.map((item) => `
        <a class="preview-link" href="${fileUrl(item.path)}" target="_blank" rel="noreferrer" title="${escapeHtml(normalizeProjectPath(item.path))}">
          ${escapeHtml(item.label)}
        </a>
      `).join('')}
    </div>
  `;
}

function renderAutomationClosureCards(digest, options = {}) {
  const payload = digest || {};
  const tasks = Array.isArray(payload.ordered_tasks) ? payload.ordered_tasks : [];
  const detailed = Boolean(options.detailed);
  if (!tasks.length) {
    return '<div class="empty-state-inline">暂无记录</div>';
  }
  return tasks.map((task) => {
    const headline = [
      formatScheduledRunStatus(task),
      formatSchedulerTaskOutcome(task),
    ].filter(Boolean).join(' / ');
    const screeningText = task.screening_triggered
      ? `已触发重筛${task.screening_run_id ? `：${task.screening_run_id}` : ''}`
      : task.report_refresh_triggered
        ? `已重建报告${task.report_refresh_run_id ? `：${task.report_refresh_run_id}` : ''}`
        : '本次未触发重筛';
    const snapshotText = detailed
      ? `
        <div class="automation-closure-snapshot-grid">
          <div class="automation-closure-snapshot">
            <strong>触发前快照</strong>
            <span>${escapeHtml(summarizeSchedulerSnapshot(task.before_effective_nav_snapshot))}</span>
          </div>
          <div class="automation-closure-snapshot">
            <strong>触发后快照</strong>
            <span>${escapeHtml(summarizeSchedulerSnapshot(task.after_effective_nav_snapshot))}</span>
          </div>
          <div class="automation-closure-snapshot">
            <strong>当时已发布快照</strong>
            <span>${escapeHtml(summarizeSchedulerSnapshot(task.latest_screened_snapshot))}</span>
          </div>
        </div>
      `
      : '';
    const badges = [
      task.snapshot_changed_during_task ? '任务内快照有推进' : '',
      task.snapshot_advanced_without_screening ? '快照推进但未重筛' : '',
      task.effective_snapshot_ahead_of_screened ? '后快照领先已发布快照' : '',
    ].filter(Boolean).map((item) => `<span class="automation-chip automation-chip-warning">${escapeHtml(item)}</span>`).join('');
    const reasons = [
      task.screening_reason ? formatReasonCode(task.screening_reason) : '',
      task.skip_reason ? formatReasonCode(task.skip_reason) : '',
      task.warning_reason ? formatReasonCode(task.warning_reason) : '',
    ].filter(Boolean).join(' / ');
    return `
      <article class="automation-closure-card ${task.attention_needed ? 'is-attention' : ''}">
        <div class="automation-closure-head">
          <div>
            <strong>${escapeHtml(task.task_label || task.task_key || '-')}</strong>
            <span>${escapeHtml(headline || '-')}</span>
          </div>
          <div class="automation-closure-meta">
            <span>${escapeHtml(formatDateTime(task.finished_at || task.started_at))}</span>
          </div>
        </div>
        <div class="automation-closure-body">
          <p>${escapeHtml(screeningText)}</p>
          ${reasons ? `<p>${escapeHtml(reasons)}</p>` : ''}
          ${renderSchedulerTaskArtifactLinks(task)}
          ${badges ? `<div class="automation-chip-row">${badges}</div>` : ''}
          <div class="automation-chip-row">${renderSchedulerMaintenanceChanges(task.maintenance_change_items)}</div>
          ${snapshotText}
          <div class="automation-closure-subtasks">
            ${renderSchedulerChildTaskSummary(task.child_task_digest)}
          </div>
        </div>
      </article>
    `;
  }).join('');
}
