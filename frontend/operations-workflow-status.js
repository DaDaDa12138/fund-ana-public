(function () {
  function getOperationsWorkflowLogs(state) {
    if (Array.isArray(state?.logs)) return state.logs;
    if (Array.isArray(state?.logs_tail)) return state.logs_tail;
    return [];
  }

  function renderOperationsWorkflowStatus(state, options = {}) {
    const statusElement = options.statusElement;
    const narrativeElement = options.narrativeElement;
    const logsElement = options.logsElement;
    if (!statusElement || !logsElement) return;

    const payload = state || {};
    const logs = getOperationsWorkflowLogs(payload);
    const latestMoment = payload.finished_at || payload.started_at || '';
    const latestMomentLabel = payload.finished_at
      ? '最近结束'
      : payload.started_at
        ? '最近开始'
        : '最近时间';
    const items = [
      ['运行状态', formatWorkflowStatus(payload.status || ''), '当前工作流状态'],
      ['当前阶段', formatWorkflowPhase(payload.phase || ''), '当前执行阶段'],
      ['批次 ID', payload.run_id || '-', '当前或最近一次运行批次'],
      [
        latestMomentLabel,
        formatDateTime(latestMoment),
        payload.finished_at ? '最近一次完成时间' : (payload.started_at ? '当前批次开始时间' : '暂无时间'),
      ],
    ];
    statusElement.innerHTML = items.map(([label, value, hint]) => buildStatCard(label, value, hint)).join('');

    if (narrativeElement) {
      narrativeElement.innerHTML = `
        <strong>工作流</strong>
        <span>${escapeHtml([
          `状态 ${formatWorkflowStatus(payload.status || 'unknown')}`,
          `阶段 ${formatWorkflowPhase(payload.phase || 'unknown')}`,
          payload.run_id ? `批次 ${payload.run_id}` : '',
          payload.started_at ? `开始 ${formatDateTime(payload.started_at)}` : '',
          payload.finished_at ? `结束 ${formatDateTime(payload.finished_at)}` : '',
          payload.error ? `错误 ${formatReasonCode(payload.error || '')}` : '',
        ].filter(Boolean).join('；') || '暂无状态')}</span>
      `;
    }
    logsElement.textContent = logs.join('\n').trim() || '暂无日志';
  }

  function renderOperationsWorkflowStatusFailure(error, options = {}) {
    const statusElement = options.statusElement;
    const narrativeElement = options.narrativeElement;
    const logsElement = options.logsElement;
    const message = error?.message || '状态接口暂不可用';
    if (statusElement) {
      renderRetryableEmptyState(statusElement, {
        title: '工作流状态加载失败',
        message,
        actionLabel: '重新加载',
        onRetry: options.onRetry,
      });
    }
    if (narrativeElement) {
      narrativeElement.innerHTML = `
        <strong>工作流</strong>
        <span>${escapeHtml(message)}</span>
      `;
    }
    if (logsElement) {
      logsElement.textContent = message;
    }
  }

  window.OperationsWorkflowStatus = Object.freeze({
    getOperationsWorkflowLogs,
    renderOperationsWorkflowStatus,
    renderOperationsWorkflowStatusFailure,
  });
}());
