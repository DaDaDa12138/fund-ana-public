(function () {
  function renderOperationsAutomationLayers(layers, publicationStatus = {}, options = {}) {
    const statsElement = options.statsElement;
    const narrativeElement = options.narrativeElement;
    if (!statsElement || !narrativeElement) return;

    const latestDb = layers?.latest_database_snapshot || {};
    const latestSuccess = layers?.latest_successful_run || {};
    const latestPublishable = layers?.latest_publishable_run || {};
    const currentPublished = layers?.current_published_run || {};
    const blockers = Array.isArray(latestSuccess?.publish_blockers) ? latestSuccess.publish_blockers : [];
    const primaryBlocker = blockers[0] || {};
    const replayPrecision = latestSuccess?.replay_precision || {};

    statsElement.innerHTML = [
      buildStatCard('最新数据库快照', latestDb.candidate_date || '-', latestDb.ready ? '已就绪' : formatReasonCode(latestDb.readiness_reason || '')),
      buildStatCard('最新筛选运行', latestSuccess.run_id || '-', latestSuccess.generated_at ? formatDateTime(latestSuccess.generated_at) : '暂无'),
      buildStatCard('最新可发布运行', latestPublishable.run_id || '-', latestPublishable.generated_at ? formatDateTime(latestPublishable.generated_at) : '暂无'),
      buildStatCard('当前正式发布', currentPublished.run_id || '-', currentPublished.generated_at ? formatDateTime(currentPublished.generated_at) : '暂无'),
      buildStatCard('最新运行可发布', latestSuccess.run_id ? (latestSuccess.publishable ? '可以' : '不可以') : '暂无', latestSuccess.run_id ? (latestSuccess.publishable ? '发布条件已满足' : (primaryBlocker.label || '仍有阻断项')) : '暂无'),
      buildStatCard('最新运行已发布', latestSuccess.run_id ? (layers?.latest_successful_is_currently_published ? '是' : '否') : '暂无', latestSuccess.run_id ? '判断最新运行是否已成为正式版本' : '暂无'),
      buildStatCard('历史回放验证', replayPrecision.status_label || '暂无', replayPrecision.detail || publicationStatus.replay_precision_detail || '尚未验证历史回放结果'),
      buildStatCard('当前发布判断', publicationStatus.status_label || '-', publicationStatus.recommended_action || '暂无额外说明'),
    ].join('');

    narrativeElement.innerHTML = `
      <strong>${escapeHtml(publicationStatus.status_label || '分层')}</strong>
      <span>${escapeHtml([
        `快照 ${latestDb.candidate_date || '-'}`,
        `运行 ${latestSuccess.run_id || '-'}`,
        `可发 ${latestPublishable.run_id || '-'}`,
        `发布 ${currentPublished.run_id || '-'}`,
      ].join('；'))}</span>
      ${publicationStatus.detail ? `<small>${escapeHtml(publicationStatus.detail)}</small>` : ''}
    `;
  }

  window.OperationsAutomationLayers = Object.freeze({
    renderOperationsAutomationLayers,
  });
}());
