(function () {
  function numericValueOrNull(value) {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : null;
  }

  function firstPresentNumber(...values) {
    for (const value of values) {
      const numeric = numericValueOrNull(value);
      if (numeric !== null) return numeric;
    }
    return null;
  }

  function renderInlineState(container, {
    noun = '数据',
    loading = false,
  } = {}) {
    if (!container) return;
    const text = loading ? `${noun}补充加载中` : `暂无${noun}`;
    container.innerHTML = `<div class="empty-state-inline">${escapeHtml(text)}</div>`;
  }

  function renderOperationsRegressionSmoke(summary, smokeTask, options = {}) {
    const statsElement = options.statsElement;
    const narrativeElement = options.narrativeElement;
    const artifactsElement = options.artifactsElement;
    if (!statsElement || !narrativeElement || !artifactsElement) return;

    const payload = summary || {};
    const taskPayload = smokeTask || {};
    const detailPending = Boolean(options.detailPending);
    const groupResults = Array.isArray(payload?.group_results) ? payload.group_results : [];
    const groups = Array.isArray(payload?.groups) ? payload.groups : [];
    const passedGroupTotal = firstPresentNumber(payload?.passed_group_total);
    const failedGroupTotal = firstPresentNumber(payload?.failed_group_total);
    const summaryPath = payload?.summary_path || taskPayload?.summary_path || '';
    const artifactDir = payload?.artifact_dir || taskPayload?.artifact_dir || '';
    const source = payload?.source || taskPayload?.source || '';
    const generatedAt = payload?.generated_at || taskPayload?.generated_at || '';
    const overallStatus = payload?.overall_status || taskPayload?.overall_status || '';
    const taskStatus = taskPayload?.status || payload?.status || '';
    const issueClassificationStatus = payload?.issue_classification_status || taskPayload?.issue_classification_status || '';
    const issueClassificationLabel = payload?.issue_classification_label || taskPayload?.issue_classification_label || issueClassificationStatus || '';
    const testExpectationMismatchTotal = firstPresentNumber(payload?.test_expectation_mismatch_total, taskPayload?.test_expectation_mismatch_total);
    const productionDegradationTotal = firstPresentNumber(payload?.production_degradation_total, taskPayload?.production_degradation_total);
    const firstFailedGroup = groupResults.find((item) => String(item?.status || '').toLowerCase() === 'failed') || null;

    if (!Object.keys(payload).length && !Object.keys(taskPayload).length) {
      statsElement.innerHTML = buildStatCard('Smoke 自检', '暂无', '最近一次周维护尚未写入');
      narrativeElement.innerHTML = `
        <strong>自检</strong>
        <span>暂无最近一次 smoke 结果</span>
      `;
      renderInlineState(artifactsElement, {
        noun: detailPending ? 'Smoke 产物' : '产物',
        loading: detailPending,
      });
      return;
    }

    statsElement.innerHTML = [
      buildStatCard('总体状态', formatWorkflowStatus(overallStatus || taskStatus || 'unknown'), source === 'latest_artifact_fallback' ? '暂由最近产物补齐展示' : '来自最近一次周维护'),
      buildStatCard('已通过分组', formatNumber(passedGroupTotal), `共 ${formatNumber(firstPresentNumber(groups.length, taskPayload?.candidate_total))} 组`),
      buildStatCard('失败分组', formatNumber(failedGroupTotal), firstFailedGroup ? String(firstFailedGroup.group || '-') : '当前无失败分组'),
      buildStatCard(
        '失败分类',
        issueClassificationLabel || '-',
        issueClassificationStatus
          ? `测试口径 ${formatNumber(testExpectationMismatchTotal)} / 真实退化 ${formatNumber(productionDegradationTotal)}`
          : '暂无分类'
      ),
      buildStatCard('最近生成时间', formatDateTime(generatedAt), summaryPath ? '已生成摘要产物' : '暂无摘要文件'),
      buildStatCard('运行态子任务', formatWorkflowStatus(taskStatus || 'unknown'), taskPayload?.processed_total ? `处理 ${formatNumber(taskPayload.processed_total)} 组` : '暂无子任务处理量'),
      buildStatCard('结果来源', source === 'latest_artifact_fallback' ? '最近产物补齐' : '周维护实时写入', source === 'latest_artifact_fallback' ? '当前结果先用最近产物补齐，下一次周维护后会切回实时写入' : '直接来自周维护实时写入'),
    ].join('');

    narrativeElement.innerHTML = `
      <strong>自检</strong>
      <span>${escapeHtml([
        overallStatus ? `总体 ${formatWorkflowStatus(overallStatus)}` : '',
        issueClassificationLabel ? `分类 ${issueClassificationLabel}` : '',
        groups.length ? `分组 ${groups.join(' / ')}` : '',
        source === 'latest_artifact_fallback' ? '当前结果先用最近产物补齐，下一次周维护后会恢复为实时写入口径' : '',
      ].filter(Boolean).join('；') || '暂无摘要')}</span>
    `;

    const cards = [];
    if (summaryPath) {
      cards.push(`
        <article class="data-card">
          <div class="data-card-item">
            <strong>摘要文件</strong>
            <span>${escapeHtml(summaryPath.split('/').pop() || summaryPath)}</span>
          </div>
          <div class="data-card-item">
            <strong>路径</strong>
            <span>${escapeHtml(compactUiText(normalizeProjectPath(summaryPath), 36) || '-')}</span>
          </div>
          <div class="data-card-item">
            <strong>操作</strong>
            <span><a class="preview-link" href="${fileUrl(summaryPath)}" target="_blank" rel="noreferrer">查看</a></span>
          </div>
        </article>
      `);
    }
    if (artifactDir) {
      cards.push(`
        <article class="data-card">
          <div class="data-card-item">
            <strong>产物目录</strong>
            <span>${escapeHtml(artifactDir.split('/').pop() || artifactDir)}</span>
          </div>
          <div class="data-card-item">
            <strong>来源</strong>
            <span>${escapeHtml(source || '-')}</span>
          </div>
          <div class="data-card-item">
            <strong>分组结果</strong>
            <span>${escapeHtml(groupResults.slice(0, 3).map((item) => `${item?.group || '-'}:${formatWorkflowStatus(item?.status || 'unknown')}`).join(' / ') || '-')}</span>
          </div>
        </article>
      `);
    }
    if (cards.length) {
      artifactsElement.innerHTML = cards.join('');
    } else {
      renderInlineState(artifactsElement, {
        noun: detailPending ? 'Smoke 产物' : '产物',
        loading: detailPending,
      });
    }
  }

  window.OperationsRegressionSmoke = Object.freeze({
    renderOperationsRegressionSmoke,
  });
}());
