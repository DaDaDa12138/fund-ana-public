(function () {
  function renderOperationsSamplePills(items) {
    const values = (Array.isArray(items) ? items : [])
      .map((item) => String(item || '').trim())
      .filter(Boolean);
    if (!values.length) return '';
    return `
      <div class="operations-sample-pill-list">
        ${values.map((text) => `<span class="operations-sample-pill">${escapeHtml(text)}</span>`).join('')}
      </div>
    `;
  }

  function renderOperationsIntegrityAudits(publicationIntegrity, artifactConsistency, options = {}) {
    const elements = options.elements || {};
    const statsElement = elements.statsElement;
    const narrativeElement = elements.narrativeElement;
    if (!statsElement || !narrativeElement) return;

    const publication = publicationIntegrity || {};
    const artifact = artifactConsistency || {};
    const publicationBlocker = Array.isArray(publication.blockers) ? publication.blockers[0] || {} : {};
    const artifactBlocker = Array.isArray(artifact.blockers) ? artifact.blockers[0] || {} : {};

    statsElement.innerHTML = [
      buildStatCard(
        '运行内产物一致性',
        artifact.check_count ? (artifact.aligned ? '通过' : '异常') : '暂无',
        artifact.check_count ? `检查 ${formatNumber(artifact.check_count)} 项` : '当前报告未提供'
      ),
      buildStatCard(
        '运行内阻断项',
        formatNumber(artifact.blocker_count),
        artifactBlocker.name ? `${artifactBlocker.name}` : '无阻断项'
      ),
      buildStatCard(
        '正式发布链一致性',
        publication.check_count ? (publication.aligned ? '通过' : '异常') : '暂无',
        publication.check_count ? `检查 ${formatNumber(publication.check_count)} 项` : '当前发布层未提供'
      ),
      buildStatCard(
        '正式发布链阻断项',
        formatNumber(publication.blocker_count),
        publicationBlocker.name ? `${publicationBlocker.name}` : '无阻断项'
      ),
    ].join('');

    narrativeElement.innerHTML = `
      <strong>审计</strong>
      <span>${escapeHtml([
        `运行链路 ${artifact.check_count ? (artifact.aligned ? '通过' : '异常') : '暂无'}`,
        `发布链路 ${publication.check_count ? (publication.aligned ? '通过' : '异常') : '暂无'}`,
        artifactBlocker.name ? `运行问题 ${artifactBlocker.name}` : '',
        publicationBlocker.name ? `发布问题 ${publicationBlocker.name}` : '',
      ].filter(Boolean).join('；') || '暂无摘要')}</span>
    `;
  }

  function renderOperationsExclusionSummary(summary, options = {}) {
    const elements = options.elements || {};
    const statsElement = elements.statsElement;
    const narrativeElement = elements.narrativeElement;
    const samplesElement = elements.samplesElement;
    const renderInlineState = options.renderInlineState;
    if (!statsElement || !narrativeElement || !samplesElement) return;
    if (typeof renderInlineState !== 'function') {
      throw new Error('Operations dashboard watch helpers are incomplete');
    }

    const bucketCounts = Array.isArray(summary?.high_score_bucket_counts) ? summary.high_score_bucket_counts : [];
    const topReasons = Array.isArray(summary?.top_reasons) ? summary.top_reasons : [];
    const samples = Array.isArray(summary?.top_high_score_excluded) ? summary.top_high_score_excluded : [];

    statsElement.innerHTML = [
      buildStatCard('高收益未入正式主榜', formatNumber(summary?.high_score_candidate_total), '按项目治理候选池中的高收益未入正式主榜样本汇总'),
      buildStatCard(
        '首要分类',
        bucketCounts[0] ? `${bucketCounts[0].label}` : '暂无',
        bucketCounts[0] ? `数量 ${formatNumber(bucketCounts[0].count)}` : '暂无诊断样本'
      ),
      buildStatCard(
        '次要分类',
        bucketCounts[1] ? `${bucketCounts[1].label}` : '暂无',
        bucketCounts[1] ? `数量 ${formatNumber(bucketCounts[1].count)}` : '暂无第二分类'
      ),
      buildStatCard(
        '首要细分原因',
        topReasons[0] ? `${topReasons[0].reason}` : '暂无',
        topReasons[0] ? `数量 ${formatNumber(topReasons[0].count)}` : '暂无细分原因'
      ),
    ].join('');

    narrativeElement.innerHTML = `
      <strong>诊断</strong>
      <span>${escapeHtml(bucketCounts[0] ? `当前主要未入榜原因是 ${bucketCounts[0].label}（${formatNumber(bucketCounts[0].count)}只）` : '暂无主要未入榜原因')}</span>
    `;

    if (!samples.length) {
      renderInlineState(samplesElement, { noun: '未入榜样本' });
      return;
    }

    samplesElement.innerHTML = samples.map((item) => `
      <article class="data-card operations-sample-card operations-sample-card-exclusion">
        <div class="operations-sample-head">
          <div class="operations-sample-title">
            <span class="operations-sample-code table-code">${escapeHtml(item.fcode || '-')}</span>
            <strong class="operations-sample-name">${escapeHtml(item.fund_name || '-')}</strong>
          </div>
          <div class="operations-sample-emphasis">
            <span>综合得分</span>
            <strong>${escapeHtml(String(item.weighted_score ?? '-'))}</strong>
          </div>
        </div>
        ${renderOperationsSamplePills([
          item.bucket_label ? `分类：${item.bucket_label}` : '',
          item.style_label ? `风格：${item.style_label}` : '',
        ])}
        <div class="operations-sample-summary">
          <strong>排除原因</strong>
          <span>${escapeHtml(item.excluded_reason || '-')}</span>
        </div>
      </article>
    `).join('');
  }

  function renderOperationsLowVolPositiveRatioWatch(watch, options = {}) {
    const elements = options.elements || {};
    const statsElement = elements.statsElement;
    const narrativeElement = elements.narrativeElement;
    const samplesElement = elements.samplesElement;
    const renderInlineState = options.renderInlineState;
    if (!statsElement || !narrativeElement || !samplesElement) return;
    if (typeof renderInlineState !== 'function') {
      throw new Error('Operations dashboard watch helpers are incomplete');
    }

    const styleSummary = Array.isArray(watch?.style_summary) ? watch.style_summary : [];
    const styleGapReasonSummary = Array.isArray(watch?.style_gap_reason_summary) ? watch.style_gap_reason_summary : [];
    const gapCompanionReasonSummary = Array.isArray(watch?.gap_companion_reason_summary) ? watch.gap_companion_reason_summary : [];
    const overlapSummary = watch?.low_vol_overlap_summary || {};
    const samples = Array.isArray(watch?.preview) ? watch.preview : [];
    const topStyle = styleSummary[0] || null;
    const secondStyle = styleSummary[1] || null;
    const topCompanionReason = gapCompanionReasonSummary[0] || null;

    if (!watch?.available) {
      statsElement.innerHTML = buildStatCard('低波观察', '暂无', '当前未生成');
      narrativeElement.innerHTML = `
        <strong>观察</strong>
        <span>暂无数据</span>
      `;
      renderInlineState(samplesElement, { noun: '低波观察样本' });
      return;
    }

    statsElement.innerHTML = [
      buildStatCard('观察样本', formatNumber(watch?.focus_sample_count), '低波风格'),
      buildStatCard('门槛命中', formatNumber(watch?.positive_ratio_gate_hit_count), '命中正收益门槛'),
      buildStatCard('高差样本', formatNumber(watch?.positive_ratio_gap_alert_count), '自然月差>=15'),
      buildStatCard('可复算样本', formatNumber(watch?.monthly_shadow_ready_count), '自然月可复算'),
      buildStatCard(
        '双命中样本',
        overlapSummary?.available ? formatNumber(overlapSummary.annual_positive_targeted_count) : '暂无',
        overlapSummary?.available ? `同时命中年化和正收益` : '审计未接入'
      ),
      buildStatCard(
        '双命中直达主榜',
        overlapSummary?.available ? formatNumber(overlapSummary.annual_positive_direct_final_impact_count) : '暂无',
        overlapSummary?.available ? `双命中后直接进入主榜` : '审计未接入'
      ),
      buildStatCard(
        '仅年化直达主榜',
        overlapSummary?.available ? formatNumber(overlapSummary.annual_without_positive_direct_final_impact_count) : '暂无',
        overlapSummary?.available ? `对应样本 ${formatNumber(overlapSummary.annual_without_positive_targeted_count)}` : '审计未接入'
      ),
      buildStatCard(
        '仅正收益直达主榜',
        overlapSummary?.available ? formatNumber(overlapSummary.positive_without_annual_direct_final_impact_count) : '暂无',
        overlapSummary?.available ? `对应样本 ${formatNumber(overlapSummary.positive_without_annual_targeted_count)}` : '审计未接入'
      ),
      buildStatCard(
        '高差首位风格',
        topStyle ? String(topStyle['债基风格标签'] || '-') : '暂无',
        topStyle ? `高差 ${formatNumber(topStyle['自然月正收益差>=15pct样本数'])}` : '暂无'
      ),
      buildStatCard(
        '高差次位风格',
        secondStyle ? String(secondStyle['债基风格标签'] || '-') : '暂无',
        secondStyle ? `高差 ${formatNumber(secondStyle['自然月正收益差>=15pct样本数'])}` : '暂无'
      ),
      buildStatCard('主伴随原因', topCompanionReason ? String(topCompanionReason['原因标签'] || '-') : '暂无', topCompanionReason ? `样本 ${formatNumber(topCompanionReason['样本数'])}` : '暂无'),
    ].join('');

    const styleText = styleGapReasonSummary.length
      ? styleGapReasonSummary
        .slice(0, 4)
        .map((item) => `${compactUiText(item?.['债基风格标签'] || '-', 6)} ${compactUiText(item?.['主伴随原因'] || '-', 10)}`)
        .join(' / ')
      : styleSummary
        .slice(0, 4)
        .map((item) => `${compactUiText(item?.['债基风格标签'] || '-', 6)} ${formatNumber(item?.['自然月正收益差>=15pct样本数'])}`)
        .join(' / ');
    const overlapText = overlapSummary?.available
      ? `年化与正收益同时命中的样本 ${formatNumber(overlapSummary.annual_positive_targeted_count)}，其中直达主榜 ${formatNumber(overlapSummary.annual_positive_direct_final_impact_count)}；仅年化命中直达 ${formatNumber(overlapSummary.annual_without_positive_direct_final_impact_count)}；仅正收益命中直达 ${formatNumber(overlapSummary.positive_without_annual_direct_final_impact_count)}`
      : '';
    const recommendationText = Array.isArray(overlapSummary?.recommendation) ? overlapSummary.recommendation[0] || '' : '';
    const semanticsText = watch?.semantics?.definition
      ? `正收益口径 ${watch.semantics.definition}，${watch?.semantics?.industry_standard === false ? '这里使用的是项目内部稳定性代理指标' : ''}`
      : '';

    narrativeElement.innerHTML = `
      <strong>观察</strong>
      <span>${escapeHtml([semanticsText, styleText, overlapText, recommendationText, watch?.explanation || ''].filter(Boolean).join('；') || '暂无摘要')}</span>
    `;

    if (!samples.length) {
      renderInlineState(samplesElement, { noun: '低波观察样本' });
      return;
    }

    samplesElement.innerHTML = samples.slice(0, 8).map((item) => `
      <article class="data-card operations-sample-card operations-sample-card-watch">
        <div class="operations-sample-head">
          <div class="operations-sample-title">
            <span class="operations-sample-code table-code">${escapeHtml(item['基金代码'] || '-')}</span>
            <strong class="operations-sample-name">${escapeHtml(item['基金名称'] || '-')}</strong>
          </div>
          <div class="operations-sample-emphasis">
            <span>差值</span>
            <strong>${escapeHtml(String(item.positive_ratio_daily_vs_calendar_gap_pct ?? '-'))}</strong>
          </div>
        </div>
        ${renderOperationsSamplePills([
          item['债基风格标签'] ? `风格：${item['债基风格标签']}` : '',
          item['正式投资评分'] !== undefined && item['正式投资评分'] !== null ? `评分：${item['正式投资评分']}` : '',
          item['1年-正收益比率'] !== undefined && item['1年-正收益比率'] !== null ? `日频正收益：${item['1年-正收益比率']}` : '',
          item.positive_month_ratio_calendar_pct !== undefined && item.positive_month_ratio_calendar_pct !== null ? `自然月正收益：${item.positive_month_ratio_calendar_pct}` : '',
        ])}
        <div class="operations-sample-summary">
          <strong>原因码</strong>
          <span>${escapeHtml(compactUiText(String(item.dynamic_threshold_reason_codes || '-'), 42))}</span>
        </div>
      </article>
    `).join('');
  }

  window.OperationsDashboardWatch = Object.freeze({
    renderOperationsIntegrityAudits,
    renderOperationsExclusionSummary,
    renderOperationsLowVolPositiveRatioWatch,
  });
}());
