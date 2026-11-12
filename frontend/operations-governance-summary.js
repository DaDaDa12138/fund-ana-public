(function () {
  function numericValueOrNull(value) {
    if (value === null || value === undefined || value === '') return null;
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

  function sumPresentNumbers(...values) {
    let seen = false;
    let total = 0;
    values.forEach((value) => {
      const numeric = numericValueOrNull(value);
      if (numeric === null) return;
      total += numeric;
      seen = true;
    });
    return seen ? total : null;
  }

  function formatCoverageRatioPercent(value) {
    return formatRatioPercent(value);
  }

  function formatAnnualGapShape(value) {
    const key = String(value || '').trim();
    const labels = {
      missing_history: '原始年度行缺失',
      sparse_year_row_gap: '年度行数不足',
      null_year_gap: '年度空值缺口',
    };
    return labels[key] || key || '-';
  }

  function formatAnnualGapYears(item) {
    const missingYears = Array.isArray(item?.missing_years) ? item.missing_years : [];
    const nullYears = Array.isArray(item?.null_years) ? item.null_years : [];
    const parts = [
      missingYears.length ? `缺失 ${missingYears.join('/')}` : '',
      nullYears.length ? `空值 ${nullYears.join('/')}` : '',
    ].filter(Boolean);
    return parts.join('；') || '-';
  }

  function renderOperationsLongTermGovernance(summary, options = {}) {
    const elements = options.elements || {};
    const statsElement = elements.statsElement;
    const narrativeElement = elements.narrativeElement;
    const samplesElement = elements.samplesElement;
    const renderInlineState = options.renderInlineState;
    if (!statsElement || !narrativeElement || !samplesElement) return;
    if (typeof renderInlineState !== 'function') {
      throw new Error('Operations governance summary helpers are incomplete');
    }

    const payload = summary || {};
    const governancePools = Array.isArray(payload.governance_pool_summary) ? payload.governance_pool_summary : [];
    const localRetryPool = governancePools.find((item) => item?.pool_key === 'local_retry_pool') || {};
    const upstreamLimitedPool = governancePools.find((item) => item?.pool_key === 'upstream_limited_watchlist') || {};
    const maturityPool = governancePools.find((item) => item?.pool_key === 'await_history_maturity') || {};
    const priorityBlockedPool = governancePools.find((item) => item?.pool_key === 'priority_blocked') || {};
    const staleScopePool = governancePools.find((item) => item?.pool_key === 'stale_scope_not_ready') || {};
    const localRetrySamples = Array.isArray(payload.local_retry_samples) ? payload.local_retry_samples : [];
    const priorityBlockedSamples = Array.isArray(payload.priority_blocked_samples) ? payload.priority_blocked_samples : [];
    const staleScopeSamples = Array.isArray(payload.stale_scope_not_ready_samples) ? payload.stale_scope_not_ready_samples : [];
    const buyableActiveStaleSamples = Array.isArray(payload.buyable_active_stale_scope_samples) ? payload.buyable_active_stale_scope_samples : [];
    const scopeFilteredNoRetrySamples = Array.isArray(payload.scope_filtered_no_retry_samples) ? payload.scope_filtered_no_retry_samples : [];
    const upstreamLimitedSamples = Array.isArray(payload.upstream_limited_watchlist_samples) ? payload.upstream_limited_watchlist_samples : [];
    const annualUpstreamLimitedGapSamples = Array.isArray(payload.annual_feature_upstream_limited_gap_samples) ? payload.annual_feature_upstream_limited_gap_samples : [];
    const sampleRows = [
      ...annualUpstreamLimitedGapSamples.map((item) => ({ ...item, __poolLabel: '年度上游受限缺口' })),
      ...localRetrySamples.map((item) => ({ ...item, __poolLabel: '本地重试池' })),
      ...priorityBlockedSamples.map((item) => ({ ...item, __poolLabel: '高优先级阻塞池' })),
      ...buyableActiveStaleSamples.map((item) => ({ ...item, __poolLabel: '可买入停更恢复观察' })),
      ...scopeFilteredNoRetrySamples.map((item) => ({ ...item, __poolLabel: 'Scope 过滤沉底' })),
      ...staleScopeSamples.map((item) => ({ ...item, __poolLabel: '停更范围未恢复池' })),
      ...upstreamLimitedSamples.map((item) => ({ ...item, __poolLabel: '上游受限观察池' })),
    ].slice(0, 8);
    const staleScopeSummary = payload.stale_scope_not_ready_summary || {};
    const scopeFilteredSummary = payload.scope_filtered_no_retry_summary || {};
    const annualSemanticNullBridge = payload.annual_semantic_null_bridge_summary || {};
    const semanticNullSnapshotAudit = payload.semantic_null_snapshot_audit_summary || {};
    const upstreamLimitedStructure = payload.upstream_limited_structure_summary || {};
    const priorityScopeNotReadySummary = payload.priority_scope_not_ready_summary || {};
    const buyableActiveStaleRecoverability = payload.buyable_active_stale_scope_recoverability_summary || {};
    const buyableActiveStaleJudgement = buyableActiveStaleRecoverability.recoverability_judgement || '';
    const buyableActiveRecoverableTotal = firstPresentNumber(
      buyableActiveStaleRecoverability.recoverable_total,
      sumPresentNumbers(
        buyableActiveStaleRecoverability.fresh_recoverable_total,
        buyableActiveStaleRecoverability.short_recoverable_total
      ),
      buyableActiveStaleRecoverability.short_recoverable_total
    );

    if (!Object.keys(payload).length) {
      statsElement.innerHTML = buildStatCard('长周期治理', '加载失败', '治理摘要暂不可用');
      narrativeElement.innerHTML = `
        <strong>治理</strong>
        <span>暂无数据</span>
      `;
      renderInlineState(samplesElement, { noun: '治理样本' });
      return;
    }

    const snapshotCards = [
      buildStatCard('长周期就绪率', formatCoverageRatioPercent(payload.full_feature_ready_coverage_ratio), '年度收益与历史排名'),
      buildStatCard('中性化基金', formatNumber(payload.neutralized_fund_count), `候选 ${formatNumber(payload.candidate_fund_count)} 只`),
      buildStatCard('待成熟样本', formatNumber(firstPresentNumber(payload.annual_feature_pending_maturity_fund_count, maturityPool.count, payload.await_history_maturity_count)), `纯待成熟 ${formatNumber(payload.annual_feature_pending_maturity_excluding_semantic_null_fund_count)} / 含语义空值子集`),
      buildStatCard('可执行缺口', formatNumber(payload.annual_feature_actionable_gap_fund_count), `缺历史 ${formatNumber(payload.annual_feature_actionable_missing_history_fund_count)} / 行数不足 ${formatNumber(payload.annual_feature_actionable_sparse_year_row_gap_fund_count)} / 空值 ${formatNumber(payload.annual_feature_actionable_null_year_gap_fund_count)}`),
      buildStatCard('年度上游受限缺口', formatNumber(payload.annual_feature_upstream_limited_gap_fund_count), `缺历史 ${formatNumber(payload.annual_feature_upstream_limited_missing_history_fund_count)} / 行数不足 ${formatNumber(payload.annual_feature_upstream_limited_sparse_year_row_gap_fund_count)} / 空值 ${formatNumber(payload.annual_feature_upstream_limited_null_year_gap_fund_count)}`),
      buildStatCard(
        '上游语义空值',
        formatNumber(payload.annual_feature_upstream_semantic_null_year_gap_fund_count),
        `执行观察 ${formatNumber(annualSemanticNullBridge.execution_total)} / 待成熟层 ${formatNumber(firstPresentNumber(annualSemanticNullBridge.remaining_pending_maturity_layer_total, annualSemanticNullBridge.remaining_total))} / 快照外尾部 ${formatNumber(annualSemanticNullBridge.scope_filtered_tail_total)} / mature=false ${formatNumber((semanticNullSnapshotAudit.mature_counts || {}).mature_false)}`
      ),
      buildStatCard('历史缺口总量', formatNumber(sumPresentNumbers(payload.annual_history_missing_total, payload.rank_history_missing_total)), `年度 ${formatNumber(payload.annual_history_missing_total)} / 排名 ${formatNumber(payload.rank_history_missing_total)}`),
      buildStatCard('停更样本', formatNumber(payload.full_feature_stale_fund_count), `排名 ${formatNumber(payload.rank_feature_stale_fund_count)} / 年度 ${formatNumber(payload.annual_feature_stale_fund_count)} / 深停更 ${formatNumber(staleScopeSummary.deep_stale_candidate_total)}`),
    ];
    const maintenanceCards = [
      buildStatCard('维护候选集', formatNumber(payload.maintenance_candidate_total), `可筛选 ${formatNumber(payload.maintenance_screenable_candidate_total)} / 可买入 ${formatNumber(payload.maintenance_buyable_candidate_total)}`),
      buildStatCard('显式治理池', formatNumber(payload.maintenance_actionable_governance_pool_total), `缺口总量 ${formatNumber(payload.maintenance_gap_fund_count)} / scope-filtered ${formatNumber(payload.maintenance_scope_filtered_no_retry_total)}`),
      buildStatCard('等待历史成熟', formatNumber(firstPresentNumber(maturityPool.count, payload.await_history_maturity_count)), '按维护候选范围统计'),
      buildStatCard('本地重试池', formatNumber(firstPresentNumber(localRetryPool.count, payload.local_retry_pool_count)), `窗口 ${formatNumber(payload.local_retry_window_pending_count)} / 冷却 ${formatNumber(payload.local_retry_cooldown_pending_count)} / 立即 ${formatNumber(payload.local_retry_ready_count)} / 其它 ${formatNumber(payload.local_retry_other_pending_count)}`),
      buildStatCard('Scope 过滤沉底', formatNumber(payload.maintenance_scope_filtered_no_retry_total), `持有期 ${formatNumber(scopeFilteredSummary.holding_period_blocked_total)} / 运行排除 ${formatNumber(scopeFilteredSummary.runtime_excluded_total)} / 上游空载荷 ${formatNumber(scopeFilteredSummary.upstream_empty_payload_total)}`),
      buildStatCard('高优先级阻塞', formatNumber(firstPresentNumber(priorityBlockedPool.count, payload.priority_blocked_total)), `元数据 ${formatNumber(payload.buyable_active_metadata_blocked_total)} / 范围 ${formatNumber(payload.buyable_active_scope_not_ready_total)}`),
      buildStatCard(
        '上游受限观察',
        formatNumber(firstPresentNumber(upstreamLimitedPool.count, payload.upstream_limited_watchlist_count)),
        `当前观察 ${formatNumber(firstPresentNumber(payload.upstream_limited_current_watchlist_total, upstreamLimitedStructure.current_watchlist_total))} / 语义空值观察 ${formatNumber(firstPresentNumber(payload.upstream_limited_semantic_null_observe_total, upstreamLimitedStructure.semantic_null_observe_total))} / scope-filtered 尾部 ${formatNumber(firstPresentNumber(payload.scope_filtered_upstream_limited_total, upstreamLimitedStructure.scope_filtered_total))}`
      ),
      buildStatCard('恢复观察池', formatNumber(firstPresentNumber(staleScopePool.count, payload.stale_scope_not_ready_total)), `screenable ${formatNumber(priorityScopeNotReadySummary.screenable_priority_total)} / buyable-active ${formatNumber(priorityScopeNotReadySummary.buyable_active_priority_total)} / ${buyableActiveStaleJudgement === 'explanation_only' ? '优先恢复 -，转解释观察' : `可优先恢复 ${formatNumber(buyableActiveRecoverableTotal)}`}`),
    ];

    statsElement.innerHTML = `
      <section class="operations-stat-section">
        <div class="operations-stat-section-heading">
          <strong>${escapeHtml(payload.snapshot_scope_label || '长期快照全集统计')}</strong>
          <span>年度收益与历史排名的覆盖情况</span>
        </div>
        <div class="stats-grid compact">${snapshotCards.slice(0, 4).join('')}</div>
        <details class="compact-disclosure operations-governance-details">
          <summary>更多长期指标</summary>
          <div class="stats-grid compact">${snapshotCards.slice(4).join('')}</div>
        </details>
      </section>
      <section class="operations-stat-section">
        <div class="operations-stat-section-heading">
          <strong>${escapeHtml(payload.maintenance_scope_label || '当前维护治理池统计')}</strong>
          <span>维护候选和自动重试</span>
        </div>
        <div class="stats-grid compact">${maintenanceCards.slice(0, 4).join('')}</div>
        <details class="compact-disclosure operations-governance-details">
          <summary>更多维护指标</summary>
          <div class="stats-grid compact">${maintenanceCards.slice(4).join('')}</div>
        </details>
      </section>
    `;

    narrativeElement.innerHTML = `
      <strong>治理摘要</strong>
      <span>${escapeHtml(
        `长期候选 ${formatNumber(payload.candidate_fund_count)} 只，全特征就绪率 ${formatCoverageRatioPercent(payload.full_feature_ready_coverage_ratio)}。`
        + `维护候选 ${formatNumber(payload.maintenance_candidate_total)} 只，本地重试 ${formatNumber(firstPresentNumber(localRetryPool.count, payload.local_retry_pool_count))} 只，`
        + `上游受限观察 ${formatNumber(firstPresentNumber(upstreamLimitedPool.count, payload.upstream_limited_watchlist_count))} 只。两组范围分别统计。`
      )}</span>
      <details class="compact-disclosure operations-governance-details">
        <summary>详细统计与口径</summary>
        <p class="operations-governance-long-text" tabindex="0" aria-label="治理详细统计与口径">${escapeHtml([
        payload.headline,
        payload.explanation,
        payload.snapshot_scope_note,
        payload.maintenance_scope_note,
        `${payload.snapshot_scope_label || '长期快照全集统计'}：候选 ${formatNumber(payload.candidate_fund_count)} 只`,
        `${payload.maintenance_scope_label || '当前维护治理池统计'}：维护候选 ${formatNumber(payload.maintenance_candidate_total)} 只，显式治理池 ${formatNumber(payload.maintenance_actionable_governance_pool_total)} 只`,
        `scope_filtered_no_retry ${formatNumber(payload.maintenance_scope_filtered_no_retry_total)} 只，其中 holding_period_blocked ${formatNumber(scopeFilteredSummary.holding_period_blocked_total)} / runtime_excluded ${formatNumber(scopeFilteredSummary.runtime_excluded_total)}`,
        `持有期阻塞：可申购 ${formatNumber(scopeFilteredSummary.buyable_holding_period_blocked_total)} 只，不可申购 ${formatNumber(scopeFilteredSummary.non_buyable_holding_period_blocked_total)} 只`,
        scopeFilteredSummary.structure_counts?.unknown_buyability_holding_period_blocked > 0
          ? `持有期阻塞且申购状态未知 ${formatNumber(scopeFilteredSummary.structure_counts.unknown_buyability_holding_period_blocked)} 只`
          : '',
        `上游语义空值桥接：快照子集 ${formatNumber(payload.annual_feature_upstream_semantic_null_year_gap_fund_count)} 只，其中执行观察 ${formatNumber(annualSemanticNullBridge.execution_total)} 只（observe ${formatNumber(annualSemanticNullBridge.observe_total)}），剩余待成熟解释层 ${formatNumber(firstPresentNumber(annualSemanticNullBridge.remaining_pending_maturity_layer_total, annualSemanticNullBridge.remaining_total))} 只；快照外 scope-filtered 尾部 ${formatNumber(annualSemanticNullBridge.scope_filtered_tail_total)} 只；另有纯待成熟非语义空值 ${formatNumber(firstPresentNumber(annualSemanticNullBridge.pending_maturity_excluding_semantic_null_total, payload.annual_feature_pending_maturity_excluding_semantic_null_fund_count))} 只`,
        semanticNullSnapshotAudit.available ? `语义空值快照审计：mature=false ${formatNumber((semanticNullSnapshotAudit.mature_counts || {}).mature_false)} / mature=true ${formatNumber((semanticNullSnapshotAudit.mature_counts || {}).mature_true)}；主形态 ${(semanticNullSnapshotAudit.present_shape_top || []).slice(0, 2).map((item) => `${item.shape}:${formatNumber(item.count)}`).join(' / ')}` : '',
        `上游受限执行观察 ${formatNumber(payload.upstream_limited_watchlist_count)} 只，其中当前观察池 ${formatNumber(firstPresentNumber(payload.upstream_limited_current_watchlist_total, upstreamLimitedStructure.current_watchlist_total))} / 语义空值观察 ${formatNumber(firstPresentNumber(payload.upstream_limited_semantic_null_observe_total, upstreamLimitedStructure.semantic_null_observe_total))} / stale_no_progress ${formatNumber(firstPresentNumber(payload.upstream_limited_stale_no_progress_watchlist_total, upstreamLimitedStructure.stale_no_progress_watchlist_total))} / 空载荷观察 ${formatNumber(firstPresentNumber(payload.upstream_limited_empty_payload_watchlist_total, upstreamLimitedStructure.empty_payload_watchlist_total))}`,
        `上游受限优先级分布 buyable-active ${formatNumber(firstPresentNumber(payload.upstream_limited_buyable_active_total, upstreamLimitedStructure.buyable_active_total))} / publishable ${formatNumber(firstPresentNumber(payload.upstream_limited_publishable_total, upstreamLimitedStructure.publishable_total))} / screenable ${formatNumber(firstPresentNumber(payload.upstream_limited_screenable_total, upstreamLimitedStructure.screenable_total))}`,
        `scope-filtered 上游受限尾部 ${formatNumber(firstPresentNumber(payload.scope_filtered_upstream_limited_total, upstreamLimitedStructure.scope_filtered_total))} 只，其中空载荷 ${formatNumber(firstPresentNumber(upstreamLimitedStructure.scope_filtered_empty_payload_total, scopeFilteredSummary.upstream_empty_payload_total))} / 语义空值 ${formatNumber(firstPresentNumber(upstreamLimitedStructure.scope_filtered_semantic_null_total, scopeFilteredSummary.upstream_semantic_null_year_total))}`,
        `stale scope_not_ready ${formatNumber(payload.stale_scope_not_ready_total)} 只，其中 screenable ${formatNumber(priorityScopeNotReadySummary.screenable_priority_total)} / buyable-active ${formatNumber(priorityScopeNotReadySummary.buyable_active_priority_total)} / active ${formatNumber(priorityScopeNotReadySummary.active_priority_total)}`,
        `buyable-active stale 中 fresh_0_30d ${formatNumber(buyableActiveStaleRecoverability.fresh_recoverable_total)} / short_31_60d ${formatNumber(buyableActiveStaleRecoverability.short_recoverable_total)} / medium_61_120d ${formatNumber(buyableActiveStaleRecoverability.medium_watch_total)} / deep+terminal ${formatNumber(sumPresentNumbers(buyableActiveStaleRecoverability.deep_watch_total, buyableActiveStaleRecoverability.terminal_candidate_total))}`,
        `优先治理焦点：buyable-active stale ${formatNumber(buyableActiveStaleRecoverability.total)} 只${buyableActiveStaleJudgement === 'explanation_only' ? '当前应以解释性观察为主' : '仍包含可恢复子池'}，其中 medium_61_120d ${formatNumber(buyableActiveStaleRecoverability.medium_watch_total)} 只 / deep+terminal ${formatNumber(sumPresentNumbers(buyableActiveStaleRecoverability.deep_watch_total, buyableActiveStaleRecoverability.terminal_candidate_total))}`,
        `待成熟 ${formatNumber(firstPresentNumber(payload.annual_feature_pending_maturity_fund_count, maturityPool.count, payload.await_history_maturity_count))} 只`,
        `其中纯待成熟 ${formatNumber(payload.annual_feature_pending_maturity_excluding_semantic_null_fund_count)} 只`,
        `可执行缺口 ${formatNumber(payload.annual_feature_actionable_gap_fund_count)} 只`,
        `年度上游受限缺口 ${formatNumber(payload.annual_feature_upstream_limited_gap_fund_count)} 只`,
        `上游语义空值子集 ${formatNumber(payload.annual_feature_upstream_semantic_null_year_gap_fund_count)} 只`,
        `本地重试 ${formatNumber(firstPresentNumber(localRetryPool.count, payload.local_retry_pool_count))} 只`,
        `高优先级阻塞 ${formatNumber(firstPresentNumber(priorityBlockedPool.count, payload.priority_blocked_total))} 只`,
        payload.maintenance_no_action_category ? `维护结果 ${formatSchedulerNoActionCategory(payload.maintenance_no_action_category)}` : '',
        payload.maintenance_status ? `状态 ${formatWorkflowStatus(payload.maintenance_status)}` : '',
      ].filter(Boolean).join('；') || '暂无摘要')}</p>
      </details>
    `;

    if (!sampleRows.length) {
      renderInlineState(samplesElement, { noun: '治理样本' });
      return;
    }

    samplesElement.innerHTML = sampleRows.map((item) => `
      <article class="data-card">
        <div class="data-card-item">
          <strong>治理池</strong>
          <span>${escapeHtml(item.__poolLabel || '-')}</span>
        </div>
        <div class="data-card-item">
          <strong>基金代码</strong>
          <span class="table-code">${escapeHtml(item.fcode || '-')}</span>
        </div>
        <div class="data-card-item">
          <strong>基金名称</strong>
          <span class="table-name">${escapeHtml(item.shortname || '-')}</span>
        </div>
        <div class="data-card-item">
          <strong>建议动作</strong>
          <span>${escapeHtml(item.recommended_action || '-')}</span>
        </div>
        <div class="data-card-item">
          <strong>阻塞分类</strong>
          <span>${escapeHtml(item.blocker_category || '-')}</span>
        </div>
        ${(item.gap_shape || (Array.isArray(item.missing_years) && item.missing_years.length) || (Array.isArray(item.null_years) && item.null_years.length)) ? `
          <div class="data-card-item">
            <strong>缺口形态</strong>
            <span>${escapeHtml(formatAnnualGapShape(item.gap_shape))}</span>
          </div>
          <div class="data-card-item">
            <strong>缺失/空值年份</strong>
            <span>${escapeHtml(formatAnnualGapYears(item))}</span>
          </div>
        ` : ''}
        <div class="data-card-item">
          <strong>最近原因</strong>
          <span>${escapeHtml(compactUiText(String(item.cooldown_last_error || '-'), 24))}</span>
        </div>
        <div class="data-card-item">
          <strong>下次观察</strong>
          <span>${escapeHtml(formatDateTime(item.cooldown_next_retry_after))}</span>
        </div>
      </article>
    `).join('');
  }

  window.OperationsGovernanceSummary = Object.freeze({
    renderOperationsLongTermGovernance,
  });
}());
