const settingsHeaderRunBadge = document.getElementById('settingsHeaderRunBadge');
const settingsHeaderHealthBadge = document.getElementById('settingsHeaderHealthBadge');
const settingsHeroMeta = document.getElementById('settingsHeroMeta');
const settingsMeta = document.getElementById('settingsMeta');
const settingsStats = document.getElementById('settingsStats');
const settingsBanner = document.getElementById('settingsBanner');
const reloadSettingsBtn = document.getElementById('reloadSettingsBtn');
const saveSettingsBtn = document.getElementById('saveSettingsBtn');
const saveAndProbeSettingsBtn = document.getElementById('saveAndProbeSettingsBtn');
const resetSettingsBtn = document.getElementById('resetSettingsBtn');
const probeTiantianBtn = document.getElementById('probeTiantianBtn');
const tiantianBaseUrlInput = document.getElementById('tiantianBaseUrlInput');
const autostockBaseUrlInput = document.getElementById('autostockBaseUrlInput');
const probeResult = document.getElementById('probeResult');

let currentSettingsPayload = null;

function apiSettingsHealthPayload(payload) {
  return {
    tiantian_base_url: payload?.runtime?.tiantian_base_url || payload?.configured?.tiantian_base_url || '',
    autostock_base_url: payload?.runtime?.autostock_base_url || payload?.configured?.autostock_base_url || '',
    api_runtime: payload || {},
  };
}

function setSettingsBanner(kind, title, message) {
  if (!settingsBanner) return;
  settingsBanner.className = `info-banner ${kind ? `info-banner-${kind}` : ''}`;
  settingsBanner.innerHTML = `
    <strong>${escapeHtml(title || '状态')}</strong>
    <span>${escapeHtml(message || '')}</span>
  `;
}

function describeSettingsSaveEffect(payload) {
  const scheduler = payload?.scheduler || {};
  if (scheduler.restart_required_after_change) {
    return scheduler.restart_note || '配置文件已更新；Web 已刷新运行配置，调度容器需要重启或重新部署后读取新地址。';
  }
  return '配置文件已更新，当前 Web 进程与内置调度配置已刷新。';
}

function setBusy(button, busy, textWhenBusy = '处理中') {
  if (!button) return;
  if (busy) {
    button.dataset.originalText = button.textContent || '';
    button.textContent = textWhenBusy;
    button.disabled = true;
    return;
  }
  button.disabled = false;
  if (button.dataset.originalText) {
    button.textContent = button.dataset.originalText;
  }
}

function fillInputs(payload) {
  if (tiantianBaseUrlInput) {
    tiantianBaseUrlInput.value = payload?.configured?.tiantian_base_url || '';
  }
  if (autostockBaseUrlInput) {
    autostockBaseUrlInput.value = payload?.configured?.autostock_base_url || '';
  }
}

function renderSettings(payload) {
  currentSettingsPayload = payload || {};
  const configured = currentSettingsPayload.configured || {};
  const runtime = currentSettingsPayload.runtime || {};
  const scheduler = currentSettingsPayload.scheduler || {};
  const frontend = currentSettingsPayload.frontend || {};
  const schedulerRuntime = scheduler.runtime || {};
  const matches = currentSettingsPayload.runtime_matches_config || {};
  const schedulerMatches = scheduler.runtime_matches_config || {};
  const env = currentSettingsPayload.environment_overrides || {};
  const fetchedAt = formatDateTime(currentSettingsPayload.fetched_at);
  fillInputs(currentSettingsPayload);
  renderHealthBadge(settingsHeaderHealthBadge, apiSettingsHealthPayload(currentSettingsPayload));
  settingsMeta.textContent = `${currentSettingsPayload.config_file || '-'} / 读取 ${fetchedAt}`;

  const runtimeMatchState = apiConfigurationMatchState(matches, configured);
  const runtimeMatchText = runtimeMatchState === 'consistent' ? '一致'
    : runtimeMatchState === 'mismatch' ? '待重载' : '未核验';
  const schedulerEvidenceAvailable = scheduler.runtime_evidence_available === true;
  const schedulerMatchState = schedulerEvidenceAvailable
    ? apiConfigurationMatchState(schedulerMatches, configured) : 'unknown';
  const schedulerMatchText = schedulerMatchState === 'consistent' ? '一致'
    : schedulerMatchState === 'mismatch' ? '待重启' : schedulerEvidenceAvailable ? '未核验' : '未上报';
  settingsHeroMeta.innerHTML = [
    buildStatCard('Tiantian配置', configured.tiantian_base_url || '-', '配置文件'),
    buildStatCard('Web运行值', runtime.tiantian_base_url || '-', runtimeMatchText),
    buildStatCard('调度运行值', schedulerRuntime.tiantian_base_url || '-', schedulerMatchText),
    buildStatCard('调度模式', scheduler.host_mode || '-', scheduler.restart_required_after_change ? '保存后需重启调度' : '保存后同步刷新'),
    buildStatCard('前端配置', frontend.editable_from_frontend ? '已启用' : '未启用', frontend.save_endpoint || '/api/settings/apis'),
    buildStatCard('读取时间', fetchedAt, 'no-store 实时配置'),
  ].join('');

  settingsStats.innerHTML = [
    buildStatCard('Tiantian API', configured.tiantian_base_url || '-', '基金列表、净值、收益与排名'),
    buildStatCard('正式发布校验', publicationValidationProviderLabel(configured.publication_validation_provider), '当前配置；校验结果随正式运行记录'),
    buildStatCard('净值与收益主接口', configured.tiantian_valuation_primary_url || '-', 'FundValuationLast'),
    buildStatCard('主接口备用域名', configured.tiantian_valuation_fallback_url || '-', 'FundValuationLast 同供应商备用入口'),
    buildStatCard('净值日期复核接口', configured.eastmoney_fund_info_url || '-', 'FundMNFInfo；未提供区间收益'),
    buildStatCard('AutoStock 状态', configuredAutoStockStatus(configured), configured.autostock_base_url || '原接口地址未提供'),
    buildStatCard('运行一致性', runtimeMatchText, '配置文件与当前 Web 进程'),
    buildStatCard('调度运行值', schedulerRuntime.tiantian_base_url || '-', schedulerRuntime.captured_at ? formatDateTime(schedulerRuntime.captured_at) : schedulerMatchText),
    buildStatCard('调度一致性', schedulerMatchText, schedulerRuntime.host_context || scheduler.host_mode || '-'),
    buildStatCard('环境覆盖', env.tiantian_override_active ? '启用' : '未启用', env.TIANTIAN_API_URL || 'TIANTIAN_API_URL'),
    buildStatCard('配置文件', currentSettingsPayload.config_file ? currentSettingsPayload.config_file.split('/').pop() : '-', '权威来源'),
    buildStatCard('首页来源', currentSettingsPayload?.frontend?.homepage_api_source || '/api/settings/apis', '实时设置接口'),
    buildStatCard('保存接口', currentSettingsPayload?.frontend?.save_endpoint || '/api/settings/apis', frontend.editable_from_frontend ? '前端可写配置' : '前端不可写'),
    buildStatCard('测试接口', currentSettingsPayload?.frontend?.probe_endpoint || '/api/settings/apis/probe', '只读业务探针'),
    buildStatCard('前端资源', currentSettingsPayload?.frontend?.asset_version || '-', currentSettingsPayload?.frontend?.settings_aliases?.join(', ') || '/api-settings'),
    buildStatCard('读取时间', fetchedAt, '接口响应 no-store'),
    buildStatCard('调度刷新', scheduler.restart_required_after_change ? '保存后需重启' : schedulerMatchText, scheduler.host_mode || '-'),
  ].join('');

  if (env.tiantian_override_active) {
    setSettingsBanner('warning', '环境覆盖', `TIANTIAN_API_URL 当前覆盖为 ${env.TIANTIAN_API_URL}`);
  } else if (runtimeMatchState === 'mismatch') {
    setSettingsBanner('warning', 'Web 待重载', '配置文件已变化，当前 Web 运行配置尚未完全同步。');
  } else if (runtimeMatchState === 'unknown') {
    setSettingsBanner('warning', 'Web 证据缺失', 'Web 运行配置的核对证据不完整，尚不能确认已同步。');
  } else if (schedulerMatchState === 'mismatch') {
    setSettingsBanner('warning', '调度待重启', 'Web 已读取当前配置；调度容器仍在旧接口地址，需重启或重新部署后生效。');
  } else if (schedulerMatchState === 'unknown') {
    setSettingsBanner('warning', '调度证据缺失', 'Web 已读取当前配置；调度运行配置的核对证据不完整，需等待心跳后复核。');
  } else if (scheduler.restart_required_after_change) {
    setSettingsBanner('success', '配置已对齐', 'Web 与调度运行配置已对齐；保存新的接口配置后需重启调度容器。');
  } else {
    setSettingsBanner('success', '已加载', '当前 Web 进程已读取配置文件中的 API 地址');
  }
}

function renderProbe(payload) {
  const probe = payload?.probe || {};
  const semantics = payload?.response_semantics || {};
  const criteria = semantics.success_criteria || {};
  const errors = Array.isArray(probe.errors) ? probe.errors.filter(Boolean) : [];
  const truthNote = payload?.truth_note || semantics.truth_policy || '';
  const ok = Boolean(probe.ok);
  probeResult.className = `api-probe-result ${ok ? 'api-probe-result-ok' : 'api-probe-result-error'}`;
  probeResult.innerHTML = `
    <div class="api-probe-status">
      <strong>${escapeHtml(ok ? '可用' : '未通过')}</strong>
      <span>${escapeHtml(probe.status || payload?.status || '-')}</span>
    </div>
    <dl class="api-probe-list">
      <div><dt>地址</dt><dd>${escapeHtml(probe.base_url || payload?.base_url || '-')}</dd></div>
      <div><dt>请求</dt><dd>${escapeHtml(probe.url || '-')}</dd></div>
      <div><dt>HTTP</dt><dd>${escapeHtml(probe.http_status ?? '-')}</dd></div>
      <div><dt>Success</dt><dd>${escapeHtml(probe.payload_success ?? '-')}</dd></div>
      <div><dt>Datas</dt><dd>${escapeHtml(probe.datas_count ?? '-')}</dd></div>
      <div><dt>Total</dt><dd>${escapeHtml(probe.total_count ?? '-')}</dd></div>
      <div><dt>ErrCode</dt><dd>${escapeHtml(probe.err_code ?? '-')}</dd></div>
      <div><dt>ErrMsg</dt><dd>${escapeHtml(probe.err_msg || '-')}</dd></div>
      <div><dt>JSON</dt><dd>${escapeHtml(probe.json_error || '可解析')}</dd></div>
      <div><dt>业务状态</dt><dd>${escapeHtml(semantics.current_business_status || probe.status || '-')}</dd></div>
      <div><dt>只读探针</dt><dd>${escapeHtml(semantics.writes_database === false && semantics.writes_business_artifacts === false ? '不写库/不写业务产物' : '-')}</dd></div>
    </dl>
    <div class="api-probe-criteria">
      <span class="${criteria.http_status_2xx ? 'is-pass' : 'is-fail'}">HTTP 2xx</span>
      <span class="${criteria.payload_success_true ? 'is-pass' : 'is-fail'}">Success=true</span>
      <span class="${criteria.datas_non_empty ? 'is-pass' : 'is-fail'}">Datas 非空</span>
    </div>
    ${probe.message ? `<p>${escapeHtml(probe.message)}</p>` : ''}
    ${errors.length ? `<details class="api-probe-errors"><summary>错误证据</summary><ul>${errors.map((item) => `<li>${escapeHtml(item)}</li>`).join('')}</ul></details>` : ''}
    ${truthNote ? `<p class="api-probe-truth-note">${escapeHtml(truthNote)}</p>` : ''}
  `;
}

function collectFormPayload() {
  return {
    tiantian_base_url: String(tiantianBaseUrlInput?.value || '').trim(),
    autostock_base_url: String(autostockBaseUrlInput?.value || '').trim(),
  };
}

async function persistSettingsFromForm() {
  const payload = await saveApiSettings(collectFormPayload());
  renderSettings(payload);
  return payload;
}

async function refreshSettings() {
  setBusy(reloadSettingsBtn, true, '刷新中');
  try {
    const [settingsResult, bootstrapResult] = await Promise.allSettled([
      loadApiSettings(),
      loadHealth(),
    ]);
    if (bootstrapResult.status === 'fulfilled') {
      renderRunBadge(settingsHeaderRunBadge, { status: bootstrapResult.value?.automation_mode || bootstrapResult.value?.status || 'ok' });
    } else {
      renderUnavailableBadge(settingsHeaderRunBadge, '运行状态', '接口暂不可用');
    }
    if (settingsResult.status !== 'fulfilled') {
      renderUnavailableBadge(settingsHeaderHealthBadge, '数据源状态', '接口暂不可用');
      setSettingsBanner('error', '加载失败', settingsResult.reason?.message || '接口设置暂不可用');
      return;
    }
    renderSettings(settingsResult.value);
  } finally {
    setBusy(reloadSettingsBtn, false);
  }
}

async function saveSettingsFromForm() {
  setBusy(saveSettingsBtn, true, '保存中');
  try {
    const payload = await persistSettingsFromForm();
    setSettingsBanner('success', '已保存', describeSettingsSaveEffect(payload));
    return payload;
  } catch (error) {
    setSettingsBanner('error', '保存失败', error?.message || '配置未保存');
    return null;
  } finally {
    setBusy(saveSettingsBtn, false);
  }
}

async function runTiantianProbe(baseUrl) {
  probeResult.className = 'api-probe-result empty-state-inline';
  probeResult.textContent = '正在测试…';
  try {
    const payload = await probeApiSettings({
      tiantian_base_url: String(baseUrl || '').trim(),
    });
    renderProbe(payload);
    return payload;
  } catch (error) {
    probeResult.className = 'api-probe-result api-probe-result-error';
    probeResult.innerHTML = `
      <div class="api-probe-status">
        <strong>未通过</strong>
        <span>${escapeHtml(error?.message || '连接测试失败')}</span>
      </div>
    `;
    throw error;
  }
}

async function probeTiantianFromForm() {
  setBusy(probeTiantianBtn, true, '测试中');
  try {
    await runTiantianProbe(String(tiantianBaseUrlInput?.value || '').trim());
  } catch (_error) {
    // runTiantianProbe 已渲染失败状态；这里避免按钮事件产生未处理异常。
  } finally {
    setBusy(probeTiantianBtn, false);
  }
}

async function saveAndProbeSettingsFromForm() {
  setBusy(saveAndProbeSettingsBtn, true, '保存并测试中');
  let savedPayload = null;
  try {
    savedPayload = await persistSettingsFromForm();
    const targetBaseUrl = savedPayload?.runtime?.tiantian_base_url
      || savedPayload?.configured?.tiantian_base_url
      || String(tiantianBaseUrlInput?.value || '').trim();
    const probePayload = await runTiantianProbe(targetBaseUrl);
    const probeOk = Boolean(probePayload?.probe?.ok);
    setSettingsBanner(
      probeOk ? 'success' : 'warning',
      probeOk ? '已保存并通过测试' : '已保存，测试未通过',
      probeOk ? describeSettingsSaveEffect(savedPayload) : (probePayload?.probe?.message || 'Tiantian 业务探针未通过')
    );
  } catch (error) {
    if (savedPayload) {
      setSettingsBanner('warning', '已保存，测试失败', error?.message || '连接测试失败');
    } else {
      setSettingsBanner('error', '保存失败', error?.message || '配置未保存');
    }
  } finally {
    setBusy(saveAndProbeSettingsBtn, false);
  }
}

reloadSettingsBtn?.addEventListener('click', refreshSettings);
saveSettingsBtn?.addEventListener('click', saveSettingsFromForm);
saveAndProbeSettingsBtn?.addEventListener('click', saveAndProbeSettingsFromForm);
resetSettingsBtn?.addEventListener('click', () => fillInputs(currentSettingsPayload));
probeTiantianBtn?.addEventListener('click', probeTiantianFromForm);

refreshSettings();
