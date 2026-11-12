"""Browser source contracts; synthetic state markers never represent fund data."""
from __future__ import annotations

from pathlib import Path
import shutil
import subprocess

import pytest


PROJECT = Path(__file__).resolve().parents[1]
FRONTEND = PROJECT / "frontend"


def _read(name: str) -> str:
    return (FRONTEND / name).read_text(encoding="utf-8")


def test_neutral_validation_fields_are_preserved_without_legacy_fallback() -> None:
    shared = _read("shared.js")
    recommendations = _read("recommendations.js")
    assert "FUND_DATA_VALIDATION_FIELDS.some((field) => Object.hasOwn(record, field))" in shared
    assert "neutral ? record['数据校验原始状态'] ?? '' : record['AutoStock原始校验状态'] ?? ''" in shared
    assert "...copyFundValidationFields(fund)" in recommendations
    assert "item['AutoStock原始校验状态'] === 'strict_match'" not in recommendations
    assert "天天基金／东方财富跨接口校验（非独立供应商）" in shared
    assert "validation.contractVersion" in shared
    assert "validation.note" in shared and "validation.warnings" in shared


@pytest.mark.parametrize("name", ["app.js", "settings.js"])
def test_current_api_cards_show_actual_validation_provider_and_disabled_source(name: str) -> None:
    source = _read(name)
    for field in ["publication_validation_provider", "tiantian_valuation_primary_url",
                  "tiantian_valuation_fallback_url", "eastmoney_fund_info_url"]:
        assert f"configured.{field}" in source
    assert "apiConfigurationMatchState(matches, configured)" in source
    assert "apiConfigurationMatchState(schedulerMatches, configured)" in source
    assert "scheduler.runtime_evidence_available === true" in source
    assert "configuredAutoStockStatus(configured)" in source
    assert "FundMNFInfo；未提供区间收益" in source
    assert "估值校验与辅助状态" not in source


def test_excel_validation_preset_and_favorite_tooltip_use_source_contract() -> None:
    viewer = _read("excel_viewer.js")
    favorites = _read("favorites.js")
    assert "FUND_DATA_VALIDATION_FIELDS.includes(column)" in viewer
    assert "? FUND_DATA_VALIDATION_FIELDS : ['AutoStock校验状态', 'AutoStock校验备注']" in viewer
    assert "publicationValidationProviderLabel(originalCell)" in viewer
    assert "const sourceLabel = favoriteLiveSourceLabel(row);" in favorites
    assert "优先展示 AutoStock" not in favorites
    assert "暂以 AutoStock" not in favorites
    assert "；AutoStock ${meta.label}" not in favorites


def test_existing_missing_returns_and_detail_loading_repairs_stay_present() -> None:
    tracker = _read("tracker.js")
    recommendation = _read("recommendations.js")
    assert "const recent3m = parseMetricDisplayNumber(row['近3月(%)']);" in tracker
    assert "recent3m === null || !Number.isFinite(recent3m)" in tracker
    assert "const detailLoaded = Array.isArray(group?.funds);" in recommendation
    assert "headline: '明细尚未加载'" in recommendation


NODE_BEHAVIOR_SCRIPT = r"""
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const root = process.cwd();
const read = name => fs.readFileSync(path.join(root, 'frontend', name), 'utf8');
const extract = (source, name, nextName) => {
  const start = source.indexOf(`function ${name}(`);
  const end = source.indexOf(`function ${nextName}(`, start);
  assert(start >= 0 && end > start, `Missing browser function ${name}`);
  return source.slice(start, end);
};
const context = vm.createContext({
  FUND_COPYWRITING: {sinceInceptionAnnualizedLabel: 'state-fixture'},
  compactUiText: value => value,
  RECOMMENDATION_GROUP_SHORT_HINTS: {}, RECOMMENDATION_STABILITY_SHORT_LABELS: {},
  formatSignedPercentOrDash: value => value == null ? '-' : String(value),
  formatSignedPercentText: value => String(value),
  FAVORITE_LIVE_RETURN_FIELD_MAP: {'state-fixture': {
    sourceField: 'selection', liveField: 'live', label: 'state-fixture',
  }},
});
const shared = read('shared.js');
vm.runInContext(shared.slice(shared.indexOf('const FUND_DATA_VALIDATION_FIELDS'),
  shared.indexOf('function formatNavSnapshotComparison(')) +
  extract(read('recommendations.js'), 'normalizeRecommendationStatus', 'compactReasonText') +
  extract(read('recommendations.js'), 'compactReasonText', 'compactRiskText') +
  extract(read('recommendations.js'), 'compactRiskText', 'shortRecommendationGroupHint') +
  extract(read('recommendations.js'), 'normalizeRecommendationStabilityLabel', 'recommendationSortPresetSortState') +
  extract(read('recommendations.js'), 'buildRecommendationQualityFlags', 'buildRecommendationQualityHintText') +
  extract(read('recommendations.js'), 'buildRecommendationDigest', 'summarizeGroupNarrative') +
  extract(read('favorites.js'), 'buildFavoritePeriodicMetricDescription', 'renderFavoritePeriodicReturnTooltipCell'), context);
// These are isolated UI state/conflict markers, not real business records.
const legacy = {'AutoStock原始校验状态':'strict_match'};
assert.equal(context.fundValidationContract(legacy).rawStatus, 'strict_match');
assert.equal(context.fundValidationContract(legacy).sourceLabel, 'AutoStock 历史校验');
for (const current of [
  {'数据校验来源':'eastmoney_cross_endpoint'},
  {'数据校验来源':null}, {'数据校验原始状态':null},
  {'数据校验契约版本':'isolated-state-fixture'},
]) {
  const row = {...legacy, ...current};
  assert.equal(context.fundValidationContract(row).rawStatus, '');
  assert.equal(context.fundValidationContract(row).neutral, true);
  assert.equal(Object.hasOwn(context.copyFundValidationFields(row),'AutoStock原始校验状态'), false);
  assert.equal(context.buildRecommendationDigest(row).includes('严格匹配'), false);
  assert(context.buildRecommendationQualityFlags(row).includes('校验状态缺失'));
}
const blocked = {...legacy, '数据校验来源':'eastmoney_cross_endpoint', '数据校验原始状态':'hard_block'};
assert(context.buildRecommendationQualityFlags(blocked).includes('校验已拦截'));
assert(context.buildRecommendationDigest(blocked).includes('非独立供应商'));
assert(context.buildRecommendationDigest(blocked).includes('已拦截'));
assert.equal(context.publicationValidationProviderLabel('unknown-isolated-source'), 'unknown-isolated-source');
assert.equal(context.configuredAutoStockStatus({autostock_enabled:false}), '已禁用');
assert.equal(context.configuredAutoStockStatus({}), '启用状态未提供');
const liveRow = {selection:'live_api',live:'0',__favorite_live_source:'tiantian_fund_valuation_last'};
const tooltip = context.buildFavoritePeriodicMetricDescription('state-fixture',0,liveRow,'');
assert(tooltip.includes('天天基金 FundValuationLast'));
assert(!tooltip.includes('AutoStock'));
assert.equal(context.favoriteLiveSourceLabel({}, {source:'tiantian_fund_valuation_last'}), '天天基金 FundValuationLast');
assert.equal(context.favoriteLiveSourceLabel({__favorite_live_source:null}), '当前 API（来源未提供）');
assert.equal(context.favoriteLiveSourceLabel({__favorite_live_source:'unknown-isolated-source'}), 'unknown-isolated-source');
vm.runInContext(extract(shared, 'parseMetricDisplayNumber', 'formatMetricDisplayValue') +
  extract(read('tracker.js'), 'isTrackerValuationMarkedMissing', 'bindTrackerFilterBar') +
  extract(read('recommendations.js'), 'summarizeGroupNarrative', 'renderRecommendationGroupNarrative'), context);
for (const value of [null,undefined,'','bad']) {
  assert.equal(context.parseMetricDisplayNumber(value),null);
  assert.equal(context.filterTrackerRows([{'近3月(%)':value}],{recentStrength:'weak'}).length,0);
}
assert.equal(context.filterTrackerRows([{'近3月(%)':0}],{recentStrength:'weak'}).length,1);
assert.equal(context.summarizeGroupNarrative({count:5}).headline,'明细尚未加载');
assert.equal(context.summarizeGroupNarrative({count:0,funds:[]}).headline,'暂无基金');
vm.runInContext(extract(shared, 'apiConfigurationMatchState', 'renderHealthBadge') +
  extract(read('app.js'), 'renderApiSettingsOverview', 'setApiSettingsOverviewBusy') +
  extract(read('settings.js'), 'renderSettings', 'renderProbe'), context);
Object.assign(context, {
  apiSettingsOverviewStats: {}, apiSettingsOverviewBanner: {},
  settingsHeaderHealthBadge: {}, settingsMeta: {}, settingsHeroMeta: {}, settingsStats: {},
  fillInputs: () => {}, renderHealthBadge: () => {}, apiSettingsHealthPayload: value => value,
  formatDateTime: value => value || '-', escapeHtml: value => String(value),
  buildStatCard: (label, value, detail) => `${label}:${value}:${detail};`,
  setSettingsBanner: (kind, title, message) => { context.lastSettingsBanner = {kind, title, message}; },
});
const configKeys = ['tiantian_base_url', 'autostock_base_url', 'publication_validation_provider',
  'tiantian_valuation_primary_url', 'tiantian_valuation_fallback_url', 'eastmoney_fund_info_url', 'autostock_enabled'];
const configured = Object.fromEntries(configKeys.map(key => [key, 'isolated-config-marker']));
configured.autostock_enabled = false;
const allMatches = Object.fromEntries(configKeys.map(key => [key, true]));
const settingsState = (webMatches, schedulerMatches, available = true) => ({
  configured, runtime: configured, runtime_matches_config: webMatches,
  scheduler: {runtime: configured, runtime_matches_config: schedulerMatches,
    runtime_evidence_available: available, external_process: true, restart_required_after_change: true},
});
const renderSettingsState = state => {
  context.renderApiSettingsOverview(state);
  context.renderSettings(state);
};
renderSettingsState(settingsState(allMatches, allMatches));
assert(context.apiSettingsOverviewStats.innerHTML.includes('Tiantian 当前运行:isolated-config-marker:已同步;'));
assert(context.settingsStats.innerHTML.includes('运行一致性:一致:'));
assert(context.settingsStats.innerHTML.includes('调度一致性:一致:'));
assert.equal(context.lastSettingsBanner.kind, 'success');
assert(context.apiSettingsOverviewBanner.className.endsWith('success'));
for (const key of configKeys) {
  for (const absent of [null, undefined, 0, 'true']) {
    const incomplete = {...allMatches, [key]: absent};
    assert.equal(context.apiConfigurationMatchState(incomplete, configured), 'unknown');
    renderSettingsState(settingsState(incomplete, allMatches));
    assert(context.settingsStats.innerHTML.includes('运行一致性:未核验:'));
    assert(context.apiSettingsOverviewStats.innerHTML.includes('Tiantian 当前运行:isolated-config-marker:未核验;'));
    assert.equal(context.lastSettingsBanner.kind, 'warning');
    renderSettingsState(settingsState(allMatches, incomplete));
    assert(context.settingsStats.innerHTML.includes('调度一致性:未核验:'));
    assert(context.apiSettingsOverviewStats.innerHTML.includes('调度当前运行:isolated-config-marker:未核验;'));
    assert.equal(context.lastSettingsBanner.kind, 'warning');
  }
  const mismatch = {...allMatches, [key]: false};
  assert.equal(context.apiConfigurationMatchState(mismatch, configured), 'mismatch');
  renderSettingsState(settingsState(mismatch, allMatches));
  assert(context.settingsStats.innerHTML.includes('运行一致性:待重载:'));
  renderSettingsState(settingsState(allMatches, mismatch));
  assert(context.settingsStats.innerHTML.includes('调度一致性:待重启:'));
}
renderSettingsState(settingsState(allMatches, allMatches, false));
assert(context.settingsStats.innerHTML.includes('调度一致性:未上报:'));
assert.equal(context.lastSettingsBanner.kind, 'warning');
assert(context.apiSettingsOverviewBanner.className.endsWith('warning'));
console.log('749 frontend authority, source labels, NULL and loading behavior passed');
"""


def test_browser_contract_behavior_with_node() -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node runtime absent; static guards run in production image, Node behavior verified separately")
    result = subprocess.run([node, "-e", NODE_BEHAVIOR_SCRIPT], cwd=PROJECT,
                            capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stdout + result.stderr
