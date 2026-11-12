from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
FRONTEND = PROJECT_ROOT / "frontend"


def _read(name: str) -> str:
    return (FRONTEND / name).read_text(encoding="utf-8")


def test_frontend_pages_consume_metric_and_source_contracts() -> None:
    shared = _read("shared.js")
    recommendations = _read("recommendations.js")
    tracker = _read("tracker.js")
    favorites = _read("favorites.js")
    reports = _read("reports.js")
    excel_viewer = _read("excel_viewer.js")

    assert "function setMetricUnits(metricUnits)" in shared
    assert "function normalizePayloadDataContract(payload = {})" in shared
    assert "function applyPayloadDataContract(payload = {})" in shared
    assert "metadata.display_multiplier" in shared
    assert "metadata.display_digits" in shared
    assert "metadata.display_suffix" in shared
    assert "preserve_missing_semantics" in shared

    for source in (recommendations, tracker, favorites, reports, excel_viewer):
        assert "applyPayloadDataContract(" in source
        assert "payloadSourceContractSummary(" in source


def test_excel_viewer_loads_shared_contract_adapter_before_page_script() -> None:
    html = _read("excel_viewer.html")

    shared_index = html.index('<script src="/shared.js?v=')
    viewer_index = html.index('<script src="/excel-viewer.js?v=')
    assert shared_index < viewer_index


def test_fund_pages_preserve_missing_count_semantics_in_primary_summaries() -> None:
    recommendations = _read("recommendations.js")
    favorites = _read("favorites.js")
    reports = _read("reports.js")

    assert "portfolio.fund_count ?? 0" not in recommendations
    assert "portfolio.candidate_count ?? 0" not in recommendations
    assert "payload?.requested_count || 0" not in favorites
    assert "payload?.matched_count || 0" not in favorites
    assert "sumPresentNumbers(counts['正式主榜'], counts['自动诊断池'])" in favorites
    assert "sumPresentNumbers(focusMetricMap.sharpe?.intercepted_count" in reports


def test_primary_fund_pages_remove_redundant_first_screen_explanations() -> None:
    recommendations = _read("recommendations.html")
    tracker = _read("tracker.html")
    reports = _read("reports.html")

    assert "份额合并后展示" not in recommendations
    assert "同快照不重复" not in tracker
    assert "同快照仅留一版" not in reports
    assert "避免误读历史运行残留文件" not in reports


def test_homepage_prioritizes_cold_overview_before_ancillary_requests() -> None:
    shared = _read("shared.js")
    app = _read("app.js")

    overview_loader = """return fetchJson('/api/dashboard/overview', {
    cacheTtlMs: 15000,
    timeoutMs: 30000,
  });"""
    assert overview_loader in shared

    overview_index = app.index("const dashboardPayload = await loadDashboardOverview();")
    ancillary_index = app.index("const [bootstrapResult, apiSettingsResult] = await Promise.allSettled(")
    assert overview_index < ancillary_index
    assert "loadPublicationSummary()" not in app
