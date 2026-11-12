"""Cache contract tests use isolated missing-value fixtures, never business data."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta
import json
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "core")]
from backend import report_builder
from backend.report_autostock_cache import cache_reuse_rejection, snapshot_cache_bindings
from backend.report_autostock_evidence import COMPARE_COLUMNS, content_sha256


NORMALIZERS = {"normalize_code": report_builder._normalize_code,
               "normalize_date": report_builder._normalize_nav_date_cache_key}


@pytest.fixture
def bound_cache():
    local = {column: None for column in COMPARE_COLUMNS}
    local.update({"基金代码": "000001", "基金名称": "isolated missing-value fixture", "最新净值日期": "2026-01-01"})
    config = report_builder._load_autostock_cross_validation_config_for_report()
    validated = datetime.now().astimezone() - timedelta(minutes=1)
    raw = {"code": "000001", "fixture_scope": "cache orchestration only"}
    output = {"基金代码": "000001", "AutoStock原始校验状态": "hard_block",
              "AutoStock校验备注": "latest_nav_missing"}
    record = {
        "configuration_sha256": content_sha256(config), "finished_at": validated.isoformat(),
        "logical_request": {"base_url": "https://example.invalid/v1"},
        "upstream_records": [raw], "output_statuses": [output], "cache_reuses": [],
        "comparisons": [{"code": "000001", "local_row": local, "upstream_record_sha256": content_sha256(raw),
                         "result": {"passed": False, "hard_reasons": ["latest_nav_missing"], "warning_reasons": []}}],
    }
    record["record_sha256"] = content_sha256(record)
    snapshot = {"records": [record], "code_sha256": {"isolated_fixture.py": content_sha256("fixture source")}}
    descriptor = {"path": "fixture/snapshot.json.gz", "run_id": "fixture", "revision": "original"}
    binding = snapshot_cache_bindings(snapshot, descriptor, **NORMALIZERS)[("000001", "2026-01-01")]
    cached = {"raw_status": "hard_block", "display_status": "拦截", "note": "latest_nav_missing",
              "source_row": local, "source": {"path": "fixture.csv", "sha256": content_sha256(local)},
              "validation_binding": binding}
    kwargs = {"configuration": config, "code_sha256": snapshot["code_sha256"],
              "base_url": "https://example.invalid/v1", "ttl_seconds": 14400,
              "now": validated + timedelta(minutes=1), **NORMALIZERS}
    return cached, deepcopy(local), kwargs, snapshot, descriptor


def test_matching_binding_reuses_the_actual_result(bound_cache):
    cached, local, kwargs, _, _ = bound_cache
    assert cache_reuse_rejection(cached, local, **kwargs) is None
    assert cached["raw_status"] == "hard_block"


def test_csv_export_roundtrip_keeps_all_comparison_inputs_and_hard_block(bound_cache, tmp_path):
    from backend.project_report_exports import REFERENCE_CSV_COLUMNS, REFERENCE_FINAL_COLUMNS
    from backend.report_workbook import _materialize_multisheet_workbook

    cached, local, kwargs, _, _ = bound_cache
    row = {**local, "AutoStock校验状态": "拦截", "AutoStock校验备注": cached["note"]}
    frame = pd.DataFrame([row])
    csv_path = tmp_path / "formal.csv"
    _materialize_multisheet_workbook(
        settings=SimpleNamespace(project_root=tmp_path, reports_dir=tmp_path),
        output_file=tmp_path / "formal.xlsx", artifact_key="filtered_excel",
        artifact_meta_key="filtered_excel_meta", main_sheet_name="Sheet1",
        main_df=frame.reindex(columns=REFERENCE_FINAL_COLUMNS),
        csv_output_file=csv_path, csv_artifact_key="filtered_csv",
        csv_df=frame.reindex(columns=REFERENCE_CSV_COLUMNS),
    )
    loaded = report_builder._load_autostock_status_cache_from_csv(csv_path)[("000001", "2026-01-01")]
    loaded["validation_binding"] = cached["validation_binding"]
    assert cache_reuse_rejection(loaded, local, **kwargs) is None
    assert loaded["raw_status"] == "hard_block"
    local["近1年(%)"] = 0.0
    assert cache_reuse_rejection(loaded, local, **kwargs) == "cache_comparison_input_changed"


@pytest.mark.parametrize("column", ["区间收益基准日", "区间收益来源", "区间收益来源SHA256"])
def test_missing_csv_source_columns_have_an_explicit_rejection(bound_cache, column):
    cached, local, kwargs, _, _ = bound_cache
    cached["source_row"].pop(column)
    assert cache_reuse_rejection(cached, local, **kwargs) == "cache_export_comparison_columns_missing"


@pytest.mark.parametrize("column", ["区间收益基准日", "区间收益来源", "区间收益来源SHA256"])
def test_incomplete_local_schema_stays_unchecked_without_a_request(bound_cache, column, monkeypatch, tmp_path):
    _, local, kwargs, _, _ = bound_cache
    local.pop(column)
    local["AutoStock原始校验状态"] = "not_checked"
    monkeypatch.setattr(report_builder, "_load_latest_official_autostock_cache", lambda settings: {})
    loader = Mock(side_effect=AssertionError("incomplete input must not request a comparison"))
    monkeypatch.setattr(report_builder, "_load_autostock_runtime_for_report", loader)
    context = {}
    output = report_builder._supplement_official_autostock_statuses(pd.DataFrame([local]),
        settings=SimpleNamespace(project_root=tmp_path, autostock_base_url=kwargs["base_url"]), status_context=context)
    loader.assert_not_called()
    assert output.iloc[0]["AutoStock原始校验状态"] == "not_checked"
    record = context["autostock_supplement_records"][0]
    assert record["outcome"] == "missing_compare_columns"
    assert record["missing_compare_columns"] == [column]
    assert record["logical_request_submitted_to_client"] is False


@pytest.mark.parametrize("column", ["最新单位净值", "基金类型", "近1年(%)", "区间收益基准日", "区间收益来源", "区间收益来源SHA256"])
def test_changed_comparison_field_invalidates_even_with_the_same_nav_date(bound_cache, column):
    cached, local, kwargs, _, _ = bound_cache
    local[column] = ("2026-01-02" if column == "区间收益基准日" else "isolated changed source"
                     if column in {"基金类型", "区间收益来源", "区间收益来源SHA256"} else 0.0)
    assert cache_reuse_rejection(cached, local, **kwargs) == "cache_comparison_input_changed"


def test_timestamp_date_representation_and_leading_zero_code_are_equivalent(bound_cache):
    cached, local, kwargs, _, _ = bound_cache
    local["基金代码"] = 1
    local["最新净值日期"] = "2026-01-01 00:00:00"
    assert cache_reuse_rejection(cached, local, **kwargs) is None


@pytest.mark.parametrize("change,reason", [
    ("configuration", "cache_comparison_configuration_changed"),
    ("code", "cache_comparison_code_changed"),
    ("endpoint", "cache_upstream_endpoint_changed"),
    ("expiry", "cache_validation_expired"),
    ("future", "cache_validation_expired"),
    ("disabled", "cache_validation_expired"),
    ("legacy", "legacy_cache_missing_comparison_binding"),
    ("missing_column", "cache_comparison_input_changed"),
    ("result", "cache_export_result_changed"),
    ("conflict", "cache_track_result_conflict"),
    ("binding_corruption", "cache_binding_invalid"),
    ("raw_record_corruption", "cache_upstream_record_invalid"),
])
def test_incompatible_evidence_never_becomes_a_cache_hit(bound_cache, change, reason):
    cached, local, kwargs, _, _ = bound_cache
    if change == "configuration":
        kwargs["configuration"] = {**kwargs["configuration"], "nav_absolute_tolerance": 0.002}
    elif change == "code":
        kwargs["code_sha256"] = {"isolated_fixture.py": content_sha256("changed source")}
    elif change == "endpoint":
        kwargs["base_url"] = "https://another.example.invalid/v1"
    elif change == "expiry":
        kwargs["now"] += timedelta(hours=5)
    elif change == "future":
        kwargs["now"] -= timedelta(hours=1)
    elif change == "disabled":
        kwargs["ttl_seconds"] = 0
    elif change == "legacy":
        cached.pop("validation_binding")
    elif change == "missing_column":
        local.pop("最新单位净值")
    elif change == "result":
        cached["raw_status"] = "strict_match"
    elif change == "conflict":
        cached["conflicting_track_sources"] = [{"path": "formal.csv"}, {"path": "dynamic.csv"}]
    elif change == "binding_corruption":
        cached["validation_binding"]["note"] = "damaged"
    elif change == "raw_record_corruption":
        binding = cached["validation_binding"]
        binding["upstream_record"] = {"code": "000002"}
        binding["binding_sha256"] = content_sha256({k: v for k, v in binding.items() if k != "binding_sha256"})
    assert cache_reuse_rejection(cached, local, **kwargs) == reason


def test_repeated_reuse_preserves_original_time_and_has_no_recursive_growth(bound_cache):
    cached, local, kwargs, _, descriptor = bound_cache
    original = deepcopy(cached["validation_binding"])
    size = len(json.dumps(cached))
    for _ in range(4):
        snapshot = {"code_sha256": kwargs["code_sha256"], "records": [{
            "finished_at": (kwargs["now"] + timedelta(hours=3)).isoformat(),
            "output_statuses": [], "comparisons": [], "cache_reuses": [{"cached": cached}],
        }]}
        bindings = snapshot_cache_bindings(snapshot, {**descriptor, "revision": "later"}, **NORMALIZERS)
        cached = {**cached, "validation_binding": bindings[("000001", "2026-01-01")]}
        assert cached["validation_binding"] == original
        assert len(json.dumps(cached)) == size
    assert cache_reuse_rejection(cached, local, **{**kwargs, "now": kwargs["now"] + timedelta(hours=5)}) == "cache_validation_expired"


def test_changed_input_is_sent_for_comparison_and_does_not_keep_the_old_result(bound_cache, monkeypatch, tmp_path):
    cached, local, kwargs, _, _ = bound_cache
    local["最新单位净值"] = 0.0  # Explicit zero/missing boundary injection.
    local["AutoStock原始校验状态"] = "not_checked"
    calls = []

    class Client:
        SAFE_FUND_BATCH_SIZE = 50

        def __init__(self, **kwargs):
            pass

        def get_fund_batch_with_meta(self, codes, batch_size):
            calls.append(codes)
            return [], {"missing_count": len(codes)}

    monkeypatch.setattr(report_builder, "_load_latest_official_autostock_cache", lambda settings: {("000001", "2026-01-01"): cached})
    monkeypatch.setattr(report_builder, "comparison_code_hashes", lambda root: kwargs["code_sha256"])
    monkeypatch.setattr(report_builder, "_load_autostock_runtime_for_report", lambda settings: (Client, None, None))
    monkeypatch.setattr(report_builder, "read_autostock_quota_snapshot", lambda **kwargs: {})
    monkeypatch.setattr(report_builder, "plan_autostock_call_budget", lambda **kwargs: {"allowed_call_count": 1})
    settings = SimpleNamespace(project_root=tmp_path, autostock_base_url=kwargs["base_url"],
        autostock_batch_fund_default=50, autostock_rate_limit_shared_state_path=tmp_path / "quota.json",
        autostock_rate_limit_max_calls_per_hour=100, autostock_rate_limit_min_interval_seconds=2.0)
    context = {}
    output = report_builder._supplement_official_autostock_statuses(pd.DataFrame([local]), settings=settings, status_context=context)
    assert calls == [["000001"]]
    assert output.iloc[0]["AutoStock原始校验状态"] == "not_checked"
    record = context["autostock_supplement_records"][0]
    assert record["cache_rejections"][0]["reason"] == "cache_comparison_input_changed"
    assert not record["cache_reuses"]
    assert record["logical_request_submitted_to_client"] is True
    assert record["logical_request_sha256"] == content_sha256(record["logical_request"])


def test_matching_binding_skips_network_and_keeps_hard_block(bound_cache, monkeypatch, tmp_path):
    cached, local, kwargs, _, _ = bound_cache
    local["AutoStock原始校验状态"] = "not_checked"
    monkeypatch.setattr(report_builder, "_load_latest_official_autostock_cache", lambda settings: {("000001", "2026-01-01"): cached})
    monkeypatch.setattr(report_builder, "comparison_code_hashes", lambda root: kwargs["code_sha256"])
    loader = Mock(side_effect=AssertionError("a valid cache hit must not call the client"))
    monkeypatch.setattr(report_builder, "_load_autostock_runtime_for_report", loader)
    context = {}
    output = report_builder._supplement_official_autostock_statuses(
        pd.DataFrame([local]), settings=SimpleNamespace(project_root=tmp_path, autostock_base_url=kwargs["base_url"]),
        status_context=context,
    )
    loader.assert_not_called()
    assert output.iloc[0]["AutoStock原始校验状态"] == "hard_block"
    assert output.iloc[0]["AutoStock校验备注"] == "latest_nav_missing"
    assert context["autostock_supplement_records"][0]["logical_request"] is None
    assert context["autostock_supplement_records"][0]["cache_reuses"][0]["cached"]["validation_binding"] == cached["validation_binding"]


def test_loader_uses_committed_report_even_if_latest_alias_points_elsewhere(bound_cache, monkeypatch, tmp_path):
    from backend import publication_transaction
    cached, local, kwargs, snapshot, descriptor = bound_cache
    reports = tmp_path / "reports"
    reports.mkdir()
    source_row = {**local, "AutoStock校验状态": "拦截", "AutoStock校验备注": "latest_nav_missing"}
    pd.DataFrame([source_row]).to_csv(reports / "formal.csv", index=False)
    committed = reports / "committed.json"
    committed.write_text(json.dumps({
        "artifacts": {"filtered_csv": "reports/formal.csv"},
        "publication_control": {"revision": "original"},
        "autostock_validation_audit": {"supplement_snapshot": descriptor},
    }))
    (reports / "latest.json").write_text(json.dumps({"artifacts": {"filtered_csv": "reports/uncommitted.csv"}}))
    monkeypatch.setattr(publication_transaction, "load_publication_head", lambda settings: {
        "current_published_run_id": "fixture", "committed_runs": {"fixture": {"report_path": "reports/committed.json"}},
    })
    loader = Mock(return_value=snapshot)
    monkeypatch.setattr(report_builder, "load_supplement_snapshot", loader)
    result = report_builder._load_latest_official_autostock_cache(SimpleNamespace(project_root=tmp_path, reports_dir=reports))
    assert result[("000001", "2026-01-01")]["validation_binding"] == cached["validation_binding"]
    loader.assert_called_once()
    assert loader.call_args.kwargs["run_id"] == "fixture"
    assert loader.call_args.kwargs["revision"] == "original"
