"""Verify actual CSV-to-replay transport without synthetic financial values."""
from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path
import sys

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "core")]

from backend.fund_cross_validation import CONTRACT, PROVIDER
from backend.project_publish_decisions import DATA_VALIDATION_COLUMNS, validation_status_value
from backend.publish_pipeline import apply_exported_autostock_status_map, demote_autostock_hard_block_rows
from backend import report_builder
from backend.report_inputs import load_actual_autostock_status_map
from backend.screening_baseline_replay import missing_autostock_observation_codes
from backend.screening_replay_snapshot import CODE_FILES, load_screening_input_snapshot


CODE = "900001"
NEUTRAL = {
    "数据校验原始状态": "strict_match", "数据校验状态": "通过",
    "数据校验备注": "", "数据校验告警原因": "",
    "数据校验来源": PROVIDER, "数据校验契约版本": CONTRACT,
}
LEGACY = {
    "AutoStock原始校验状态": "strict_match", "AutoStock校验状态": "通过",
    "AutoStock校验备注": "", "AutoStock告警原因": "",
}


def read_map(tmp_path: Path, **exports: dict) -> dict:
    artifacts = {}
    for key, validation in exports.items():
        path = tmp_path / f"{key}.csv"
        pd.DataFrame([{"基金代码": CODE, **validation}]).to_csv(path, index=False)
        artifacts[key] = str(path.relative_to(tmp_path))
    return load_actual_autostock_status_map(artifacts, project_root=tmp_path)


def overlay(statuses: dict) -> pd.DataFrame:
    candidate = pd.DataFrame([{"基金代码": CODE, "项目推荐结论": "正式推荐", **LEGACY}])
    return apply_exported_autostock_status_map(candidate, autostock_status_map=statuses)


@pytest.mark.parametrize("legacy_status", ["strict_match", "hard_block"])
def test_new_csv_source_and_contract_survive_overlay_without_autostock_relabeling(tmp_path: Path, legacy_status):
    statuses = read_map(tmp_path, filtered_csv={**NEUTRAL, **LEGACY, "AutoStock原始校验状态": legacy_status})
    assert statuses[CODE] == NEUTRAL
    result = overlay(statuses)
    assert result.iloc[0][list(DATA_VALIDATION_COLUMNS)].to_dict() == NEUTRAL
    assert all(result.iloc[0][column] == "" for column in LEGACY)
    assert validation_status_value(result.iloc[0]) == "strict_match"
    assert missing_autostock_observation_codes({CODE}, statuses) == []
    kept, blocked = demote_autostock_hard_block_rows(result)
    assert kept["基金代码"].tolist() == [CODE] and blocked.empty


def test_current_new_provider_hard_block_overrides_legacy_pass_and_is_demoted(tmp_path: Path):
    canonical = {**NEUTRAL, "数据校验原始状态": "hard_block", "数据校验状态": "拦截",
                 "数据校验告警原因": "isolated comparison mismatch", "数据校验备注": "isolated comparison mismatch"}
    statuses = read_map(tmp_path, filtered_csv={**canonical, **LEGACY})
    kept, blocked = demote_autostock_hard_block_rows(overlay(statuses))
    assert kept.empty and blocked["基金代码"].tolist() == [CODE]
    assert blocked.iloc[0]["数据校验来源"] == PROVIDER
    assert blocked.iloc[0]["数据校验契约版本"] == CONTRACT
    assert "数据校验硬拦截" in blocked.iloc[0]["推荐理由"]
    assert blocked.iloc[0]["AutoStock原始校验状态"] == ""


def test_disabled_legacy_autostock_hard_block_is_not_replayed_as_a_gate():
    frame = pd.DataFrame([{
        "基金代码": CODE,
        "项目推荐结论": "正式推荐",
        "AutoStock原始校验状态": "hard_block",
    }])
    kept, blocked = report_builder._demote_autostock_hard_block_rows(
        frame,
        allow_legacy_autostock=False,
    )
    assert kept["基金代码"].tolist() == [CODE]
    assert blocked.empty


@pytest.mark.parametrize("status", [None, "", "unknown", "not_checked"])
def test_missing_new_status_does_not_inherit_legacy_pass_or_count_as_observation(tmp_path: Path, status):
    statuses = read_map(tmp_path, filtered_csv={**NEUTRAL, **LEGACY, "数据校验原始状态": status})
    result = overlay(statuses)
    expected = "" if status is None else status
    assert statuses[CODE]["数据校验原始状态"] == expected
    assert result.iloc[0]["数据校验原始状态"] == expected
    assert validation_status_value(result.iloc[0]) != "strict_match"
    assert missing_autostock_observation_codes({CODE}, statuses) == [CODE]
    # Historical replay can reproduce its recorded degradation, without
    # claiming that a comparison occurred or granting it to a new entrant.
    observed_missing = [] if status in {"unknown", "not_checked"} else [CODE]
    assert missing_autostock_observation_codes({CODE}, statuses, observed_unvalidated_codes={CODE}) == observed_missing


@pytest.mark.parametrize("field", ["数据校验来源", "数据校验契约版本"])
def test_comparison_without_provider_or_contract_is_incomplete(tmp_path: Path, field):
    statuses = read_map(tmp_path, filtered_csv={**NEUTRAL, field: None, **LEGACY})
    assert statuses[CODE][field] == ""
    assert overlay(statuses).iloc[0][field] == ""
    assert missing_autostock_observation_codes({CODE}, statuses) == [CODE]


@pytest.mark.parametrize("later_status", ["unknown", "not_checked", "strict_match"])
def test_conservative_new_track_merge_preserves_hard_block_and_its_contract(tmp_path: Path, later_status):
    hard = {**NEUTRAL, "数据校验原始状态": "hard_block", "数据校验状态": "拦截",
            "数据校验告警原因": "isolated mismatch", "数据校验备注": "isolated mismatch"}
    statuses = read_map(tmp_path, dynamic_watch_csv=hard,
                        filtered_csv={**NEUTRAL, "数据校验原始状态": later_status})
    assert statuses[CODE] == hard
    kept, blocked = demote_autostock_hard_block_rows(overlay(statuses))
    assert kept.empty and len(blocked) == 1


@pytest.mark.parametrize("neutral_first", [True, False])
def test_missing_neutral_observation_cannot_be_replaced_by_another_providers_pass(tmp_path: Path, neutral_first):
    missing = {**NEUTRAL, "数据校验原始状态": None, "数据校验状态": None}
    exports = {"dynamic_watch_csv": missing if neutral_first else LEGACY,
               "filtered_csv": LEGACY if neutral_first else missing}
    statuses = read_map(tmp_path, **exports)
    assert statuses[CODE]["数据校验原始状态"] == ""
    assert statuses[CODE]["数据校验来源"] == PROVIDER
    assert missing_autostock_observation_codes({CODE}, statuses) == [CODE]


def test_legacy_csv_display_status_and_old_hard_block_remain_compatible(tmp_path: Path):
    statuses = read_map(tmp_path, filtered_csv={"AutoStock校验状态": "通过（含软告警）",
                                               "AutoStock校验备注": "isolated historical warning"})
    assert statuses[CODE]["AutoStock原始校验状态"] == "soft_pass_with_warnings"
    assert statuses[CODE]["AutoStock校验备注"] == "isolated historical warning"
    assert not set(DATA_VALIDATION_COLUMNS).intersection(statuses[CODE])
    result = overlay(statuses)
    assert result.iloc[0]["AutoStock原始校验状态"] == "soft_pass_with_warnings"
    assert not set(DATA_VALIDATION_COLUMNS).intersection(result.columns)
    hard = {**LEGACY, "AutoStock原始校验状态": "hard_block", "AutoStock校验状态": "拦截"}
    statuses = read_map(tmp_path, dynamic_watch_csv=hard, watch_csv={"AutoStock原始校验状态": "unknown"})
    kept, blocked = demote_autostock_hard_block_rows(overlay(statuses))
    assert kept.empty and blocked["基金代码"].tolist() == [CODE]


def test_overlay_uses_each_rows_actual_schema_and_drops_prior_other_provider_pass():
    other_code = "900002"
    prior = pd.DataFrame([{"基金代码": code, "项目推荐结论": "正式推荐", **NEUTRAL, **LEGACY}
                          for code in (CODE, other_code)])
    legacy_block = {**LEGACY, "AutoStock原始校验状态": "hard_block", "AutoStock校验状态": "拦截"}
    result = apply_exported_autostock_status_map(prior, autostock_status_map={CODE: NEUTRAL, other_code: legacy_block})
    assert result.iloc[0]["AutoStock原始校验状态"] == ""
    assert result.iloc[1]["数据校验原始状态"] == ""
    assert result.iloc[1]["数据校验来源"] == ""
    kept, blocked = demote_autostock_hard_block_rows(result)
    assert kept["基金代码"].tolist() == [CODE]
    assert blocked["基金代码"].tolist() == [other_code]


def test_snapshot_binds_reader_code_and_does_not_require_it_in_old_snapshots(tmp_path: Path):
    assert {"backend/report_inputs.py", "backend/publish_pipeline.py"} <= set(CODE_FILES)
    inputs = {"frames": {}}
    encoded = json.dumps(inputs, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    payload = {"schema_version": 1, "run_id": "isolated_legacy_snapshot", "inputs": inputs,
               "inputs_sha256": hashlib.sha256(encoded).hexdigest(), "code_sha256": {"legacy.py": "isolated"}}
    path = tmp_path / "legacy.json.gz"
    path.write_bytes(gzip.compress(json.dumps(payload).encode()))
    assert load_screening_input_snapshot(path, expected_run_id=payload["run_id"])["code_sha256"] == payload["code_sha256"]
