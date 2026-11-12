"""Integrity and lifecycle contracts for indexes over complete workbooks."""
from __future__ import annotations

from copy import deepcopy
import json
import os
from pathlib import Path
import shutil
from types import SimpleNamespace

import pandas as pd
import pytest
from openpyxl import load_workbook

from backend import forward_validation_cache as storage
from backend.forward_validation import (
    FORWARD_RETURN_CONTRACT,
    _load_forward_validation_cache,
    _write_forward_validation_artifacts,
    _write_forward_validation_cache,
)


@pytest.fixture
def cache_case(tmp_path: Path) -> dict:
    settings = SimpleNamespace(
        project_root=tmp_path, reports_dir=tmp_path / "reports",
        runs_dir=tmp_path / "reports/runs",
        archived_runs_dir=tmp_path / "reports/archive/superseded_runs", preview_top_n=15,
    )
    run_id = "20260521_060000_sync"
    generated_at = "2026-05-21T06:15:00"
    summary = {
        "return_contract": FORWARD_RETURN_CONTRACT,
        "available": True, "generated_at": generated_at,
        "latest_snapshot_date": "2026-05-20", "latest_snapshot_run_id": run_id,
        "latest_final_count": 2, "latest_stable_final_count": 1, "latest_enhanced_final_count": 1,
        "window_summaries": [], "incremental_review": {"review_preview": []},
    }
    frames = {
        "snapshot_df": pd.DataFrame([{"快照日期": "2026-05-20", "运行批次": run_id, "生成时间": generated_at}]),
        "comparison_df": pd.DataFrame([{"快照日期": "2026-05-20", "运行批次": run_id}]),
        "detail_df": pd.DataFrame([
            {"快照日期": "2026-05-20", "运行批次": run_id,
             "基金代码": f"{index:06d}", "收益(%)": None if index == 1 else (0.0 if index == 2 else 0.125)}
            for index in range(1, 37)
        ]),
    }
    artifacts = _write_forward_validation_artifacts(
        settings=settings, run_id=run_id, summary=summary, **frames,
    )
    metadata = {"source_run_id": run_id, "source_generated_at": generated_at,
                "source_json_path": artifacts["json"]}
    paths = _write_forward_validation_cache(
        settings, cache_key="a" * 40, summary=summary, **frames, metadata=metadata,
        source_excel_path=Path(artifacts["excel"]),
    )
    return {"settings": settings, "artifacts": artifacts, "frames": frames, "summary": summary,
            "metadata": metadata, "paths": paths, "key": "a" * 40,
            "payload": json.loads(paths["json"].read_text()),
            "fingerprint": storage.file_fingerprint(paths["excel"])}


def test_cache_index_preserves_preview_and_full_workbook_without_a_second_copy(cache_case: dict) -> None:
    case = cache_case
    payload = case["payload"]
    assert len(payload["detail_rows"]) == 24
    assert len(json.loads(Path(case["artifacts"]["json"]).read_text())["detail_rows"]) == 15
    assert payload["detail_row_count"] == 36 and payload["detail_rows_truncated"]
    assert payload["detail_rows"][0]["收益(%)"] == ""
    assert payload["detail_rows"][1]["收益(%)"] == "0.0"
    assert not list(case["paths"]["cache_dir"].glob("*.xlsx"))
    loaded = _load_forward_validation_cache(case["settings"], cache_key=case["key"])
    assert loaded["excel_path"] == case["paths"]["excel"]
    workbook = load_workbook(loaded["excel_path"], read_only=True)
    try:
        assert workbook.sheetnames == ["快照清单", "验证摘要", "基金明细", "窗口摘要", "新增样本体检"]
        assert workbook["基金明细"].max_row == 37
        assert workbook["基金明细"].cell(2, 4).value is None
        assert workbook["基金明细"].cell(3, 4).value == 0
    finally:
        workbook.close()
    assert storage.file_fingerprint(case["paths"]["excel"]) == case["fingerprint"]


def test_reference_survives_host_container_root_change_and_archiving(cache_case: dict, tmp_path: Path) -> None:
    case = cache_case
    moved_root = tmp_path / "new_mount"
    shutil.copytree(case["settings"].reports_dir, moved_root / "reports")
    moved = SimpleNamespace(
        project_root=moved_root, reports_dir=moved_root / "reports",
        runs_dir=moved_root / "reports/runs", archived_runs_dir=moved_root / "reports/archive/superseded_runs",
    )
    source_run = moved.runs_dir / case["metadata"]["source_run_id"]
    moved.archived_runs_dir.mkdir(parents=True)
    archived = moved.archived_runs_dir / source_run.name
    shutil.move(source_run, archived)
    # The old host files still exist: only the new mount may be used.
    loaded = _load_forward_validation_cache(moved, cache_key=case["key"])
    assert loaded["excel_path"] == archived / case["paths"]["excel"].name
    assert loaded["excel_fingerprint"] == case["fingerprint"]


@pytest.mark.parametrize("damage", ["missing", "same_size_change", "size_change"])
def test_invalid_source_is_a_miss_even_when_an_old_physical_cache_exists(cache_case: dict, damage: str) -> None:
    case = cache_case
    source = case["paths"]["excel"]
    legacy = case["paths"]["json"].with_suffix(".xlsx")
    shutil.copyfile(source, legacy)
    if damage == "missing":
        source.unlink()
    elif damage == "same_size_change":
        content = bytearray(source.read_bytes())
        content[100] ^= 1
        source.write_bytes(content)
    else:
        with source.open("ab") as stream:
            stream.write(b"changed")
    assert _load_forward_validation_cache(case["settings"], cache_key=case["key"]) is None
    assert storage.file_fingerprint(legacy) == case["fingerprint"]


@pytest.mark.parametrize("field,value", [
    ("schema_version", 9), ("reports_relative_path", "../elsewhere.xlsx"),
    ("reports_relative_path", "/app/reports/elsewhere.xlsx"),
    ("sha256", "x" * 64), ("size_bytes", True), ("size_bytes", 0),
])
def test_malformed_reference_is_never_trusted(cache_case: dict, field: str, value: object) -> None:
    case = cache_case
    payload = deepcopy(case["payload"])
    payload["excel_reference"][field] = value
    storage.replace_cache_json(case["paths"]["json"], payload)
    assert _load_forward_validation_cache(case["settings"], cache_key=case["key"]) is None


def test_legacy_cache_can_be_read_and_new_writer_does_not_overwrite_its_workbook(cache_case: dict) -> None:
    case = cache_case
    legacy = case["paths"]["json"].with_suffix(".xlsx")
    shutil.copyfile(case["paths"]["excel"], legacy)
    payload = deepcopy(case["payload"])
    payload.pop("excel_reference")
    storage.replace_cache_json(case["paths"]["json"], payload)
    loaded = _load_forward_validation_cache(case["settings"], cache_key=case["key"])
    assert loaded["excel_path"] == legacy
    _write_forward_validation_cache(
        case["settings"], cache_key=case["key"], summary=case["summary"], **case["frames"],
        metadata=case["metadata"], source_excel_path=case["paths"]["excel"],
    )
    assert storage.file_fingerprint(legacy) == case["fingerprint"]
    assert storage.file_fingerprint(case["paths"]["excel"]) == case["fingerprint"]


def test_atomic_index_failure_keeps_previous_bytes_and_no_temporary_file(cache_case: dict, monkeypatch) -> None:
    path = cache_case["paths"]["json"]
    original = path.read_bytes()
    original_replace = storage.os.replace

    def fail_replace(source, target):
        if target == path:
            raise OSError("injected replacement failure")
        return original_replace(source, target)

    monkeypatch.setattr(storage.os, "replace", fail_replace)
    with pytest.raises(OSError, match="injected replacement failure"):
        storage.replace_cache_json(path, {"summary": {"changed": True}})
    assert path.read_bytes() == original
    assert not list(path.parent.glob("*.tmp"))


@pytest.mark.parametrize("change", ["missing", "changed"])
def test_verified_copy_preserves_target_on_source_change(cache_case: dict, tmp_path: Path, change: str) -> None:
    source = cache_case["paths"]["excel"]
    target = tmp_path / "unchanged.xlsx"
    target.write_bytes(b"previous target")
    if change == "missing":
        source.unlink()
    else:
        content = bytearray(source.read_bytes())
        content[80] ^= 1
        source.write_bytes(content)
    with pytest.raises(storage.WorkbookSourceChanged):
        storage.copy_verified_workbook(source, target, cache_case["fingerprint"])
    assert target.read_bytes() == b"previous target"
    assert not list(tmp_path.glob("*.tmp"))


def test_automatic_index_retention_is_bounded_and_preserves_unique_or_unknown_evidence(cache_case: dict, monkeypatch) -> None:
    case = cache_case
    cache_dir = case["paths"]["cache_dir"]
    monkeypatch.setattr(storage, "REFERENCE_CACHE_KEEP_COUNT", 2)
    monkeypatch.setattr(storage, "REFERENCE_CACHE_PRUNE_BATCH", 3)
    protected = []
    for index in range(12):
        path = cache_dir / f"{index:040x}.json"
        payload = deepcopy(case["payload"])
        if index == 0:
            payload.pop("excel_reference")
            shutil.copyfile(case["paths"]["excel"], path.with_suffix(".xlsx"))
            protected.append(path)
        elif index == 1:
            payload["excel_reference"]["sha256"] = "0" * 64
            protected.append(path)
        elif index == 2:
            payload["summary"]["unique_history_note"] = "must remain"
            protected.append(path)
        storage.replace_cache_json(path, payload)
        os.utime(path, ns=(index + 1, index + 1))
    before = set(cache_dir.glob("*.json"))
    # Exercise retention through the production writer, not just its helper.
    _write_forward_validation_cache(
        case["settings"], cache_key=case["key"], summary=case["summary"], **case["frames"],
        metadata=case["metadata"], source_excel_path=case["paths"]["excel"],
    )
    after = set(cache_dir.glob("*.json"))
    assert len(before - after) == 3
    assert all(path.exists() for path in protected)
    assert case["paths"]["json"].exists()
    assert protected[0].with_suffix(".xlsx").exists()
    assert storage.file_fingerprint(case["paths"]["excel"]) == case["fingerprint"]


def test_output_write_failure_is_not_misreported_as_an_invalid_source(cache_case: dict, tmp_path: Path, monkeypatch) -> None:
    import errno

    source = cache_case["paths"]["excel"]
    target = tmp_path / "failed_output.xlsx"

    def fail_fsync(descriptor):
        raise OSError(errno.ENOSPC, "injected output disk full")

    monkeypatch.setattr(storage.os, "fsync", fail_fsync)
    with pytest.raises(OSError) as failure:
        storage.copy_verified_workbook(source, target, cache_case["fingerprint"])
    assert failure.value.errno == errno.ENOSPC
    assert not isinstance(failure.value, storage.WorkbookSourceChanged)
    assert not target.exists()
    assert storage.file_fingerprint(source) == cache_case["fingerprint"]


def test_report_preview_flag_describes_emitted_rows_even_if_cache_preview_is_complete(cache_case: dict) -> None:
    case = cache_case
    frames = {**case["frames"], "detail_df": case["frames"]["detail_df"].head(20)}
    artifacts = _write_forward_validation_artifacts(
        settings=case["settings"], run_id="preview_contract", summary=case["summary"],
        **frames, detail_row_count=20, detail_rows_truncated=False,
    )
    payload = json.loads(Path(artifacts["json"]).read_text())
    assert len(payload["detail_rows"]) == 15
    assert payload["detail_row_count"] == 20 and payload["detail_rows_truncated"] is True
    workbook = load_workbook(artifacts["excel"], read_only=True)
    try:
        assert workbook["基金明细"].max_row == 21
    finally:
        workbook.close()


@pytest.mark.parametrize("copy_fails", [False, True])
def test_archive_cleanup_preserves_full_referenced_workbook_or_keeps_source_on_failure(
    cache_case: dict, monkeypatch, copy_fails: bool,
) -> None:
    from datetime import datetime
    from scripts.cleanup_disk_usage import run_cleanup

    case = cache_case
    settings = case["settings"]
    run_dir = case["paths"]["excel"].parent
    archive_dir = settings.archived_runs_dir / run_dir.name
    settings.archived_runs_dir.mkdir(parents=True)
    shutil.move(run_dir, archive_dir)
    if copy_fails:
        def fail_copy(*args, **kwargs):
            raise OSError("injected insufficient storage")
        monkeypatch.setattr(storage, "copy_verified_workbook", fail_copy)
    report = run_cleanup(settings, archive_keep_count=0, archive_keep_days=0,
                         now=datetime(2026, 6, 1), apply=True)
    if copy_fails:
        assert archive_dir.exists() and archive_dir in report.archive_kept
        assert not report.archive_removed
        assert any("injected insufficient storage" in error for error in report.errors)
        assert "excel_reference" in json.loads(case["paths"]["json"].read_text())
    else:
        assert not report.errors and not archive_dir.exists()
        assert len(report.forward_cache_source_preservation) == 1
        payload = json.loads(case["paths"]["json"].read_text())
        assert "excel_reference" not in payload
        assert payload["detached_excel_source"] == case["payload"]["excel_reference"]
        assert payload["summary"] == case["summary"] and len(payload["detail_rows"]) == 24
    loaded = _load_forward_validation_cache(settings, cache_key=case["key"])
    assert loaded is not None and loaded["excel_fingerprint"] == case["fingerprint"]
    workbook = load_workbook(loaded["excel_path"], read_only=True)
    try:
        assert workbook["基金明细"].max_row == 37
    finally:
        workbook.close()
