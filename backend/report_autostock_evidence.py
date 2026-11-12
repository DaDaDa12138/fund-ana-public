"""Freeze the actual report supplement inputs; a saved status is not a new query."""
from __future__ import annotations

from datetime import datetime
import gzip
import hashlib
import json
import os
from pathlib import Path
from typing import Any

import pandas as pd

from backend.screening_replay_snapshot import _encode, freeze_frame


COMPARE_COLUMNS = (
    "基金代码", "基金名称", "基金类型", "最新净值日期", "最新单位净值",
    "近1周(%)", "近1月(%)", "近3月(%)", "近6月(%)", "近1年(%)",
    "区间收益基准日", "区间收益来源", "区间收益来源SHA256",
)
STATUS_COLUMNS = ("AutoStock原始校验状态", "AutoStock告警原因", "AutoStock校验状态", "AutoStock校验备注")
CODE_FILES = (
    "backend/report_autostock_evidence.py", "backend/report_builder.py",
    "backend/report_autostock_cache.py",
    "core/api/autostock_client.py", "core/api/autostock_endpoints.py",
    "core/bond_fund_screening/tasks/final_screening_autostock_helpers.py",
)
MAX_SNAPSHOT_BYTES = 64 * 1024 * 1024


def comparison_code_hashes(project_root: Path) -> dict[str, str]:
    return {name: hashlib.sha256((project_root / name).read_bytes()).hexdigest() for name in CODE_FILES}


def json_value(value: Any) -> Any:
    return _encode(value)


def content_sha256(value: Any) -> str:
    body = json.dumps(json_value(value), ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")
    return hashlib.sha256(body).hexdigest()


def begin_supplement_record(frame: pd.DataFrame, config: dict[str, Any]) -> dict[str, Any]:
    columns = [column for column in (*COMPARE_COLUMNS, *STATUS_COLUMNS) if column in frame]
    frozen = freeze_frame(frame[columns])
    return {
        "started_at": datetime.now().astimezone().isoformat(),
        "input_frame": frozen, "input_frame_sha256": content_sha256(frozen),
        "configuration": json_value(config), "configuration_sha256": content_sha256(config),
        "cache_reuses": [], "cache_rejections": [], "requested_codes": [], "budget": None,
        "logical_request": None, "logical_request_sha256": None,
        "logical_request_submitted_to_client": False,
        "upstream_records": None, "upstream_records_sha256": None,
        "upstream_metadata": None, "upstream_error": None,
        "upstream_record_scope": "original_accepted_client_records; not_raw_HTTP_bodies_or_rejected_records",
        "comparisons": [], "status_map": {},
    }


def finish_supplement_record(
    record: dict[str, Any], frame: pd.DataFrame, context: dict[str, Any] | None,
    *, outcome: str,
) -> pd.DataFrame:
    columns = [column for column in ("基金代码", *STATUS_COLUMNS) if column in frame]
    record.update({
        "finished_at": datetime.now().astimezone().isoformat(), "outcome": outcome,
        "output_statuses": json_value(frame[columns].to_dict(orient="records")),
    })
    record["record_sha256"] = content_sha256(record)
    if isinstance(context, dict):
        context.setdefault("autostock_supplement_records", []).append(record)
    return frame


def write_supplement_snapshot(
    *, project_root: Path, run_dir: Path, run_id: str, generated_at: str,
    context: dict[str, Any],
) -> dict[str, Any]:
    if run_dir.parent.name != "revisions" or run_dir.parent.parent.name != run_id:
        raise ValueError("autostock_supplement_revision_identity_mismatch")
    records = json_value(context.get("autostock_supplement_records", []))
    payload = {
        "schema_version": 1, "run_id": run_id, "revision": run_dir.name,
        "generated_at": generated_at, "records": records,
        "track_union": json_value(context.get("autostock_track_union")),
        "code_sha256": comparison_code_hashes(project_root),
        "truth_policy": "actual_report_time_inputs_and_accepted_API_records; legacy_cache_is_not_a_fresh_validation",
    }
    payload["content_sha256"] = content_sha256(payload)
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
    if len(encoded) > MAX_SNAPSHOT_BYTES:
        raise ValueError("autostock_supplement_snapshot_size_limit")
    path = run_dir / "autostock_supplement_snapshot.json.gz"
    with path.open("xb") as stream:
        with gzip.GzipFile(fileobj=stream, mode="wb", mtime=0) as compressed:
            compressed.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())
    try:
        stored_path = str(path.relative_to(project_root))
    except ValueError:
        stored_path = str(path)
    descriptor = {
        "schema_version": 1, "run_id": run_id, "revision": run_dir.name,
        "path": stored_path, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "content_sha256": payload["content_sha256"], "bytes": path.stat().st_size,
        "record_count": len(records),
        "comparison_count": sum(len(record["comparisons"]) for record in records),
        "cache_reuse_count": sum(len(record["cache_reuses"]) for record in records),
    }
    load_supplement_snapshot(project_root, descriptor, reports_dir=run_dir.parents[3],
                             run_id=run_id, revision=run_dir.name)
    return descriptor


def load_supplement_snapshot(
    project_root: Path, descriptor: dict[str, Any], *, reports_dir: Path, run_id: str, revision: str,
) -> dict[str, Any]:
    path = project_root / str(descriptor["path"])
    expected_dir = reports_dir / "runs" / run_id / "revisions" / revision
    if (path.parent.resolve() != expected_dir.resolve() or path.is_symlink()
            or descriptor.get("run_id") != run_id or descriptor.get("revision") != revision):
        raise ValueError("autostock_supplement_snapshot_identity_mismatch")
    if path.stat().st_size > MAX_SNAPSHOT_BYTES:
        raise ValueError("autostock_supplement_snapshot_size_limit")
    if hashlib.sha256(path.read_bytes()).hexdigest() != descriptor.get("sha256"):
        raise ValueError("autostock_supplement_snapshot_file_hash_mismatch")
    with gzip.open(path, "rb") as stream:
        encoded = stream.read(MAX_SNAPSHOT_BYTES + 1)
    if len(encoded) > MAX_SNAPSHOT_BYTES:
        raise ValueError("autostock_supplement_snapshot_size_limit")
    payload = json.loads(encoded)
    signed = {key: value for key, value in payload.items() if key != "content_sha256"}
    if (payload.get("schema_version") != 1 or payload.get("run_id") != run_id
            or payload.get("revision") != revision):
        raise ValueError("autostock_supplement_snapshot_identity_mismatch")
    if content_sha256(signed) != payload.get("content_sha256") or payload.get("content_sha256") != descriptor.get("content_sha256"):
        raise ValueError("autostock_supplement_snapshot_content_hash_mismatch")
    expected_counts = {
        "record_count": len(payload["records"]),
        "comparison_count": sum(len(record["comparisons"]) for record in payload["records"]),
        "cache_reuse_count": sum(len(record["cache_reuses"]) for record in payload["records"]),
    }
    if any(descriptor.get(key) != value for key, value in expected_counts.items()):
        raise ValueError("autostock_supplement_snapshot_counts_mismatch")
    for record in payload["records"]:
        validate_supplement_record(record)
    return payload


def validate_supplement_record(record: dict[str, Any]) -> None:
    """Validate the same record contract in publication and prospective inputs."""
    unsigned = {key: value for key, value in record.items() if key != "record_sha256"}
    if content_sha256(unsigned) != record.get("record_sha256"):
        raise ValueError("autostock_supplement_record_hash_mismatch")
    if content_sha256(record["input_frame"]) != record.get("input_frame_sha256"):
        raise ValueError("autostock_supplement_input_hash_mismatch")
    if content_sha256(record["configuration"]) != record.get("configuration_sha256"):
        raise ValueError("autostock_supplement_config_hash_mismatch")
    if record.get("logical_request") is not None:
        if content_sha256(record["logical_request"]) != record.get("logical_request_sha256"):
            raise ValueError("autostock_supplement_logical_request_hash_mismatch")
        if record["logical_request"].get("ordered_codes") != record["requested_codes"]:
            raise ValueError("autostock_supplement_logical_request_codes_mismatch")
    upstream = record.get("upstream_records")
    expected = None if upstream is None else content_sha256(upstream)
    if record.get("upstream_records_sha256") != expected:
        raise ValueError("autostock_supplement_response_hash_mismatch")
    upstream_hashes = {content_sha256(row) for row in upstream or []}
    for comparison in record["comparisons"]:
        if (comparison["code"] not in record["requested_codes"]
                or comparison["local_row"].get("基金代码") != comparison["code"]
                or comparison["upstream_record_sha256"] not in upstream_hashes):
            raise ValueError("autostock_supplement_comparison_source_mismatch")
