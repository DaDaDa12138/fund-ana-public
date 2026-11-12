"""Frozen, provider-labelled report comparisons; no legacy validation reuse."""
from __future__ import annotations

import gzip
import hashlib
import json
import os
from pathlib import Path
from typing import Any

from backend.report_autostock_evidence import content_sha256, json_value

STATUS_COLUMNS = (
    "数据校验原始状态", "数据校验状态", "数据校验备注", "数据校验告警原因",
    "数据校验来源", "数据校验契约版本",
)
CODE_FILES = (
    "backend/fund_cross_validation.py", "backend/report_cross_validation_evidence.py",
    "backend/report_builder.py", "backend/project_publish_decisions.py",
)
MAX_BYTES = 64 * 1024 * 1024


def write_snapshot(*, project_root: Path, run_dir: Path, run_id: str,
                   generated_at: str, context: dict[str, Any]) -> dict[str, Any]:
    if run_dir.parent.name != "revisions" or run_dir.parent.parent.name != run_id:
        raise ValueError("data_validation_revision_identity_mismatch")
    payload = {
        "schema_version": 1, "run_id": run_id, "revision": run_dir.name,
        "generated_at": generated_at, "records": json_value(context.get("data_validation_records", [])),
        "track_union": json_value(context.get("data_validation_track_union")),
        "code_sha256": {name: hashlib.sha256((project_root / name).read_bytes()).hexdigest() for name in CODE_FILES},
        "truth_policy": "fresh_endpoint_records; same_supplier_cross_endpoint; not_independent_vendor_validation",
    }
    payload["content_sha256"] = content_sha256(payload)
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()
    if len(raw) > MAX_BYTES:
        raise ValueError("data_validation_snapshot_size_limit")
    path = run_dir / "data_validation_snapshot.json.gz"
    with path.open("xb") as stream:
        with gzip.GzipFile(fileobj=stream, mode="wb", mtime=0) as compressed:
            compressed.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    descriptor = {
        "schema_version": 1, "run_id": run_id, "revision": run_dir.name,
        "path": str(path.relative_to(project_root)), "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "content_sha256": payload["content_sha256"], "bytes": path.stat().st_size,
        "record_count": len(payload["records"]),
        "comparison_count": sum(len(record["comparisons"]) for record in payload["records"]),
    }
    load_snapshot(project_root, descriptor, reports_dir=run_dir.parents[3], run_id=run_id, revision=run_dir.name)
    return descriptor


def load_snapshot(project_root: Path, descriptor: dict[str, Any], *, reports_dir: Path,
                  run_id: str, revision: str) -> dict[str, Any]:
    path = project_root / str(descriptor["path"])
    expected = reports_dir / "runs" / run_id / "revisions" / revision
    if (path.parent.resolve() != expected.resolve() or path.is_symlink()
            or descriptor.get("run_id") != run_id or descriptor.get("revision") != revision):
        raise ValueError("data_validation_snapshot_identity_mismatch")
    if path.stat().st_size > MAX_BYTES or hashlib.sha256(path.read_bytes()).hexdigest() != descriptor.get("sha256"):
        raise ValueError("data_validation_snapshot_file_hash_mismatch")
    with gzip.open(path, "rb") as stream:
        raw = stream.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        raise ValueError("data_validation_snapshot_size_limit")
    payload = json.loads(raw)
    if (payload.get("schema_version") != 1 or payload.get("run_id") != run_id
            or payload.get("revision") != revision):
        raise ValueError("data_validation_snapshot_identity_mismatch")
    unsigned = {key: value for key, value in payload.items() if key != "content_sha256"}
    if (content_sha256(unsigned) != payload.get("content_sha256")
            or payload["content_sha256"] != descriptor.get("content_sha256")):
        raise ValueError("data_validation_snapshot_content_hash_mismatch")
    if (len(payload["records"]) != descriptor.get("record_count")
            or sum(len(record["comparisons"]) for record in payload["records"]) != descriptor.get("comparison_count")):
        raise ValueError("data_validation_snapshot_counts_mismatch")
    for record in payload["records"]:
        unsigned_record = {key: value for key, value in record.items() if key != "record_sha256"}
        if content_sha256(unsigned_record) != record.get("record_sha256"):
            raise ValueError("data_validation_record_hash_mismatch")
        for key in ("local_rows", "upstream_records", "upstream_metadata", "configuration"):
            if content_sha256(record[key]) != record.get(f"{key}_sha256"):
                raise ValueError(f"data_validation_{key}_hash_mismatch")
        local_lookup = {row["基金代码"]: row for row in record["local_rows"]}
        outputs = {row["基金代码"]: row for row in record["output_statuses"]}
        if (len(local_lookup) != len(record["requested_codes"]) or set(outputs) != set(record["requested_codes"])
                or len(record["comparisons"]) != len(record["requested_codes"])):
            raise ValueError("data_validation_comparison_coverage_mismatch")
        for comparison in record["comparisons"]:
            code = comparison["code"]
            if (code not in record["requested_codes"] or comparison["local_row"].get("基金代码") != code
                    or comparison["local_row"] != local_lookup.get(code)
                    or content_sha256(record["upstream_records"].get(code)) != comparison["upstream_record_sha256"]):
                raise ValueError("data_validation_comparison_source_mismatch")
            result = comparison["result"]
            expected_status = ("hard_block" if result["hard_reasons"] else
                               "soft_pass_with_warnings" if result["warning_reasons"] else "strict_match")
            if (result["passed"] is not (not result["hard_reasons"])
                    or outputs[code]["数据校验原始状态"] != expected_status
                    or outputs[code]["数据校验来源"] != record["provider"]
                    or not outputs[code]["数据校验契约版本"]):
                raise ValueError("data_validation_comparison_result_mismatch")
    return payload
