"""Reuse a report comparison only while its original evidence still applies."""
from __future__ import annotations

from datetime import datetime
import math
from typing import Any, Callable

import pandas as pd

from backend.report_autostock_evidence import COMPARE_COLUMNS, content_sha256


def comparison_input(
    row: Any, *, normalize_code: Callable, normalize_date: Callable,
) -> dict[str, Any] | None:
    """Normalize CSV representations without rounding or converting missing to zero."""
    if any(column not in row for column in COMPARE_COLUMNS):
        return None
    result: dict[str, Any] = {}
    for column in COMPARE_COLUMNS:
        value = row[column]
        if isinstance(value, (dict, list, tuple)):
            return None
        if column == "基金代码":
            result[column] = normalize_code(value)
        elif column in {"最新净值日期", "区间收益基准日"}:
            result[column] = normalize_date(value)
        elif value is None or pd.isna(value):
            result[column] = None
        elif column in {"基金名称", "基金类型", "区间收益来源", "区间收益来源SHA256"}:
            result[column] = str(value).strip()
        else:
            try:
                number = float(value)
            except (TypeError, ValueError):
                return None
            if not math.isfinite(number):
                return None
            result[column] = number
    return result


def snapshot_cache_bindings(
    snapshot: dict[str, Any], descriptor: dict[str, Any], *,
    normalize_code: Callable, normalize_date: Callable,
) -> dict[tuple[str, str], dict[str, Any]]:
    """Keep original timestamps and raw records through repeated cache reuse."""
    bindings: dict[tuple[str, str], dict[str, Any]] = {}

    def add(binding: dict[str, Any]) -> None:
        unsigned = {key: value for key, value in binding.items() if key != "binding_sha256"}
        if binding.get("binding_sha256") != content_sha256(unsigned):
            raise ValueError("autostock_cache_binding_hash_mismatch")
        local = binding["comparison_input"]
        key = (normalize_code(local.get("基金代码")), normalize_date(local.get("最新净值日期")))
        if key in bindings and bindings[key] != binding:
            raise ValueError(f"autostock_cache_binding_conflict:{key[0]}")
        bindings[key] = binding

    for record in snapshot["records"]:
        outputs = {normalize_code(row.get("基金代码")): row for row in record["output_statuses"]}
        raw_records = {content_sha256(row): row for row in record.get("upstream_records") or []}
        for comparison in record["comparisons"]:
            code = normalize_code(comparison["code"])
            local = comparison_input(comparison["local_row"], normalize_code=normalize_code, normalize_date=normalize_date)
            if local is None:
                continue
            output = outputs[code]
            request = record.get("logical_request") or {}
            upstream_source = (record.get("upstream_metadata") or {}).get("source_evidence") or {}
            raw = raw_records[comparison["upstream_record_sha256"]]
            binding = {
                "schema_version": 1,
                "comparison_input": local, "comparison_input_sha256": content_sha256(local),
                "configuration_sha256": record["configuration_sha256"],
                "code_sha256": snapshot["code_sha256"],
                "base_url": request.get("base_url") or upstream_source.get("base_url"),
                "endpoint_path": "/fund",
                "validated_at": record["finished_at"],
                "raw_status": output["AutoStock原始校验状态"],
                "note": output.get("AutoStock校验备注") or "",
                "upstream_record": raw, "upstream_record_sha256": comparison["upstream_record_sha256"],
                "comparison_result": comparison["result"],
                "original_snapshot": descriptor, "original_record_sha256": record["record_sha256"],
            }
            binding["binding_sha256"] = content_sha256(binding)
            add(binding)
        for reuse in record["cache_reuses"]:
            binding = (reuse.get("cached") or {}).get("validation_binding")
            if isinstance(binding, dict):
                # The complete accepted record travels with the binding. Never
                # wrap a prior cache entry or refresh its validation timestamp.
                add(binding)
    return bindings


def cache_reuse_rejection(
    cached: dict[str, Any], row: Any, *, configuration: dict[str, Any],
    code_sha256: dict[str, str], base_url: str, ttl_seconds: int,
    normalize_code: Callable, normalize_date: Callable, now: datetime | None = None,
) -> str | None:
    if cached.get("conflicting_track_sources"):
        return "cache_track_result_conflict"
    binding = cached.get("validation_binding")
    if not isinstance(binding, dict):
        return "legacy_cache_missing_comparison_binding"
    unsigned = {key: value for key, value in binding.items() if key != "binding_sha256"}
    if binding.get("schema_version") != 1 or binding.get("binding_sha256") != content_sha256(unsigned):
        return "cache_binding_invalid"
    if binding.get("configuration_sha256") != content_sha256(configuration):
        return "cache_comparison_configuration_changed"
    if binding.get("code_sha256") != code_sha256:
        return "cache_comparison_code_changed"
    if str(binding.get("base_url") or "").rstrip("/") != str(base_url).rstrip("/") or binding.get("endpoint_path") != "/fund":
        return "cache_upstream_endpoint_changed"
    local = comparison_input(row, normalize_code=normalize_code, normalize_date=normalize_date)
    source_row = cached.get("source_row") or {}
    if any(column not in source_row for column in COMPARE_COLUMNS):
        return "cache_export_comparison_columns_missing"
    source = comparison_input(source_row, normalize_code=normalize_code, normalize_date=normalize_date)
    expected = binding.get("comparison_input_sha256")
    if (local is None or source is None or content_sha256(local) != expected
            or content_sha256(source) != expected or content_sha256(binding.get("comparison_input")) != expected):
        return "cache_comparison_input_changed"
    if (cached.get("raw_status") != binding.get("raw_status")
            or str(cached.get("note") or "") != str(binding.get("note") or "")):
        return "cache_export_result_changed"
    if content_sha256(binding.get("upstream_record")) != binding.get("upstream_record_sha256"):
        return "cache_upstream_record_invalid"
    try:
        validated = datetime.fromisoformat(binding["validated_at"])
        if validated.tzinfo is None:
            return "cache_validation_time_unknown"
        age = ((now or datetime.now().astimezone()) - validated).total_seconds()
    except (TypeError, ValueError, KeyError):
        return "cache_validation_time_unknown"
    if ttl_seconds <= 0 or age < 0 or age > ttl_seconds:
        return "cache_validation_expired"
    return None
