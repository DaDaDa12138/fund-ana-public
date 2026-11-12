from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping
import json
import math


FEE_STATUS_EXACT = "exact"
FEE_STATUS_OFFICIAL_FLOOR = "official_placeholder_confirmed"
FEE_STATUS_SHADOW_CODE = "shadow_code_candidate"
FEE_STATUS_SALES_MISSING = "sales_fee_missing_unresolved"
FEE_STATUS_CORE_MISSING = "core_fee_missing"
FEE_STATUS_NON_POSITIVE = "fee_non_positive"


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def load_sales_fee_validation_classification_map(
    *,
    audit_dir: Path | None = None,
) -> dict[str, str]:
    """读取最新销售服务费定向复核结果；缺失时保持严格缺失语义。"""
    base_dir = audit_dir or (Path(__file__).resolve().parent.parent / "core" / "audit")
    candidates = sorted(base_dir.glob("sales_fee_refetch_validation_audit_*.json"))
    if not candidates:
        return {}
    try:
        payload = json.loads(candidates[-1].read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return {}
    result: dict[str, str] = {}
    for row in payload.get("rows") or []:
        code = str((row or {}).get("fcode") or "").strip()
        classification = str((row or {}).get("classification") or "").strip()
        if code and classification:
            result[code] = classification
    return result


def evaluate_fee_semantics(
    *,
    fcode: Any,
    mgr_fee_rate: Any,
    trust_fee_rate: Any,
    sales_fee_rate: Any,
    validation_map: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """生成与 Core Layer5.5 一致的费率筛选和展示语义。"""
    code = str(fcode or "").strip()
    mgr_fee = _safe_float(mgr_fee_rate)
    trust_fee = _safe_float(trust_fee_rate)
    sales_fee = _safe_float(sales_fee_rate)
    classification = str((validation_map or {}).get(code) or "").strip()

    if mgr_fee is None or trust_fee is None:
        status = FEE_STATUS_CORE_MISSING
        core_fee = None
        exact_total = None
        lower_bound = None
        screening_value = None
    else:
        core_fee = mgr_fee + trust_fee
        exact_total = None
        lower_bound = None
        screening_value = None
        if sales_fee is not None:
            status = FEE_STATUS_EXACT
            exact_total = core_fee + sales_fee
            screening_value = exact_total
        elif classification == FEE_STATUS_OFFICIAL_FLOOR:
            status = FEE_STATUS_OFFICIAL_FLOOR
            lower_bound = core_fee
            screening_value = core_fee
        elif classification == FEE_STATUS_SHADOW_CODE:
            status = FEE_STATUS_SHADOW_CODE
        else:
            status = FEE_STATUS_SALES_MISSING

    if screening_value is not None and screening_value <= 0:
        status = FEE_STATUS_NON_POSITIVE
        screening_value = None
        exact_total = None
        lower_bound = None

    return {
        "status": status,
        "screening_ready": screening_value is not None,
        "screening_value": screening_value,
        "exact_total": exact_total,
        "lower_bound": lower_bound,
        "uses_floor": status == FEE_STATUS_OFFICIAL_FLOOR,
    }


def resolve_fee_screening_value(row: Mapping[str, Any]) -> float | None:
    value = _safe_float(row.get("费率筛选值(%)"))
    if value is not None:
        return value
    status = str(row.get("费率口径") or row.get("sales_fee_source_status") or "").strip()
    if status and status not in {FEE_STATUS_EXACT, FEE_STATUS_OFFICIAL_FLOOR}:
        return None
    return _safe_float(row.get("费率合计(%)"))


def fee_semantics_ready(row: Mapping[str, Any]) -> bool:
    explicit = row.get("费率筛选就绪")
    if explicit is None:
        explicit = row.get("费率数据完整")
    if explicit is not None and str(explicit).strip() != "":
        if isinstance(explicit, bool):
            return explicit
        return str(explicit).strip().lower() in {"1", "true", "yes", "是"}
    return resolve_fee_screening_value(row) is not None
