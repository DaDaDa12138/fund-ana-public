#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
天天基金数据可信度闸门。

目标：
1. 区分“请求失败/上游业务失败”与“确实没有数据”；
2. 避免 APIFailureResult、HTTP 200 + Success=false 被误当成可消费数据；
3. 为补数/采集脚本提供统一、可复用的数据质量判定。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from api.client import APIFailureResult


RETRYABLE_UPSTREAM_BUSY_CODES = {"61136"}
RETRYABLE_UPSTREAM_BUSY_MARKERS = ("网络繁忙", "请稍后重试")
FAILURE_KIND_ALIASES = {
    "json_decode_error": "invalid_json",
}


@dataclass(frozen=True)
class TiantianPayloadQuality:
    """统一描述单次 Tiantian 返回体的可信度。"""

    accepted: bool
    has_data: bool
    status: str
    message: str
    payload_type: Optional[str] = None
    failure_kind: Optional[str] = None
    error_type: Optional[str] = None
    http_status: Optional[int] = None
    upstream_business_code: Optional[str] = None
    upstream_business_message: Optional[str] = None
    record_count: Optional[int] = None


def _payload_type_name(value: Any) -> str:
    """返回 payload 的类型名称，便于日志与报告引用。"""
    return type(value).__name__


def normalize_tiantian_failure_kind(value: Any) -> Optional[str]:
    """统一 Tiantian 失败类型口径，兼容历史别名。"""
    normalized = str(value or "").strip()
    if not normalized:
        return None
    return FAILURE_KIND_ALIASES.get(normalized, normalized)


def _is_structured_failure_result(result: Any) -> bool:
    """识别 APIClient 返回的结构化失败对象。"""
    return isinstance(result, APIFailureResult) or (
        isinstance(result, dict)
        and result.get("ok") is False
        and "failure_kind" in result
    )


def _is_retryable_upstream_busy(err_code: Optional[str], err_msg: Optional[str]) -> bool:
    """识别上游业务级降级/繁忙。"""
    normalized_code = str(err_code).strip() if err_code not in (None, "") else None
    normalized_msg = str(err_msg or "").strip()
    if normalized_code in RETRYABLE_UPSTREAM_BUSY_CODES:
        return True
    return all(marker in normalized_msg for marker in RETRYABLE_UPSTREAM_BUSY_MARKERS)


def _has_meaningful_scalar(value: Any) -> bool:
    """判断上游字段是否包含可消费的有效值。"""
    if value is None:
        return False
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return False
        return normalized not in {"--", "---", "暂无", "N/A", "n/a", "null", "NULL"}
    return True


def _build_structured_failure_quality(
    result: Dict[str, Any],
    *,
    endpoint_label: str,
) -> TiantianPayloadQuality:
    """把 APIClient 的结构化失败对象映射为统一质量结果。"""
    upstream_business_code = result.get("upstream_business_code")
    if upstream_business_code not in (None, ""):
        upstream_business_code = str(upstream_business_code).strip() or None
    else:
        upstream_business_code = None

    upstream_business_message = str(result.get("upstream_business_message") or "").strip() or None
    failure_kind = normalize_tiantian_failure_kind(result.get("failure_kind"))
    if upstream_business_code or upstream_business_message:
        normalized_message = upstream_business_message or str(result.get("ErrMsg") or result.get("error") or "unknown").strip()
        status = (
            "upstream_business_degraded"
            if _is_retryable_upstream_busy(upstream_business_code, normalized_message)
            else "upstream_business_failure"
        )
        return TiantianPayloadQuality(
            accepted=False,
            has_data=False,
            status=status,
            message=f"{endpoint_label} 上游业务失败: ErrCode={upstream_business_code} ErrMsg={normalized_message}",
            payload_type=_payload_type_name(result),
            failure_kind=failure_kind,
            error_type=result.get("error_type"),
            http_status=result.get("status_code"),
            upstream_business_code=upstream_business_code,
            upstream_business_message=normalized_message,
        )

    failure_kind = failure_kind or "unknown"
    error_message = str(result.get("error") or "unknown").strip() or "unknown"
    detail_parts = [f"failure_kind={failure_kind}", f"error={error_message}"]

    err_code = result.get("ErrCode")
    if err_code not in (None, ""):
        detail_parts.append(f"err_code={err_code}")

    err_msg = str(result.get("ErrMsg") or "").strip()
    if err_msg and err_msg != error_message:
        detail_parts.append(f"err_msg={err_msg}")

    if result.get("retryable") is not None:
        detail_parts.append(f"retryable={bool(result.get('retryable'))}")

    return TiantianPayloadQuality(
        accepted=False,
        has_data=False,
        status="request_failed",
        message=f"{endpoint_label} 请求失败: {' '.join(detail_parts)}",
        payload_type=_payload_type_name(result),
        failure_kind=failure_kind,
        error_type=result.get("error_type"),
        http_status=result.get("status_code"),
    )


def _inspect_common_datas_envelope(
    result: Any,
    *,
    endpoint_label: str,
) -> Tuple[Optional[Any], TiantianPayloadQuality]:
    """检查统一的 `Datas` 响应外壳。"""
    if result is None:
        return None, TiantianPayloadQuality(
            accepted=False,
            has_data=False,
            status="empty_response",
            message=f"{endpoint_label} 返回空响应",
            payload_type="NoneType",
        )

    if _is_structured_failure_result(result):
        return None, _build_structured_failure_quality(result, endpoint_label=endpoint_label)

    if not isinstance(result, dict):
        payload_type = _payload_type_name(result)
        return None, TiantianPayloadQuality(
            accepted=False,
            has_data=False,
            status="invalid_root_type",
            message=f"{endpoint_label} 返回根结构异常: {payload_type}",
            payload_type=payload_type,
        )

    success_value = result.get("Success")
    if success_value is None and "success" in result:
        success_value = result.get("success")

    if success_value is False:
        err_code = result.get("ErrCode")
        if err_code is None and "errorCode" in result:
            err_code = result.get("errorCode")
        err_msg = str(result.get("ErrMsg") or result.get("errorMessage") or result.get("firstError") or "").strip() or "unknown"
        status = "upstream_business_degraded" if _is_retryable_upstream_busy(err_code, err_msg) else "upstream_business_failure"
        return None, TiantianPayloadQuality(
            accepted=False,
            has_data=False,
            status=status,
            message=f"{endpoint_label} 上游业务失败: ErrCode={err_code} ErrMsg={err_msg}",
            payload_type="dict",
            upstream_business_code=str(err_code).strip() if err_code not in (None, "") else None,
            upstream_business_message=err_msg,
        )

    if "Datas" not in result:
        return None, TiantianPayloadQuality(
            accepted=False,
            has_data=False,
            status="missing_datas_key",
            message=f"{endpoint_label} 返回缺少 Datas 字段",
            payload_type="dict",
        )

    return result.get("Datas"), TiantianPayloadQuality(
        accepted=True,
        has_data=True,
        status="envelope_ok",
        message=f"{endpoint_label} 外层结构可解析",
        payload_type=_payload_type_name(result.get("Datas")),
    )


def inspect_tiantian_detail_payload(
    result: Any,
    *,
    endpoint_label: str = "/fundMNDetailInformation",
) -> Tuple[Optional[Dict[str, Any]], TiantianPayloadQuality]:
    """检查详情接口返回是否可安全消费。"""
    datas, envelope_quality = _inspect_common_datas_envelope(result, endpoint_label=endpoint_label)
    if not envelope_quality.accepted:
        return None, envelope_quality

    if not isinstance(datas, dict):
        payload_type = _payload_type_name(datas)
        return None, TiantianPayloadQuality(
            accepted=False,
            has_data=False,
            status="invalid_datas_type",
            message=f"{endpoint_label} 的 Datas 类型异常: {payload_type}",
            payload_type=payload_type,
        )

    if not datas:
        return None, TiantianPayloadQuality(
            accepted=False,
            has_data=False,
            status="empty_datas",
            message=f"{endpoint_label} 的 Datas 为空对象",
            payload_type="dict",
        )

    return datas, TiantianPayloadQuality(
        accepted=True,
        has_data=True,
        status="ok",
        message=f"{endpoint_label} 详情 payload 可消费",
        payload_type="dict",
    )


def inspect_tiantian_period_increase_payload(
    result: Any,
    *,
    endpoint_label: str = "/fundMNPeriodIncrease",
) -> Tuple[List[Dict[str, Any]], TiantianPayloadQuality]:
    """检查周期收益率接口返回是否可安全消费。"""
    datas, envelope_quality = _inspect_common_datas_envelope(result, endpoint_label=endpoint_label)
    if not envelope_quality.accepted:
        return [], envelope_quality

    if not isinstance(datas, list):
        payload_type = _payload_type_name(datas)
        return [], TiantianPayloadQuality(
            accepted=False,
            has_data=False,
            status="invalid_datas_type",
            message=f"{endpoint_label} 的 Datas 类型异常: {payload_type}",
            payload_type=payload_type,
        )

    if not datas:
        return [], TiantianPayloadQuality(
            accepted=True,
            has_data=False,
            status="empty_period_records",
            message=f"{endpoint_label} 返回空收益率记录",
            payload_type="list",
            record_count=0,
        )

    return datas, TiantianPayloadQuality(
        accepted=True,
        has_data=True,
        status="ok",
        message=f"{endpoint_label} 收益率 payload 可消费",
        payload_type="list",
        record_count=len(datas),
    )


def inspect_tiantian_rank_payload(
    result: Any,
    *,
    endpoint_label: str = "/fundMNRank",
) -> Tuple[List[Dict[str, Any]], TiantianPayloadQuality]:
    """检查排行接口返回是否可安全消费。"""
    datas, envelope_quality = _inspect_common_datas_envelope(result, endpoint_label=endpoint_label)
    if not envelope_quality.accepted:
        return [], envelope_quality

    if not isinstance(datas, list):
        payload_type = _payload_type_name(datas)
        return [], TiantianPayloadQuality(
            accepted=False,
            has_data=False,
            status="invalid_datas_type",
            message=f"{endpoint_label} 的 Datas 类型异常: {payload_type}",
            payload_type=payload_type,
        )

    if not datas:
        return [], TiantianPayloadQuality(
            accepted=True,
            has_data=False,
            status="empty_rank_records",
            message=f"{endpoint_label} 返回空排行记录",
            payload_type="list",
            record_count=0,
        )

    return datas, TiantianPayloadQuality(
        accepted=True,
        has_data=True,
        status="ok",
        message=f"{endpoint_label} 排行 payload 可消费",
        payload_type="list",
        record_count=len(datas),
    )


def inspect_tiantian_manager_list_payload(
    result: Any,
    *,
    endpoint_label: str = "/fundMNMangerList",
) -> Tuple[List[Dict[str, Any]], TiantianPayloadQuality]:
    """检查基金经理列表接口返回是否可安全消费。"""
    datas, envelope_quality = _inspect_common_datas_envelope(result, endpoint_label=endpoint_label)
    if not envelope_quality.accepted:
        return [], envelope_quality

    if not isinstance(datas, list):
        payload_type = _payload_type_name(datas)
        return [], TiantianPayloadQuality(
            accepted=False,
            has_data=False,
            status="invalid_datas_type",
            message=f"{endpoint_label} 的 Datas 类型异常: {payload_type}",
            payload_type=payload_type,
        )

    if not datas:
        return [], TiantianPayloadQuality(
            accepted=True,
            has_data=False,
            status="empty_manager_list_records",
            message=f"{endpoint_label} 返回空经理列表记录",
            payload_type="list",
            record_count=0,
        )

    return datas, TiantianPayloadQuality(
        accepted=True,
        has_data=True,
        status="ok",
        message=f"{endpoint_label} 经理列表 payload 可消费",
        payload_type="list",
        record_count=len(datas),
    )


def inspect_tiantian_manager_info_payload(
    result: Any,
    *,
    endpoint_label: str = "/fundMSNMangerInfo",
    expected_manager_id: str | None = None,
) -> Tuple[Optional[Dict[str, Any]], TiantianPayloadQuality]:
    """检查基金经理详情接口返回是否可安全消费。"""
    datas, envelope_quality = _inspect_common_datas_envelope(result, endpoint_label=endpoint_label)
    if not envelope_quality.accepted:
        return None, envelope_quality

    return inspect_tiantian_manager_info_record(
        datas, expected_manager_id=expected_manager_id, endpoint_label=endpoint_label,
    )


def inspect_tiantian_manager_info_record(
    datas: Any,
    *,
    expected_manager_id: str | None = None,
    endpoint_label: str = "/fundMSNMangerInfo",
) -> Tuple[Optional[Dict[str, Any]], TiantianPayloadQuality]:
    """Bind the returned MGRID to the request before any profile can be saved.

    The endpoint's request parameter is FCODE, but response FCODE lists managed
    funds. Only MGRID identifies the manager; a name cannot replace that key.
    """
    if not isinstance(datas, dict):
        payload_type = _payload_type_name(datas)
        return None, TiantianPayloadQuality(
            accepted=False,
            has_data=False,
            status="invalid_datas_type",
            message=f"{endpoint_label} 的 Datas 类型异常: {payload_type}",
            payload_type=payload_type,
        )

    if not datas:
        return None, TiantianPayloadQuality(
            accepted=True,
            has_data=False,
            status="empty_manager_info",
            message=f"{endpoint_label} 返回空经理详情",
            payload_type="dict",
        )

    raw_id = datas.get("MGRID")
    identity = str(raw_id).strip() if isinstance(raw_id, (str, int)) and not isinstance(raw_id, bool) else ""
    expected = str(expected_manager_id).strip() if expected_manager_id is not None else None
    raw_name = datas.get("MGRNAME")
    name = raw_name.strip() if isinstance(raw_name, str) else ""
    placeholders = {"-", "None", "none", "NaN", "nan"}
    if not identity or not _has_meaningful_scalar(identity) or identity in placeholders:
        status = "invalid_manager_identity"
        message = f"{endpoint_label} 缺少有效 MGRID，不能确认经理身份"
    elif expected is not None and (not expected or identity != expected):
        status = "manager_identity_mismatch"
        message = f"{endpoint_label} 返回 MGRID={identity} 与请求经理 {expected} 不一致"
    elif not name or not _has_meaningful_scalar(name) or name in placeholders:
        status = "invalid_manager_name"
        message = f"{endpoint_label} 缺少有效经理姓名"
    else:
        status = "ok"
        message = f"{endpoint_label} 经理详情 payload 可消费"
    if status != "ok":
        return None, TiantianPayloadQuality(
            accepted=False, has_data=False, status=status, message=message,
            payload_type="dict", record_count=0,
        )

    return datas, TiantianPayloadQuality(
        accepted=True,
        has_data=True,
        status="ok",
        message=message,
        payload_type="dict",
        record_count=1,
    )


def inspect_tiantian_company_payload(
    result: Any,
    *,
    endpoint_label: str = "/companyApi2",
) -> Tuple[Optional[Dict[str, Any]], TiantianPayloadQuality]:
    """检查基金公司详情接口返回是否可安全消费。"""
    if result is None:
        return None, TiantianPayloadQuality(
            accepted=False,
            has_data=False,
            status="empty_response",
            message=f"{endpoint_label} 返回空响应",
            payload_type="NoneType",
        )

    if _is_structured_failure_result(result):
        return None, _build_structured_failure_quality(result, endpoint_label=endpoint_label)

    if not isinstance(result, dict):
        payload_type = _payload_type_name(result)
        return None, TiantianPayloadQuality(
            accepted=False,
            has_data=False,
            status="invalid_root_type",
            message=f"{endpoint_label} 返回根结构异常: {payload_type}",
            payload_type=payload_type,
        )

    raw_code = result.get("code")
    normalized_code = str(raw_code).strip() if raw_code not in (None, "") else None
    if normalized_code is None:
        return None, TiantianPayloadQuality(
            accepted=False,
            has_data=False,
            status="missing_success_code",
            message=f"{endpoint_label} 返回缺少 code=1 成功标记",
            payload_type="dict",
        )
    if normalized_code not in (None, "1"):
        error_message = (
            str(result.get("message") or result.get("msg") or result.get("ErrMsg") or "").strip()
            or "unknown"
        )
        status = (
            "upstream_business_degraded"
            if _is_retryable_upstream_busy(normalized_code, error_message)
            else "upstream_business_failure"
        )
        return None, TiantianPayloadQuality(
            accepted=False,
            has_data=False,
            status=status,
            message=f"{endpoint_label} 上游业务失败: code={normalized_code} message={error_message}",
            payload_type="dict",
            upstream_business_code=normalized_code,
            upstream_business_message=error_message,
        )

    if "data" not in result:
        return None, TiantianPayloadQuality(
            accepted=False,
            has_data=False,
            status="missing_data_key",
            message=f"{endpoint_label} 返回缺少 data 字段",
            payload_type="dict",
        )

    data = result.get("data")
    if data is None:
        return None, TiantianPayloadQuality(
            accepted=True,
            has_data=False,
            status="empty_company_data",
            message=f"{endpoint_label} 返回空公司详情",
            payload_type="NoneType",
        )

    if not isinstance(data, dict):
        payload_type = _payload_type_name(data)
        return None, TiantianPayloadQuality(
            accepted=False,
            has_data=False,
            status="invalid_data_type",
            message=f"{endpoint_label} 的 data 类型异常: {payload_type}",
            payload_type=payload_type,
        )

    if not data:
        return None, TiantianPayloadQuality(
            accepted=True,
            has_data=False,
            status="empty_company_data",
            message=f"{endpoint_label} 返回空公司详情",
            payload_type="dict",
        )

    return data, TiantianPayloadQuality(
        accepted=True,
        has_data=True,
        status="ok",
        message=f"{endpoint_label} 公司详情 payload 可消费",
        payload_type="dict",
    )


def inspect_tiantian_search_payload(
    result: Any,
    *,
    endpoint_label: str = "/fundSearch",
    data_key: str = "Datas",
    success_field: Optional[str] = None,
    error_code_field: Optional[str] = "ErrCode",
    error_message_fields: Optional[List[str]] = None,
) -> Tuple[List[Dict[str, Any]], TiantianPayloadQuality]:
    """检查搜索类接口返回是否可安全消费。"""
    error_message_fields = list(error_message_fields or [])

    if result is None:
        return [], TiantianPayloadQuality(
            accepted=False,
            has_data=False,
            status="empty_response",
            message=f"{endpoint_label} 返回空响应",
            payload_type="NoneType",
        )

    if _is_structured_failure_result(result):
        return [], _build_structured_failure_quality(result, endpoint_label=endpoint_label)

    if isinstance(result, list):
        if not result:
            return [], TiantianPayloadQuality(
                accepted=True,
                has_data=False,
                status="empty_search_records",
                message=f"{endpoint_label} 返回空搜索记录",
                payload_type="list",
                record_count=0,
            )
        return result, TiantianPayloadQuality(
            accepted=True,
            has_data=True,
            status="ok",
            message=f"{endpoint_label} 搜索 payload 可消费",
            payload_type="list",
            record_count=len(result),
        )

    if not isinstance(result, dict):
        payload_type = _payload_type_name(result)
        return [], TiantianPayloadQuality(
            accepted=False,
            has_data=False,
            status="invalid_root_type",
            message=f"{endpoint_label} 返回根结构异常: {payload_type}",
            payload_type=payload_type,
        )

    if success_field:
        success_value = result.get(success_field)
        if success_value is False:
            error_message = "unknown"
            for field in error_message_fields:
                candidate = str(result.get(field) or "").strip()
                if candidate:
                    error_message = candidate
                    break
            status = "upstream_business_degraded" if _is_retryable_upstream_busy(None, error_message) else "upstream_business_failure"
            return [], TiantianPayloadQuality(
                accepted=False,
                has_data=False,
                status=status,
                message=f"{endpoint_label} 上游业务失败: success_field={success_field} message={error_message}",
                payload_type="dict",
                upstream_business_message=error_message,
            )

    if error_code_field:
        raw_error_code = result.get(error_code_field)
        normalized_error_code = str(raw_error_code).strip() if raw_error_code not in (None, "") else None
        if normalized_error_code not in (None, "0"):
            error_message = "unknown"
            for field in error_message_fields:
                candidate = str(result.get(field) or "").strip()
                if candidate:
                    error_message = candidate
                    break
            status = (
                "upstream_business_degraded"
                if _is_retryable_upstream_busy(normalized_error_code, error_message)
                else "upstream_business_failure"
            )
            return [], TiantianPayloadQuality(
                accepted=False,
                has_data=False,
                status=status,
                message=f"{endpoint_label} 上游业务失败: {error_code_field}={normalized_error_code} message={error_message}",
                payload_type="dict",
                upstream_business_code=normalized_error_code,
                upstream_business_message=error_message,
            )

    if data_key not in result:
        return [], TiantianPayloadQuality(
            accepted=False,
            has_data=False,
            status="missing_data_key",
            message=f"{endpoint_label} 返回缺少 {data_key} 字段",
            payload_type="dict",
        )

    datas = result.get(data_key)
    if not isinstance(datas, list):
        payload_type = _payload_type_name(datas)
        return [], TiantianPayloadQuality(
            accepted=False,
            has_data=False,
            status="invalid_data_type",
            message=f"{endpoint_label} 的 {data_key} 类型异常: {payload_type}",
            payload_type=payload_type,
        )

    if not datas:
        return [], TiantianPayloadQuality(
            accepted=True,
            has_data=False,
            status="empty_search_records",
            message=f"{endpoint_label} 返回空搜索记录",
            payload_type="list",
            record_count=0,
        )

    return datas, TiantianPayloadQuality(
        accepted=True,
        has_data=True,
        status="ok",
        message=f"{endpoint_label} 搜索 payload 可消费",
        payload_type="list",
        record_count=len(datas),
    )


def inspect_tiantian_nav_history_payload(
    result: Any,
    *,
    endpoint_label: str = "/fundMNHisNetList",
) -> Tuple[List[Dict[str, Any]], TiantianPayloadQuality]:
    """检查历史净值接口返回是否可安全消费。"""
    datas, envelope_quality = _inspect_common_datas_envelope(result, endpoint_label=endpoint_label)
    if not envelope_quality.accepted:
        return [], envelope_quality

    if isinstance(datas, list):
        records = datas
    elif isinstance(datas, dict):
        if "LSJZList" not in datas:
            return [], TiantianPayloadQuality(
                accepted=False,
                has_data=False,
                status="missing_lsjz_list",
                message=f"{endpoint_label} 的 Datas 缺少 LSJZList 字段",
                payload_type="dict",
            )
        records = datas.get("LSJZList") or []
        if not isinstance(records, list):
            payload_type = _payload_type_name(records)
            return [], TiantianPayloadQuality(
                accepted=False,
                has_data=False,
                status="invalid_lsjz_list_type",
                message=f"{endpoint_label} 的 LSJZList 类型异常: {payload_type}",
                payload_type=payload_type,
            )
    else:
        payload_type = _payload_type_name(datas)
        return [], TiantianPayloadQuality(
            accepted=False,
            has_data=False,
            status="invalid_datas_type",
            message=f"{endpoint_label} 的 Datas 类型异常: {payload_type}",
            payload_type=payload_type,
        )

    if not records:
        return [], TiantianPayloadQuality(
            accepted=True,
            has_data=False,
            status="empty_nav_records",
            message=f"{endpoint_label} 返回空净值记录",
            payload_type=_payload_type_name(datas),
            record_count=0,
        )

    return records, TiantianPayloadQuality(
        accepted=True,
        has_data=True,
        status="ok",
        message=f"{endpoint_label} 净值 payload 可消费",
        payload_type=_payload_type_name(datas),
        record_count=len(records),
    )


def inspect_tiantian_valuation_payload(
    result: Any,
    *,
    endpoint_label: str = "/fundVarietieValuationDetail",
) -> Tuple[Optional[Dict[str, Any]], TiantianPayloadQuality]:
    """检查估算涨跌幅接口返回是否可安全消费。"""
    if result is None:
        return None, TiantianPayloadQuality(
            accepted=False,
            has_data=False,
            status="empty_response",
            message=f"{endpoint_label} 返回空响应",
            payload_type="NoneType",
        )

    if _is_structured_failure_result(result):
        return None, _build_structured_failure_quality(result, endpoint_label=endpoint_label)

    if not isinstance(result, dict):
        payload_type = _payload_type_name(result)
        return None, TiantianPayloadQuality(
            accepted=False,
            has_data=False,
            status="invalid_root_type",
            message=f"{endpoint_label} 返回根结构异常: {payload_type}",
            payload_type=payload_type,
        )

    if result.get("Success") is False:
        err_code = result.get("ErrCode")
        err_msg = str(result.get("ErrMsg") or "").strip() or "unknown"
        status = "upstream_business_degraded" if _is_retryable_upstream_busy(err_code, err_msg) else "upstream_business_failure"
        return None, TiantianPayloadQuality(
            accepted=False,
            has_data=False,
            status=status,
            message=f"{endpoint_label} 上游业务失败: ErrCode={err_code} ErrMsg={err_msg}",
            payload_type="dict",
            upstream_business_code=str(err_code).strip() if err_code not in (None, "") else None,
            upstream_business_message=err_msg,
        )

    has_expansion_key = "Expansion" in result or "expansion" in result
    if not has_expansion_key:
        return None, TiantianPayloadQuality(
            accepted=False,
            has_data=False,
            status="missing_expansion_key",
            message=f"{endpoint_label} 返回缺少 Expansion 字段",
            payload_type="dict",
        )

    expansion = result.get("Expansion")
    if expansion is None and "expansion" in result:
        expansion = result.get("expansion")
    if expansion is None:
        return None, TiantianPayloadQuality(
            accepted=True,
            has_data=False,
            status="empty_estimate",
            message=f"{endpoint_label} 返回空估算数据",
            payload_type="NoneType",
        )

    if not isinstance(expansion, dict):
        payload_type = _payload_type_name(expansion)
        return None, TiantianPayloadQuality(
            accepted=False,
            has_data=False,
            status="invalid_expansion_type",
            message=f"{endpoint_label} 的 Expansion 类型异常: {payload_type}",
            payload_type=payload_type,
        )

    if not expansion or not _has_meaningful_scalar(expansion.get("GSZZL")):
        return None, TiantianPayloadQuality(
            accepted=True,
            has_data=False,
            status="empty_estimate",
            message=f"{endpoint_label} 返回空估算数据",
            payload_type="dict",
        )

    return expansion, TiantianPayloadQuality(
        accepted=True,
        has_data=True,
        status="ok",
        message=f"{endpoint_label} 估算 payload 可消费",
        payload_type="dict",
    )
