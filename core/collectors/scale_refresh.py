"""Bounded daily recovery of missing scales, independent of full detail refresh."""
from __future__ import annotations

from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from datetime import date, datetime, timedelta
import hashlib
import json
import math
from pathlib import Path
import tempfile

from database.models import CollectionLog, Fund
from utils import parse_date
from utils.detail_collection_summary import build_detail_collection_summary
from utils.fund_exclusion_rules import get_runtime_excluded_fund_codes
from utils.tiantian_data_quality import inspect_tiantian_detail_payload


TASK_TYPE = "采集基金详情-规模补采"


def load_missing_scale_targets(collector, target_fcodes=None) -> list[str]:
    """Apply exclusions and cooldown before the limit, including explicit targets."""
    config = collector.detail_config
    retry_hours = max(1, int(config.get("scale_refresh_retry_hours", 24)))
    limit = max(1, int(config.get("scale_refresh_max_targets_per_run", 120)))
    excluded = set(get_runtime_excluded_fund_codes())
    with collector.db_manager.get_session(readonly=True, task_name="scale_refresh_targets") as session:
        query = session.query(Fund.fcode).filter(Fund.scale_billion.is_(None))
        if target_fcodes is not None:
            codes = list(dict.fromkeys(str(code).strip() for code in target_fcodes))
            if any(len(code) != 6 or not code.isdigit() for code in codes):
                raise ValueError("invalid_explicit_scale_fcode")
            query = query.filter(Fund.fcode.in_(codes))
        cooled = session.query(CollectionLog.fcode).filter(
            CollectionLog.task_type == TASK_TYPE,
            CollectionLog.fcode.is_not(None),
            CollectionLog.created_at >= datetime.now() - timedelta(hours=retry_hours),
        )
        return [row.fcode for row in query.filter(~Fund.fcode.in_(cooled)).order_by(
            Fund.detail_updated_at.asc(), Fund.fcode.asc(),
        ).all() if row.fcode not in excluded][:limit]


def _parse_scale(data: dict) -> tuple[dict | None, str]:
    raw = data.get("ENDNAV")
    if raw is None or str(raw).strip() in ("", "--", "---", "null", "NULL"):
        return None, "upstream_scale_missing"
    try:
        scale_yuan = float(raw)
    except (TypeError, ValueError, OverflowError):
        return None, "invalid_scale_value"
    if isinstance(raw, bool) or not math.isfinite(scale_yuan) or scale_yuan <= 0:
        return None, "invalid_scale_value"
    # Prefer the authoritative report period; only absent/placeholder dates fall back.
    raw_date = data.get("FEGMRQ")
    if raw_date is None or str(raw_date).strip() in ("", "--", "---"):
        raw_date = data.get("ENDNAVDT")
    report_date = parse_date(raw_date) if raw_date else None
    if report_date is None:
        return None, "missing_or_invalid_scale_report_date"
    if report_date > date.today():
        return None, "future_scale_report_date"
    return {"scale_billion": scale_yuan / 100000000, "scale_report_date": report_date}, "scale_billion_available"


def _log(collector, fcode: str, status: str, message: str) -> None:
    # A failed cooldown write must fail the stage, rather than silently retry forever.
    with collector.db_manager.get_session(task_name="scale_refresh_log") as session:
        session.add(CollectionLog(task_type=TASK_TYPE, fcode=fcode, status=status, message=message[:1000]))


def collect_missing_scales(collector, *, workers: int, target_fcodes=None) -> int:
    codes = load_missing_scale_targets(collector, target_fcodes)
    collector.reset_api_source_evidence(
        collection_mode="scale_missing_only", target_count=len(codes),
        retry_hours=max(1, int(collector.detail_config.get("scale_refresh_retry_hours", 24))),
        max_targets_per_run=max(1, int(collector.detail_config.get("scale_refresh_max_targets_per_run", 120))),
        write_scope="scale_billion_scale_report_date_last_updated_at_only",
    )
    collector.detail_collection_summary = None
    collector.empty_detail_report_path = None
    evidence = collector._ensure_detail_api_source_evidence()
    counts = dict.fromkeys(("target_count", "fetch_ready_count", "fetch_failed_count", "unavailable_count",
                            "write_success_count", "write_failed_count"), 0)
    counts["target_count"] = len(codes)
    reasons = Counter()
    records = []
    archive = None
    if codes:
        archive = Path(tempfile.mkdtemp(prefix="scale_refresh_" + datetime.now().strftime("%Y%m%d_%H%M%S_"),
                                        dir=collector.empty_detail_report_dir))

    def fetch(code):
        evidence.record_request()
        fetched_at = datetime.now().isoformat(timespec="seconds")
        try:
            response = collector.api.get_fund_detail(code)
        except Exception as exc:
            evidence.record_transport_exception(fcode=code, exc=exc)
            return None, {"fcode": code, "fetched_at": fetched_at, "reason": "request_exception", "error": str(exc)[:500]}, True
        data, quality = inspect_tiantian_detail_payload(response, endpoint_label=f"/fundMNDetailInformation FCODE={code}")
        if quality.accepted and str(data.get("FCODE") or "").strip() != code:
            quality = replace(quality, accepted=False, status="fund_identity_mismatch", message="response FCODE missing or mismatched")
        evidence.record_response_quality(fcode=code, quality=quality, records=[data] if data else [])
        # This is serialized parsed JSON, not the original HTTP wire bytes.
        body = json.dumps(response, ensure_ascii=False, sort_keys=True, allow_nan=False).encode("utf-8")
        response_path = archive / f"{code}.response.json"
        response_path.write_bytes(body)
        record = {"fcode": code, "fetched_at": fetched_at, "source": "fundMNDetailInformation",
                  "response_file": response_path.name, "response_sha256": hashlib.sha256(body).hexdigest(),
                  "response_representation": "serialized_parsed_json", "quality_status": quality.status}
        if not quality.accepted:
            record["reason"] = quality.status
            return None, record, quality.status not in ("empty_response", "empty_datas", "invalid_datas_type")
        update, reason = _parse_scale(data)
        record["reason"] = reason
        return update, record, False

    with ThreadPoolExecutor(max_workers=max(1, int(workers))) as executor:
        futures = {executor.submit(fetch, code): code for code in codes}
        for future in as_completed(futures):
            code = futures[future]
            try:
                update, record, failed = future.result()
            except Exception as exc:
                # Parsing/archive failures cannot be counted as an unavailable business value.
                update, record, failed = None, {"fcode": code, "reason": "response_processing_failed", "error": str(exc)[:500]}, True
            if failed:
                counts["fetch_failed_count"] += 1
                _log(collector, code, "failed", record["reason"] + ":" + record.get("error", ""))
                record["outcome"] = "fetch_failed"
            elif update is None:
                counts["unavailable_count"] += 1
                _log(collector, code, "missing_scale", record["reason"])
                record["outcome"] = "unavailable"
            else:
                try:
                    with collector.db_manager.get_session(task_name="scale_refresh_write") as session:
                        written = session.query(Fund).filter(Fund.fcode == code, Fund.scale_billion.is_(None)).update(
                            {**update, "last_updated_at": datetime.now()}, synchronize_session=False)
                        session.add(CollectionLog(task_type=TASK_TYPE, fcode=code,
                                                  status="success" if written else "skipped",
                                                  message="scale_billion_available" if written else "scale_no_longer_missing"))
                    if written:
                        counts["fetch_ready_count"] += 1
                        counts["write_success_count"] += 1
                        evidence.record_saved_records(fcode=code, record_count=1)
                        record["outcome"] = "written"
                        record["scale_report_date"] = update["scale_report_date"].isoformat()
                    else:
                        counts["unavailable_count"] += 1
                        record.update(outcome="unavailable", reason="scale_no_longer_missing")
                except Exception as exc:
                    counts["fetch_ready_count"] += 1
                    counts["write_failed_count"] += 1
                    evidence.record_save_failure(fcode=code, message=str(exc))
                    _log(collector, code, "failed", "scale_write_failed:" + str(exc))
                    record.update(outcome="write_failed", reason="scale_write_failed", error=str(exc)[:500])
            records.append(record)
            reasons[record["reason"]] += 1

    summary = build_detail_collection_summary(counts)
    manifest = None
    if archive:
        manifest = archive / "manifest.json"
        manifest.write_text(json.dumps({"schema_version": 1, "source": "fundMNDetailInformation",
                            "generated_at": datetime.now().isoformat(), "summary": summary,
                            "funds": sorted(records, key=lambda item: item["fcode"])}, ensure_ascii=False, indent=2), encoding="utf-8")
    collector.detail_collection_summary = summary
    with collector._stats_lock:
        collector.stats = {"total": len(codes), "success": summary["success_count"],
                           "failed": summary["failed_count"], "skipped": summary["skipped_count"]}
    collector.update_api_source_evidence_context(
        success_count=summary["success_count"], failed_count=summary["failed_count"],
        skipped_count=summary["skipped_count"], scale_refresh_reason_counts=dict(reasons),
        scale_refresh_manifest_path=str(manifest) if manifest else None,
        scale_refresh_manifest_sha256=hashlib.sha256(manifest.read_bytes()).hexdigest() if manifest else None,
    )
    collector.print_stats()
    return summary["success_count"]
