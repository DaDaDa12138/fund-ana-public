"""Collect company profiles with bounded refreshes and explicit source outcomes."""

from __future__ import annotations

import logging
import math
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import func

from collectors.base_collector import BaseCollector
from collectors.source_evidence import ApiSourceEvidence
from database.models import CollectionLog, FundCompany, FundManager
from utils import parse_date
from utils.tiantian_data_quality import inspect_tiantian_company_payload

logger = logging.getLogger(__name__)
COMPANY_FAILURE_LOG_TYPE = "company-profile"
_MISSING_TEXT = {"", "-", "--", "---", "none", "null", "nan", "n/a"}


def _company_text(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("company text field must be a string")
    text = value.strip()
    return None if text.lower() in _MISSING_TEXT else text


def _company_count(value: Any) -> int | None:
    if value is None or (isinstance(value, str) and value.strip().lower() in _MISSING_TEXT):
        return None
    if isinstance(value, bool) or not re.fullmatch(r"[0-9]+", str(value).strip()):
        raise ValueError("company count must be a non-negative integer")
    return int(value)


def _company_scale_yi(value: Any) -> float | None:
    if value is None or (isinstance(value, str) and value.strip().lower() in _MISSING_TEXT):
        return None
    # A real numeric zero is invariant under currency-unit conversion. Other
    # values need an explicit unit; bare numbers must not be guessed as 亿元.
    if not isinstance(value, bool) and isinstance(value, (int, float)) and value == 0:
        return 0.0
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)\s*(亿元|万元|元)", str(value).strip())
    if not match:
        raise ValueError("GLGM must include a supported currency unit")
    divisor = {"亿元": Decimal(1), "万元": Decimal(10000), "元": Decimal(100000000)}[match[2]]
    scale = Decimal(match[1]) / divisor
    if not scale.is_finite() or not math.isfinite(float(scale)):
        raise ValueError("GLGM is outside the supported numeric range")
    return float(scale)


def _company_fields(data: dict[str, Any]) -> dict[str, Any]:
    name = _company_text(data.get("FDMC"))
    if not name:
        raise ValueError("companyarchives FDMC is missing")
    date_text = _company_text(data.get("CLRQ"))
    established = parse_date(date_text) if date_text else None
    if date_text and established is None:
        raise ValueError("companyarchives CLRQ is invalid")
    return {
        "company_name": name, "full_name": name,
        "legal_rep": _company_text(data.get("FRDB")),
        "ceo": _company_text(data.get("Manager")),
        "establish_date": established,
        "registered_capital": _company_text(data.get("ZCZB")),
        "total_scale": _company_scale_yi(data.get("GLGM")),
        "fund_count": _company_count(data.get("Count")),
        "manager_count": _company_count(data.get("ManagerCount")),
        "address": _company_text(data.get("BGDZ")),
        "hotline": _company_text(data.get("KFRX")),
    }


class CompanyCollector(BaseCollector):
    """Refresh profiles referenced by real manager company identifiers.

    last_updated_at is the retrieval time, not a disclosure date. The GLGM
    snapshot is never dated using the unrelated Datas2 quarterly series.
    """

    def __init__(self, config=None, db_manager=None, api_client=None):
        super().__init__(config, db_manager, api_client)
        self.company_config = self.config.get("collection", {}).get("company", {})
        self.default_workers = max(1, min(3, int(self.company_config.get("workers", 2))))
        self.refresh_stale_days = max(1, int(self.company_config.get("refresh_stale_days", 28)))
        self.max_targets_per_run = max(1, int(self.company_config.get("max_targets_per_run", 200)))
        self.failure_retry_hours = max(1, int(self.company_config.get("failure_retry_hours", 24)))
        self.last_run_summary: dict[str, Any] = {}
        self._invalid_company_ids: list[str] = []
        self._inserted_count = 0
        self._api_source_evidence = ApiSourceEvidence(task="collect-companies", source="companyApi2")

    def get_api_source_evidence(self, **overrides: Any) -> dict[str, Any]:
        return self._api_source_evidence.snapshot(**overrides)

    def get_api_source_evidence_json(self, **overrides: Any) -> str:
        return self._api_source_evidence.snapshot_json(**overrides)

    def _get_unique_company_ids(self) -> list[str]:
        with self.db_manager.get_session(readonly=True) as session:
            rows = session.query(FundManager.company_id).distinct().all()
        ids: set[str] = set()
        invalid: set[str] = set()
        for (raw,) in rows:
            text = str(raw).strip() if raw is not None else ""
            if re.fullmatch(r"[0-9]{1,20}", text) and int(text) > 0:
                ids.add(text)
            elif text:
                invalid.add(text)
        self._invalid_company_ids = sorted(invalid)
        return sorted(ids)

    def collect_all_companies(self, workers=None, force_update=False) -> int:
        workers = max(1, min(3, int(workers or self.default_workers)))
        self.reset_stats()
        self._inserted_count = 0
        self.last_run_summary = {}
        company_ids = self._get_unique_company_ids()
        now = datetime.now()
        stale_cutoff = now - timedelta(days=self.refresh_stale_days)
        retry_cutoff = now - timedelta(hours=self.failure_retry_hours)
        with self.db_manager.get_session(readonly=True) as session:
            existing = dict(session.query(FundCompany.company_id, FundCompany.last_updated_at).all())
            failures = dict(session.query(CollectionLog.fcode, func.max(CollectionLog.created_at)).filter(
                CollectionLog.task_type == COMPANY_FAILURE_LOG_TYPE,
                CollectionLog.status == "failed",
            ).group_by(CollectionLog.fcode).all())
        eligible: list[str] = []
        fresh_count = cooldown_count = 0
        for company_id in company_ids:
            updated_at = existing.get(company_id)
            if not force_update and updated_at is not None and stale_cutoff < updated_at <= now:
                fresh_count += 1
                continue
            last_failure = failures.get(company_id)
            if (not force_update and last_failure is not None and last_failure > retry_cutoff
                    and (updated_at is None or last_failure > updated_at)):
                cooldown_count += 1
                continue
            eligible.append(company_id)
        # Recent failures sort after never-attempted targets to avoid starvation.
        eligible.sort(key=lambda cid: (max(existing.get(cid) or datetime.min,
                                          failures.get(cid) or datetime.min), cid))
        targets = eligible[:self.max_targets_per_run]
        context = {
            "stage": "company_profiles", "action": "companyarchives",
            "target_entity": "company",
            "target_source": "fund_managers.company_id", "source_company_count": len(company_ids),
            "target_count": len(targets), "eligible_count": len(eligible),
            "remaining_eligible_count": len(eligible) - len(targets),
            "fresh_skipped_count": fresh_count, "cooldown_count": cooldown_count,
            "invalid_company_id_count": len(self._invalid_company_ids),
            "invalid_company_id_samples": self._invalid_company_ids[:5],
            "refresh_stale_days": self.refresh_stale_days,
            "failure_retry_hours": self.failure_retry_hours,
            "max_targets_per_run": self.max_targets_per_run, "workers": workers,
            "force_update": bool(force_update),
            "request_count_semantics": "logical_client_calls; client_transport_retries_not_counted",
            "http_attempt_count": None,
            "updated_at_semantics": "successful_retrieval_time_not_disclosure_date",
        }
        self._api_source_evidence.reset(**context)
        # The CLI configures its own named logger. This module's INFO logger
        # need not reach stdout, which the subprocess idle guard monitors.
        print(f"公司资料刷新: 有效来源={len(company_ids)} 目标={len(targets)} "
              f"新鲜跳过={fresh_count} 冷却={cooldown_count} "
              f"余量={len(eligible) - len(targets)} 无效ID={len(self._invalid_company_ids)}", flush=True)
        last_progress = time.monotonic()
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(self.collect_company_detail, cid): cid for cid in targets}
            for done, future in enumerate(as_completed(futures), start=1):
                try:
                    success = bool(future.result())
                except Exception as exc:
                    success = False
                    self._api_source_evidence.record_save_failure(fcode=futures[future], message=str(exc))
                    self._record_failure(futures[future], "collector_exception", str(exc))
                # Every selected target is attempted; False is a failure.
                self.update_stats(success=success)
                if done == len(targets) or done % 10 == 0 or time.monotonic() - last_progress >= 30:
                    print(f"公司资料刷新进度: {done}/{len(targets)} "
                          f"成功={self.stats['success']} 失败={self.stats['failed']}", flush=True)
                    last_progress = time.monotonic()
        stats = self.get_stats()
        self.last_run_summary = {
            **context, "success_count": stats["success"], "failed_count": stats["failed"],
            "skipped_count": stats["skipped"], "inserted_count": self._inserted_count,
            "updated_count": stats["success"] - self._inserted_count,
            "status": "degraded" if stats["failed"] or cooldown_count else "success" if targets else "skipped",
            "skip_reason": (None if targets else "no_valid_company_ids" if not company_ids
                            else "fresh_or_retry_cooldown"),
        }
        self._api_source_evidence.update_context(**self.last_run_summary)
        self.print_stats()
        return stats["success"]

    def _record_failure(self, company_id: str, status: str, message: str) -> None:
        self.record_recent_outcome(company_id, success=False, status=status, message=message)
        logger.warning("公司%s资料刷新失败: %s: %s", company_id, status, message)
        self.log_collection(task_type=COMPANY_FAILURE_LOG_TYPE, fcode=company_id,
                            status="failed", message=f"{status}: {message}"[:500])

    def collect_company_detail(self, company_id: str) -> bool:
        evidence = self._api_source_evidence
        if not re.fullmatch(r"[0-9]{1,20}", str(company_id)) or int(company_id) <= 0:
            self.record_recent_outcome(str(company_id), success=False, status="invalid_company_id")
            return False
        evidence.record_request()
        try:
            detail = self.api.get_company_info(company_id, action="companyarchives")
        except Exception as exc:
            evidence.record_transport_exception(fcode=company_id, exc=exc)
            self._record_failure(company_id, "transport_exception", str(exc))
            return False
        payload, quality = inspect_tiantian_company_payload(
            detail, endpoint_label=f"/companyApi2 action=companyarchives cc={company_id}",
        )
        fields = None
        if quality.accepted and quality.has_data:
            try:
                fields = _company_fields(payload)
            except (TypeError, ValueError) as exc:
                quality = replace(quality, accepted=False, has_data=False,
                                  status="invalid_company_fields", message=str(exc))
        evidence.record_response_quality(fcode=company_id, quality=quality,
                                         records=[payload] if quality.has_data else [])
        if fields is None:
            self._record_failure(company_id, quality.status, quality.message)
            return False
        return self._save_company_fields(company_id, fields)

    def _save_company_fields(self, company_id: str, fields: dict[str, Any]) -> bool:
        try:
            with self.db_manager.get_session() as session:
                company = session.query(FundCompany).filter_by(company_id=company_id).first()
                inserted = company is None
                if inserted:
                    # Required fields precede every flush/commit.
                    company = FundCompany(company_id=company_id, **fields)
                    session.add(company)
                else:
                    # Unavailable values in a new snapshot stay NULL. Retaining
                    # an old field would silently renew its retrieval timestamp.
                    for key, value in fields.items():
                        setattr(company, key, value)
                company.last_updated_at = datetime.now()
            self._api_source_evidence.record_saved_records(fcode=company_id, record_count=1)
            if inserted:
                with self._stats_lock:
                    self._inserted_count += 1
            self.record_recent_outcome(company_id, success=True, status="ok")
            return True
        except Exception as exc:
            self._api_source_evidence.record_save_failure(fcode=company_id, message=str(exc))
            self._record_failure(company_id, "save_exception", str(exc))
            return False
