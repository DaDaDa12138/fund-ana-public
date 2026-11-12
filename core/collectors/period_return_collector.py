"""Bounded, resumable refresh of dated period returns for active bond NAVs."""
from __future__ import annotations

from datetime import datetime
import hashlib
import json
import time
from typing import Any

from collectors.fund_collector import FundCollector
from database.models import Fund, FundReturns
from utils.fund_type_filters import build_bond_filter
from utils.holding_period_policy import HOLDING_PERIOD_LIMIT, combine_holding_period_names, holding_period_gate_reason
from utils.period_return_snapshot import (
    RETURN_FIELDS, RETURN_PROVENANCE_FIELDS, has_dated_period_snapshot,
    select_return_update, snapshot_date, local_observation_time, period_collection_blocking_count,
    build_period_snapshot,
    period_number,
    RETURN_REJECTION_FIELDS, INITIALIZATION_QUARANTINE,
    initialization_placeholder, verified_initialization_quarantine,
)


class PeriodReturnCollector(FundCollector):
    """Use rank snapshots first; query only missing, misdated or stale rows.

    Attempt state is committed before HTTP, so interruption cannot create an
    unbounded retry loop. Success means the complete dated snapshot committed.
    A batch's completion does not assert that the remaining queue is empty.
    """

    def __init__(self, config=None, db_manager=None, api_client=None):
        super().__init__(config=config, db_manager=db_manager, api_client=api_client)
        options = self.config.get("collection", {}).get("period_return", {})
        self.max_targets = max(1, min(500, int(options.get("max_targets_per_run", 200))))
        self.refresh_hours = max(1, int(options.get("refresh_stale_hours", 24)))
        self.failure_hours = max(1, int(options.get("failure_retry_hours", 12)))
        self.max_nav_age_days = max(1, int(options.get("max_nav_age_days", 14)))
        self.last_run_summary: dict[str, Any] = {}
        self.response_observations: list[dict[str, Any]] = []

    @staticmethod
    def _unavailable_without_returns(record: dict) -> bool:
        # Only an accepted, explicitly empty upstream response for a row
        # that has never supplied period values can be non-blocking. A
        # timeout, invalid response or failed refresh of old values cannot.
        return record.get("period_return_last_status") == "missing" and all(
            record.get(field) is None for field in (*RETURN_FIELDS, *RETURN_PROVENANCE_FIELDS)
        )

    @staticmethod
    def _quarantined_date_lag(record: dict) -> bool:
        nav_date = snapshot_date(record.get("latest_nav_date"))
        return bool(
            record.get("period_return_last_status") == "date_mismatch_quarantined"
            and nav_date is not None and has_dated_period_snapshot(record, verify_payload=True)
            and snapshot_date(record["period_return_date"]) < nav_date
        )

    @staticmethod
    def _verified_current_rank_snapshot(record: dict) -> bool:
        """Reproject the newer stored response before isolating an old refresh.

        A digest alone proves the payload was retained, not that the database
        values still correspond to it. This exemption needs the rank response's
        identity, NAV, date and all ten return fields to match the current row.
        """
        if (record.get("period_return_source") != "fundMNRank"
                or not has_dated_period_snapshot(record, verify_payload=True)
                or not any(record.get(field) is not None for field in RETURN_FIELDS)):
            return False
        nav_date = snapshot_date(record.get("latest_nav_date"))
        observed_at = local_observation_time(record.get("period_return_observed_at"))
        if (nav_date is None or nav_date > datetime.now().date()
                or snapshot_date(record.get("period_return_date")) != nav_date
                or observed_at is None or observed_at > datetime.now()):
            return False
        rank_fields = ("SYL_Z", "SYL_Y", "SYL_3Y", "SYL_6Y", "SYL_1N",
                       "SYL_2N", "SYL_3N", "SYL_5N", "SYL_JN", "SYL_LN")
        try:
            payload = json.loads(record["period_return_source_json"])
            return bool(
                isinstance(payload, dict) and payload.get("FCODE") == record.get("fcode")
                and snapshot_date(payload.get("FSRQ")) == nav_date
                and period_number(payload.get("DWJZ")) is not None
                and period_number(payload["DWJZ"]) > 0
                and period_number(payload["DWJZ"]) == record.get("latest_nav")
                and all(source in payload and period_number(payload[source]) == record.get(field)
                        for field, source in zip(RETURN_FIELDS, rank_fields))
            )
        except (TypeError, ValueError, KeyError):
            return False

    @classmethod
    def _superseded_refresh_rejected(cls, record: dict) -> bool:
        return bool(record.get("period_return_last_status") == "superseded_refresh_rejected"
                    and cls._verified_current_rank_snapshot(record))

    def _rejected_dated_snapshot(self, fcode: str) -> dict | None:
        """A valid lagging response can prove isolation; invalid dates cannot."""
        response = self._last_period_return_response
        try:
            return build_period_snapshot(
                self._map_period_increase_returns(response["Datas"]),
                as_of_date=response["Expansion"]["TIME"], observed_at=datetime.now(),
                source="fundMNPeriodIncrease", source_payload={"fcode": fcode, "response": response},
            )
        except (KeyError, TypeError, ValueError):
            return None

    def _select_targets(self, *, now: datetime, fcodes: list[str] | None = None,
                        force_refresh: bool = False) -> tuple[list[dict], dict]:
        with self.db_manager.get_session(readonly=True) as session:
            query = (session.query(
                Fund.fcode, Fund.holding_days, Fund.has_holding_period,
                Fund.shortname, Fund.fullname, Fund.holding_period_source,
                Fund.holding_keywords, FundReturns,
            )
                     .outerjoin(FundReturns, FundReturns.fcode == Fund.fcode)
                     .filter(build_bond_filter()))
            if fcodes is not None:
                query = query.filter(Fund.fcode.in_(fcodes))
            rows = query.all()
        counts = {"universe_count": len(rows), "fresh_count": 0, "cooldown_count": 0,
                  "no_current_nav_count": 0, "long_holding_count": 0, "eligible_count": 0,
                  "cooldown_outcomes": []}
        eligible = []
        for fcode, holding_days, has_holding_period, shortname, fullname, holding_source, holding_keywords, returns in rows:
            record = ({column.name: getattr(returns, column.name) for column in FundReturns.__table__.columns}
                      if returns is not None else {"fcode": fcode})
            nav_date = snapshot_date(record.get("latest_nav_date"))
            if nav_date is None or not 0 <= (now.date() - nav_date).days <= self.max_nav_age_days:
                counts["no_current_nav_count"] += 1
                continue
            holding_reason = holding_period_gate_reason(
                has_holding_period=has_holding_period, holding_days=holding_days,
                fund_name=combine_holding_period_names(shortname, fullname),
                holding_period_source=holding_source, holding_keywords=holding_keywords,
            )
            if holding_reason == HOLDING_PERIOD_LIMIT and fcodes is None:
                counts["long_holding_count"] += 1
                continue
            observed_at = local_observation_time(record.get("period_return_observed_at"))
            dated = has_dated_period_snapshot(record, verify_payload=True)
            if (not force_refresh and not any(record.get(field) for field in RETURN_REJECTION_FIELDS)
                    and dated and snapshot_date(record.get("period_return_date")) == nav_date
                    and observed_at is not None and 0 <= (now - observed_at).total_seconds() < self.refresh_hours * 3600):
                counts["fresh_count"] += 1
                continue
            attempted_at = local_observation_time(record.get("period_return_attempted_at"))
            if (not force_refresh and attempted_at is not None and record.get("period_return_last_status") != "ok"
                    and (now - attempted_at).total_seconds() < self.failure_hours * 3600):
                counts["cooldown_count"] += 1
                counts["cooldown_outcomes"].append({
                    "fcode": fcode, "status": record.get("period_return_last_status"),
                    "unavailable_without_returns": self._unavailable_without_returns(record),
                    "quarantined_date_lag": self._quarantined_date_lag(record),
                    "superseded_refresh_rejected": self._superseded_refresh_rejected(record),
                    "initialization_placeholder_quarantined": (
                        record.get("period_return_last_status") == INITIALIZATION_QUARANTINE
                        and verified_initialization_quarantine(record)),
                    "rejection_sha256": record.get("period_return_rejection_sha256"),
                })
                continue
            record["needs_date_repair"] = not dated or snapshot_date(record.get("period_return_date")) != nav_date
            eligible.append(record)
        # Never-attempted rows come first. After a failure, the persisted
        # attempt time rotates that fund behind the other eligible rows.
        eligible.sort(key=lambda row: (
            not row["needs_date_repair"],
            str(row.get("period_return_attempted_at") or ""),
            str(row.get("period_return_observed_at") or ""), row["fcode"],
        ))
        counts["eligible_count"] = len(eligible)
        counts["unselected_count"] = max(0, len(eligible) - self.max_targets)
        return eligible[:self.max_targets], counts

    def _record_attempt(self, fcode: str, attempted_at: datetime, status: str,
                        *, rejected_snapshot: dict | None = None, expected_nav_date=None,
                        expected_nav_value=None) -> dict:
        with self.db_manager.get_session() as session:
            row = session.query(FundReturns).filter_by(fcode=fcode).one()
            record = {field: getattr(row, field) for field in
                      (*RETURN_FIELDS, *RETURN_PROVENANCE_FIELDS, *RETURN_REJECTION_FIELDS,
                       "fcode", "latest_nav", "latest_nav_date")}
            fund = session.get(Fund, fcode)
            if (status == "date_mismatch"
                    and snapshot_date(expected_nav_date) == snapshot_date(record.get("latest_nav_date"))
                    and expected_nav_value == record.get("latest_nav")
                    and has_dated_period_snapshot(record, verify_payload=True)
                    and snapshot_date(record.get("period_return_date")) == snapshot_date(record.get("latest_nav_date"))
                    and snapshot_date(record.get("latest_nav_date")) <= attempted_at.date()
                    and fund is not None
                    and initialization_placeholder(self._last_period_return_response,
                        estab_date=fund.estab_date, nav_date=record.get("latest_nav_date"))):
                proof = {"contract": "period-initialization-rejection-v1", "fcode": fcode,
                         "estab_date": str(fund.estab_date), "nav_date": str(row.latest_nav_date),
                         "nav_value": row.latest_nav, "observed_at": attempted_at.isoformat(),
                         "accepted_source_sha256": row.period_return_source_sha256,
                         "response": self._last_period_return_response}
                encoded = json.dumps(proof, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
                if len(encoded.encode()) <= 65536:
                    record["period_return_rejection_json"] = encoded
                    record["period_return_rejection_sha256"] = hashlib.sha256(encoded.encode()).hexdigest()
                    if verified_initialization_quarantine(record):
                        status = INITIALIZATION_QUARANTINE
            # Exempt only a valid response that exactly corroborates all ten
            # stored returns and their older date. Conflicting values, an
            # unproven old row, a future date or an HTTP failure still block.
            if (status == "date_mismatch" and rejected_snapshot
                    and has_dated_period_snapshot(record, verify_payload=True)
                    and has_dated_period_snapshot(rejected_snapshot, verify_payload=True)
                    and snapshot_date(record.get("latest_nav_date")) is not None
                    and snapshot_date(rejected_snapshot["period_return_date"]) == snapshot_date(record["period_return_date"])
                    and snapshot_date(record["period_return_date"]) < snapshot_date(record["latest_nav_date"])
                    and all(record[field] == rejected_snapshot[field] for field in RETURN_FIELDS)):
                status = "date_mismatch_quarantined"
            elif (status == "date_mismatch" and rejected_snapshot
                    and snapshot_date(expected_nav_date) == snapshot_date(record.get("latest_nav_date"))
                    and expected_nav_value == record.get("latest_nav")
                    and self._verified_current_rank_snapshot(record)
                    and has_dated_period_snapshot(rejected_snapshot, verify_payload=True)
                    and snapshot_date(rejected_snapshot["period_return_date"]) < snapshot_date(record["period_return_date"])):
                # Keep the failed refresh and its raw response, but do not
                # invalidate a verified newer rank snapshot or overwrite it.
                status = "superseded_refresh_rejected"
            session.query(FundReturns).filter_by(fcode=fcode).update({
                FundReturns.period_return_attempted_at: attempted_at,
                FundReturns.period_return_last_status: status,
                FundReturns.period_return_rejection_json: record["period_return_rejection_json"],
                FundReturns.period_return_rejection_sha256: record["period_return_rejection_sha256"],
                # Override SQLAlchemy's onupdate clock with the current
                # database value. A retry is not a new business observation.
                FundReturns.last_updated_at: FundReturns.last_updated_at,
            }, synchronize_session=False)
            record["period_return_last_status"] = status
            result = {"status": status,
                      "unavailable_without_returns": self._unavailable_without_returns(record),
                      "quarantined_date_lag": self._quarantined_date_lag(record)}
            result["superseded_refresh_rejected"] = self._superseded_refresh_rejected(record)
            result["initialization_placeholder_quarantined"] = verified_initialization_quarantine(record)
            result["rejection_sha256"] = record.get("period_return_rejection_sha256")
            # A new retry can be in progress while old rejection evidence is
            # retained. It must not claim the exemption under another status.
            result["initialization_placeholder_quarantined"] &= status == INITIALIZATION_QUARANTINE
        return result

    def _save_period(self, fcode: str, snapshot: dict, *, expected_nav_date, attempted_at: datetime) -> bool:
        with self.db_manager.get_session() as session:
            row = session.query(FundReturns).filter_by(fcode=fcode).one()
            if row.latest_nav_date != expected_nav_date:
                raise ValueError("nav_changed_during_period_request")
            previous = {column.name: getattr(row, column.name) for column in FundReturns.__table__.columns}
            update = select_return_update(previous, snapshot)
            if "period_return_date" not in update:
                raise ValueError("period_response_older_than_stored_snapshot")
            changed = any(previous.get(field) != update.get(field)
                          for field in (*RETURN_FIELDS, "period_return_date"))
            # A transition from unproven to dated is a business-input change,
            # even when all numeric returns happen to be identical.
            changed = changed or not has_dated_period_snapshot(previous, verify_payload=True)
            for field in (*RETURN_FIELDS, *RETURN_PROVENANCE_FIELDS):
                setattr(row, field, update[field])
            for field in RETURN_REJECTION_FIELDS:
                setattr(row, field, None)
            row.period_return_attempted_at = attempted_at
            row.period_return_last_status = "ok"
            row.last_updated_at = datetime.now()
        return changed

    def collect_current(self, *, fcodes: list[str] | None = None, force_refresh: bool = False) -> dict[str, Any]:
        if force_refresh and not fcodes:
            raise ValueError("forced_period_refresh_requires_explicit_fcodes")
        self.response_observations = []
        now = datetime.now()
        targets, counts = self._select_targets(now=now, fcodes=fcodes, force_refresh=force_refresh)
        summary = {
            "schema_version": 2, "task": "collect-period-returns", "source": "fundMNPeriodIncrease",
            "started_at": now.isoformat(), **counts, "target_count": len(targets),
            "request_count": 0, "saved_fund_count": 0, "failed_count": 0,
            "changed_fcodes": [], "outcomes": [], "max_targets_per_run": self.max_targets,
            "failure_retry_hours": self.failure_hours, "refresh_stale_hours": self.refresh_hours,
            "max_nav_age_days": self.max_nav_age_days,
            "request_count_semantics": "logical_client_calls; transport_retries_not_counted",
            "http_attempt_count": None,
            "forced_refresh": bool(force_refresh),
            "scope": "current_bond_NAVs; scheduled_supplement_excludes_holding_days_over_90",
        }
        self.last_run_summary = summary
        print(f"区间收益补采: 目标={len(targets)} 新鲜={counts['fresh_count']} "
              f"冷却={counts['cooldown_count']} 剩余={counts['unselected_count']}", flush=True)
        last_progress = time.monotonic()
        for index, target in enumerate(targets, 1):
            code = target["fcode"]
            attempted_at = datetime.now()
            outcome = {"fcode": code, "expected_nav_date": str(target["latest_nav_date"]),
                       "attempted_at": attempted_at.isoformat(), "status": "attempt_record_failed"}
            try:
                self._record_attempt(code, attempted_at, "in_progress")
            except Exception as exc:
                outcome["error"] = str(exc)
                summary["failed_count"] += 1
                summary["outcomes"].append(outcome)
                continue  # No HTTP without durable retry ownership.
            summary["request_count"] += 1
            try:
                snapshot, stats = self._fetch_period_increase_returns(
                    code, expected_nav_date=target["latest_nav_date"],
                )
                self.response_observations.append({
                    "fcode": code, "requested_at": attempted_at.isoformat(),
                    "expected_nav_date": str(target["latest_nav_date"]),
                    "response": self._last_period_return_response, "validation": stats,
                })
                if not snapshot:
                    status = "date_mismatch" if stats["date_mismatch"] else "missing" if stats["missing"] else "request_failed"
                    state = self._record_attempt(code, attempted_at, status,
                        rejected_snapshot=self._rejected_dated_snapshot(code) if status == "date_mismatch" else None,
                        expected_nav_date=target["latest_nav_date"], expected_nav_value=target.get("latest_nav"))
                    outcome.update(state)
                    summary["failed_count"] += 1
                else:
                    changed = self._save_period(code, snapshot, expected_nav_date=target["latest_nav_date"],
                                                attempted_at=attempted_at)
                    outcome.update(status="ok", period_return_date=str(snapshot["period_return_date"]),
                                   source_sha256=snapshot["period_return_source_sha256"], changed=changed)
                    summary["saved_fund_count"] += 1
                    if changed:
                        summary["changed_fcodes"].append(code)
            except Exception as exc:
                summary["failed_count"] += 1
                outcome.update(status="write_or_validation_failed", error=str(exc))
                try:
                    self._record_attempt(code, attempted_at, outcome["status"])
                except Exception as state_exc:
                    outcome["status_write_error"] = str(state_exc)
            summary["outcomes"].append(outcome)
            if index == len(targets) or index % 10 == 0 or time.monotonic() - last_progress >= 30:
                print(f"区间收益补采进度: {index}/{len(targets)} 已落库={summary['saved_fund_count']} "
                      f"失败={summary['failed_count']}", flush=True)
                last_progress = time.monotonic()
        summary.update(
            finished_at=datetime.now().isoformat(),
            status="degraded" if summary["failed_count"] or counts["cooldown_count"] else "batch_complete" if targets else "skipped",
            changed_count=len(summary["changed_fcodes"]),
            pending_count=counts["cooldown_count"] + counts["unselected_count"] + summary["failed_count"],
        )
        summary["nonblocking_missing_fcodes"] = sorted(
            item["fcode"] for item in [*summary["outcomes"], *summary["cooldown_outcomes"]]
            if item.get("unavailable_without_returns") is True
        )
        summary["quarantined_date_lag_fcodes"] = sorted(
            item["fcode"] for item in [*summary["outcomes"], *summary["cooldown_outcomes"]]
            if item.get("quarantined_date_lag") is True
        )
        summary["superseded_refresh_fcodes"] = sorted(
            item["fcode"] for item in [*summary["outcomes"], *summary["cooldown_outcomes"]]
            if item.get("superseded_refresh_rejected") is True
        )
        summary["initialization_placeholder_fcodes"] = sorted(
            item["fcode"] for item in [*summary["outcomes"], *summary["cooldown_outcomes"]]
            if item.get("initialization_placeholder_quarantined") is True
        )
        summary["blocking_count"] = period_collection_blocking_count(summary)
        summary["screening_blocked"] = summary["blocking_count"] > 0
        return summary
