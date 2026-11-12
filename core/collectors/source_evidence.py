"""Small structured source-evidence helpers for collector API calls."""

from __future__ import annotations

import threading
from datetime import date, datetime
from typing import Any

from utils import json_dumps, safe_int


DEFAULT_SAMPLE_LIMIT = 5


class ApiSourceEvidence:
    """Thread-safe non-business evidence for one upstream collection source."""

    def __init__(
        self,
        *,
        task: str,
        source: str,
        sample_limit: int = DEFAULT_SAMPLE_LIMIT,
    ) -> None:
        self.task = str(task or "").strip()
        self.source = str(source or "").strip()
        self.sample_limit = max(1, int(sample_limit or DEFAULT_SAMPLE_LIMIT))
        self._lock = threading.Lock()
        self._payload = self._new_payload()

    def _new_payload(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "task": self.task,
            "source": self.source,
            "target_count": 0,
            "request_count": 0,
            "response_count": 0,
            "accepted_response_count": 0,
            "failed_response_count": 0,
            "has_data_response_count": 0,
            "empty_response_count": 0,
            "transport_exception_count": 0,
            "quality_status_counts": {},
            "quality_payload_type_counts": {},
            "returned_record_count": 0,
            "saved_fund_count": 0,
            "written_record_count": 0,
            "archive_reused_record_count": 0,
            "archive_conflict_record_count": 0,
            "archive_lookup_error_count": 0,
            "save_failed_count": 0,
            "failure_samples": [],
            "empty_response_samples": [],
            "save_failure_samples": [],
        }

    def reset(self, **context: Any) -> None:
        payload = self._new_payload()
        for key, value in context.items():
            payload[key] = self._normalize_value(value)
        with self._lock:
            self._payload = payload

    def update_context(self, **context: Any) -> None:
        with self._lock:
            for key, value in context.items():
                self._payload[key] = self._normalize_value(value)

    def record_request(self) -> None:
        with self._lock:
            self._payload["request_count"] = safe_int(self._payload.get("request_count"), 0) + 1

    def record_transport_exception(self, *, fcode: str, exc: Exception) -> None:
        with self._lock:
            self._payload["transport_exception_count"] = safe_int(
                self._payload.get("transport_exception_count"),
                0,
            ) + 1
            self._payload["failed_response_count"] = safe_int(
                self._payload.get("failed_response_count"),
                0,
            ) + 1
            self._increment_count(self._payload.setdefault("quality_status_counts", {}), "transport_exception")
            self._append_sample(
                self._payload.setdefault("failure_samples", []),
                {
                    "fcode": str(fcode or "").strip(),
                    "status": "transport_exception",
                    "message": str(exc)[:300],
                },
            )

    def record_response_quality(self, *, fcode: str, quality: Any, records: list[dict[str, Any]]) -> None:
        status = str(getattr(quality, "status", "") or "unknown").strip() or "unknown"
        payload_type = str(getattr(quality, "payload_type", "") or "unknown").strip() or "unknown"
        accepted = bool(getattr(quality, "accepted", False))
        has_data = bool(getattr(quality, "has_data", False))
        record_count = len(records or [])
        sample = {
            "fcode": str(fcode or "").strip(),
            "status": status,
            "message": str(getattr(quality, "message", "") or "").strip()[:300],
        }

        with self._lock:
            self._payload["response_count"] = safe_int(self._payload.get("response_count"), 0) + 1
            if accepted:
                self._payload["accepted_response_count"] = safe_int(
                    self._payload.get("accepted_response_count"),
                    0,
                ) + 1
            else:
                self._payload["failed_response_count"] = safe_int(
                    self._payload.get("failed_response_count"),
                    0,
                ) + 1
            if has_data:
                self._payload["has_data_response_count"] = safe_int(
                    self._payload.get("has_data_response_count"),
                    0,
                ) + 1
            else:
                self._payload["empty_response_count"] = safe_int(
                    self._payload.get("empty_response_count"),
                    0,
                ) + 1
            self._payload["returned_record_count"] = safe_int(
                self._payload.get("returned_record_count"),
                0,
            ) + record_count
            self._increment_count(self._payload.setdefault("quality_status_counts", {}), status)
            self._increment_count(self._payload.setdefault("quality_payload_type_counts", {}), payload_type)
            if not accepted:
                self._append_sample(self._payload.setdefault("failure_samples", []), sample)
            elif not has_data:
                self._append_sample(self._payload.setdefault("empty_response_samples", []), sample)

    def record_saved_records(self, *, fcode: str, record_count: int) -> None:
        with self._lock:
            self._payload["saved_fund_count"] = safe_int(self._payload.get("saved_fund_count"), 0) + 1
            self._payload["written_record_count"] = safe_int(
                self._payload.get("written_record_count"),
                0,
            ) + safe_int(record_count, 0)

    def record_archive_storage_decision(
        self,
        *,
        reused_record_count: int = 0,
        conflict_record_count: int = 0,
        lookup_error: bool = False,
    ) -> None:
        """Record archive-aware storage routing without changing business values."""
        with self._lock:
            self._payload["archive_reused_record_count"] = safe_int(
                self._payload.get("archive_reused_record_count"),
                0,
            ) + safe_int(reused_record_count, 0)
            self._payload["archive_conflict_record_count"] = safe_int(
                self._payload.get("archive_conflict_record_count"),
                0,
            ) + safe_int(conflict_record_count, 0)
            if lookup_error:
                self._payload["archive_lookup_error_count"] = safe_int(
                    self._payload.get("archive_lookup_error_count"),
                    0,
                ) + 1

    def record_save_failure(self, *, fcode: str, message: str) -> None:
        with self._lock:
            self._payload["save_failed_count"] = safe_int(self._payload.get("save_failed_count"), 0) + 1
            self._append_sample(
                self._payload.setdefault("save_failure_samples", []),
                {
                    "fcode": str(fcode or "").strip(),
                    "status": "save_exception",
                    "message": str(message or "")[:300],
                },
            )

    def snapshot(self, **overrides: Any) -> dict[str, Any]:
        with self._lock:
            payload = dict(self._payload)
            for key in ("quality_status_counts", "quality_payload_type_counts"):
                payload[key] = dict(payload.get(key) or {})
            for key in ("failure_samples", "empty_response_samples", "save_failure_samples"):
                payload[key] = list(payload.get(key) or [])
        for key, value in overrides.items():
            payload[key] = self._normalize_value(value)
        return payload

    def snapshot_json(self, **overrides: Any) -> str:
        return json_dumps(self.snapshot(**overrides))

    @staticmethod
    def _normalize_value(value: Any) -> Any:
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        return value

    @staticmethod
    def _increment_count(mapping: dict[str, int], key: Any) -> None:
        normalized_key = str(key or "unknown").strip() or "unknown"
        mapping[normalized_key] = safe_int(mapping.get(normalized_key), 0) + 1

    def _append_sample(self, samples: list[dict[str, Any]], sample: dict[str, Any]) -> None:
        if len(samples) >= self.sample_limit:
            return
        samples.append(sample)
