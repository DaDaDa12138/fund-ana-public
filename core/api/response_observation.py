"""Bind rank-page transport evidence to its unchanged parsed business payload."""
from copy import deepcopy
from datetime import datetime
import hashlib
import json
from typing import Any
from zoneinfo import ZoneInfo


def payload_sha256(payload: Any) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True,
                     separators=(",", ":"), allow_nan=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


class ObservedRankResponse(dict):
    """Metadata is an attribute, never injected into upstream business fields."""

    def __init__(self, payload: dict, *, body: bytes, request_id: str,
                 http_status: int, proxy_request_id: str | None = None):
        super().__init__(payload)
        try:
            parsed_digest = payload_sha256(payload)
        except (TypeError, ValueError):
            # Observation must not change the existing payload acceptance or
            # missing-value policy. Unverifiable parsed values stay unbound.
            parsed_digest = None
        self._observation = {
            "status": "observed", "endpoint": "/fundMNRank",
            "observed_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(),
            "http_status": http_status, "request_id": request_id,
            "proxy_request_id": proxy_request_id,
            "response_body_bytes": len(body),
            "response_body_sha256": hashlib.sha256(body).hexdigest(),
            "parsed_payload_sha256": parsed_digest,
            "body_scope": "HTTP entity after content decoding, before character decoding",
        }

    def transport_observation(self) -> dict[str, Any]:
        if self._observation["parsed_payload_sha256"] is None:
            return {"status": "parsed_payload_not_canonical_json", "response_body_sha256": None}
        try:
            unchanged = payload_sha256(self) == self._observation["parsed_payload_sha256"]
        except (TypeError, ValueError):
            unchanged = False
        if not unchanged:
            return {"status": "parsed_payload_changed", "response_body_sha256": None}
        return deepcopy(self._observation)


def rank_response_observation(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, ObservedRankResponse):
        return {"status": "transport_not_observed", "response_body_sha256": None}
    return payload.transport_observation()
