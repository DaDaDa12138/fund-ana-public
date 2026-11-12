"""Small, content-bound verification records separate from immutable reports."""
from __future__ import annotations
from functools import lru_cache
import hashlib
import json
import os
from pathlib import Path
from typing import Any
from uuid import uuid4

REQUIRED_BASELINE_CHECKS = {
    "complete_scoring_universe", "style_thresholds", "global_thresholds", "style_meta",
    "candidate_decisions_and_reasons", "post_review_members_scores_reasons",
    "formal_members_and_scores", "dynamic_members_and_scores", "all_final_selection_rejection_reasons",
}
REQUIRED_BASELINE_SOURCES = {"report_json", "filtered_csv", "dynamic_filtered_csv"}
REPORT_BASELINE_SCOPE = "report_selection_from_complete_scored_universe"


@lru_cache(maxsize=256)
def _content_hash(path: str, identity: tuple[int, int, int, int]) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def content_hash(path: Path) -> str:
    if not path.exists():
        from backend.task_audit_storage import read_original_artifact_bytes

        return hashlib.sha256(read_original_artifact_bytes(path)).hexdigest()
    stat = path.stat()
    return _content_hash(str(path), (stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns))


def evidence_path(root: Path, snapshot_hash: str) -> Path:
    return root / "reports/audit/screening_baseline_replay/verified_inputs" / f"{snapshot_hash}.json"


def write_evidence(root: Path, summary: dict[str, Any]) -> None:
    if not summary.get("snapshot_sha256"):
        return
    target = evidence_path(root, summary["snapshot_sha256"])
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            json.dump(summary, stream, ensure_ascii=False, indent=2, allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(target)
    finally:
        temporary.unlink(missing_ok=True)


def read_evidence(root: Path, snapshot: Path) -> dict[str, Any]:
    try:
        signature = content_hash(snapshot)
        path = evidence_path(root, signature)
        if not path.exists():
            return {"status": "not_verified", "baseline_verified": False}
        payload = json.loads(path.read_text())
        if not isinstance(payload, dict):
            raise ValueError("replay_evidence_invalid_payload")
        if payload.get("snapshot_sha256") != signature:
            raise ValueError("replay_evidence_snapshot_mismatch")
        sources = payload.get("source_files")
        if not isinstance(sources, dict) or not REQUIRED_BASELINE_SOURCES.issubset(sources):
            raise ValueError("replay_evidence_required_sources_missing")
        if payload.get("scope") != REPORT_BASELINE_SCOPE:
            raise ValueError("replay_evidence_scope_mismatch")
        for source in sources.values():
            if not isinstance(source, dict):
                raise ValueError("replay_evidence_invalid_source")
            artifact = root / source["path"]
            if not artifact.resolve().is_relative_to(root.resolve()) or content_hash(artifact) != source["sha256"]:
                raise ValueError("replay_evidence_source_changed")
        checks = payload.get("checks") or []
        if not isinstance(checks, list) or not all(isinstance(item, dict) for item in checks):
            raise ValueError("replay_evidence_invalid_checks")
        verified = bool(payload.get("baseline_verified") and payload.get("status") == "matched"
                        and REQUIRED_BASELINE_CHECKS.issubset({item.get("name") for item in checks})
                        and all(item.get("aligned") is True for item in checks)
                        and payload.get("later_database_reads") is False
                        and payload.get("missing_history_imputation") is False)
        return {"status": "matched" if verified else "mismatch", "baseline_verified": verified,
                "scope": payload.get("scope"), "generated_at": payload.get("generated_at"),
                "evidence_path": str(path.relative_to(root)), "snapshot_sha256": signature,
                "failed_check_count": payload.get("failed_check_count"),
                "upstream_collection_and_core_risk_recalculation_verified": False}
    except (OSError, ValueError, TypeError, KeyError) as exc:
        return {"status": "invalid_evidence", "baseline_verified": False, "reason": str(exc)}
