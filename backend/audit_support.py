from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
from typing import Any

import pandas as pd

from backend.publication import load_publication_registry
from backend.task_audit_storage import is_maintenance_audit_path, read_task_audit


def read_json_file(path: Path | str) -> dict[str, Any]:
    current = Path(path)
    try:
        if current.name.endswith((".json.gz", ".json.legacy.gz")) or is_maintenance_audit_path(current):
            return read_task_audit(current)
        payload = json.loads(current.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def resolve_latest_run_payload(settings) -> tuple[str, dict[str, Any]]:
    latest_payload = read_json_file(Path(settings.reports_dir) / "latest.json")
    latest_run_id = str(latest_payload.get("run_id") or "").strip()
    if latest_run_id:
        return latest_run_id, latest_payload

    publication = load_publication_registry(Path(settings.reports_dir) / "publication_registry.json")
    latest_run_id = str(publication.get("latest_canonical_run_id") or "").strip()
    if not latest_run_id:
        raise RuntimeError("无法解析最新 canonical run_id。")
    run_report = Path(settings.runs_dir) / latest_run_id / f"bond_analysis_report_{latest_run_id}.json"
    return latest_run_id, read_json_file(run_report)


def build_audit_output_dir(settings, *parts: str) -> Path:
    output_dir = Path(settings.reports_dir) / ".audit"
    for part in parts:
        if str(part or "").strip():
            output_dir = output_dir / str(part).strip()
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def write_audit_payload(path: Path | str, payload: dict[str, Any]) -> Path:
    current = Path(path)
    current.parent.mkdir(parents=True, exist_ok=True)
    current.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return current


def write_audit_csv(path: Path | str, df: pd.DataFrame) -> Path:
    current = Path(path)
    current.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(current, index=False, encoding="utf-8-sig")
    return current


def write_compressed_audit_csv(path: Path | str, df: pd.DataFrame) -> dict[str, Any]:
    """Write complete CSV directly to gzip, verifying it before publication."""
    current = Path(path)
    if not current.name.endswith(".csv.gz"):
        raise ValueError("compressed_audit_csv_suffix_required")
    current.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=current.parent, prefix=f".{current.name}.", suffix=".tmp", delete=False,
        ) as raw:
            temporary = Path(raw.name)
            with gzip.GzipFile(filename="", fileobj=raw, mode="wb", compresslevel=6, mtime=0) as compressed:
                with io.TextIOWrapper(compressed, encoding="utf-8-sig", newline="") as text:
                    df.to_csv(text, index=False)
            raw.flush()
            os.fsync(raw.fileno())
        raw_digest = hashlib.sha256()
        raw_bytes = 0
        with gzip.open(temporary, "rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                raw_digest.update(chunk)
                raw_bytes += len(chunk)
        stored_digest = hashlib.sha256()
        with temporary.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                stored_digest.update(chunk)
        metadata = {
            "format": "csv_gzip", "encoding": "utf-8-sig", "rows": len(df),
            "columns": len(df.columns), "sha256": stored_digest.hexdigest(),
            "bytes": temporary.stat().st_size, "uncompressed_bytes": raw_bytes,
            "uncompressed_sha256": raw_digest.hexdigest(),
        }
        # link is atomic and refuses to replace an existing evidence file.
        os.link(temporary, current)
        return metadata
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def write_audit_markdown(path: Path | str, content: str) -> Path:
    current = Path(path)
    current.parent.mkdir(parents=True, exist_ok=True)
    current.write_text(str(content or ""), encoding="utf-8")
    return current
