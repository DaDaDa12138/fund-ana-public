#!/usr/bin/env python3
"""Preview or apply a lossless migration of completed plain task audits.

The apply step requires the exact preview, takes the workflow lock, verifies
every original SHA again, and never writes task SQL, state or publication data.
New task audits already use gzip; this command handles the historical backlog.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import gzip
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "core")]

from backend.config import load_settings
from backend.legacy_task_audit_compression import (
    AUDIT_SCOPES, compress_legacy_task_audit, plan_legacy_task_audit_compression, restore_legacy_task_audit,
)
from backend.storage import WorkflowStore
from backend.task_audit_storage import read_task_audit


def save(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = (json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode()
    stored = gzip.compress(raw, mtime=0) if path.name.endswith(".gz") else raw
    with path.open("xb") as stream:
        stream.write(stored)
        stream.flush()
        os.fsync(stream.fileno())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--max-files", type=int, default=20000)
    parser.add_argument("--scope", choices=tuple(AUDIT_SCOPES),
                        help="Preview one historical namespace; apply uses the scope bound in --plan")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--restore", type=Path, help="Original .json path to reconstruct")
    parser.add_argument("--destination", type=Path, help="New independent restoration file")
    args = parser.parse_args()
    if args.restore:
        if not args.destination or args.apply or args.plan:
            parser.error("--restore requires --destination and cannot be combined with --apply/--plan")
        print(json.dumps(restore_legacy_task_audit(args.restore, args.destination), ensure_ascii=False))
        return 0
    if args.destination or (args.apply and not args.plan) or (args.plan and not args.apply):
        parser.error("--apply requires --plan; --destination requires --restore")
    settings = load_settings(args.config)
    output = args.output or (settings.reports_dir / "audit/legacy_task_audit_compression"
                            / f"migration_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.json.gz")
    if output.exists():
        parser.error("output already exists")
    if not args.apply:
        result = plan_legacy_task_audit_compression(settings.project_root, max_files=args.max_files,
                    scope=args.scope or "legacy_scheduler_task_audit_json")
    else:
        plan = read_task_audit(args.plan)
        candidates = plan.get("candidates")
        if (plan.get("schema_version") != 1 or plan.get("scope") not in AUDIT_SCOPES
                or (args.scope is not None and args.scope != plan["scope"])
                or plan.get("mode") != "dry-run" or not isinstance(candidates, list)
                or len(candidates) > 20000 or len({item["path"] for item in candidates}) != len(candidates)):
            parser.error("invalid migration plan")
        expected_prefix = ("reports", ".audit", AUDIT_SCOPES[plan["scope"]])
        if any(Path(item["path"]).parts[:3] != expected_prefix
               or len(Path(item["path"]).parts) != 5 or ".." in Path(item["path"]).parts for item in candidates):
            parser.error("migration candidate outside declared scope")
        result = {"schema_version": 1, "scope": plan["scope"], "mode": "apply",
                  "started_at": datetime.now().astimezone().isoformat(), "plan_path": str(args.plan),
                  "planned_count": len(candidates), "files": [], "net_reclaimed_disk_bytes": None}
        store = WorkflowStore(settings.state_file)
        try:
            with store._lock:
                store._acquire_run_lock_locked("compress_legacy_task_audits")
            for index, candidate in enumerate(candidates, 1):
                item = compress_legacy_task_audit(settings.project_root, candidate)
                result["files"].append(item)
                if item["status"] == "failed":
                    # Preserve all remaining originals; resume from a fresh
                    # preview after the specific failure has been resolved.
                    break
                if index % 500 == 0:
                    print(json.dumps({"processed": index, "planned": len(candidates)}), flush=True)
        finally:
            store.close()
            rows = result["files"]
            result.update(finished_at=datetime.now().astimezone().isoformat(),
                          removed_count=sum(row["removed"] for row in rows),
                          compressed_count=sum(row["status"] == "compressed" for row in rows),
                          retained_count=sum(row["status"] == "retained" for row in rows),
                          failed_count=sum(row["status"] == "failed" for row in rows),
                          unprocessed_count=len(candidates) - len(rows))
            result["estimated_net_file_allocation_reduction_bytes"] = (
                sum(row.get("estimated_net_file_allocation_reduction_bytes") or 0 for row in rows)
                if result["failed_count"] == 0 else None)
            save(output, result)
    if not args.apply:
        save(output, result)
    print(json.dumps({key: value for key, value in result.items()
                      if key not in {"files", "candidates", "retained"}} | {"artifact": str(output)}, ensure_ascii=False))
    return 1 if result.get("failed_count") or result.get("unprocessed_count") else 0


if __name__ == "__main__":
    raise SystemExit(main())
