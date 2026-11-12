#!/usr/bin/env python3
"""Fail before restoration unless production uses separate private storage."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from backend.github_storage import source_repository, state_repository


def validate() -> None:
    source, storage = source_repository(), state_repository()
    if source.lower() == storage.lower():
        raise RuntimeError("execution_and_storage_must_be_separate")
    if not os.environ.get("GH_TOKEN"):
        raise RuntimeError("private_storage_token_missing")
    if os.environ.get("GITHUB_EVENT_NAME") not in {"schedule", "workflow_dispatch"}:
        raise RuntimeError("production_event_not_trusted")
    if os.environ.get("GITHUB_REF") != "refs/heads/main":
        raise RuntimeError("production_ref_not_trusted")
    result = subprocess.run(["gh", "api", "repos/" + storage], capture_output=True,
                            text=True, timeout=30)
    if result.returncode:
        raise RuntimeError("private_storage_authentication_failed")
    info = json.loads(result.stdout)
    if info.get("private") is not True or info.get("full_name", "").lower() != storage.lower():
        raise RuntimeError("private_storage_identity_invalid")
    if info.get("archived") or info.get("disabled"):
        raise RuntimeError("private_storage_unavailable")


def main() -> int:
    try:
        validate()
    except (RuntimeError, ValueError, OSError, subprocess.TimeoutExpired) as exc:
        # API output, tokens and private metadata never reach the public log.
        code = str(exc) if isinstance(exc, RuntimeError) else "private_storage_preflight_failed"
        print("::error::" + code)
        return 1
    print("Separate private storage authentication and identity verified.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
