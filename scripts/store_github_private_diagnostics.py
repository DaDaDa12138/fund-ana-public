#!/usr/bin/env python3
"""Store allowlisted, credential-redacted Action logs only in private Releases."""
from __future__ import annotations

import base64
from datetime import datetime
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from backend.github_cloud_state import Remote
from backend.github_storage import source_repository, state_repository
from backend.shanghai_time import SHANGHAI

ALLOWED = ("restore", "history", "production", "runtime-api", "state", "report-sync", "failure-sync")


def archive_logs(root: Path, output: Path, secrets: list[str]) -> int:
    redactions = set(value for value in secrets if value and len(value) >= 8)
    redactions.update(base64.b64encode(("x-access-token:" + value).encode()).decode()
                      for value in list(redactions))
    paths = [root / ".cloud-private-logs" / (name + ".log") for name in ALLOWED]
    paths += [root / "cloud-export/check.json", root / "cloud-export/state-receipt.json",
              root / "cloud-failure/check.json", root / "logs/github-hosted-task.log"]
    count = 0
    with tarfile.open(output, "w:gz") as archive:
        for path in paths:
            if not path.is_file() or path.is_symlink():
                continue
            data = path.read_text(errors="replace")
            for value in sorted(redactions, key=len, reverse=True):
                data = data.replace(value, "[REDACTED]")
            encoded = data.encode()
            item = tarfile.TarInfo(path.relative_to(root).as_posix())
            item.size = len(encoded)
            item.mode = 0o600
            archive.addfile(item, io.BytesIO(encoded))
            count += 1
    return count


def main() -> int:
    try:
        run, attempt = os.environ["GITHUB_RUN_ID"], os.environ["GITHUB_RUN_ATTEMPT"]
        if not run.isdigit() or not attempt.isdigit():
            raise RuntimeError("diagnostic_identity_invalid")
        remote = Remote(state_repository())  # Enforces private visibility.
        tag = "cloud-action-diagnostics-" + datetime.now(SHANGHAI).strftime("%Y%m")
        remote.release(tag)
        source = source_repository().replace("/", "-")
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / f"{source}-{run}-{attempt}.tar.gz"
            archive_logs(ROOT, output, [os.environ.get("GH_TOKEN", ""), os.environ.get("GITHUB_TOKEN", "")])
            remote.gh(["release", "upload", tag, str(output), "--repo", remote.repository])
        print("Private diagnostics saved; no runtime data uploaded to the public repository.")
        return 0
    except Exception:
        print("::error::Private diagnostic persistence failed; inspect private repository access.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
