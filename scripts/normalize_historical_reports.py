#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
VENV_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"
if VENV_PYTHON.exists() and Path(sys.executable) != VENV_PYTHON:
    os.execv(str(VENV_PYTHON), [str(VENV_PYTHON), *sys.argv])

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.state_normalizer import (
    rewrite_normalized_json_file,
    rewrite_normalized_text_file,
)


def _iter_targets() -> list[Path]:
    targets: list[Path] = []
    for base in [PROJECT_ROOT / "reports" / "archive", PROJECT_ROOT / "reports" / "runs"]:
        if not base.exists():
            continue
        targets.extend(sorted(base.rglob("*.json")))
        targets.extend(sorted(base.rglob("*.md")))
    return targets


def main() -> int:
    changed = 0
    for path in _iter_targets():
        if path.suffix.lower() == ".json":
            updated = rewrite_normalized_json_file(path)
        else:
            updated = rewrite_normalized_text_file(path)
        if updated:
            changed += 1
            print(f"normalized: {path}")

    print(f"normalized_files={changed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
