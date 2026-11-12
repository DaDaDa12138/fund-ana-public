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

from backend.state_normalizer import rewrite_normalized_json_file


def main() -> int:
    targets = [
        PROJECT_ROOT / "backend" / "data" / "workflow_state.json",
        PROJECT_ROOT / "reports" / "latest.json",
        PROJECT_ROOT / "reports" / "publication_registry.json",
    ]

    changed = 0
    for path in targets:
        if rewrite_normalized_json_file(path):
            changed += 1
            print(f"normalized: {path}")
        else:
            print(f"unchanged: {path}")

    print(f"normalized_files={changed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
