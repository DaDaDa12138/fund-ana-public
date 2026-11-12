#!/usr/bin/env python3
"""Validate and replay frozen workflow, collection or standalone risk journals."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "core"))

from backend.calculation_input_audit import audit_calculation_inputs


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path, help="New directory outside the immutable capture directory")
    args = parser.parse_args()
    result = audit_calculation_inputs(args.index, args.output_dir, project_root=ROOT)
    print(json.dumps({key: result[key] for key in ("run_id", "passed", "counts", "elapsed_seconds", "max_rss_kib")}, ensure_ascii=False))
    return 0 if result["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
