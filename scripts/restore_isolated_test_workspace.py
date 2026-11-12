#!/usr/bin/env python3
"""Restore archived sources or a full failed-test workspace to a new directory."""
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from scripts.test_artifact_storage import restore_workspace
from scripts.test_source_storage import MANIFEST_NAME as SOURCE_MANIFEST, restore_sources


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--target", type=Path, required=True, help="New directory; existing targets are refused")
    args = parser.parse_args()
    restore = restore_sources if args.manifest.name == SOURCE_MANIFEST else restore_workspace
    print(json.dumps(restore(args.manifest, args.target), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
