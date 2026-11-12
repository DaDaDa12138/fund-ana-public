"""Repository identities for public execution and private operational storage."""
from __future__ import annotations

import os
import re

LEGACY_REPOSITORY = "DaDaDa12138/fund-ana"


def repository_name(value: str) -> str:
    if (not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", value)
            or any(part in {".", ".."} for part in value.split("/"))):
        raise ValueError("github_repository_invalid")
    return value


def state_repository() -> str:
    return repository_name(os.environ.get("FUND_ANA_STATE_REPOSITORY", LEGACY_REPOSITORY))


def source_repository() -> str:
    return repository_name(os.environ.get("GITHUB_REPOSITORY", LEGACY_REPOSITORY))
