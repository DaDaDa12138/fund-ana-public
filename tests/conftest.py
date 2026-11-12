"""Refuse unsafe collection before importing modules that bootstrap a database."""
from pathlib import Path
import sys

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.isolation_support import IsolationError, assert_isolated_context

try:
    assert_isolated_context(PROJECT_ROOT)
except IsolationError as exc:
    raise pytest.UsageError(str(exc)) from exc
