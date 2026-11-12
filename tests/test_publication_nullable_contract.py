from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "core"))

from scripts.audit_data_truthfulness import (
    _summarize_frontend_cache_basis_contract,
    _validate_live_publication_registry_summary,
)


def test_nullable_required_key_distinguishes_null_from_an_absent_key():
    for basis, expected_missing in [({"count": None}, []), ({"count": 0}, []), ({"other": 1}, ["count"])]:
        summary, blockers = _summarize_frontend_cache_basis_contract(
            basis, artifact_key="test", required_keys=("count",), expected_run_id="",
            nullable_keys=("count",),
        )
        assert summary["missing_required_keys"] == expected_missing
        assert bool(blockers) == bool(expected_missing)
    _, blockers = _summarize_frontend_cache_basis_contract(
        {"count": None}, artifact_key="test", required_keys=("count",), expected_run_id="",
    )
    assert blockers  # Null is only accepted for explicitly nullable fields.


@pytest.mark.parametrize("value", [None, 0, 4])
def test_publication_audit_retains_unavailable_and_real_zero_counts(value):
    basis = {"archived_superseded_run_count": value}
    summary, blockers = _validate_live_publication_registry_summary(
        {"archived_superseded_run_count": value, "source_basis": basis, "cache_basis": dict(basis)},
        expected_publication={},
    )
    assert summary["archived_superseded_run_count"] == value
    assert not any("archived_count" in error or "archived_superseded_run_count" in error for error in blockers)


@pytest.mark.parametrize("value", [-1, True, "0", 1.5])
def test_publication_audit_rejects_invalid_count_types(value):
    _, blockers = _validate_live_publication_registry_summary(
        {"archived_superseded_run_count": value}, expected_publication={},
    )
    assert "publication_summary_archived_count_invalid" in blockers


def test_publication_audit_blocks_zero_masquerading_as_unknown():
    basis = {"archived_superseded_run_count": 0}
    _, blockers = _validate_live_publication_registry_summary(
        {"archived_superseded_run_count": None, "source_basis": basis, "cache_basis": dict(basis)},
        expected_publication={},
    )
    assert "publication_summary_source_basis_archived_count_mismatch" in blockers


def test_publication_audit_still_requires_count_field_presence():
    _, blockers = _validate_live_publication_registry_summary({}, expected_publication={})
    assert "publication_summary_archived_count_field_missing" in blockers
