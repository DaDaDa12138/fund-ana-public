from __future__ import annotations

from datetime import datetime

from bond_fund_screening.tasks.runtime_env import screening_report_date


def test_screening_report_date_prefers_workflow_batch_date(monkeypatch):
    monkeypatch.setenv("FUND_ANA_REPORT_DATE", "20261017")

    assert screening_report_date(now=datetime(2026, 10, 18, 0, 5)) == "20261017"


def test_screening_report_date_uses_local_date_for_direct_invocation(monkeypatch):
    monkeypatch.delenv("FUND_ANA_REPORT_DATE", raising=False)

    assert screening_report_date(now=datetime(2026, 10, 18, 0, 5)) == "20261018"
