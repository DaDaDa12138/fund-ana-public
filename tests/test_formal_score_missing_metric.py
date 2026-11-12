from __future__ import annotations

import pandas as pd
import pytest

from backend.report_builder import PROJECT_SCORE_COMPONENTS, _weighted_project_component


def test_optional_missing_three_year_return_is_renormalized_but_required_metrics_stay_required():
    components = PROJECT_SCORE_COMPONENTS["long_term"]
    frame = pd.DataFrame({
        metric: [None if metric == "return_3y" else 50.0]
        for metric, _weight, _direction in components
    }, index=["000001"])

    scores, coverage = _weighted_project_component(frame, components)

    assert scores.loc["000001"] == pytest.approx(50.0)
    assert coverage.loc["000001"] == pytest.approx(0.94)

    frame.loc["000001", "calmar_ratio"] = None
    scores, _coverage = _weighted_project_component(frame, components)
    assert pd.isna(scores.loc["000001"])
