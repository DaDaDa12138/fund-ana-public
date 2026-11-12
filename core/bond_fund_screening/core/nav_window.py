"""Pure NAV period selection shared by screening and auxiliary disclosures."""
from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def select_nav_period_window(
    nav_data: pd.DataFrame | None,
    *,
    period_days: int,
    window_end_date: Any,
    boundary_lookback_days: int,
) -> tuple[pd.DataFrame | None, dict[str, Any]]:
    """Select (start, end] plus the latest observation at or before start.

    Query lookback is only context for finding the starting NAV. Earlier
    observations never enter the calculation. Invalid dates remain intact so
    callers can reject the input instead of silently dropping observations.
    """
    metadata = {
        'raw_nav_count': 0 if nav_data is None else int(len(nav_data)),
        'query_start_boundary_lookback_days': boundary_lookback_days,
        'requested_period_start': None,
        'requested_period_end': None,
        'start_boundary_point_date': None,
        'start_boundary_predecessor_used': False,
    }
    if nav_data is None or nav_data.empty or window_end_date is None:
        return nav_data, metadata
    window_end = pd.Timestamp(window_end_date).normalize()
    window_start = window_end - pd.Timedelta(days=period_days)
    metadata['requested_period_start'] = window_start.date().isoformat()
    metadata['requested_period_end'] = window_end.date().isoformat()
    if 'nav_date' not in nav_data.columns:
        return nav_data, metadata
    parsed_dates = pd.to_datetime(nav_data['nav_date'], errors='coerce')
    if parsed_dates.isna().any():
        return nav_data, metadata
    eligible_mask = parsed_dates <= window_end
    in_window_positions = np.flatnonzero(
        (eligible_mask & (parsed_dates > window_start)).to_numpy()
    ).tolist()
    boundary_positions = np.flatnonzero(
        (eligible_mask & (parsed_dates <= window_start)).to_numpy()
    )
    selected_positions = []
    if len(boundary_positions) > 0:
        boundary_position = max(
            (int(position) for position in boundary_positions),
            key=lambda position: parsed_dates.iloc[position],
        )
        boundary_date = parsed_dates.iloc[boundary_position]
        selected_positions.append(boundary_position)
        metadata['start_boundary_point_date'] = boundary_date.date().isoformat()
        metadata['start_boundary_predecessor_used'] = bool(boundary_date < window_start)
    selected_positions.extend(in_window_positions)
    if not selected_positions:
        return nav_data.iloc[0:0].copy(), metadata
    selected_positions.sort(key=lambda position: (parsed_dates.iloc[position], position))
    return nav_data.iloc[selected_positions].copy(), metadata
