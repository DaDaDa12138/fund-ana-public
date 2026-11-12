#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.config import load_settings  # noqa: E402
from backend import report_builder as report_builder_module  # noqa: E402
from database.rank_history_queries import build_latest_rank_dates_sql  # noqa: E402
from scripts.audit_runtime_utils import build_audit_artifact_path as _build_audit_artifact_path  # noqa: E402


@dataclass(frozen=True)
class QuerySpec:
    key: str
    label: str
    sql: str
    params: tuple[Any, ...] = ()
    expected_index_keywords: tuple[str, ...] = ()
    profile_role: str = "default_path"


_ANNUAL_METRICS_WINDOW_3Y = tuple(
    report_builder_module._current_complete_calendar_year_window(
        report_builder_module.LONG_TERM_STABILITY_MIN_YEARS_3
    )
)
_ANNUAL_METRICS_WINDOW_5Y = tuple(
    report_builder_module._current_complete_calendar_year_window(
        report_builder_module.LONG_TERM_STABILITY_MIN_YEARS_5
    )
)
_ANNUAL_METRICS_PARAMS = (
    *_ANNUAL_METRICS_WINDOW_3Y,
    *_ANNUAL_METRICS_WINDOW_3Y,
    *_ANNUAL_METRICS_WINDOW_3Y,
    *_ANNUAL_METRICS_WINDOW_3Y,
    *_ANNUAL_METRICS_WINDOW_5Y,
    *_ANNUAL_METRICS_WINDOW_5Y,
    *_ANNUAL_METRICS_WINDOW_5Y,
    *_ANNUAL_METRICS_WINDOW_5Y,
    *_ANNUAL_METRICS_WINDOW_5Y,
)


PROFILE_QUERIES: tuple[QuerySpec, ...] = (
    QuerySpec(
        key="nav_latest_by_fcode",
        label="NAV latest by fcode old GROUP BY comparison",
        sql="""
            SELECT fcode, MAX(nav_date) AS latest_nav_date
            FROM fund_nav_history
            GROUP BY fcode
        """,
        expected_index_keywords=("fund_nav_history", "INDEX"),
        profile_role="comparison_baseline",
    ),
    QuerySpec(
        key="fund_returns_nav_latest_summary",
        label="fund_returns latest NAV date summary with history verification",
        sql="""
            SELECT fcode, nav_latest_date
            FROM (
                SELECT
                    fcode,
                    history_latest_nav_date,
                    CASE
                        WHEN history_latest_nav_date IS NOT NULL
                          AND (
                              returns_latest_nav_date IS NULL
                              OR TRIM(CAST(returns_latest_nav_date AS TEXT)) = ''
                              OR history_latest_nav_date > returns_latest_nav_date
                          )
                        THEN history_latest_nav_date
                        ELSE returns_latest_nav_date
                    END AS nav_latest_date
                FROM (
                    SELECT
                        r.fcode,
                        r.latest_nav_date AS returns_latest_nav_date,
                        (
                            SELECT h.nav_date
                            FROM fund_nav_history h
                            WHERE h.fcode = r.fcode
                            ORDER BY h.nav_date DESC
                            LIMIT 1
                        ) AS history_latest_nav_date
                    FROM fund_returns r
                )
            )
            WHERE nav_latest_date IS NOT NULL
              AND TRIM(CAST(nav_latest_date AS TEXT)) <> ''
              AND history_latest_nav_date IS NOT NULL
        """,
        expected_index_keywords=("fund_nav_history", "INDEX"),
    ),
    QuerySpec(
        key="rank_latest_by_fcode",
        label="rank latest by fcode old GROUP BY comparison",
        sql="""
            SELECT fcode, MAX(rank_date) AS latest_rank_date
            FROM fund_rank_history
            GROUP BY fcode
        """,
        expected_index_keywords=("fund_rank_history", "INDEX"),
        profile_role="comparison_baseline",
    ),
    QuerySpec(
        key="rank_latest_index_seek",
        label="rank latest per-fund indexed seeks",
        sql=build_latest_rank_dates_sql(),
        expected_index_keywords=("fund_rank_history", "INDEX"),
    ),
    QuerySpec(
        key="risk_latest_1y",
        label="risk latest 1y per-fund latest-row batch",
        sql="""
            WITH sample_codes AS (
                SELECT fcode
                FROM fund_returns
                WHERE latest_nav_date IS NOT NULL
                ORDER BY fcode
                LIMIT 50
            )
            SELECT
                s.fcode,
                rm.end_date,
                rm.annual_return,
                rm.volatility,
                rm.max_drawdown,
                rm.raw_sharpe_ratio
            FROM sample_codes s
            LEFT JOIN fund_risk_metrics rm
                ON rm.rowid = (
                    SELECT rm_latest.rowid
                    FROM fund_risk_metrics rm_latest
                    WHERE rm_latest.fcode = s.fcode
                      AND rm_latest.period = ?
                    ORDER BY rm_latest.end_date DESC, rm_latest.rowid DESC
                    LIMIT 1
              )
        """,
        params=("1y",),
        expected_index_keywords=("INDEX",),
    ),
    QuerySpec(
        key="favorite_snapshot_latest_risk_1y_sample_50",
        label="favorites API latest 1y risk snapshot sample for 50 funds",
        sql="""
            WITH sample_codes AS (
                SELECT fcode
                FROM fund_returns
                WHERE latest_nav_date IS NOT NULL
                ORDER BY fcode
                LIMIT 50
            )
            SELECT
                s.fcode,
                rm.end_date,
                rm.annual_return,
                rm.volatility,
                rm.max_drawdown,
                rm.raw_sharpe_ratio
            FROM sample_codes s
            LEFT JOIN fund_risk_metrics rm
                ON rm.rowid = (
                    SELECT rm_latest.rowid
                    FROM fund_risk_metrics rm_latest
                    WHERE rm_latest.fcode = s.fcode
                      AND rm_latest.period = '1y'
                    ORDER BY rm_latest.end_date DESC, rm_latest.rowid DESC
                    LIMIT 1
                )
        """,
        expected_index_keywords=("INDEX",),
    ),
    QuerySpec(
        key="bond_mainlist_audit_latest_risk_sample_50",
        label="bond mainlist audit latest 1y/3y risk snapshot sample for 50 fixed-income funds",
        sql="""
            WITH sample_codes AS (
                SELECT fcode
                FROM funds
                WHERE type_category = 'fixed_income'
                LIMIT 50
            )
            SELECT
                s.fcode,
                rm1.end_date AS risk_1y_end_date,
                rm1.annual_return AS annual_return_1y,
                COALESCE(rm1.raw_sharpe_ratio, rm1.sharpe_ratio) AS sharpe_1y,
                rm1.max_drawdown AS mdd_1y,
                rm1.volatility AS vol_1y,
                rm3.end_date AS risk_3y_end_date,
                rm3.annual_return AS annual_return_3y,
                COALESCE(rm3.raw_sharpe_ratio, rm3.sharpe_ratio) AS sharpe_3y,
                rm3.max_drawdown AS mdd_3y,
                rm3.volatility AS vol_3y
            FROM sample_codes s
            LEFT JOIN fund_risk_metrics rm1
                ON rm1.rowid = (
                    SELECT rm_latest.rowid
                    FROM fund_risk_metrics rm_latest
                    WHERE rm_latest.fcode = s.fcode
                      AND rm_latest.period = '1y'
                    ORDER BY rm_latest.end_date DESC, rm_latest.rowid DESC
                    LIMIT 1
                )
            LEFT JOIN fund_risk_metrics rm3
                ON rm3.rowid = (
                    SELECT rm_latest.rowid
                    FROM fund_risk_metrics rm_latest
                    WHERE rm_latest.fcode = s.fcode
                      AND rm_latest.period = '3y'
                    ORDER BY rm_latest.end_date DESC, rm_latest.rowid DESC
                    LIMIT 1
                )
        """,
        expected_index_keywords=("INDEX",),
    ),
    QuerySpec(
        key="report_metadata_snapshot",
        label="report_builder fund metadata snapshot",
        sql="""
            SELECT
                fcode,
                shortname,
                fullname,
                ftype,
                sub_type,
                type_category,
                type_code,
                risk_level,
                COALESCE(has_holding_period, 0) AS has_holding_period,
                COALESCE(holding_days, 0) AS holding_days,
                holding_keywords,
                holding_period_source,
                company_name,
                manager_ids,
                manager_names,
                estab_date,
                scale_billion,
                scale_report_date,
                mgr_fee_rate,
                trust_fee_rate,
                sales_fee_rate,
                benchmark,
                investment_target,
                investment_strategy,
                COALESCE(is_buyable, 0) AS is_buyable,
                purchase_status,
                COALESCE(data_source, '') AS data_source,
                COALESCE(data_source_governance_flag, '') AS data_source_governance_flag
            FROM funds
        """,
        expected_index_keywords=("funds",),
    ),
    QuerySpec(
        key="report_return_snapshot",
        label="report_builder fund_returns snapshot",
        sql="""
            SELECT
                fcode,
                daily_growth,
                latest_nav_date,
                latest_nav,
                return_1w,
                return_1m,
                return_3m,
                return_6m,
                return_1y,
                return_2y,
                return_3y,
                return_ytd,
                return_since
            FROM fund_returns
        """,
        expected_index_keywords=("fund_returns",),
    ),
    QuerySpec(
        key="report_manager_snapshot",
        label="report_builder fund manager snapshot",
        sql="""
            SELECT
                mgr_id,
                total_days,
                score_overall
            FROM fund_managers
        """,
        expected_index_keywords=("fund_managers",),
    ),
    QuerySpec(
        key="report_long_term_annual_metrics_default",
        label="report_builder long-term 3y/5y annual return metrics default path",
        sql="""
            SELECT
                fcode,
                COUNT(DISTINCT CASE WHEN year IN (?, ?, ?) AND annual_return IS NOT NULL THEN year END) AS annual_3_count,
                SUM(CASE WHEN year IN (?, ?, ?) AND annual_return > 0 THEN 1 ELSE 0 END) AS positive_3y,
                AVG(CASE WHEN year IN (?, ?, ?) THEN annual_return END) AS annual_avg_3y,
                AVG(CASE WHEN year IN (?, ?, ?) THEN annual_return * annual_return END) AS annual_avg_sq_3y,
                COUNT(DISTINCT CASE WHEN year IN (?, ?, ?, ?, ?) AND annual_return IS NOT NULL THEN year END) AS annual_5_count,
                SUM(CASE WHEN year IN (?, ?, ?, ?, ?) AND annual_return > 0 THEN 1 ELSE 0 END) AS positive_5y,
                AVG(CASE WHEN year IN (?, ?, ?, ?, ?) THEN annual_return END) AS annual_avg_5y,
                AVG(CASE WHEN year IN (?, ?, ?, ?, ?) THEN annual_return * annual_return END) AS annual_avg_sq_5y
            FROM fund_annual_returns
            WHERE year IN (?, ?, ?, ?, ?)
            GROUP BY fcode
            ORDER BY fcode
        """,
        params=_ANNUAL_METRICS_PARAMS,
        expected_index_keywords=("fund_annual_returns", "INDEX"),
    ),
    QuerySpec(
        key="annual_return_breakdown_sample_50",
        label="tracker/favorites since-inception annual return breakdown sample for 50 funds",
        sql="""
            WITH sample_codes AS (
                SELECT fcode
                FROM fund_returns
                WHERE latest_nav_date IS NOT NULL
                ORDER BY fcode
                LIMIT 50
            )
            SELECT
                f.fcode,
                f.estab_date,
                ar.year,
                ar.annual_return,
                ar.rank_in_category,
                ar.category_count,
                ar.percentile
            FROM sample_codes s
            JOIN funds f ON f.fcode = s.fcode
            LEFT JOIN fund_annual_returns ar
                ON ar.fcode = f.fcode
            ORDER BY f.fcode, ar.year
        """,
        expected_index_keywords=("INDEX",),
    ),
    QuerySpec(
        key="fund_data_source_map_sample_50",
        label="tracker current derived data source map sample for 50 funds",
        sql="""
            WITH sample_codes AS (
                SELECT fcode
                FROM fund_returns
                WHERE latest_nav_date IS NOT NULL
                ORDER BY fcode
                LIMIT 50
            )
            SELECT
                f.fcode,
                COALESCE(f.data_source, '') AS data_source
            FROM sample_codes s
            JOIN funds f ON f.fcode = s.fcode
        """,
        expected_index_keywords=("funds", "INDEX"),
    ),
    QuerySpec(
        key="favorite_database_snapshot_sample_50",
        label="favorites score database snapshot sample for 50 funds",
        sql="""
            WITH sample_codes AS (
                SELECT fcode
                FROM fund_returns
                WHERE latest_nav_date IS NOT NULL
                ORDER BY fcode
                LIMIT 50
            )
            SELECT
                f.fcode,
                f.shortname,
                f.company_name,
                f.ftype,
                f.risk_level,
                f.estab_date,
                f.scale_billion,
                f.holding_days,
                f.holding_keywords,
                f.purchase_status,
                f.is_buyable,
                f.mgr_fee_rate,
                f.trust_fee_rate,
                f.sales_fee_rate,
                f.benchmark,
                fr.daily_growth,
                fr.latest_nav_date,
                fr.latest_nav,
                fr.return_1w,
                fr.return_1m,
                fr.return_3m,
                fr.return_6m,
                fr.return_1y,
                fr.return_2y,
                fr.return_3y,
                fr.return_ytd,
                fr.return_since,
                rm.volatility,
                rm.max_drawdown,
                rm.annual_return,
                COALESCE(rm.raw_sharpe_ratio, rm.sharpe_ratio) AS sharpe_1y,
                COALESCE(rm.raw_sortino_ratio, rm.sortino_ratio) AS sortino_1y,
                rm.calmar_ratio,
                rm.var_95,
                rm.var_99,
                rm.positive_ratio
            FROM sample_codes s
            JOIN funds f ON f.fcode = s.fcode
            LEFT JOIN fund_returns fr ON fr.fcode = f.fcode
            LEFT JOIN fund_risk_metrics rm
                ON rm.rowid = (
                    SELECT rm_latest.rowid
                    FROM fund_risk_metrics rm_latest
                    WHERE rm_latest.fcode = f.fcode
                      AND rm_latest.period = '1y'
                    ORDER BY rm_latest.end_date DESC, rm_latest.rowid DESC
                    LIMIT 1
                )
        """,
        expected_index_keywords=("INDEX",),
    ),
    QuerySpec(
        key="report_latest_accum_nav_hot",
        label="report_builder latest accum NAV old hot GROUP BY source",
        sql="""
            SELECT n.fcode, latest.nav_date, n.accum_nav
            FROM fund_nav_history n
            INNER JOIN (
                SELECT fcode, MAX(nav_date) AS nav_date
                FROM fund_nav_history
                GROUP BY fcode
            ) latest
                ON latest.fcode = n.fcode AND latest.nav_date = n.nav_date
        """,
        expected_index_keywords=("fund_nav_history", "INDEX"),
        profile_role="comparison_baseline",
    ),
    QuerySpec(
        key="report_latest_accum_nav_summary",
        label="report_builder latest accum NAV summary-assisted hot source",
        sql="""
            SELECT
                r.fcode,
                CASE
                    WHEN h_summary.nav_date IS NOT NULL
                      AND (h_latest.nav_date IS NULL OR h_summary.nav_date >= h_latest.nav_date)
                    THEN h_summary.nav_date
                    ELSE h_latest.nav_date
                END AS nav_date,
                CASE
                    WHEN h_summary.nav_date IS NOT NULL
                      AND (h_latest.nav_date IS NULL OR h_summary.nav_date >= h_latest.nav_date)
                    THEN h_summary.accum_nav
                    ELSE h_latest.accum_nav
                END AS accum_nav
            FROM fund_returns r
            LEFT JOIN fund_nav_history h_summary
                ON h_summary.fcode = r.fcode
               AND h_summary.nav_date = r.latest_nav_date
            LEFT JOIN fund_nav_history h_latest
                ON h_latest.fcode = r.fcode
               AND h_latest.nav_date = (
                    SELECT h2.nav_date
                    FROM fund_nav_history h2
                    WHERE h2.fcode = r.fcode
                    ORDER BY h2.nav_date DESC
                    LIMIT 1
               )
            WHERE h_summary.nav_date IS NOT NULL
               OR h_latest.nav_date IS NOT NULL
        """,
        expected_index_keywords=("fund_returns", "fund_nav_history", "INDEX"),
    ),
    QuerySpec(
        key="report_long_term_rank_metrics_hot",
        label="report_builder long-term 12m rank metrics hot source",
        sql="""
            WITH latest_rank AS (
                SELECT
                    fcode,
                    MAX(rank_date) AS latest_rank_date
                FROM fund_rank_history
                WHERE percentile IS NOT NULL
                GROUP BY fcode
            )
            SELECT
                fund_rank_history.fcode AS fcode,
                latest_rank.latest_rank_date AS latest_rank_date,
                COUNT(*) AS rank_count_12m,
                AVG(fund_rank_history.percentile) AS rank_avg_percentile_12m,
                AVG(fund_rank_history.percentile * fund_rank_history.percentile) AS rank_avg_sq_percentile_12m,
                SUM(CASE WHEN fund_rank_history.percentile <= 0.30 THEN 1 ELSE 0 END) AS rank_top30_count_12m
            FROM fund_rank_history
            JOIN latest_rank USING (fcode)
            WHERE fund_rank_history.percentile IS NOT NULL
              AND fund_rank_history.rank_date >= date(latest_rank.latest_rank_date, '-365 day')
            GROUP BY fund_rank_history.fcode
            ORDER BY fund_rank_history.fcode
        """,
        expected_index_keywords=("fund_rank_history", "INDEX"),
        profile_role="comparison_baseline",
    ),
    QuerySpec(
        key="screening_picks_latest_bond_1y",
        label="screening picks latest bond 1y by fcode",
        sql="""
            SELECT fcode, MAX(id) AS latest_pick_id
            FROM screening_picks
            WHERE fund_type = ?
              AND period_years = ?
            GROUP BY fcode
        """,
        params=("bond", 1),
        expected_index_keywords=("idx_pick_fund_type_period_fcode_id",),
    ),
    QuerySpec(
        key="report_pick_metric_materialized",
        label="report_builder materialized pick metric lookup",
        sql="""
            WITH latest AS (
                SELECT fcode, MAX(id) AS max_pick_id
                FROM screening_picks
                WHERE fund_type = 'bond'
                  AND period_years >= 0.9999
                  AND period_years <= 1.0001
                GROUP BY fcode
            )
            SELECT
                latest.fcode,
                latest.max_pick_id,
                m.pick_id AS metric_pick_id,
                m.max_drawdown_duration
            FROM latest
            LEFT JOIN screening_pick_metric_snapshots m
                ON m.pick_id = latest.max_pick_id
        """,
        expected_index_keywords=("idx_pick_fund_type_period_fcode_id", "screening_pick_metric_snapshots"),
    ),
    QuerySpec(
        key="favorites_nav_window_sample_50",
        label="favorites/report NAV 1y window sample for 50 funds",
        sql="""
            WITH sample_codes AS (
                SELECT fcode
                FROM fund_returns
                WHERE latest_nav_date IS NOT NULL
                ORDER BY fcode
                LIMIT 50
            )
            SELECT
                h.fcode,
                h.nav_date,
                h.daily_growth
            FROM fund_nav_history h
            JOIN sample_codes s ON s.fcode = h.fcode
            WHERE h.nav_date >= date((SELECT MAX(nav_date) FROM fund_nav_history), '-365 day')
              AND h.nav_date <= (SELECT MAX(nav_date) FROM fund_nav_history)
            ORDER BY h.fcode, h.nav_date
        """,
        expected_index_keywords=("fund_nav_history", "INDEX"),
    ),
    QuerySpec(
        key="favorite_metric_details_nav_full_history_sample_20",
        label="favorite metric details old full NAV history sample for 20 funds",
        sql="""
            WITH sample_codes AS (
                SELECT fcode
                FROM fund_returns
                WHERE latest_nav_date IS NOT NULL
                ORDER BY fcode
                LIMIT 20
            )
            SELECT
                h.fcode,
                h.nav_date,
                h.unit_nav,
                h.accum_nav
            FROM fund_nav_history h
            JOIN sample_codes s ON s.fcode = h.fcode
            ORDER BY h.fcode, h.nav_date
        """,
        expected_index_keywords=("fund_nav_history", "INDEX"),
        profile_role="comparison_baseline",
    ),
    QuerySpec(
        key="favorite_metric_details_nav_window_sample_20",
        label="favorite metric details bounded NAV window sample for 20 funds",
        sql="""
            WITH sample_codes AS (
                SELECT fcode
                FROM fund_returns
                WHERE latest_nav_date IS NOT NULL
                ORDER BY fcode
                LIMIT 20
            ),
            cutoffs AS (
                SELECT MIN(start_date) AS data_start_date
                FROM (
                    SELECT date('now', '-62 day') AS start_date
                    UNION ALL
                    SELECT date('now', 'start of year') AS start_date
                )
            ),
            windowed_nav AS (
                SELECT
                    h.fcode,
                    h.nav_date,
                    h.unit_nav,
                    h.accum_nav
                FROM fund_nav_history h
                JOIN sample_codes s ON s.fcode = h.fcode
                CROSS JOIN cutoffs c
                WHERE h.nav_date >= c.data_start_date
            ),
            baseline_nav AS (
                SELECT
                    h.fcode,
                    h.nav_date,
                    h.unit_nav,
                    h.accum_nav
                FROM sample_codes s
                JOIN fund_nav_history h
                    ON h.rowid = (
                        SELECT h2.rowid
                        FROM fund_nav_history h2
                        CROSS JOIN cutoffs c
                        WHERE h2.fcode = s.fcode
                          AND h2.nav_date < c.data_start_date
                        ORDER BY h2.nav_date DESC, h2.rowid DESC
                        LIMIT 1
                    )
            )
            SELECT
                fcode,
                nav_date,
                unit_nav,
                accum_nav
            FROM windowed_nav
            UNION ALL
            SELECT
                fcode,
                nav_date,
                unit_nav,
                accum_nav
            FROM baseline_nav
        """,
        expected_index_keywords=("fund_nav_history", "INDEX"),
    ),
    QuerySpec(
        key="since_inception_annualized_nav_full_history_sample_50",
        label="since-inception annualized return old full NAV history sample for 50 funds",
        sql="""
            WITH sample_codes AS (
                SELECT fcode
                FROM fund_returns
                WHERE latest_nav_date IS NOT NULL
                ORDER BY fcode
                LIMIT 50
            )
            SELECT
                h.fcode,
                h.nav_date,
                h.unit_nav,
                h.accum_nav
            FROM fund_nav_history h
            JOIN sample_codes s ON s.fcode = h.fcode
            ORDER BY h.fcode, h.nav_date
        """,
        expected_index_keywords=("fund_nav_history", "INDEX"),
        profile_role="comparison_baseline",
    ),
    QuerySpec(
        key="since_inception_annualized_nav_point_lookup_sample_50",
        label="since-inception annualized return first/last NAV point lookup sample for 50 funds",
        sql="""
            WITH sample_codes AS (
                SELECT fcode
                FROM fund_returns
                WHERE latest_nav_date IS NOT NULL
                ORDER BY fcode
                LIMIT 50
            ),
            first_nav AS (
                SELECT
                    s.fcode,
                    h.nav_date AS first_nav_date,
                    CASE
                        WHEN h.accum_nav IS NOT NULL AND h.accum_nav > 0 THEN h.accum_nav
                        WHEN h.unit_nav IS NOT NULL AND h.unit_nav > 0 THEN h.unit_nav
                        ELSE NULL
                    END AS first_nav_value
                FROM sample_codes s
                JOIN fund_nav_history h
                    ON h.rowid = (
                        SELECT h2.rowid
                        FROM fund_nav_history h2
                        WHERE h2.fcode = s.fcode
                          AND (
                              (h2.accum_nav IS NOT NULL AND h2.accum_nav > 0)
                              OR (h2.unit_nav IS NOT NULL AND h2.unit_nav > 0)
                          )
                        ORDER BY h2.nav_date ASC, h2.rowid ASC
                        LIMIT 1
                    )
            ),
            last_nav AS (
                SELECT
                    s.fcode,
                    h.nav_date AS last_nav_date,
                    CASE
                        WHEN h.accum_nav IS NOT NULL AND h.accum_nav > 0 THEN h.accum_nav
                        WHEN h.unit_nav IS NOT NULL AND h.unit_nav > 0 THEN h.unit_nav
                        ELSE NULL
                    END AS last_nav_value
                FROM sample_codes s
                JOIN fund_nav_history h
                    ON h.rowid = (
                        SELECT h2.rowid
                        FROM fund_nav_history h2
                        WHERE h2.fcode = s.fcode
                          AND (
                              (h2.accum_nav IS NOT NULL AND h2.accum_nav > 0)
                              OR (h2.unit_nav IS NOT NULL AND h2.unit_nav > 0)
                          )
                        ORDER BY h2.nav_date DESC, h2.rowid DESC
                        LIMIT 1
                    )
            )
            SELECT
                s.fcode,
                first_nav.first_nav_date,
                first_nav.first_nav_value,
                last_nav.last_nav_date,
                last_nav.last_nav_value
            FROM sample_codes s
            LEFT JOIN first_nav ON first_nav.fcode = s.fcode
            LEFT JOIN last_nav ON last_nav.fcode = s.fcode
        """,
        expected_index_keywords=("fund_nav_history", "INDEX"),
    ),
    QuerySpec(
        key="report_low_vol_shadow_risk_window_sample_200",
        label="report low-vol positive-ratio shadow risk window sample for 200 fixed-income funds",
        sql="""
            WITH sample_codes AS (
                SELECT f.fcode
                FROM funds f
                JOIN fund_returns r ON r.fcode = f.fcode
                WHERE f.type_category = 'fixed_income'
                  AND (
                      COALESCE(f.ftype, '') LIKE '%中短债%'
                      OR COALESCE(f.ftype, '') LIKE '%固收%'
                      OR COALESCE(f.ftype, '') LIKE '%长债%'
                  )
                  AND r.latest_nav_date IS NOT NULL
                LIMIT 200
            )
            SELECT
                rm.fcode,
                rm.start_date,
                rm.end_date
            FROM sample_codes s
            JOIN fund_risk_metrics rm
                ON rm.rowid = (
                    SELECT latest.rowid
                    FROM fund_risk_metrics latest
                    WHERE latest.fcode = s.fcode
                      AND latest.period = '1y'
                    ORDER BY COALESCE(TRIM(latest.end_date), '') DESC, latest.rowid DESC
                    LIMIT 1
                )
            WHERE rm.start_date IS NOT NULL
              AND rm.end_date IS NOT NULL
            ORDER BY rm.fcode
        """,
        expected_index_keywords=("INDEX",),
    ),
    QuerySpec(
        key="report_low_vol_shadow_nav_window_sample_200",
        label="report low-vol positive-ratio shadow NAV window sample for 200 fixed-income funds",
        sql="""
            WITH sample_codes AS (
                SELECT f.fcode
                FROM funds f
                JOIN fund_returns r ON r.fcode = f.fcode
                WHERE f.type_category = 'fixed_income'
                  AND (
                      COALESCE(f.ftype, '') LIKE '%中短债%'
                      OR COALESCE(f.ftype, '') LIKE '%固收%'
                      OR COALESCE(f.ftype, '') LIKE '%长债%'
                  )
                  AND r.latest_nav_date IS NOT NULL
                LIMIT 200
            ),
            risk_window AS (
                SELECT
                    MIN(rm.start_date) AS start_date,
                    MAX(rm.end_date) AS end_date
                FROM sample_codes s
                JOIN fund_risk_metrics rm
                    ON rm.rowid = (
                        SELECT latest.rowid
                        FROM fund_risk_metrics latest
                        WHERE latest.fcode = s.fcode
                          AND latest.period = '1y'
                        ORDER BY COALESCE(TRIM(latest.end_date), '') DESC, latest.rowid DESC
                        LIMIT 1
                    )
                WHERE rm.start_date IS NOT NULL
                  AND rm.end_date IS NOT NULL
            )
            SELECT
                h.fcode,
                h.nav_date,
                h.daily_growth
            FROM fund_nav_history h
            JOIN sample_codes s ON s.fcode = h.fcode
            JOIN risk_window w
            WHERE w.start_date IS NOT NULL
              AND w.end_date IS NOT NULL
              AND h.nav_date >= w.start_date
              AND h.nav_date <= w.end_date
            ORDER BY h.fcode, h.nav_date
        """,
        expected_index_keywords=("fund_nav_history", "INDEX"),
    ),
    QuerySpec(
        key="task_history_attention_order",
        label="task history attention order",
        sql="""
            SELECT id, task_name, status, started_at
            FROM task_history
            WHERE status IN (?, ?, ?)
            ORDER BY started_at DESC
            LIMIT 20
        """,
        params=("failed", "degraded", "running"),
        expected_index_keywords=("idx_task_history_status",),
    ),
)

TABLE_COUNT_NAMES: tuple[str, ...] = (
    "funds",
    "fund_returns",
    "fund_nav_history",
    "fund_rank_history",
    "fund_risk_metrics",
    "fund_managers",
    "screening_runs",
    "screening_decisions",
    "screening_picks",
    "screening_pick_metric_snapshots",
    "task_history",
)


def _clean_sql(sql: str) -> str:
    return " ".join(str(sql or "").split())


def _connect_read_only(database_path: Path) -> sqlite3.Connection:
    uri = f"file:{database_path.resolve()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    return conn


def _safe_scalar(conn: sqlite3.Connection, sql: str, params: Sequence[Any] = ()) -> Any:
    try:
        row = conn.execute(sql, tuple(params)).fetchone()
    except sqlite3.Error:
        return None
    if row is None:
        return None
    return row[0]


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    value = _safe_scalar(
        conn,
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    )
    return value == 1


def _database_stats(conn: sqlite3.Connection) -> dict[str, Any]:
    table_counts: dict[str, int | None] = {}
    for table_name in TABLE_COUNT_NAMES:
        if _table_exists(conn, table_name):
            count_value = _safe_scalar(conn, f"SELECT COUNT(*) FROM {table_name}")
            table_counts[table_name] = int(count_value) if count_value is not None else None
        else:
            table_counts[table_name] = None

    return {
        "page_size": _safe_scalar(conn, "PRAGMA page_size"),
        "page_count": _safe_scalar(conn, "PRAGMA page_count"),
        "freelist_count": _safe_scalar(conn, "PRAGMA freelist_count"),
        "journal_mode": _safe_scalar(conn, "PRAGMA journal_mode"),
        "table_counts": table_counts,
    }


def _explain_query_plan(
    conn: sqlite3.Connection,
    spec: QuerySpec,
) -> tuple[list[dict[str, Any]], str | None]:
    try:
        rows = conn.execute(f"EXPLAIN QUERY PLAN {_clean_sql(spec.sql)}", spec.params).fetchall()
    except sqlite3.Error as exc:
        return [], str(exc)

    plan_rows: list[dict[str, Any]] = []
    for row in rows:
        keys = row.keys()
        plan_rows.append(
            {
                "id": row["id"] if "id" in keys else row[0],
                "parent": row["parent"] if "parent" in keys else row[1],
                "notused": row["notused"] if "notused" in keys else row[2],
                "detail": row["detail"] if "detail" in keys else row[3],
            }
        )
    return plan_rows, None


def _plan_summary(plan_rows: list[dict[str, Any]], expected_index_keywords: Sequence[str]) -> dict[str, Any]:
    details = [str(row.get("detail") or "") for row in plan_rows]
    joined = "\n".join(details).upper()
    expected_hits = []
    for keyword in expected_index_keywords:
        keyword_text = str(keyword or "").strip()
        if not keyword_text:
            continue
        expected_hits.append(
            {
                "keyword": keyword_text,
                "present": keyword_text.upper() in joined,
            }
        )
    return {
        "details": details,
        "uses_temp_btree": "USE TEMP B-TREE" in joined,
        "uses_covering_index": "COVERING INDEX" in joined,
        "uses_index": "USING INDEX" in joined or "USING COVERING INDEX" in joined,
        "has_table_scan": any(detail.upper().startswith("SCAN ") for detail in details),
        "expected_index_hits": expected_hits,
        "expected_index_all_present": all(item["present"] for item in expected_hits) if expected_hits else None,
    }


def _sample_rows(rows: Sequence[sqlite3.Row], limit: int) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    for row in rows[: max(0, int(limit))]:
        if hasattr(row, "keys"):
            samples.append({key: row[key] for key in row.keys()})
        else:
            samples.append({str(index): value for index, value in enumerate(row)})
    return samples


def _profile_query(
    conn: sqlite3.Connection,
    spec: QuerySpec,
    *,
    repeats: int,
    sample_rows: int,
    slow_threshold_seconds: float,
) -> dict[str, Any]:
    plan_rows, plan_error = _explain_query_plan(conn, spec)
    plan = _plan_summary(plan_rows, spec.expected_index_keywords)

    attempts: list[dict[str, Any]] = []
    row_count: int | None = None
    sample: list[dict[str, Any]] = []
    first_error: str | None = None
    repeat_total = max(1, int(repeats))

    for attempt_index in range(repeat_total):
        started = time.perf_counter()
        try:
            rows = conn.execute(_clean_sql(spec.sql), spec.params).fetchall()
            elapsed = round(max(0.0, time.perf_counter() - started), 6)
            row_count = len(rows)
            if attempt_index == 0:
                sample = _sample_rows(rows, sample_rows)
            attempts.append(
                {
                    "attempt": attempt_index + 1,
                    "status": "ok",
                    "elapsed_seconds": elapsed,
                    "row_count": row_count,
                }
            )
        except sqlite3.Error as exc:
            elapsed = round(max(0.0, time.perf_counter() - started), 6)
            first_error = str(exc)
            attempts.append(
                {
                    "attempt": attempt_index + 1,
                    "status": "error",
                    "elapsed_seconds": elapsed,
                    "error": first_error,
                }
            )
            break

    elapsed_values = [
        float(attempt.get("elapsed_seconds") or 0.0)
        for attempt in attempts
        if attempt.get("status") == "ok"
    ]
    max_elapsed = max(elapsed_values) if elapsed_values else None
    avg_elapsed = round(sum(elapsed_values) / len(elapsed_values), 6) if elapsed_values else None
    status = "error" if first_error else "ok"
    evidence_class = "observe"
    if first_error or plan_error:
        evidence_class = "needs_investigation"
    elif max_elapsed is not None and max_elapsed >= slow_threshold_seconds:
        evidence_class = "slow_query_profile"
    elif plan["uses_temp_btree"]:
        evidence_class = "plan_watch"

    return {
        "key": spec.key,
        "label": spec.label,
        "status": status,
        "sql": _clean_sql(spec.sql),
        "params": list(spec.params),
        "row_count": row_count,
        "attempts": attempts,
        "elapsed_seconds_avg": avg_elapsed,
        "elapsed_seconds_max": max_elapsed,
        "slow_threshold_seconds": slow_threshold_seconds,
        "sample_rows": sample,
        "plan_rows": plan_rows,
        "plan_error": plan_error,
        "plan_summary": plan,
        "profile_role": spec.profile_role,
        "evidence_class": evidence_class,
        "error": first_error,
    }


def _runtime_profile_payload(
    *,
    key: str,
    label: str,
    query: str,
    params: Sequence[Any],
    attempts: list[dict[str, Any]],
    row_count: int | None,
    sample: list[dict[str, Any]],
    plan_rows: list[dict[str, Any]],
    plan_error: str | None,
    plan: dict[str, Any],
    first_error: str | None,
    slow_threshold_seconds: float,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    elapsed_values = [
        float(attempt.get("elapsed_seconds") or 0.0)
        for attempt in attempts
        if attempt.get("status") == "ok"
    ]
    max_elapsed = max(elapsed_values) if elapsed_values else None
    avg_elapsed = round(sum(elapsed_values) / len(elapsed_values), 6) if elapsed_values else None
    status = "error" if first_error else "ok"
    evidence_class = "observe"
    if first_error or plan_error:
        evidence_class = "needs_investigation"
    elif max_elapsed is not None and max_elapsed >= slow_threshold_seconds:
        evidence_class = "slow_query_profile"
    elif plan.get("uses_temp_btree"):
        evidence_class = "plan_watch"

    payload = {
        "key": key,
        "label": label,
        "status": status,
        "sql": _clean_sql(query),
        "params": list(params),
        "row_count": row_count,
        "attempts": attempts,
        "elapsed_seconds_avg": avg_elapsed,
        "elapsed_seconds_max": max_elapsed,
        "slow_threshold_seconds": slow_threshold_seconds,
        "sample_rows": sample,
        "plan_rows": plan_rows,
        "plan_error": plan_error,
        "plan_summary": plan,
        "runtime_profile": True,
        "profile_role": "default_path",
        "evidence_class": evidence_class,
        "error": first_error,
    }
    if extra:
        payload.update(extra)
    return payload


def _profile_report_long_term_earliest_nav_dates_default(
    database_path: Path,
    *,
    repeats: int,
    sample_rows: int,
    slow_threshold_seconds: float,
) -> dict[str, Any]:
    attempts: list[dict[str, Any]] = []
    row_count: int | None = None
    sample: list[dict[str, Any]] = []
    first_error: str | None = None
    plan_rows: list[dict[str, Any]] = []
    plan_error: str | None = None
    plan: dict[str, Any] = {
        "details": ["runtime profile failed before query execution"],
        "uses_temp_btree": False,
        "uses_covering_index": False,
        "uses_index": False,
        "has_table_scan": False,
        "expected_index_hits": [],
        "expected_index_all_present": None,
    }
    archive_attach: dict[str, Any] = {}
    query = "runtime: report_builder long-term earliest NAV dates"

    try:
        conn = _connect_read_only(database_path)
    except sqlite3.Error as exc:
        first_error = str(exc)
        conn = None

    if conn is not None:
        try:
            nav_archive_binding = report_builder_module.load_latest_nav_history_archive_binding()
            if not report_builder_module.nav_history_archive_binding_matches_database(
                nav_archive_binding,
                database_path,
            ):
                nav_archive_binding = {}
            archive_attach = report_builder_module.attach_nav_history_archive_if_available(
                conn,
                archive_path=nav_archive_binding.get("archive_path"),
                archive_table_name=nav_archive_binding.get("archive_table_name") or "fund_nav_history_archive",
                archive_cutoff_date=nav_archive_binding.get("archive_cutoff_date") or "",
            )
            nav_source_sql = report_builder_module.build_nav_history_union_source_sql(
                archive_attach,
                columns=("fcode", "nav_date"),
            )
            fallback_query = f"""
                SELECT fcode, MIN(nav_date) AS earliest_nav_date
                FROM ({nav_source_sql}) AS nav_history
                GROUP BY fcode
            """
            query = fallback_query
            params: tuple[Any, ...] = ()
            archive = dict(archive_attach or {})
            if archive.get("attached"):
                alias = str(archive.get("alias") or "nav_archive_db").strip()
                table_name = str(archive.get("archive_table_name") or "fund_nav_history_archive").strip()
                if (
                    report_builder_module._is_safe_sql_identifier(alias)
                    and report_builder_module._is_safe_sql_identifier(table_name)
                ):
                    cutoff = str(archive.get("archive_cutoff_date") or "").strip()
                    cutoff_sql = "WHERE nav_date < ?" if cutoff else ""
                    params = (cutoff,) if cutoff else ()
                    query = f"""
                        WITH hot AS (
                            SELECT fcode, MIN(nav_date) AS earliest_nav_date
                            FROM fund_nav_history
                            GROUP BY fcode
                        ),
                        cold AS (
                            SELECT fcode, MIN(nav_date) AS earliest_nav_date
                            FROM {alias}.{table_name}
                            {cutoff_sql}
                            GROUP BY fcode
                        ),
                        merged AS (
                            SELECT fcode, earliest_nav_date FROM hot
                            UNION ALL
                            SELECT fcode, earliest_nav_date FROM cold
                        )
                        SELECT fcode, MIN(earliest_nav_date) AS earliest_nav_date
                        FROM merged
                        GROUP BY fcode
                    """
            temp_spec = QuerySpec(
                key="report_long_term_earliest_nav_dates_default",
                label="report_builder long-term governance earliest NAV dates default path",
                sql=query,
                params=params,
                expected_index_keywords=("fund_nav_history", "INDEX"),
            )
            plan_rows, plan_error = _explain_query_plan(conn, temp_spec)
            plan = _plan_summary(plan_rows, temp_spec.expected_index_keywords)

            for attempt_index in range(max(1, int(repeats))):
                started = time.perf_counter()
                try:
                    rows = conn.execute(_clean_sql(query), params).fetchall()
                    elapsed = round(max(0.0, time.perf_counter() - started), 6)
                    row_count = int(len(rows))
                    if attempt_index == 0:
                        sample = _sample_rows(rows, sample_rows)
                    attempts.append(
                        {
                            "attempt": attempt_index + 1,
                            "status": "ok",
                            "elapsed_seconds": elapsed,
                            "row_count": row_count,
                        }
                    )
                except sqlite3.Error as exc:
                    elapsed = round(max(0.0, time.perf_counter() - started), 6)
                    first_error = str(exc)
                    attempts.append(
                        {
                            "attempt": attempt_index + 1,
                            "status": "error",
                            "elapsed_seconds": elapsed,
                            "error": first_error,
                        }
                    )
                    break
        finally:
            try:
                if archive_attach.get("attached"):
                    report_builder_module.detach_nav_history_archive(
                        conn,
                        alias=str(archive_attach.get("alias") or "nav_archive_db"),
                    )
            finally:
                conn.close()

    elapsed_values = [
        float(attempt.get("elapsed_seconds") or 0.0)
        for attempt in attempts
        if attempt.get("status") == "ok"
    ]
    max_elapsed = max(elapsed_values) if elapsed_values else None
    avg_elapsed = round(sum(elapsed_values) / len(elapsed_values), 6) if elapsed_values else None
    status = "error" if first_error else "ok"
    evidence_class = "observe"
    if first_error or plan_error:
        evidence_class = "needs_investigation"
    elif max_elapsed is not None and max_elapsed >= slow_threshold_seconds:
        evidence_class = "slow_query_profile"
    elif plan.get("uses_temp_btree"):
        evidence_class = "plan_watch"

    return {
        "key": "report_long_term_earliest_nav_dates_default",
        "label": "report_builder long-term governance earliest NAV dates default path",
        "status": status,
        "sql": _clean_sql(query),
        "params": list(params) if "params" in locals() else [],
        "row_count": row_count,
        "attempts": attempts,
        "elapsed_seconds_avg": avg_elapsed,
        "elapsed_seconds_max": max_elapsed,
        "slow_threshold_seconds": slow_threshold_seconds,
        "sample_rows": sample,
        "plan_rows": plan_rows,
        "plan_error": plan_error,
        "plan_summary": plan,
        "runtime_profile": True,
        "profile_role": "default_path",
        "archive_attached": bool(archive_attach.get("attached")),
        "archive_reason": archive_attach.get("reason"),
        "evidence_class": evidence_class,
        "error": first_error,
    }


def _profile_report_latest_accum_nav_default(
    database_path: Path,
    *,
    repeats: int,
    sample_rows: int,
    slow_threshold_seconds: float,
) -> dict[str, Any]:
    attempts: list[dict[str, Any]] = []
    row_count: int | None = None
    sample: list[dict[str, Any]] = []
    first_error: str | None = None
    repeat_total = max(1, int(repeats))

    stage_specs = (
        ("read_sql_source", "_read_latest_accum_nav_from_sql_source"),
        ("read_hot_summary", "_read_latest_accum_nav_from_hot_summary"),
        ("read_orphan_hot_rows", "_read_latest_accum_nav_orphan_hot_rows"),
    )

    def _summarize_stage_result(result: Any) -> dict[str, Any]:
        if result is None:
            return {"returned_none": True}
        summary: dict[str, Any] = {"returned_none": False}
        try:
            summary["row_count"] = int(len(result))
        except (TypeError, ValueError):
            pass
        return summary

    def _install_stage_timing_wrappers(stage_events: list[dict[str, Any]]) -> dict[str, Any]:
        originals: dict[str, Any] = {}
        for stage_name, function_name in stage_specs:
            original = getattr(report_builder_module, function_name, None)
            if not callable(original):
                continue
            originals[function_name] = original

            def _wrapped(*args: Any, __original: Any = original, __stage_name: str = stage_name, **kwargs: Any) -> Any:
                started = time.perf_counter()
                status = "ok"
                error: str | None = None
                result: Any = None
                try:
                    result = __original(*args, **kwargs)
                    return result
                except Exception as exc:  # noqa: BLE001 - preserve runtime behavior while recording evidence
                    status = "error"
                    error = str(exc)
                    raise
                finally:
                    event = {
                        "stage": __stage_name,
                        "status": status,
                        "elapsed_seconds": round(max(0.0, time.perf_counter() - started), 6),
                    }
                    if error:
                        event["error"] = error
                    else:
                        event.update(_summarize_stage_result(result))
                    stage_events.append(event)

            setattr(report_builder_module, function_name, _wrapped)
        return originals

    def _restore_stage_timing_wrappers(originals: dict[str, Any]) -> None:
        for function_name, original in originals.items():
            setattr(report_builder_module, function_name, original)

    def _build_runtime_stage_summary() -> dict[str, Any]:
        by_stage: dict[str, list[dict[str, Any]]] = {}
        for attempt in attempts:
            for event in attempt.get("runtime_stage_timings") or []:
                stage = str(event.get("stage") or "")
                if stage:
                    by_stage.setdefault(stage, []).append(event)
        summary: dict[str, Any] = {}
        for stage, events in by_stage.items():
            elapsed_values = [
                float(event.get("elapsed_seconds") or 0.0)
                for event in events
                if event.get("status") == "ok"
            ]
            latest_event = events[-1] if events else {}
            stage_summary: dict[str, Any] = {
                "call_count": len(events),
                "error_count": sum(1 for event in events if event.get("status") != "ok"),
                "elapsed_seconds_avg": round(sum(elapsed_values) / len(elapsed_values), 6) if elapsed_values else None,
                "elapsed_seconds_max": max(elapsed_values) if elapsed_values else None,
            }
            if "row_count" in latest_event:
                stage_summary["latest_row_count"] = latest_event.get("row_count")
            if "returned_none" in latest_event:
                stage_summary["latest_returned_none"] = latest_event.get("returned_none")
            summary[stage] = stage_summary
        return summary

    for attempt_index in range(repeat_total):
        stage_events: list[dict[str, Any]] = []
        originals = _install_stage_timing_wrappers(stage_events)
        started = time.perf_counter()
        try:
            frame = report_builder_module._read_latest_accum_nav(database_path)
            elapsed = round(max(0.0, time.perf_counter() - started), 6)
            row_count = int(len(frame))
            if attempt_index == 0 and sample_rows > 0 and not frame.empty:
                sample = frame.head(max(0, int(sample_rows))).to_dict(orient="records")
            attempts.append(
                {
                    "attempt": attempt_index + 1,
                    "status": "ok",
                    "elapsed_seconds": elapsed,
                    "row_count": row_count,
                    "runtime_stage_timings": stage_events,
                }
            )
        except Exception as exc:  # noqa: BLE001 - runtime profile must record actual failure details
            elapsed = round(max(0.0, time.perf_counter() - started), 6)
            first_error = str(exc)
            attempts.append(
                {
                    "attempt": attempt_index + 1,
                    "status": "error",
                    "elapsed_seconds": elapsed,
                    "error": first_error,
                    "runtime_stage_timings": stage_events,
                }
            )
            break
        finally:
            _restore_stage_timing_wrappers(originals)

    return _runtime_profile_payload(
        key="report_latest_accum_nav_default",
        label="report_builder latest accumulated NAV default runtime path",
        query="runtime: backend.report_builder._read_latest_accum_nav",
        params=[],
        attempts=attempts,
        row_count=row_count,
        sample=sample,
        plan_rows=[],
        plan_error=None,
        plan={
            "details": [
                "Runtime profile; SQL fragments are covered by report_latest_accum_nav_summary and report_latest_accum_nav_hot."
            ],
            "uses_temp_btree": False,
            "uses_covering_index": False,
            "uses_index": False,
            "has_table_scan": False,
            "expected_index_hits": [],
            "expected_index_all_present": None,
        },
        first_error=first_error,
        slow_threshold_seconds=slow_threshold_seconds,
        extra={
            "runtime_stage_summary": _build_runtime_stage_summary(),
            "runtime_stage_timing_note": "Stage timings are measured by wrapping report_builder latest accumulated NAV helpers during this profile run.",
        },
    )


def _read_bond_candidate_codes_for_nav_window_profile(conn: sqlite3.Connection) -> set[str]:
    if not _table_exists(conn, "funds"):
        return set()
    columns = {
        str(row[1] or "").strip()
        for row in conn.execute("PRAGMA table_info(funds)").fetchall()
    }
    select_parts = ["fcode"]
    for column in ("ftype", "sub_type", "type_category"):
        select_parts.append(column if column in columns else f"'' AS {column}")
    rows = conn.execute(f"SELECT {', '.join(select_parts)} FROM funds").fetchall()
    codes: set[str] = set()
    for row in rows:
        payload = {
            "fcode": row["fcode"] if "fcode" in row.keys() else row[0],
            "ftype": row["ftype"] if "ftype" in row.keys() else "",
            "sub_type": row["sub_type"] if "sub_type" in row.keys() else "",
            "type_category": row["type_category"] if "type_category" in row.keys() else "",
        }
        if not report_builder_module._is_bond_fund_row(payload):
            continue
        code = report_builder_module._normalize_code(payload.get("fcode"))
        if code:
            codes.add(code)
    return codes


def _build_long_term_nav_window_profile_plan_query(
    *,
    archive_attach: dict[str, Any],
    nav_source_sql: str,
    candidate_codes: set[str],
    start_date: str,
    end_date: str,
    chunk_size: int = 500,
) -> tuple[str, tuple[Any, ...], tuple[str, ...]]:
    code_batch = sorted(candidate_codes)[: max(1, int(chunk_size))]
    if not code_batch:
        return "SELECT 1 WHERE 0", (), ()
    placeholders = ",".join("?" for _ in code_batch)
    try:
        plan_year = int(str(start_date)[:4])
    except (TypeError, ValueError):
        plan_year = 0
    year_start = str(start_date)
    year_end = f"{plan_year:04d}-12-31" if plan_year else str(end_date)
    archive = dict(archive_attach or {})
    if archive.get("attached"):
        alias = str(archive.get("alias") or "nav_archive_db").strip()
        table_name = str(archive.get("archive_table_name") or "fund_nav_history_archive").strip()
        if (
            report_builder_module._is_safe_sql_identifier(alias)
            and report_builder_module._is_safe_sql_identifier(table_name)
        ):
            cutoff = str(archive.get("archive_cutoff_date") or "").strip()
            cutoff_sql = "AND nav_date < ?" if cutoff else ""
            hot_params: tuple[Any, ...] = (plan_year, *code_batch, year_start, year_end)
            cold_params: tuple[Any, ...] = (plan_year, *code_batch, year_start, year_end)
            if cutoff:
                cold_params = cold_params + (cutoff,)
            return (
                f"""
                WITH hot AS (
                    SELECT fcode, ? AS nav_year, MIN(nav_date) AS first_nav_date
                    FROM fund_nav_history
                    WHERE fcode IN ({placeholders})
                      AND nav_date BETWEEN ? AND ?
                    GROUP BY fcode
                ),
                cold AS (
                    SELECT fcode, ? AS nav_year, MIN(nav_date) AS first_nav_date
                    FROM {alias}.{table_name}
                    WHERE fcode IN ({placeholders})
                      AND nav_date BETWEEN ? AND ?
                      {cutoff_sql}
                    GROUP BY fcode
                ),
                merged AS (
                    SELECT fcode, nav_year, first_nav_date FROM hot
                    UNION ALL
                    SELECT fcode, nav_year, first_nav_date FROM cold
                )
                SELECT fcode, nav_year, MIN(first_nav_date) AS first_nav_date
                FROM merged
                GROUP BY fcode, nav_year
                """,
                hot_params + cold_params,
                ("fund_nav_history", "INDEX"),
            )

    return (
        f"""
        SELECT fcode, ? AS nav_year, MIN(nav_date) AS first_nav_date
        FROM fund_nav_history
        WHERE fcode IN ({placeholders})
          AND nav_date BETWEEN ? AND ?
        GROUP BY fcode
        """,
        (plan_year, *code_batch, year_start, year_end),
        ("fund_nav_history", "INDEX"),
    )


def _profile_report_long_term_nav_window_years_default(
    database_path: Path,
    *,
    repeats: int,
    sample_rows: int,
    slow_threshold_seconds: float,
) -> dict[str, Any]:
    attempts: list[dict[str, Any]] = []
    row_count: int | None = None
    sample: list[dict[str, Any]] = []
    first_error: str | None = None
    plan_rows: list[dict[str, Any]] = []
    plan_error: str | None = None
    plan: dict[str, Any] = {
        "details": ["runtime profile failed before query planning"],
        "uses_temp_btree": False,
        "uses_covering_index": False,
        "uses_index": False,
        "has_table_scan": False,
        "expected_index_hits": [],
        "expected_index_all_present": None,
    }
    archive_attach: dict[str, Any] = {}
    candidate_codes: set[str] = set()
    annual_feature_window_years = tuple(
        report_builder_module._current_complete_calendar_year_window(
            report_builder_module.LONG_TERM_STABILITY_MIN_YEARS_5
        )
    )
    start_date = f"{annual_feature_window_years[0]:04d}-01-01"
    end_date = f"{annual_feature_window_years[-1]:04d}-12-31"
    query = "runtime: report_builder long-term NAV annual window years"
    params: tuple[Any, ...] = ()

    try:
        conn = _connect_read_only(database_path)
    except sqlite3.Error as exc:
        first_error = str(exc)
        conn = None

    if conn is not None:
        try:
            candidate_codes = _read_bond_candidate_codes_for_nav_window_profile(conn)
            nav_archive_binding = report_builder_module.load_latest_nav_history_archive_binding()
            if not report_builder_module.nav_history_archive_binding_matches_database(
                nav_archive_binding,
                database_path,
            ):
                nav_archive_binding = {}
            archive_attach = report_builder_module.attach_nav_history_archive_if_available(
                conn,
                archive_path=nav_archive_binding.get("archive_path"),
                archive_table_name=nav_archive_binding.get("archive_table_name") or "fund_nav_history_archive",
                archive_cutoff_date=nav_archive_binding.get("archive_cutoff_date") or "",
            )
            nav_source_sql = report_builder_module.build_nav_history_union_source_sql(
                archive_attach,
                columns=("fcode", "nav_date"),
            )
            query, params, expected_index_keywords = _build_long_term_nav_window_profile_plan_query(
                archive_attach=archive_attach,
                nav_source_sql=nav_source_sql,
                candidate_codes=candidate_codes,
                start_date=start_date,
                end_date=end_date,
            )
            temp_spec = QuerySpec(
                key="report_long_term_nav_window_years_default",
                label="report_builder long-term governance NAV annual window years default path",
                sql=query,
                params=params,
                expected_index_keywords=expected_index_keywords,
            )
            plan_rows, plan_error = _explain_query_plan(conn, temp_spec)
            plan = _plan_summary(plan_rows, temp_spec.expected_index_keywords)

            for attempt_index in range(max(1, int(repeats))):
                started = time.perf_counter()
                try:
                    rows = report_builder_module._read_long_term_nav_window_year_rows(
                        conn,
                        nav_source_sql=nav_source_sql,
                        nav_archive_attach=archive_attach,
                        candidate_codes=candidate_codes,
                        start_date=start_date,
                        end_date=end_date,
                    )
                    elapsed = round(max(0.0, time.perf_counter() - started), 6)
                    row_count = int(len(rows))
                    if attempt_index == 0:
                        sample = _sample_rows(rows, sample_rows)
                    attempts.append(
                        {
                            "attempt": attempt_index + 1,
                            "status": "ok",
                            "elapsed_seconds": elapsed,
                            "row_count": row_count,
                        }
                    )
                except sqlite3.Error as exc:
                    elapsed = round(max(0.0, time.perf_counter() - started), 6)
                    first_error = str(exc)
                    attempts.append(
                        {
                            "attempt": attempt_index + 1,
                            "status": "error",
                            "elapsed_seconds": elapsed,
                            "error": first_error,
                        }
                    )
                    break
        finally:
            try:
                if archive_attach.get("attached"):
                    report_builder_module.detach_nav_history_archive(
                        conn,
                        alias=str(archive_attach.get("alias") or "nav_archive_db"),
                    )
            finally:
                conn.close()

    return _runtime_profile_payload(
        key="report_long_term_nav_window_years_default",
        label="report_builder long-term governance NAV annual window years default path",
        query=query,
        params=params,
        attempts=attempts,
        row_count=row_count,
        sample=sample,
        plan_rows=plan_rows,
        plan_error=plan_error,
        plan=plan,
        first_error=first_error,
        slow_threshold_seconds=slow_threshold_seconds,
        extra={
            "archive_attached": bool(archive_attach.get("attached")),
            "archive_reason": archive_attach.get("reason"),
            "candidate_fund_count": len(candidate_codes),
            "annual_feature_window_years": list(annual_feature_window_years),
            "start_date": start_date,
            "end_date": end_date,
        },
    )


def _profile_report_long_term_rank_metrics_default(
    database_path: Path,
    *,
    repeats: int,
    sample_rows: int,
    slow_threshold_seconds: float,
) -> dict[str, Any]:
    attempts: list[dict[str, Any]] = []
    row_count: int | None = None
    sample: list[dict[str, Any]] = []
    first_error: str | None = None
    repeat_total = max(1, int(repeats))
    snapshot_stats_available = False
    snapshot_stats_refreshed = False
    snapshot_refresh_elapsed_seconds: float | None = None
    snapshot_refresh_error: str | None = None
    archive_attach: dict[str, Any] = {}

    stage_specs = (
        ("load_cached_rank_snapshot_stats", "_load_cached_long_term_history_rank_snapshot_stats"),
        ("read_hot_rank_percentile_codes", "_read_hot_rank_percentile_codes"),
        ("read_hot_rank_metrics_snapshot", "_read_long_term_rank_metrics_from_hot_using_snapshot"),
        ("read_hot_rank_metrics_fallback", "_read_long_term_rank_metrics_from_hot"),
        (
            "read_archive_rank_metrics_snapshot",
            "_read_long_term_rank_metrics_from_archive_missing_hot_using_snapshot",
        ),
        ("read_archive_rank_metrics_fallback", "_read_long_term_rank_metrics_from_archive_missing_hot"),
    )

    def _summarize_stage_result(result: Any) -> dict[str, Any]:
        summary: dict[str, Any] = {"returned_none": result is None}
        if result is None:
            return summary
        if isinstance(result, dict):
            summary["record_count"] = int(result.get("record_count") or 0)
            summary["code_count"] = len(result.get("codes") or [])
            summary["feature_ready_code_count"] = len(result.get("feature_ready_codes") or [])
            summary["latest_date_map_count"] = len(result.get("latest_date_map") or {})
            summary["hot_percentile_code_count"] = len(result.get("hot_percentile_codes") or [])
            return summary
        try:
            summary["row_count"] = int(len(result))
        except (TypeError, ValueError):
            pass
        return summary

    def _install_stage_timing_wrappers(stage_events: list[dict[str, Any]]) -> dict[str, Any]:
        originals: dict[str, Any] = {}

        for stage_name, function_name in stage_specs:
            original = getattr(report_builder_module, function_name, None)
            if not callable(original):
                continue
            originals[function_name] = original

            def _wrapped(*args: Any, __original: Any = original, __stage_name: str = stage_name, **kwargs: Any) -> Any:
                started = time.perf_counter()
                status = "ok"
                error: str | None = None
                result: Any = None
                try:
                    result = __original(*args, **kwargs)
                    return result
                except Exception as exc:  # noqa: BLE001 - preserve runtime behavior while recording evidence
                    status = "error"
                    error = str(exc)
                    raise
                finally:
                    event = {
                        "stage": __stage_name,
                        "status": status,
                        "elapsed_seconds": round(max(0.0, time.perf_counter() - started), 6),
                    }
                    if error:
                        event["error"] = error
                    else:
                        event.update(_summarize_stage_result(result))
                    stage_events.append(event)

            setattr(report_builder_module, function_name, _wrapped)

        return originals

    def _restore_stage_timing_wrappers(originals: dict[str, Any]) -> None:
        for function_name, original in originals.items():
            setattr(report_builder_module, function_name, original)

    def _build_runtime_stage_summary() -> dict[str, Any]:
        by_stage: dict[str, list[dict[str, Any]]] = {}
        for attempt in attempts:
            for event in attempt.get("runtime_stage_timings") or []:
                stage = str(event.get("stage") or "")
                if not stage:
                    continue
                by_stage.setdefault(stage, []).append(event)

        summary: dict[str, Any] = {}
        for stage, events in by_stage.items():
            elapsed_values = [
                float(event.get("elapsed_seconds") or 0.0)
                for event in events
                if event.get("status") == "ok"
            ]
            error_count = sum(1 for event in events if event.get("status") != "ok")
            latest_event = events[-1] if events else {}
            stage_summary: dict[str, Any] = {
                "call_count": len(events),
                "error_count": error_count,
                "elapsed_seconds_avg": round(sum(elapsed_values) / len(elapsed_values), 6) if elapsed_values else None,
                "elapsed_seconds_max": max(elapsed_values) if elapsed_values else None,
            }
            for field in (
                "row_count",
                "record_count",
                "code_count",
                "feature_ready_code_count",
                "latest_date_map_count",
                "hot_percentile_code_count",
                "returned_none",
            ):
                if field in latest_event:
                    stage_summary[f"latest_{field}"] = latest_event.get(field)
            summary[stage] = stage_summary
        return summary

    try:
        conn = _connect_read_only(database_path)
    except sqlite3.Error as exc:
        return {
            "key": "report_long_term_rank_metrics_default",
            "label": "report_builder long-term rank metrics default runtime path",
            "status": "error",
            "sql": "runtime: backend.report_builder._read_long_term_rank_metrics",
            "params": [],
            "row_count": None,
            "attempts": [],
            "elapsed_seconds_avg": None,
            "elapsed_seconds_max": None,
            "slow_threshold_seconds": slow_threshold_seconds,
            "sample_rows": [],
            "plan_rows": [],
            "plan_error": str(exc),
            "plan_summary": {
                "details": ["runtime profile failed before query execution"],
                "uses_temp_btree": False,
                "uses_covering_index": False,
                "uses_index": False,
                "has_table_scan": False,
                "expected_index_hits": [],
                "expected_index_all_present": None,
            },
            "runtime_profile": True,
            "profile_role": "default_path",
            "rank_snapshot_stats_available": False,
            "evidence_class": "needs_investigation",
            "error": str(exc),
        }

    try:
        rank_archive_binding = report_builder_module.load_latest_rank_history_archive_binding()
        if not report_builder_module.rank_history_archive_binding_matches_database(
            rank_archive_binding,
            database_path,
        ):
            rank_archive_binding = {}
        archive_attach = report_builder_module.attach_rank_history_archive_if_available(
            conn,
            archive_path=rank_archive_binding.get("archive_path"),
            archive_table_name=rank_archive_binding.get("archive_table_name") or "fund_rank_history_archive",
            archive_cutoff_date=rank_archive_binding.get("archive_cutoff_date") or "",
        )
        refresh_started = time.perf_counter()
        try:
            report_builder_module._read_long_term_history_rank_snapshot_stats(
                conn,
                database_path,
                archive_attach,
            )
            snapshot_stats_refreshed = True
        except Exception as exc:  # noqa: BLE001 - keep profiling fallback path evidence
            snapshot_refresh_error = str(exc)
        finally:
            snapshot_refresh_elapsed_seconds = round(
                max(0.0, time.perf_counter() - refresh_started),
                6,
            )
        snapshot_stats_available = (
            report_builder_module._load_cached_long_term_history_rank_snapshot_stats(
                database_path,
                archive_attach,
            )
            is not None
        )
        for attempt_index in range(repeat_total):
            started = time.perf_counter()
            stage_events: list[dict[str, Any]] = []
            stage_originals = _install_stage_timing_wrappers(stage_events)
            try:
                report_builder_module._LONG_TERM_RANK_METRICS_CACHE.clear()
                frame = report_builder_module._read_long_term_rank_metrics(
                    conn,
                    database_path,
                    archive_attach,
                )
                elapsed = round(max(0.0, time.perf_counter() - started), 6)
                row_count = int(len(frame))
                if attempt_index == 0:
                    sample = frame.head(max(0, int(sample_rows))).to_dict(orient="records")
                attempts.append(
                    {
                        "attempt": attempt_index + 1,
                        "status": "ok",
                        "elapsed_seconds": elapsed,
                        "row_count": row_count,
                        "runtime_stage_timings": stage_events,
                    }
                )
            except Exception as exc:  # noqa: BLE001 - audit artifact must capture runtime path failures
                elapsed = round(max(0.0, time.perf_counter() - started), 6)
                first_error = str(exc)
                attempts.append(
                    {
                        "attempt": attempt_index + 1,
                        "status": "error",
                        "elapsed_seconds": elapsed,
                        "error": first_error,
                        "runtime_stage_timings": stage_events,
                    }
                )
                break
            finally:
                _restore_stage_timing_wrappers(stage_originals)
    finally:
        try:
            if archive_attach.get("attached"):
                report_builder_module.detach_rank_history_archive(
                    conn,
                    alias=str(archive_attach.get("alias") or "rank_archive_db"),
                )
        finally:
            conn.close()

    elapsed_values = [
        float(attempt.get("elapsed_seconds") or 0.0)
        for attempt in attempts
        if attempt.get("status") == "ok"
    ]
    max_elapsed = max(elapsed_values) if elapsed_values else None
    avg_elapsed = round(sum(elapsed_values) / len(elapsed_values), 6) if elapsed_values else None
    status = "error" if first_error else "ok"
    evidence_class = "observe"
    if first_error:
        evidence_class = "needs_investigation"
    elif max_elapsed is not None and max_elapsed >= slow_threshold_seconds:
        evidence_class = "slow_query_profile"

    return {
        "key": "report_long_term_rank_metrics_default",
        "label": "report_builder long-term rank metrics default runtime path",
        "status": status,
        "sql": "runtime: backend.report_builder._read_long_term_rank_metrics",
        "params": [],
        "row_count": row_count,
        "attempts": attempts,
        "elapsed_seconds_avg": avg_elapsed,
        "elapsed_seconds_max": max_elapsed,
        "slow_threshold_seconds": slow_threshold_seconds,
        "sample_rows": sample,
        "plan_rows": [],
        "plan_error": None,
        "plan_summary": {
            "details": [
                "runtime profile; static SQL plan is covered by report_long_term_rank_metrics_hot",
            ],
            "uses_temp_btree": False,
            "uses_covering_index": False,
            "uses_index": False,
            "has_table_scan": False,
            "expected_index_hits": [],
            "expected_index_all_present": None,
        },
        "runtime_profile": True,
        "profile_role": "default_path",
        "rank_snapshot_stats_available": snapshot_stats_available,
        "rank_snapshot_stats_refreshed": snapshot_stats_refreshed,
        "rank_snapshot_refresh_elapsed_seconds": snapshot_refresh_elapsed_seconds,
        "rank_snapshot_refresh_error": snapshot_refresh_error,
        "archive_attached": bool(archive_attach.get("attached")),
        "runtime_stage_timing_note": "Stage timings are nested diagnostics from the default runtime path and are not additive.",
        "runtime_stage_summary": _build_runtime_stage_summary(),
        "evidence_class": evidence_class,
        "error": first_error,
    }


def _profile_report_risk_snapshot_default(
    database_path: Path,
    *,
    repeats: int,
    sample_rows: int,
    slow_threshold_seconds: float,
) -> dict[str, Any]:
    attempts: list[dict[str, Any]] = []
    row_count: int | None = None
    sample: list[dict[str, Any]] = []
    first_error: str | None = None
    risk_end_date = ""
    repeat_total = max(1, int(repeats))

    try:
        effective_nav_snapshot = report_builder_module.load_effective_nav_snapshot(database_path)
        risk_end_date = str(
            getattr(effective_nav_snapshot, "risk_1y_effective_end_date", "")
            or getattr(effective_nav_snapshot, "risk_1y_latest_end_date", "")
            or ""
        ).strip()
    except Exception as exc:  # noqa: BLE001 - profile must capture runtime path failures
        first_error = f"effective_nav_snapshot_error: {exc}"

    plan_rows: list[dict[str, Any]] = []
    plan_error: str | None = None
    plan = {
        "details": [
            "runtime profile; static SQL plan is covered by risk_latest_1y",
        ],
        "uses_temp_btree": False,
        "uses_covering_index": False,
        "uses_index": False,
        "has_table_scan": False,
        "expected_index_hits": [],
        "expected_index_all_present": None,
    }

    if not first_error and not risk_end_date:
        first_error = "risk_end_date_missing"

    if not first_error:
        try:
            conn = _connect_read_only(database_path)
        except sqlite3.Error as exc:
            first_error = str(exc)
            conn = None
        if conn is not None:
            try:
                columns = {
                    str(row[1] or "").strip()
                    for row in conn.execute("PRAGMA table_info(fund_risk_metrics)").fetchall()
                }
                raw_sharpe_expr = "raw_sharpe_ratio" if "raw_sharpe_ratio" in columns else "sharpe_ratio"
                raw_sortino_expr = "raw_sortino_ratio" if "raw_sortino_ratio" in columns else "sortino_ratio"
                sharpe_cap_hit_expr = "sharpe_ratio_cap_hit" if "sharpe_ratio_cap_hit" in columns else "NULL"
                sortino_cap_hit_expr = "sortino_ratio_cap_hit" if "sortino_ratio_cap_hit" in columns else "NULL"
                raw_sharpe_sql = f"risk.{raw_sharpe_expr}" if raw_sharpe_expr != "sharpe_ratio" else "risk.sharpe_ratio"
                raw_sortino_sql = f"risk.{raw_sortino_expr}" if raw_sortino_expr != "sortino_ratio" else "risk.sortino_ratio"
                sharpe_cap_hit_sql = f"risk.{sharpe_cap_hit_expr}" if sharpe_cap_hit_expr != "NULL" else "NULL"
                sortino_cap_hit_sql = f"risk.{sortino_cap_hit_expr}" if sortino_cap_hit_expr != "NULL" else "NULL"
                query = f"""
                    SELECT
                        risk.fcode,
                        risk.end_date,
                        risk.volatility,
                        risk.max_drawdown,
                        risk.annual_return,
                        risk.sharpe_ratio,
                        risk.sortino_ratio,
                        {raw_sharpe_sql} AS raw_sharpe_ratio,
                        {raw_sortino_sql} AS raw_sortino_ratio,
                        {sharpe_cap_hit_sql} AS sharpe_ratio_cap_hit,
                        {sortino_cap_hit_sql} AS sortino_ratio_cap_hit,
                        risk.calmar_ratio,
                        risk.var_95,
                        risk.var_99,
                        risk.positive_ratio
                    FROM fund_risk_metrics risk
                    WHERE risk.period = '1y'
                      AND risk.end_date = ?
                      AND risk.rowid = (
                          SELECT latest.rowid
                          FROM fund_risk_metrics latest
                          WHERE latest.fcode = risk.fcode
                            AND latest.period = risk.period
                            AND COALESCE(TRIM(latest.end_date), '') = COALESCE(TRIM(risk.end_date), '')
                          ORDER BY latest.rowid DESC
                          LIMIT 1
                      )
                """
                temp_spec = QuerySpec(
                    key="report_risk_snapshot_default",
                    label="report_builder risk snapshot default SQL path",
                    sql=query,
                    params=(risk_end_date,),
                    expected_index_keywords=("idx_risk_period_end_date_fcode",),
                )
                plan_rows, plan_error = _explain_query_plan(conn, temp_spec)
                plan = _plan_summary(plan_rows, temp_spec.expected_index_keywords)
                for attempt_index in range(repeat_total):
                    started = time.perf_counter()
                    try:
                        rows = conn.execute(_clean_sql(query), (risk_end_date,)).fetchall()
                        elapsed = round(max(0.0, time.perf_counter() - started), 6)
                        row_count = int(len(rows))
                        if attempt_index == 0:
                            sample = _sample_rows(rows, sample_rows)
                        attempts.append(
                            {
                                "attempt": attempt_index + 1,
                                "status": "ok",
                                "elapsed_seconds": elapsed,
                                "row_count": row_count,
                            }
                        )
                    except sqlite3.Error as exc:
                        elapsed = round(max(0.0, time.perf_counter() - started), 6)
                        first_error = str(exc)
                        attempts.append(
                            {
                                "attempt": attempt_index + 1,
                                "status": "error",
                                "elapsed_seconds": elapsed,
                                "error": first_error,
                            }
                        )
                        break
            finally:
                conn.close()

    elapsed_values = [
        float(attempt.get("elapsed_seconds") or 0.0)
        for attempt in attempts
        if attempt.get("status") == "ok"
    ]
    max_elapsed = max(elapsed_values) if elapsed_values else None
    avg_elapsed = round(sum(elapsed_values) / len(elapsed_values), 6) if elapsed_values else None
    status = "error" if first_error else "ok"
    evidence_class = "observe"
    if first_error or plan_error:
        evidence_class = "needs_investigation"
    elif max_elapsed is not None and max_elapsed >= slow_threshold_seconds:
        evidence_class = "slow_query_profile"

    return {
        "key": "report_risk_snapshot_default",
        "label": "report_builder risk snapshot default SQL path",
        "status": status,
        "sql": _clean_sql(query) if "query" in locals() else "runtime: backend.report_builder._read_risk_snapshot",
        "params": [risk_end_date] if risk_end_date else [],
        "row_count": row_count,
        "attempts": attempts,
        "elapsed_seconds_avg": avg_elapsed,
        "elapsed_seconds_max": max_elapsed,
        "slow_threshold_seconds": slow_threshold_seconds,
        "sample_rows": sample,
        "plan_rows": plan_rows,
        "plan_error": plan_error,
        "plan_summary": plan,
        "runtime_profile": True,
        "profile_role": "default_path",
        "risk_end_date": risk_end_date,
        "evidence_class": evidence_class,
        "error": first_error,
    }


def _build_recommendations(query_profiles: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    recommendations: list[dict[str, Any]] = []
    by_key = {str(profile.get("key") or ""): profile for profile in query_profiles}
    default_rank_profile = by_key.get("report_long_term_rank_metrics_default") or {}
    default_rank_ok = (
        default_rank_profile.get("status") == "ok"
        and float(default_rank_profile.get("elapsed_seconds_max") or 0.0) < float(
            default_rank_profile.get("slow_threshold_seconds") or 1.0
        )
    )
    for profile in query_profiles:
        key = str(profile.get("key") or "")
        evidence_class = str(profile.get("evidence_class") or "")
        profile_role = str(profile.get("profile_role") or "default_path")
        plan = profile.get("plan_summary") or {}
        max_elapsed = profile.get("elapsed_seconds_max")
        if key == "report_long_term_rank_metrics_hot" and default_rank_ok:
            continue
        if (
            evidence_class == "slow_query_profile"
            and profile_role == "default_path"
            and key in {
                "nav_latest_by_fcode",
                "rank_latest_by_fcode",
                "report_long_term_rank_metrics_hot",
                "report_long_term_rank_metrics_default",
                "report_latest_accum_nav_default",
                "report_long_term_nav_window_years_default",
            }
        ):
            recommendations.append(
                {
                    "key": f"{key}_sidecar_review",
                    "priority": "P1",
                    "action": "review_sidecar_or_materialized_latest_snapshot",
                    "evidence": {
                        "query_key": key,
                        "elapsed_seconds_max": max_elapsed,
                        "row_count": profile.get("row_count"),
                        "plan_details": plan.get("details"),
                    },
                    "data_truthfulness_note": "Only consider after preserving SQL fallback and stat-based invalidation; do not change business values.",
                }
            )
        elif plan.get("uses_temp_btree") and key == "task_history_attention_order":
            recommendations.append(
                {
                    "key": "task_history_order_index_watch",
                    "priority": "P2" if evidence_class != "slow_query_profile" else "P1",
                    "action": "keep_observing_before_composite_index",
                    "evidence": {
                        "query_key": key,
                        "elapsed_seconds_max": max_elapsed,
                        "plan_details": plan.get("details"),
                    },
                    "data_truthfulness_note": "Do not add an index unless repeated live scheduler/status latency points to this query.",
                }
            )
    return recommendations


def build_database_hotspot_profile(
    database_path: Path,
    *,
    reports_dir: Path,
    repeats: int = 3,
    sample_rows: int = 3,
    slow_threshold_seconds: float = 1.0,
) -> tuple[dict[str, Any], Path]:
    database_path = Path(database_path)
    audit_dir = Path(reports_dir) / "audit" / "database_hotspot_profile"
    artifact_path = _build_audit_artifact_path(audit_dir, "database_hotspot_profile")

    with _connect_read_only(database_path) as conn:
        stats = _database_stats(conn)
        query_profiles = [
            _profile_query(
                conn,
                spec,
                repeats=repeats,
                sample_rows=sample_rows,
                slow_threshold_seconds=slow_threshold_seconds,
            )
            for spec in PROFILE_QUERIES
        ]
    query_profiles.append(
        _profile_report_latest_accum_nav_default(
            database_path,
            repeats=repeats,
            sample_rows=sample_rows,
            slow_threshold_seconds=slow_threshold_seconds,
        )
    )
    query_profiles.append(
        _profile_report_long_term_earliest_nav_dates_default(
            database_path,
            repeats=repeats,
            sample_rows=sample_rows,
            slow_threshold_seconds=slow_threshold_seconds,
        )
    )
    query_profiles.append(
        _profile_report_long_term_nav_window_years_default(
            database_path,
            repeats=repeats,
            sample_rows=sample_rows,
            slow_threshold_seconds=slow_threshold_seconds,
        )
    )
    query_profiles.append(
        _profile_report_risk_snapshot_default(
            database_path,
            repeats=repeats,
            sample_rows=sample_rows,
            slow_threshold_seconds=slow_threshold_seconds,
        )
    )
    query_profiles.append(
        _profile_report_long_term_rank_metrics_default(
            database_path,
            repeats=repeats,
            sample_rows=sample_rows,
            slow_threshold_seconds=slow_threshold_seconds,
        )
    )

    failed = [profile for profile in query_profiles if profile.get("status") != "ok"]
    slow = [profile for profile in query_profiles if profile.get("evidence_class") == "slow_query_profile"]
    default_slow = [
        profile
        for profile in slow
        if str(profile.get("profile_role") or "default_path") == "default_path"
    ]
    comparison_slow = [
        profile
        for profile in slow
        if str(profile.get("profile_role") or "default_path") != "default_path"
    ]
    temp_btree = [
        profile
        for profile in query_profiles
        if (profile.get("plan_summary") or {}).get("uses_temp_btree")
    ]
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "database_path": str(database_path.resolve()),
        "read_only": True,
        "repeats": max(1, int(repeats)),
        "sample_rows_per_query": max(0, int(sample_rows)),
        "slow_threshold_seconds": float(slow_threshold_seconds),
        "database_stats": stats,
        "queries": query_profiles,
        "summary": {
            "query_count": len(query_profiles),
            "failed_query_count": len(failed),
            "slow_query_count": len(default_slow),
            "all_slow_query_count": len(slow),
            "comparison_slow_query_count": len(comparison_slow),
            "temp_btree_query_count": len(temp_btree),
            "slow_query_keys": [str(profile.get("key") or "") for profile in default_slow],
            "all_slow_query_keys": [str(profile.get("key") or "") for profile in slow],
            "comparison_slow_query_keys": [
                str(profile.get("key") or "") for profile in comparison_slow
            ],
            "temp_btree_query_keys": [str(profile.get("key") or "") for profile in temp_btree],
            "status": "needs_review" if failed or default_slow else ("watch" if temp_btree else "ok"),
        },
        "recommendations": _build_recommendations(query_profiles),
        "data_truthfulness_note": "This audit only reads SQLite metadata and query results from the configured database; it does not fabricate business data or mutate database tables.",
    }
    artifact_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload, artifact_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Profile SQLite hot queries used by reports and scheduler APIs.")
    parser.add_argument("--database-path", default="", help="Override configured SQLite database path.")
    parser.add_argument("--repeats", type=int, default=3, help="Execution repeats per query; default 3.")
    parser.add_argument("--sample-rows", type=int, default=3, help="Sample rows to store per query; default 3.")
    parser.add_argument("--slow-threshold-seconds", type=float, default=1.0, help="Slow query evidence threshold.")
    args = parser.parse_args()

    settings = load_settings()
    database_path = Path(args.database_path).expanduser() if args.database_path else settings.database_path
    if not database_path.is_absolute():
        database_path = (settings.project_root / database_path).resolve()

    try:
        payload, artifact_path = build_database_hotspot_profile(
            database_path,
            reports_dir=settings.reports_dir,
            repeats=args.repeats,
            sample_rows=args.sample_rows,
            slow_threshold_seconds=args.slow_threshold_seconds,
        )
    except sqlite3.Error as exc:
        print(
            json.dumps(
                {
                    "passed": False,
                    "error": str(exc),
                    "database_path": str(database_path),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 1

    response = {
        "passed": payload.get("summary", {}).get("failed_query_count") == 0,
        "status": payload.get("summary", {}).get("status"),
        "database_path": payload.get("database_path"),
        "query_count": payload.get("summary", {}).get("query_count"),
        "slow_query_keys": payload.get("summary", {}).get("slow_query_keys"),
        "comparison_slow_query_keys": payload.get("summary", {}).get("comparison_slow_query_keys"),
        "all_slow_query_keys": payload.get("summary", {}).get("all_slow_query_keys"),
        "temp_btree_query_keys": payload.get("summary", {}).get("temp_btree_query_keys"),
        "recommendation_count": len(payload.get("recommendations") or []),
        "artifact_path": str(artifact_path),
        "artifact_project_path": str(artifact_path.relative_to(settings.project_root)),
    }
    print(json.dumps(response, ensure_ascii=False, indent=2))
    return 0 if response["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
