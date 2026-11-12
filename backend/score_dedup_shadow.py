from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

import pandas as pd


SHADOW_SCORE_CONTRACT_VERSION = "score-dedup-shadow-2026-07-11.1"

# 影子方案只重新划分因子职责，不改变正式评分常量：
# - 近 1 年累计收益只留在近期组件；
# - 1 年年化收益只留在长期组件；
# - 长期组件释放的重复收益权重分配给 3 年收益、风险调整收益和费率。
SHADOW_SCORE_COMPONENTS: dict[str, list[tuple[str, float, bool]]] = {
    "short_term": [
        ("return_6m", 0.20, True),
        ("return_1y", 0.18, True),
        ("sharpe_ratio", 0.16, True),
        ("sortino_ratio", 0.14, True),
        ("max_drawdown", 0.10, False),
        ("volatility", 0.10, False),
        ("positive_ratio", 0.12, True),
    ],
    "long_term": [
        ("annual_return", 0.22, True),
        ("return_3y", 0.10, True),
        ("sharpe_ratio", 0.12, True),
        ("sortino_ratio", 0.11, True),
        ("calmar_ratio", 0.19, True),
        ("max_drawdown", 0.08, False),
        ("positive_ratio", 0.06, True),
        ("fee_total_numeric", 0.07, False),
        ("scale_billion", 0.05, True),
    ],
}

RISK_EXPOSURE_METRICS: tuple[tuple[str, bool], ...] = (
    ("annual_return", True),
    ("return_1y", True),
    ("return_3y", True),
    ("sharpe_ratio", True),
    ("sortino_ratio", True),
    ("calmar_ratio", True),
    ("max_drawdown", False),
    ("volatility", False),
    ("positive_ratio", True),
    ("fee_total_numeric", False),
    ("scale_billion", True),
)


def _normalize_code(value: Any) -> str:
    text = str(value or "").strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return ""
    digits = "".join(character for character in text if character.isdigit())
    return digits.zfill(6) if digits else text


def validate_score_components(
    components: Mapping[str, Iterable[tuple[str, float, bool]]],
) -> None:
    for component_key in ("short_term", "long_term"):
        rows = list(components.get(component_key) or [])
        if not rows:
            raise ValueError(f"missing_score_component:{component_key}")
        total_weight = sum(float(weight) for _metric, weight, _direction in rows)
        if abs(total_weight - 1.0) > 1e-9:
            raise ValueError(f"score_component_weight_not_one:{component_key}:{total_weight}")
        metric_names = [str(metric) for metric, _weight, _direction in rows]
        if len(metric_names) != len(set(metric_names)):
            raise ValueError(f"duplicate_metric_within_component:{component_key}")


def _rank_percentile(series: pd.Series, *, higher_is_better: bool) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.dropna().empty:
        return pd.Series(index=numeric.index, dtype=float)
    return numeric.rank(method="max", pct=True, ascending=higher_is_better) * 100.0


def _correlation(left: pd.Series, right: pd.Series, *, method: str) -> float | None:
    comparable = pd.concat(
        [pd.to_numeric(left, errors="coerce"), pd.to_numeric(right, errors="coerce")],
        axis=1,
    ).dropna()
    if len(comparable.index) < 2:
        return None
    if method == "spearman":
        comparable = comparable.rank(method="average")
    value = comparable.iloc[:, 0].corr(comparable.iloc[:, 1], method="pearson")
    return None if pd.isna(value) else round(float(value), 6)


def calculate_scores(
    df: pd.DataFrame,
    *,
    components: Mapping[str, Iterable[tuple[str, float, bool]]],
    short_term_weight: float,
    long_term_weight: float,
    style_column: str = "债基风格标签",
) -> pd.DataFrame:
    """按正式评分的同风格百分位语义计算一套隔离评分。"""

    validate_score_components(components)
    if abs(float(short_term_weight) + float(long_term_weight) - 1.0) > 1e-9:
        raise ValueError("blend_weights_must_sum_to_one")
    current = df.copy()
    if current.empty:
        for column in ("shadow_short_score", "shadow_long_score", "shadow_formal_score"):
            current[column] = pd.Series(dtype=float)
        return current

    styles = current.get(style_column, pd.Series("", index=current.index, dtype=object)).map(
        lambda value: str(value or "").strip() or "未知"
    )
    current["shadow_short_score"] = pd.Series(index=current.index, dtype=float)
    current["shadow_long_score"] = pd.Series(index=current.index, dtype=float)
    current["shadow_formal_score"] = pd.Series(index=current.index, dtype=float)

    for _style, group in current.groupby(styles, dropna=False):
        component_scores: dict[str, pd.Series] = {}
        for component_key in ("short_term", "long_term"):
            parts: list[pd.Series] = []
            for metric, weight, higher_is_better in list(components[component_key]):
                source = (
                    group[metric]
                    if metric in group.columns
                    else pd.Series(index=group.index, dtype=float)
                )
                parts.append(
                    _rank_percentile(source, higher_is_better=higher_is_better)
                    * float(weight)
                )
            component_scores[component_key] = pd.concat(parts, axis=1).sum(axis=1, min_count=1)

        short_score = component_scores["short_term"]
        long_score = component_scores["long_term"]
        short_component = short_score.combine_first(long_score)
        long_component = long_score.combine_first(short_score)
        formal_score = (
            short_component * float(short_term_weight)
            + long_component * float(long_term_weight)
        )
        current.loc[group.index, "shadow_short_score"] = short_score.round(4)
        current.loc[group.index, "shadow_long_score"] = long_score.round(4)
        current.loc[group.index, "shadow_formal_score"] = formal_score.round(4)
    return current


def attach_score_differences(df: pd.DataFrame) -> pd.DataFrame:
    current = df.copy()
    production_columns = {
        "正式投资评分": "production_formal_score",
        "近期得分": "production_short_score",
        "长期得分": "production_long_score",
    }
    for source, target in production_columns.items():
        current[target] = pd.to_numeric(current.get(source), errors="coerce")
    for shadow_column in ("shadow_formal_score", "shadow_short_score", "shadow_long_score"):
        current[shadow_column] = pd.to_numeric(current.get(shadow_column), errors="coerce")
    current["formal_score_delta"] = (
        current["shadow_formal_score"] - current["production_formal_score"]
    ).round(4)
    current["short_score_delta"] = (
        current["shadow_short_score"] - current["production_short_score"]
    ).round(4)
    current["long_score_delta"] = (
        current["shadow_long_score"] - current["production_long_score"]
    ).round(4)

    styles = current.get("债基风格标签", pd.Series("", index=current.index, dtype=object)).map(
        lambda value: str(value or "").strip() or "未知"
    )
    current["production_style_rank"] = current.groupby(styles)["production_formal_score"].rank(
        method="min", ascending=False
    )
    current["shadow_style_rank"] = current.groupby(styles)["shadow_formal_score"].rank(
        method="min", ascending=False
    )
    current["style_rank_change"] = (
        current["production_style_rank"] - current["shadow_style_rank"]
    )
    return current


def build_duplicate_return_correlation(df: pd.DataFrame) -> dict[str, Any]:
    current = df.copy()
    current["return_1y"] = pd.to_numeric(current.get("return_1y"), errors="coerce")
    current["annual_return"] = pd.to_numeric(current.get("annual_return"), errors="coerce")

    def _summary(group: pd.DataFrame, style: str) -> dict[str, Any]:
        comparable = group[["return_1y", "annual_return"]].dropna()
        return {
            "style": style,
            "comparable_count": int(len(comparable.index)),
            "pearson": _correlation(
                comparable.get("return_1y", pd.Series(dtype=float)),
                comparable.get("annual_return", pd.Series(dtype=float)),
                method="pearson",
            ),
            "spearman": _correlation(
                comparable.get("return_1y", pd.Series(dtype=float)),
                comparable.get("annual_return", pd.Series(dtype=float)),
                method="spearman",
            ),
            "median_absolute_gap_pct": (
                None
                if comparable.empty
                else round(float((comparable["return_1y"] - comparable["annual_return"]).abs().median()), 6)
            ),
        }

    style_rows = []
    styles = current.get("债基风格标签", pd.Series("", index=current.index, dtype=object)).map(
        lambda value: str(value or "").strip() or "未知"
    )
    for style, group in current.groupby(styles, dropna=False):
        style_rows.append(_summary(group, str(style)))
    return {
        "metric_pair": ["return_1y", "annual_return"],
        "overall": _summary(current, "ALL"),
        "by_style": sorted(style_rows, key=lambda row: row["style"]),
    }


def _cohort_metric_summary(df: pd.DataFrame, label: str) -> dict[str, Any]:
    row: dict[str, Any] = {"cohort": label, "fund_count": int(len(df.index)), "metrics": {}}
    for metric, higher_is_better in RISK_EXPOSURE_METRICS:
        source = df[metric] if metric in df.columns else pd.Series(index=df.index, dtype=float)
        values = pd.to_numeric(source, errors="coerce").dropna()
        row["metrics"][metric] = {
            "observed_count": int(len(values.index)),
            "mean": None if values.empty else round(float(values.mean()), 6),
            "median": None if values.empty else round(float(values.median()), 6),
            "preferred_direction": "higher" if higher_is_better else "lower",
        }
    return row


def build_risk_exposure_comparison(
    baseline_df: pd.DataFrame,
    shadow_df: pd.DataFrame,
) -> dict[str, Any]:
    baseline = _cohort_metric_summary(baseline_df, "baseline_official")
    shadow = _cohort_metric_summary(shadow_df, "shadow_official")
    deltas: dict[str, Any] = {}
    for metric, higher_is_better in RISK_EXPOSURE_METRICS:
        baseline_mean = baseline["metrics"][metric]["mean"]
        shadow_mean = shadow["metrics"][metric]["mean"]
        delta = None if baseline_mean is None or shadow_mean is None else round(shadow_mean - baseline_mean, 6)
        preferred = None
        if delta is not None:
            preferred = delta >= 0 if higher_is_better else delta <= 0
        deltas[metric] = {
            "mean_delta": delta,
            "shadow_direction_preferred": preferred,
        }
    return {"baseline": baseline, "shadow": shadow, "mean_deltas": deltas}


def build_membership_comparison(
    scored_df: pd.DataFrame,
    baseline_official_df: pd.DataFrame,
    shadow_official_df: pd.DataFrame,
) -> tuple[dict[str, Any], pd.DataFrame]:
    current = scored_df.copy()
    current["基金代码"] = current.get("基金代码", pd.Series(dtype=object)).map(_normalize_code)
    baseline_codes = {
        _normalize_code(code)
        for code in baseline_official_df.get("基金代码", pd.Series(dtype=object)).tolist()
        if _normalize_code(code)
    }
    shadow_codes = {
        _normalize_code(code)
        for code in shadow_official_df.get("基金代码", pd.Series(dtype=object)).tolist()
        if _normalize_code(code)
    }
    added = shadow_codes - baseline_codes
    removed = baseline_codes - shadow_codes
    retained = baseline_codes & shadow_codes
    current["membership_change"] = current["基金代码"].map(
        lambda code: (
            "shadow_added"
            if code in added
            else "shadow_removed"
            if code in removed
            else "retained"
            if code in retained
            else "outside_both"
        )
    )

    baseline_style = baseline_official_df.copy()
    shadow_style = shadow_official_df.copy()
    for frame in (baseline_style, shadow_style):
        frame["债基风格标签"] = frame.get("债基风格标签", pd.Series("", index=frame.index)).map(
            lambda value: str(value or "").strip() or "未知"
        )
    baseline_counts = baseline_style["债基风格标签"].value_counts().to_dict()
    shadow_counts = shadow_style["债基风格标签"].value_counts().to_dict()
    style_rows = []
    for style in sorted(set(baseline_counts) | set(shadow_counts)):
        base_count = int(baseline_counts.get(style, 0))
        new_count = int(shadow_counts.get(style, 0))
        style_rows.append(
            {
                "style": style,
                "baseline_count": base_count,
                "shadow_count": new_count,
                "net_delta": new_count - base_count,
            }
        )
    summary = {
        "baseline_count": int(len(baseline_codes)),
        "shadow_count": int(len(shadow_codes)),
        "added_count": int(len(added)),
        "removed_count": int(len(removed)),
        "retained_count": int(len(retained)),
        "jaccard": round(float(len(retained)) / float(len(baseline_codes | shadow_codes)), 6)
        if baseline_codes or shadow_codes
        else 1.0,
        "added_codes": sorted(added),
        "removed_codes": sorted(removed),
        "style_impact": style_rows,
    }
    return summary, current


def build_ranking_stability(scored_df: pd.DataFrame) -> dict[str, Any]:
    current = scored_df.copy()

    def _summary(group: pd.DataFrame, style: str) -> dict[str, Any]:
        comparable = group[["production_formal_score", "shadow_formal_score"]].dropna()
        absolute_rank_change = pd.to_numeric(group.get("style_rank_change"), errors="coerce").abs().dropna()
        return {
            "style": style,
            "comparable_count": int(len(comparable.index)),
            "spearman": _correlation(
                comparable.get("production_formal_score", pd.Series(dtype=float)),
                comparable.get("shadow_formal_score", pd.Series(dtype=float)),
                method="spearman",
            ),
            "median_absolute_rank_change": (
                None if absolute_rank_change.empty else round(float(absolute_rank_change.median()), 4)
            ),
            "p95_absolute_rank_change": (
                None if absolute_rank_change.empty else round(float(absolute_rank_change.quantile(0.95)), 4)
            ),
        }

    styles = current.get("债基风格标签", pd.Series("", index=current.index, dtype=object)).map(
        lambda value: str(value or "").strip() or "未知"
    )
    style_rows = [_summary(group, str(style)) for style, group in current.groupby(styles, dropna=False)]
    overall = current[["production_formal_score", "shadow_formal_score"]].dropna()
    return {
        "overall_comparable_count": int(len(overall.index)),
        "overall_spearman": _correlation(
            overall.get("production_formal_score", pd.Series(dtype=float)),
            overall.get("shadow_formal_score", pd.Series(dtype=float)),
            method="spearman",
        ),
        "by_style": sorted(style_rows, key=lambda row: row["style"]),
    }


def build_shadow_contract(
    *,
    production_components: Mapping[str, Iterable[tuple[str, float, bool]]],
    short_term_weight: float,
    long_term_weight: float,
) -> dict[str, Any]:
    validate_score_components(production_components)
    validate_score_components(SHADOW_SCORE_COMPONENTS)

    def _rows(components: Mapping[str, Iterable[tuple[str, float, bool]]]) -> dict[str, list[dict[str, Any]]]:
        return {
            key: [
                {
                    "metric": metric,
                    "weight": float(weight),
                    "direction": "higher_is_better" if direction else "lower_is_better",
                    "effective_weight": round(
                        float(weight)
                        * (float(short_term_weight) if key == "short_term" else float(long_term_weight)),
                        6,
                    ),
                }
                for metric, weight, direction in list(components[key])
            ]
            for key in ("short_term", "long_term")
        }

    return {
        "contract_version": SHADOW_SCORE_CONTRACT_VERSION,
        "production_score_mutated": False,
        "comparison_scope": "within_bond_style",
        "missing_value_policy": "preserve_missing_without_reweighting_and_fallback_to_available_component",
        "role_boundary": {
            "return_1y": "short_term_trailing_one_year_cumulative_return_only",
            "annual_return": "long_term_one_year_annualized_return_only",
            "return_3y": "long_term_horizon_anchor",
            "risk_adjusted_return": "sharpe_sortino_calmar",
            "friction_cost": "fee_total_numeric",
        },
        "gate_substitution": (
            "仅在旁路回放中用 shadow_formal/short/long 替换三项评分字段，随后重新计算动态风格阈值；"
            "基础门禁、收益风险硬门禁、费率、规模、持有期、AutoStock 与最终精选逻辑保持生产实现。"
        ),
        "blend_weights": {
            "short_term": float(short_term_weight),
            "long_term": float(long_term_weight),
        },
        "production_components": _rows(production_components),
        "shadow_components": _rows(SHADOW_SCORE_COMPONENTS),
        "rollback": "删除/忽略 shadow artifact 即可；正式 PROJECT_SCORE_COMPONENTS 与发布产物从未被修改。",
    }
