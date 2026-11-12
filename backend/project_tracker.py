from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from collections.abc import Callable
from typing import Any

import pandas as pd
from openpyxl import load_workbook
from openpyxl.comments import Comment
from openpyxl.formatting.rule import ColorScaleRule, DataBarRule
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

from backend.config import Settings
from core.utils import report_history_store as history_store
from backend.manager_metrics import normalize_manager_frame
from core.utils.drawdown_duration import normalize_drawdown_duration_columns
from backend.project_formal_groups import (
    ENHANCED_BOND_GROUP_LABEL,
    FORMAL_GROUP_LABEL,
    FORMAL_SCORE_LABEL,
    STABLE_BOND_GROUP_LABEL,
    formal_group_from_row,
)
from backend.project_publish_decisions import DATA_VALIDATION_COLUMNS
from backend.publication import build_publication_registry
from backend.report_workbook import _project_relative, _replace_sheet_with_style


CODE_CANDIDATES = ["基金代码", "fcode", "代码", "fund_code"]
TRACKER_HISTORY_INPUT_CONTRACT = "publishable_canonical_sources_v2_validation_source"


def _tracker_snapshot_key(nav_date: Any, signature: Any) -> str:
    date_text = str(nav_date or "").strip()
    signature_text = str(signature or "").strip()
    return f"nav:{date_text}" if date_text else (f"signature:{signature_text}" if signature_text else "")


def _tracker_history_input_signature(settings: Settings, history: list[dict[str, Any]], current_run_id: str) -> str:
    """Invalidate a cached accumulation when canonical history or its bytes change."""
    root = Path(settings.project_root).resolve()
    sources = []
    for item in history:
        if item["run_id"] == current_run_id:
            continue  # Current metrics are refreshed separately on a duplicate snapshot.
        path = Path(item["excel_path"]).resolve()
        digest = hashlib.sha256()
        source_digest = history_store.original_hash(path)
        sources.append({"run_id": item["run_id"], "generated_at": item["generated_at"],
                        "nav_snapshot_date": item.get("nav_snapshot_date"), "final_count": item["final_count"],
                        "source": str(path.relative_to(root)), "sha256": source_digest})
    return hashlib.sha256(json.dumps([TRACKER_HISTORY_INPUT_CONTRACT, sources], ensure_ascii=False,
                                    sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _load_tracker_cache_summary(settings: Settings) -> dict[str, Any]:
    root = Path(getattr(settings, "reports_dir", settings.project_root))
    try:
        metadata = json.loads((root / "最终优质基金累积跟踪_report_meta.json").read_text(encoding="utf-8"))
        summary = metadata.get("accumulation_summary")
        return summary if isinstance(summary, dict) else {}
    except (OSError, ValueError, AttributeError):
        return {}

PROJECT_TRACKER_STATE_PREVIEW_COLUMNS = [
    "基金代码",
    "基金名称",
    "正式榜分组",
    "正式投资评分",
    "出现次数",
    "出现频率(%)",
    "首次出现日期",
    "最后出现日期",
    "连续缺席期数",
    "当前是否在榜",
    "基金公司",
    "债基风格标签",
    "最新年化收益(%)",
    "最新夏普比率",
    "最新最大回撤(%)",
    "最新波动率",
    "最新近1周(%)",
    "最新近1月(%)",
    "最新近3月(%)",
    "区间收益基准日",
    "最新净值日期",
    "最新持有期状态",
    "项目质量信心",
]

PROJECT_TRACKER_COLUMNS = [
    "基金代码",
    "基金名称",
    "正式榜分组",
    "正式投资评分",
    "出现次数",
    "出现频率(%)",
    "首次出现日期",
    "最后出现日期",
    "连续缺席期数",
    "当前是否在榜",
    "基金公司",
    "债基风格标签",
    "最新基金规模(亿)",
    "最新年化收益(%)",
    "最新夏普比率",
    "最新最大回撤(%)",
    "最新卡玛比率",
    "最新波动率",
    "最新费率合计(%)",
    "最新综合加权得分",
    "最新近期得分",
    "最新长期得分",
    "项目质量信心",
    "最新基金类型",
    "最新风险等级",
    "最新成立日期",
    "最新基金经理",
    "最新项目推荐结论",
    "最新推荐理由",
    "最新推荐备注",
    "最新持有期状态",
    "最新优选类别",
    "最新是否可申购",
    "最新申购状态",
    "最新基准归类",
    "最新经理从业与评分状态",
    "最新经理最短从业天数",
    "最新经理平均综合评分",
    "最新经理评分覆盖",
    "最新经理从业数据覆盖",
    "最新规模数据状态",
    "最新规模数据账龄(天)",
    "最新是否含权益/转债增强",
    "最新近1周(%)",
    "最新近1月(%)",
    "最新近3月(%)",
    "最新近6月(%)",
    "最新近1年(%)",
    "最新近2年(%)",
    "最新近3年(%)",
    "最新今年来(%)",
    "最新成立来(%)",
    "区间收益基准日",
    "区间收益获取时间",
    "区间收益来源",
    "区间收益来源SHA256",
    "最新索提诺比率",
    "最新VaR95(%)",
    "最新VaR99(%)",
    "最新正收益比率",
    "最新日增长率(%)",
    "最新净值日期",
    "最新单位净值",
    "最新累计净值",
    "最新1年-最长回撤观测数",
    *[f"最新{column}" for column in DATA_VALIDATION_COLUMNS],
    "最新AutoStock校验状态",
    "最新AutoStock校验备注",
    "最新AutoStock原始校验状态",
    "最新AutoStock告警原因",
    "最新长期稳定性标签",
    "最新母基金名称",
    "最新关联份额数",
    "最新关联份额名称",
    "最新关联份额代码",
]

PROJECT_TRACKER_CURRENT_COLUMNS = [
    "基金代码",
    "基金名称",
    "正式榜分组",
    "正式投资评分",
    "出现次数",
    "出现频率(%)",
    "首次出现日期",
    "最后出现日期",
    "连续缺席期数",
    "当前是否在榜",
    "基金公司",
    "债基风格标签",
    "最新基金规模(亿)",
    "最新年化收益(%)",
    "最新夏普比率",
    "最新最大回撤(%)",
    "项目质量信心",
]

PROJECT_TRACKER_EXPORT_COLUMNS = [
    "序号",
    "基金代码",
    "基金名称",
    "正式榜分组",
    "正式投资评分",
    "出现次数",
    "出现频率(%)",
    "首次出现日期",
    "最后出现日期",
    "连续缺席期数",
    "当前是否在榜",
    "基金类型",
    "风险等级",
    "成立日期",
    "基金规模(亿)",
    "基金公司",
    "基金经理",
    "债基风格标签",
    "基准归类",
    "项目推荐结论",
    "推荐备注",
    "持有期状态",
    "优选类别",
    "项目质量信心",
    "是否可申购",
    "申购状态",
    "经理从业与评分状态",
    "经理最短从业天数",
    "经理平均综合评分",
    "经理评分覆盖",
    "经理从业数据覆盖",
    "规模数据状态",
    "规模数据账龄(天)",
    "是否含权益/转债增强",
    "费率合计(%)",
    "近1周(%)",
    "近1月(%)",
    "近3月(%)",
    "近6月(%)",
    "近1年(%)",
    "近2年(%)",
    "近3年(%)",
    "今年来(%)",
    "成立来(%)",
    "区间收益基准日",
    "区间收益获取时间",
    "区间收益来源",
    "区间收益来源SHA256",
    "1年-波动率",
    "1年-最大回撤(%)",
    "1年-年化收益(%)",
    "1年-夏普比率",
    "1年-索提诺比率",
    "1年-卡玛比率",
    "1年-VaR95(%)",
    "1年-VaR99(%)",
    "1年-正收益比率",
    "最新日增长率(%)",
    "最新净值日期",
    "最新单位净值",
    "最新累计净值",
    "1年-最长回撤观测数",
    "综合加权得分",
    "近期得分",
    "长期得分",
    *DATA_VALIDATION_COLUMNS,
    "AutoStock校验状态",
    "AutoStock校验备注",
    "AutoStock原始校验状态",
    "AutoStock告警原因",
    "长期稳定性标签",
    "推荐理由",
    "母基金名称",
    "关联份额数",
    "关联份额名称",
    "关联份额代码",
]

PROJECT_TRACKER_EXPORT_COLUMN_WIDTHS = {
    "序号": 8,
    "基金代码": 12,
    "基金名称": 24,
    "正式榜分组": 14,
    "正式投资评分": 14,
    "出现次数": 10,
    "出现频率(%)": 12,
    "首次出现日期": 12,
    "最后出现日期": 12,
    "连续缺席期数": 12,
    "当前是否在榜": 10,
    "基金类型": 14,
    "风险等级": 10,
    "成立日期": 12,
    "基金规模(亿)": 14,
    "基金公司": 18,
    "基金经理": 18,
    "债基风格标签": 14,
    "基准归类": 14,
    "项目推荐结论": 14,
    "推荐备注": 30,
    "持有期状态": 14,
    "优选类别": 12,
    "项目质量信心": 12,
    "是否可申购": 10,
    "申购状态": 12,
    "经理从业与评分状态": 26,
    "经理最短从业天数": 14,
    "经理平均综合评分": 16,
    "经理评分覆盖": 16,
    "经理从业数据覆盖": 18,
    "规模数据状态": 14,
    "规模数据账龄(天)": 14,
    "是否含权益/转债增强": 14,
    "费率合计(%)": 12,
    "最新净值日期": 12,
    "区间收益基准日": 14,
    "区间收益获取时间": 22,
    "区间收益来源": 24,
    "区间收益来源SHA256": 24,
    "最新单位净值": 12,
    "最新累计净值": 12,
    "1年-最长回撤观测数": 16,
    "综合加权得分": 14,
    "近期得分": 12,
    "长期得分": 12,
    "AutoStock校验状态": 16,
    "AutoStock校验备注": 30,
    "AutoStock原始校验状态": 16,
    "AutoStock告警原因": 28,
    "长期稳定性标签": 16,
    "推荐理由": 40,
    "母基金名称": 22,
    "关联份额数": 10,
    "关联份额名称": 24,
    "关联份额代码": 16,
}

PROJECT_TRACKER_TOP_DECILE_RULES = {
    "1年-年化收益(%)": "desc",
    "近1月(%)": "desc",
    "近3月(%)": "desc",
    "1年-夏普比率": "desc",
    "1年-最大回撤(%)": "asc",
    "1年-波动率": "asc",
}

PROJECT_TRACKER_GROUP_HEADER_SCHEMES = [
    ("跟踪信息", [
        "序号", "基金代码", "基金名称", "正式榜分组", "正式投资评分", "出现次数", "出现频率(%)",
        "首次出现日期", "最后出现日期", "连续缺席期数", "当前是否在榜",
    ], "D9EAF7"),
    ("基础属性", [
        "基金类型", "风险等级", "成立日期", "基金规模(亿)", "基金公司", "基金经理",
        "债基风格标签", "基准归类",
    ], "E8F3E6"),
    ("项目判断", [
        "项目推荐结论", "推荐备注", "持有期状态", "优选类别", "项目质量信心",
        "是否可申购", "申购状态", "经理从业与评分状态", "经理最短从业天数", "经理平均综合评分",
        "经理评分覆盖", "经理从业数据覆盖",
        "规模数据状态", "规模数据账龄(天)", "是否含权益/转债增强", "费率合计(%)",
    ], "FCE4D6"),
    ("收益表现", [
        "近1周(%)", "近1月(%)", "近3月(%)", "近6月(%)", "近1年(%)",
        "近2年(%)", "近3年(%)", "今年来(%)", "成立来(%)",
        "区间收益基准日", "区间收益获取时间", "区间收益来源", "区间收益来源SHA256",
    ], "FFF2CC"),
    ("风险收益", [
        "1年-波动率", "1年-最大回撤(%)", "1年-年化收益(%)", "1年-夏普比率",
        "1年-索提诺比率", "1年-卡玛比率", "1年-VaR95(%)", "1年-VaR99(%)",
        "1年-正收益比率",
    ], "EADCF8"),
    ("净值与校验", [
        "最新日增长率(%)", "最新净值日期", "最新单位净值", "最新累计净值", "1年-最长回撤观测数",
        "综合加权得分", "近期得分", "长期得分",
        "AutoStock校验状态", "AutoStock校验备注", "AutoStock原始校验状态",
        "AutoStock告警原因", "长期稳定性标签",
    ], "F4CCCC"),
    ("扩展关联", [
        "推荐理由", "母基金名称", "关联份额数", "关联份额名称", "关联份额代码",
    ], "EDEDED"),
]

PROJECT_TRACKER_SUMMARY_ITEMS = [
    ("总跟踪基金数", "tracked_fund_count"),
    ("当前在榜数", "current_active_count"),
    ("重复出现基金数", "repeated_fund_count"),
    ("累计跟踪期数", "total_periods"),
    ("新增入榜", "latest_new_entry_count"),
    ("连续在榜", "latest_continued_count"),
    ("缺席后回归", "latest_returned_count"),
    ("本期掉出", "latest_dropped_count"),
    ("已处理批次数", "processed_run_count"),
    ("唯一快照数", "unique_snapshot_count"),
    ("最新业务日期", "latest_business_date"),
    ("最新净值日期", "latest_nav_snapshot_date"),
]

PROJECT_TRACKER_TRANSITION_ITEMS = [
    ("new_entry_count", "新增入榜"),
    ("continued_count", "连续在榜"),
    ("returned_count", "缺席后回归"),
    ("dropped_count", "本期掉出"),
]

PROJECT_TRACKER_HEADER_REQUIRED_COLUMNS = {
    "基金代码",
    "基金名称",
    "正式投资评分",
}

PROJECT_TRACKER_DEFAULT_SORT_COLUMNS = [
    ("正式投资评分", True),
    ("出现频率(%)", True),
]

PROJECT_TRACKER_PROCESS_COLUMNS = [
    "业务日期",
    "净值快照日期",
    "快照签名",
    "项目批次",
    "生成时间",
    "正式推荐数量",
    "来源文件",
]

PROJECT_TRACKER_EXPORT_TO_INTERNAL_RENAME = {
    "基金类型": "最新基金类型",
    "风险等级": "最新风险等级",
    "成立日期": "最新成立日期",
    "基金规模(亿)": "最新基金规模(亿)",
    "基金经理": "最新基金经理",
    "项目推荐结论": "最新项目推荐结论",
    "推荐理由": "最新推荐理由",
    "推荐备注": "最新推荐备注",
    "持有期状态": "最新持有期状态",
    "优选类别": "最新优选类别",
    "是否可申购": "最新是否可申购",
    "申购状态": "最新申购状态",
    "基准归类": "最新基准归类",
    "经理从业与评分状态": "最新经理从业与评分状态",
    "经理最短从业天数": "最新经理最短从业天数",
    "经理平均综合评分": "最新经理平均综合评分",
    "经理评分覆盖": "最新经理评分覆盖",
    "经理从业数据覆盖": "最新经理从业数据覆盖",
    "规模数据状态": "最新规模数据状态",
    "规模数据账龄(天)": "最新规模数据账龄(天)",
    "是否含权益/转债增强": "最新是否含权益/转债增强",
    "费率合计(%)": "最新费率合计(%)",
    "近1周(%)": "最新近1周(%)",
    "近1月(%)": "最新近1月(%)",
    "近3月(%)": "最新近3月(%)",
    "近6月(%)": "最新近6月(%)",
    "近1年(%)": "最新近1年(%)",
    "近2年(%)": "最新近2年(%)",
    "近3年(%)": "最新近3年(%)",
    "今年来(%)": "最新今年来(%)",
    "成立来(%)": "最新成立来(%)",
    "区间收益基准日": "区间收益基准日",
    "区间收益获取时间": "区间收益获取时间",
    "区间收益来源": "区间收益来源",
    "区间收益来源SHA256": "区间收益来源SHA256",
    "1年-波动率": "最新波动率",
    "1年-最大回撤(%)": "最新最大回撤(%)",
    "1年-年化收益(%)": "最新年化收益(%)",
    "1年-夏普比率": "最新夏普比率",
    "1年-索提诺比率": "最新索提诺比率",
    "1年-卡玛比率": "最新卡玛比率",
    "1年-VaR95(%)": "最新VaR95(%)",
    "1年-VaR99(%)": "最新VaR99(%)",
    "1年-正收益比率": "最新正收益比率",
    "最新日增长率(%)": "最新日增长率(%)",
    "最新净值日期": "最新净值日期",
    "最新单位净值": "最新单位净值",
    "最新累计净值": "最新累计净值",
    "1年-最长回撤观测数": "最新1年-最长回撤观测数",
    "综合加权得分": "最新综合加权得分",
    "近期得分": "最新近期得分",
    "长期得分": "最新长期得分",
    **{column: f"最新{column}" for column in DATA_VALIDATION_COLUMNS},
    "AutoStock校验状态": "最新AutoStock校验状态",
    "AutoStock校验备注": "最新AutoStock校验备注",
    "AutoStock原始校验状态": "最新AutoStock原始校验状态",
    "AutoStock告警原因": "最新AutoStock告警原因",
    "长期稳定性标签": "最新长期稳定性标签",
    "母基金名称": "最新母基金名称",
    "关联份额数": "最新关联份额数",
    "关联份额名称": "最新关联份额名称",
    "关联份额代码": "最新关联份额代码",
}

PROJECT_TRACKER_INTERNAL_TO_EXPORT_RENAME = {
    internal_name: export_name
    for export_name, internal_name in PROJECT_TRACKER_EXPORT_TO_INTERNAL_RENAME.items()
}


def _safe_float(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(numeric):
        return None
    return numeric


def _normalize_code(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.zfill(6) if digits else text


def _snapshot_code_set(df: pd.DataFrame) -> set[str]:
    if not isinstance(df, pd.DataFrame) or df.empty or "基金代码" not in df.columns:
        return set()
    return {
        code
        for code in df["基金代码"].map(_normalize_code).tolist()
        if code
    }


def _sample_codes(codes: set[str], *, limit: int = 20) -> list[str]:
    return sorted(codes)[:limit]


def _snapshot_label(snapshot: dict[str, Any] | None) -> str:
    if not snapshot:
        return ""
    return str(snapshot.get("nav_snapshot_date") or snapshot.get("business_date") or "").strip()


def _format_transition_count_items(counts: dict[str, int]) -> list[dict[str, Any]]:
    return [
        {
            "key": key.replace("_count", ""),
            "label": label,
            "count": int(counts.get(key) or 0),
        }
        for key, label in PROJECT_TRACKER_TRANSITION_ITEMS
    ]


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator * 100, 2)


def _empty_project_tracker_membership_change_summary() -> dict[str, Any]:
    counts = {
        "new_entry_count": 0,
        "continued_count": 0,
        "returned_count": 0,
        "dropped_count": 0,
    }
    latest_changes = "当前尚未形成可用的正式发布名单变化摘要。"
    return {
        "latest_transition_counts": _format_transition_count_items(counts),
        "latest_membership_change_summary": {
            "available": False,
            "reason": "no_formal_snapshot",
            "has_previous_snapshot": False,
            "current_count": 0,
            "previous_count": 0,
            **counts,
            "new_or_returned_count": 0,
            "current_change_rate_pct": 0.0,
            "new_entry_rate_pct": 0.0,
            "continued_rate_pct": 0.0,
            "returned_rate_pct": 0.0,
            "dropped_rate_pct": 0.0,
            "rolling_change_stability_summary": {
                "available": False,
                "reason": "insufficient_unique_snapshots",
                "window_size": 0,
                "comparison_count": 0,
                "comparison_periods": [],
                "mean_current_change_rate_pct": 0.0,
                "median_current_change_rate_pct": 0.0,
                "max_current_change_rate_pct": 0.0,
                "mean_dropped_rate_pct": 0.0,
                "median_dropped_rate_pct": 0.0,
                "max_dropped_rate_pct": 0.0,
                "mean_returned_rate_pct": 0.0,
                "mean_new_entry_rate_pct": 0.0,
                "current_vs_rolling_change_gap_pct": 0.0,
                "current_vs_rolling_drop_gap_pct": 0.0,
                "elevated_change_alert": False,
                "elevated_drop_alert": False,
                "rolling_alert_codes": [],
                "latest_comparison": {},
            },
            "current_period": "",
            "previous_period": "",
            "current_run_id": "",
            "previous_run_id": "",
            "current_nav_snapshot_date": "",
            "previous_nav_snapshot_date": "",
            "new_entry_codes_sample": [],
            "returned_codes_sample": [],
            "dropped_codes_sample": [],
        },
        "latest_changes": latest_changes,
        "narrative_summary": {
            "overview": "正式主榜累计跟踪尚未形成有效快照。",
            "latest_changes": latest_changes,
            "risk_hint": "需要至少两个唯一正式快照，才能判断连续、回归与掉出。",
        },
        "headline_highlights": [
            "正式主榜累计跟踪尚未形成有效快照。",
            latest_changes,
            "需要至少两个唯一正式快照，才能判断连续、回归与掉出。",
        ],
    }


def _compute_sorted_median(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(value) for value in values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return round(ordered[midpoint], 2)
    return round((ordered[midpoint - 1] + ordered[midpoint]) / 2, 2)


def _build_project_tracker_rolling_change_stability_summary(
    snapshots: list[dict[str, Any]],
    *,
    window_size: int = 5,
) -> dict[str, Any]:
    if len(snapshots) < 2:
        return {
            "available": False,
            "reason": "insufficient_unique_snapshots",
            "window_size": 0,
            "comparison_count": 0,
            "comparison_periods": [],
            "mean_current_change_rate_pct": 0.0,
            "median_current_change_rate_pct": 0.0,
            "max_current_change_rate_pct": 0.0,
            "mean_dropped_rate_pct": 0.0,
            "median_dropped_rate_pct": 0.0,
            "max_dropped_rate_pct": 0.0,
            "mean_returned_rate_pct": 0.0,
            "mean_new_entry_rate_pct": 0.0,
            "current_vs_rolling_change_gap_pct": 0.0,
            "current_vs_rolling_drop_gap_pct": 0.0,
            "elevated_change_alert": False,
            "elevated_drop_alert": False,
            "rolling_alert_codes": [],
            "latest_comparison": {},
        }

    comparisons: list[dict[str, Any]] = []
    historical_codes: set[str] = set()
    previous_codes: set[str] = set()
    previous_snapshot: dict[str, Any] | None = None

    for index, snapshot in enumerate(snapshots):
        current_codes = _snapshot_code_set(snapshot.get("csv_df"))
        if index == 0:
            previous_codes = current_codes
            previous_snapshot = snapshot
            continue

        new_entry_codes = current_codes - historical_codes - previous_codes
        continued_codes = current_codes & previous_codes
        returned_codes = (current_codes - previous_codes) & historical_codes
        dropped_codes = previous_codes - current_codes
        current_count = len(current_codes)
        previous_count = len(previous_codes)
        new_or_returned_count = len(new_entry_codes) + len(returned_codes)

        comparisons.append(
            {
                "current_period": _snapshot_label(snapshot),
                "previous_period": _snapshot_label(previous_snapshot),
                "current_run_id": str(snapshot.get("run_id") or "").strip(),
                "previous_run_id": str((previous_snapshot or {}).get("run_id") or "").strip(),
                "current_count": int(current_count),
                "previous_count": int(previous_count),
                "new_entry_count": int(len(new_entry_codes)),
                "continued_count": int(len(continued_codes)),
                "returned_count": int(len(returned_codes)),
                "dropped_count": int(len(dropped_codes)),
                "new_or_returned_count": int(new_or_returned_count),
                "current_change_rate_pct": _ratio(new_or_returned_count, current_count),
                "new_entry_rate_pct": _ratio(len(new_entry_codes), current_count),
                "returned_rate_pct": _ratio(len(returned_codes), current_count),
                "dropped_rate_pct": _ratio(len(dropped_codes), previous_count),
            }
        )

        historical_codes.update(previous_codes)
        previous_codes = current_codes
        previous_snapshot = snapshot

    rolling_window = comparisons[-window_size:]
    change_rates = [float(item.get("current_change_rate_pct") or 0.0) for item in rolling_window]
    drop_rates = [float(item.get("dropped_rate_pct") or 0.0) for item in rolling_window]
    returned_rates = [float(item.get("returned_rate_pct") or 0.0) for item in rolling_window]
    new_entry_rates = [float(item.get("new_entry_rate_pct") or 0.0) for item in rolling_window]
    mean_change_rate = round(sum(change_rates) / len(change_rates), 2) if change_rates else 0.0
    mean_drop_rate = round(sum(drop_rates) / len(drop_rates), 2) if drop_rates else 0.0
    mean_returned_rate = round(sum(returned_rates) / len(returned_rates), 2) if returned_rates else 0.0
    mean_new_entry_rate = round(sum(new_entry_rates) / len(new_entry_rates), 2) if new_entry_rates else 0.0
    latest_comparison = dict(rolling_window[-1]) if rolling_window else {}
    current_change_rate = float(latest_comparison.get("current_change_rate_pct") or 0.0)
    current_drop_rate = float(latest_comparison.get("dropped_rate_pct") or 0.0)
    current_vs_rolling_change_gap = round(current_change_rate - mean_change_rate, 2)
    current_vs_rolling_drop_gap = round(current_drop_rate - mean_drop_rate, 2)
    elevated_change_alert = bool(
        len(rolling_window) >= 3
        and current_change_rate >= max(mean_change_rate + 3.0, mean_change_rate * 1.5 if mean_change_rate > 0 else 0.0)
    )
    elevated_drop_alert = bool(
        len(rolling_window) >= 3
        and current_drop_rate >= max(mean_drop_rate + 3.0, mean_drop_rate * 1.5 if mean_drop_rate > 0 else 0.0)
    )
    alert_codes: list[str] = []
    if elevated_change_alert:
        alert_codes.append("elevated_current_change_rate")
    if elevated_drop_alert:
        alert_codes.append("elevated_drop_rate")

    return {
        "available": True,
        "reason": "ok",
        "window_size": int(len(rolling_window)),
        "comparison_count": int(len(comparisons)),
        "comparison_periods": [str(item.get("current_period") or "") for item in rolling_window],
        "mean_current_change_rate_pct": mean_change_rate,
        "median_current_change_rate_pct": _compute_sorted_median(change_rates),
        "max_current_change_rate_pct": round(max(change_rates), 2) if change_rates else 0.0,
        "mean_dropped_rate_pct": mean_drop_rate,
        "median_dropped_rate_pct": _compute_sorted_median(drop_rates),
        "max_dropped_rate_pct": round(max(drop_rates), 2) if drop_rates else 0.0,
        "mean_returned_rate_pct": mean_returned_rate,
        "mean_new_entry_rate_pct": mean_new_entry_rate,
        "current_vs_rolling_change_gap_pct": current_vs_rolling_change_gap,
        "current_vs_rolling_drop_gap_pct": current_vs_rolling_drop_gap,
        "elevated_change_alert": elevated_change_alert,
        "elevated_drop_alert": elevated_drop_alert,
        "rolling_alert_codes": alert_codes,
        "latest_comparison": latest_comparison,
    }


def _build_project_tracker_membership_change_summary(
    unique_snapshots: list[dict[str, Any]],
    *,
    tracked_fund_count: int = 0,
    repeated_fund_count: int = 0,
) -> dict[str, Any]:
    snapshots = [
        item
        for item in unique_snapshots
        if isinstance(item, dict) and isinstance(item.get("csv_df"), pd.DataFrame)
    ]
    if not snapshots:
        return _empty_project_tracker_membership_change_summary()

    current_snapshot = snapshots[-1]
    previous_snapshot = snapshots[-2] if len(snapshots) >= 2 else None
    current_codes = _snapshot_code_set(current_snapshot.get("csv_df"))
    previous_codes = _snapshot_code_set(previous_snapshot.get("csv_df")) if previous_snapshot else set()
    earlier_codes: set[str] = set()
    for snapshot in snapshots[:-2]:
        earlier_codes.update(_snapshot_code_set(snapshot.get("csv_df")))
    historical_before_current = set(previous_codes)
    historical_before_current.update(earlier_codes)

    new_entry_codes = current_codes - historical_before_current
    continued_codes = current_codes & previous_codes
    returned_codes = (current_codes - previous_codes) & earlier_codes
    dropped_codes = previous_codes - current_codes
    counts = {
        "new_entry_count": len(new_entry_codes),
        "continued_count": len(continued_codes),
        "returned_count": len(returned_codes),
        "dropped_count": len(dropped_codes),
    }
    current_count = len(current_codes)
    previous_count = len(previous_codes)
    new_or_returned_count = counts["new_entry_count"] + counts["returned_count"]
    current_period = _snapshot_label(current_snapshot)
    previous_period = _snapshot_label(previous_snapshot)
    has_previous_snapshot = previous_snapshot is not None

    if has_previous_snapshot:
        latest_changes = (
            f"最新一期（{current_period or '未知日期'}）的正式发布名单变化为："
            f"新增入榜{counts['new_entry_count']}只、"
            f"连续在榜{counts['continued_count']}只、"
            f"缺席后回归{counts['returned_count']}只、"
            f"本期掉出{counts['dropped_count']}只；"
            f"当前在榜{current_count}只，上一期{previous_count}只。"
        )
        reason = "ok"
    else:
        latest_changes = (
            f"最新一期（{current_period or '未知日期'}）为首个正式发布快照，"
            f"当前在榜{current_count}只；暂无法计算连续在榜、缺席后回归和本期掉出。"
        )
        reason = "single_formal_snapshot"

    current_change_rate = _ratio(new_or_returned_count, current_count)
    rolling_change_stability_summary = _build_project_tracker_rolling_change_stability_summary(snapshots)
    overview = (
        f"正式主榜累计跟踪已覆盖 {len(snapshots)} 个唯一快照，"
        f"累计记录 {int(tracked_fund_count or len(historical_before_current | current_codes))} 只基金，"
        f"当前在榜 {current_count} 只。"
    )
    if has_previous_snapshot:
        risk_hint = (
            f"本期新入或回归 {new_or_returned_count} 只，占当前在榜 {current_change_rate:.2f}%；"
            f"本期掉出 {counts['dropped_count']} 只，占上一期 { _ratio(counts['dropped_count'], previous_count):.2f}%。"
        )
        if rolling_change_stability_summary.get("available"):
            risk_hint += (
                f" 近{int(rolling_change_stability_summary.get('window_size') or 0)}期平均新入/回归比"
                f" {float(rolling_change_stability_summary.get('mean_current_change_rate_pct') or 0.0):.2f}%，"
                f"当前偏离 {float(rolling_change_stability_summary.get('current_vs_rolling_change_gap_pct') or 0.0):+.2f}pct。"
            )
    else:
        risk_hint = "需要至少两个唯一正式快照，才能判断连续、回归与掉出。"

    change_summary = {
        "available": bool(has_previous_snapshot),
        "reason": reason,
        "has_previous_snapshot": bool(has_previous_snapshot),
        "current_count": int(current_count),
        "previous_count": int(previous_count),
        **counts,
        "new_or_returned_count": int(new_or_returned_count),
        "current_change_rate_pct": current_change_rate,
        "new_entry_rate_pct": _ratio(counts["new_entry_count"], current_count),
        "continued_rate_pct": _ratio(counts["continued_count"], current_count),
        "returned_rate_pct": _ratio(counts["returned_count"], current_count),
        "dropped_rate_pct": _ratio(counts["dropped_count"], previous_count),
        "rolling_change_stability_summary": rolling_change_stability_summary,
        "current_period": current_period,
        "previous_period": previous_period,
        "current_business_date": str(current_snapshot.get("business_date") or "").strip(),
        "previous_business_date": str((previous_snapshot or {}).get("business_date") or "").strip(),
        "current_run_id": str(current_snapshot.get("run_id") or "").strip(),
        "previous_run_id": str((previous_snapshot or {}).get("run_id") or "").strip(),
        "current_nav_snapshot_date": str(current_snapshot.get("nav_snapshot_date") or "").strip(),
        "previous_nav_snapshot_date": str((previous_snapshot or {}).get("nav_snapshot_date") or "").strip(),
        "current_snapshot_signature": str(current_snapshot.get("snapshot_signature") or "").strip(),
        "previous_snapshot_signature": str((previous_snapshot or {}).get("snapshot_signature") or "").strip(),
        "new_entry_codes_sample": _sample_codes(new_entry_codes),
        "returned_codes_sample": _sample_codes(returned_codes),
        "dropped_codes_sample": _sample_codes(dropped_codes),
    }

    return {
        "latest_transition_counts": _format_transition_count_items(counts),
        "latest_membership_change_summary": change_summary,
        "latest_changes": latest_changes,
        "latest_new_entry_count": int(counts["new_entry_count"]),
        "latest_continued_count": int(counts["continued_count"]),
        "latest_returned_count": int(counts["returned_count"]),
        "latest_dropped_count": int(counts["dropped_count"]),
        "narrative_summary": {
            "overview": overview,
            "latest_changes": latest_changes,
            "risk_hint": risk_hint,
        },
        "headline_highlights": [
            overview,
            latest_changes,
            risk_hint,
            f"累计重复上榜基金 {int(repeated_fund_count or 0)} 只；成员变化解释只基于正式发布主榜唯一快照。",
            (
                "近期开启成员波动滚动审计；若连续多期明显偏离历史均值，再评估风格内稳健阈值。"
                if rolling_change_stability_summary.get("available")
                else "滚动成员波动审计需至少两个唯一正式快照后才可生效。"
            ),
        ],
    }


def _build_project_tracker_unique_snapshots_from_process_rows(
    settings: Settings,
    process_df: pd.DataFrame,
    *,
    load_tracker_source_frame: Callable[[Path], tuple[pd.DataFrame, str, str]],
) -> list[dict[str, Any]]:
    if process_df.empty:
        return []
    snapshots_by_key: dict[str, dict[str, Any]] = {}
    project_root = Path(getattr(settings, "project_root", Path(".")))
    for row in process_df.fillna("").to_dict(orient="records"):
        source_text = str(row.get("来源文件") or "").strip()
        if not source_text:
            continue
        source_path = Path(source_text)
        if not source_path.is_absolute():
            source_path = project_root / source_path
        if not source_path.exists():
            continue
        csv_df, loaded_signature, loaded_nav_snapshot_date = load_tracker_source_frame(source_path)
        if csv_df.empty:
            continue
        snapshot_signature = str(row.get("快照签名") or loaded_signature or "").strip()
        nav_snapshot_date = str(row.get("净值快照日期") or loaded_nav_snapshot_date or "").strip()
        snapshot_key = _tracker_snapshot_key(nav_snapshot_date, snapshot_signature)
        if not snapshot_key:
            continue
        snapshots_by_key[snapshot_key] = {
            "business_date": str(row.get("业务日期") or "").strip(),
            "nav_snapshot_date": nav_snapshot_date,
            "snapshot_signature": snapshot_signature,
            "run_id": str(row.get("项目批次") or "").strip(),
            "generated_at": str(row.get("生成时间") or "").strip(),
            "csv_df": csv_df.copy(),
        }
    return sorted(snapshots_by_key.values(), key=lambda item: (item["generated_at"], item["run_id"]))


def _pick_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    return None


def _resolve_formal_score_value(row: dict[str, Any] | pd.Series | None) -> float | None:
    if row is None:
        return None
    if isinstance(row, pd.Series):
        row = row.to_dict()
    if not isinstance(row, dict):
        return None
    formal_score = _safe_float(row.get(FORMAL_SCORE_LABEL))
    if formal_score is not None:
        return formal_score
    return _safe_float(row.get("综合加权得分"))


def _drop_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    return df.loc[:, ~df.columns.duplicated()].copy()


def _coalesce_merge_conflict_columns(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    left_name = f"{column_name}_x"
    right_name = f"{column_name}_y"
    if left_name not in df.columns and right_name not in df.columns:
        return df

    current = df.copy()
    base_values = current[column_name] if column_name in current.columns else pd.Series([""] * len(current.index), index=current.index)
    left_values = current[left_name] if left_name in current.columns else pd.Series([""] * len(current.index), index=current.index)
    right_values = current[right_name] if right_name in current.columns else pd.Series([""] * len(current.index), index=current.index)

    def _clean(series: pd.Series) -> pd.Series:
        return series.map(lambda value: "" if pd.isna(value) else str(value).strip())

    merged = _clean(base_values)
    for candidate in (_clean(left_values), _clean(right_values)):
        merged = merged.where(merged.ne(""), candidate)

    current[column_name] = merged
    drop_columns = [name for name in [left_name, right_name] if name in current.columns]
    if drop_columns:
        current = current.drop(columns=drop_columns)
    return current


def _select_unique_columns(df: pd.DataFrame, preferred: list[str]) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    selected_positions: list[int] = []
    selected_names: set[str] = set()
    for column in preferred:
        if column in selected_names:
            continue
        matches = [idx for idx, name in enumerate(df.columns) if name == column]
        if not matches:
            continue
        selected_positions.append(matches[0])
        selected_names.add(column)
    if not selected_positions:
        return pd.DataFrame(index=df.index)
    return df.iloc[:, selected_positions].copy()


def _read_project_tracker_export_sheet(path: Path, sheet_name: str) -> pd.DataFrame:
    from backend.report_history import workbook_frame
    recorded=workbook_frame(path,sheet_name,header=2)
    if recorded is not None:
        result=_normalize_project_tracker_export_frame(recorded)
        if history_store.project_options(path)[2]=='shadow':
            actual=_legacy_read_project_tracker_export_sheet(path,sheet_name)
            if history_store.digest(history_store.frame_payload(actual))!=history_store.digest(history_store.frame_payload(result)):
                raise history_store.HistoryError('history_tracker_export_shadow_mismatch:'+str(path))
        return result
    return _legacy_read_project_tracker_export_sheet(path,sheet_name)


def _legacy_read_project_tracker_export_sheet(path: Path, sheet_name: str) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_excel(path, sheet_name=sheet_name, dtype=str, header=2)
    except Exception:
        return pd.DataFrame()
    return _normalize_project_tracker_export_frame(df)


def _normalize_project_tracker_export_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    df = df.fillna("")
    if "基金代码" in df.columns:
        df["基金代码"] = df["基金代码"].map(_normalize_code)
        df = df[df["基金代码"].astype(str).str.strip() != ""].copy()
    return normalize_drawdown_duration_columns(normalize_manager_frame(df)).reset_index(drop=True)


def _restore_project_tracker_internal_view(export_df: pd.DataFrame) -> pd.DataFrame:
    if export_df.empty:
        return pd.DataFrame(columns=PROJECT_TRACKER_COLUMNS)
    current = normalize_drawdown_duration_columns(normalize_manager_frame(export_df))
    if "序号" in current.columns:
        current = current.drop(columns=["序号"])
    current = current.rename(columns=PROJECT_TRACKER_EXPORT_TO_INTERNAL_RENAME)
    current = _drop_duplicate_columns(current)
    for column in ("最新经理评分覆盖", "最新经理从业数据覆盖"):
        if column not in current:
            current[column] = "未记录"
    for column in PROJECT_TRACKER_COLUMNS:
        if column not in current.columns:
            current[column] = ""
    return current[PROJECT_TRACKER_COLUMNS].copy()


def _read_project_tracker_process_sheet(path: Path) -> pd.DataFrame:
    from backend.report_history import workbook_frame
    recorded=workbook_frame(path,'处理记录',header=0)
    if recorded is not None:
        df=recorded
        if history_store.project_options(path)[2]=='shadow':
            actual=_legacy_read_project_tracker_process_sheet(path)
            expected=_normalize_project_tracker_process_frame(df)
            if history_store.digest(history_store.frame_payload(actual))!=history_store.digest(history_store.frame_payload(expected)):
                raise history_store.HistoryError('history_tracker_process_shadow_mismatch:'+str(path))
        return _normalize_project_tracker_process_frame(df)
    return _legacy_read_project_tracker_process_sheet(path)


def _legacy_read_project_tracker_process_sheet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=PROJECT_TRACKER_PROCESS_COLUMNS)
    try:
        df = pd.read_excel(path, sheet_name="处理记录", dtype=str, header=0)
    except Exception:
        return pd.DataFrame(columns=PROJECT_TRACKER_PROCESS_COLUMNS)
    return _normalize_project_tracker_process_frame(df)


def _normalize_project_tracker_process_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=PROJECT_TRACKER_PROCESS_COLUMNS)
    df = df.fillna("")
    for column in PROJECT_TRACKER_PROCESS_COLUMNS:
        if column not in df.columns:
            df[column] = ""
    return df[PROJECT_TRACKER_PROCESS_COLUMNS].copy()


def _load_tracker_source_frame(excel_path: Path) -> tuple[pd.DataFrame, str, str]:
    found = history_store.view(excel_path, 'tracker')
    if found is not None:
        if not excel_path.exists() and not history_store.available(excel_path,'tracker'):
            raise history_store.HistoryError('history_original_missing_without_retirement')
        value, proof = found
        frame = history_store.payload_frame(value['frame'])
        if history_store.project_options(excel_path)[2] == 'shadow':
            actual = _legacy_load_tracker_source_frame(excel_path)
            expected = {'frame': history_store.frame_payload(actual[0]), 'signature': actual[1], 'nav_date': actual[2]}
            if history_store.digest(expected) != history_store.digest(value):
                raise history_store.HistoryError('history_tracker_shadow_mismatch:' + str(excel_path))
            return actual
        return frame, value['signature'], value['nav_date']
    return _legacy_load_tracker_source_frame(excel_path)


def _legacy_load_tracker_source_frame(excel_path: Path) -> tuple[pd.DataFrame, str, str]:
    try:
        main_df = pd.read_excel(excel_path, sheet_name="Sheet1", dtype=str)
    except Exception:
        main_df = pd.DataFrame()
    try:
        detail_df = pd.read_excel(excel_path, sheet_name="推荐理由与风格", dtype=str)
    except Exception:
        detail_df = pd.DataFrame()
    if main_df.empty:
        return pd.DataFrame(), "", ""

    csv_df = normalize_drawdown_duration_columns(normalize_manager_frame(main_df.fillna("")))
    if not detail_df.empty:
        detail_df = normalize_drawdown_duration_columns(normalize_manager_frame(detail_df.fillna("")))
        detail_code_col = _pick_column(detail_df, CODE_CANDIDATES)
        if detail_code_col:
            detail_df[detail_code_col] = detail_df[detail_code_col].map(_normalize_code)
            preferred_merge_columns = [
                "债基风格标签",
                "费率合计(%)",
                "项目推荐结论",
                "推荐理由",
                "推荐备注",
                "基金经理",
                "是否可申购",
                "申购状态",
                "基准归类",
                "经理从业与评分状态",
                "经理最短从业天数",
                "经理平均综合评分",
                "经理评分覆盖",
                "经理从业数据覆盖",
                "规模数据状态",
                "规模数据账龄(天)",
                "是否含权益/转债增强",
                # Sheet1 carries the canonical validation snapshot. Re-merging
                # the same columns would replace their names with _x/_y and
                # drop them from the tracker; even an empty canonical value
                # must stay empty instead of inheriting a detail-sheet pass.
                *[column for column in DATA_VALIDATION_COLUMNS if column not in csv_df.columns],
                "AutoStock原始校验状态",
                "AutoStock告警原因",
                "长期稳定性标签",
                "综合加权得分",
                "近期得分",
                "长期得分",
                "持有期状态",
            ]
            if FORMAL_GROUP_LABEL not in csv_df.columns:
                preferred_merge_columns.insert(0, FORMAL_GROUP_LABEL)
            preferred_merge_columns.extend(
                column for column in (
                    "项目质量信心", "区间收益基准日", "区间收益获取时间", "区间收益来源", "区间收益来源SHA256",
                ) if column not in csv_df.columns
            )
            merge_columns = [column for column in preferred_merge_columns if column in detail_df.columns]
            if merge_columns:
                csv_df = csv_df.merge(
                    detail_df[[detail_code_col, *merge_columns]].rename(columns={detail_code_col: "基金代码"}),
                    on="基金代码",
                    how="left",
                )
                for conflict_column in ["持有期状态", FORMAL_GROUP_LABEL]:
                    csv_df = _coalesce_merge_conflict_columns(csv_df, conflict_column)
                csv_df = _drop_duplicate_columns(csv_df)

    snapshot_signature, nav_snapshot_date = _tracker_source_identity(csv_df)
    return csv_df, snapshot_signature, nav_snapshot_date


def _tracker_source_identity(csv_df: pd.DataFrame) -> tuple[str, str]:
    """Use the same observed NAV/content identity for workbook and in-memory inputs."""
    if csv_df.empty:
        return "", ""
    core_columns = [
        column
        for column in [
            "基金代码",
            "最新净值日期",
            "最新单位净值",
            "最新累计净值",
            "最新日增长率(%)",
            "近1周(%)",
            "近1月(%)",
            "近3月(%)",
            "近6月(%)",
            "近1年(%)",
        ]
        if column in csv_df.columns
    ]
    snapshot_df = csv_df[core_columns].copy() if core_columns else csv_df.copy()
    if "基金代码" in snapshot_df.columns:
        snapshot_df["基金代码"] = snapshot_df["基金代码"].map(_normalize_code)
        snapshot_df = snapshot_df.sort_values("基金代码", kind="mergesort")
    snapshot_payload = snapshot_df.fillna("").astype(str).to_json(orient="records", force_ascii=False)
    snapshot_signature = hashlib.sha1(snapshot_payload.encode("utf-8")).hexdigest()
    nav_dates = []
    if "最新净值日期" in csv_df.columns:
        nav_dates = sorted({str(value).strip() for value in csv_df["最新净值日期"].dropna().tolist() if str(value).strip()})
    nav_snapshot_date = nav_dates[-1] if nav_dates else ""
    return snapshot_signature, nav_snapshot_date


def _try_reuse_project_final_tracker(
    settings: Settings,
    *,
    current_run_id: str,
    current_generated_at: str,
    current_filtered_excel: Path,
    current_final_count: int,
    current_nav_snapshot_date: str = "",
    read_project_tracker_export_sheet: Callable[[Path, str], pd.DataFrame] | None = None,
    read_project_tracker_process_sheet: Callable[[Path], pd.DataFrame] | None = None,
    load_tracker_source_frame: Callable[[Path], tuple[pd.DataFrame, str, str]] | None = None,
    restore_project_tracker_internal_view: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
    project_relative: Callable[[Settings, Path], str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]] | None:
    read_project_tracker_export_sheet = read_project_tracker_export_sheet or _read_project_tracker_export_sheet
    read_project_tracker_process_sheet = read_project_tracker_process_sheet or _read_project_tracker_process_sheet
    load_tracker_source_frame = load_tracker_source_frame or _load_tracker_source_frame
    restore_project_tracker_internal_view = restore_project_tracker_internal_view or _restore_project_tracker_internal_view
    project_relative = project_relative or _project_relative
    reports_dir = Path(getattr(settings, "reports_dir", getattr(settings, "project_root", Path("."))))
    tracker_path = reports_dir / "最终优质基金累积跟踪.xlsx"
    export_df = read_project_tracker_export_sheet(tracker_path, "完整数据")
    process_df = read_project_tracker_process_sheet(tracker_path)
    if export_df.empty or process_df.empty:
        return None

    current_source_df, current_signature, resolved_nav_snapshot_date = load_tracker_source_frame(current_filtered_excel)
    current_nav_snapshot_date = str(current_nav_snapshot_date or resolved_nav_snapshot_date or "").strip()
    if not current_signature or not current_nav_snapshot_date:
        return None

    existing_signatures = {
        str(value).strip()
        for value in process_df.get("快照签名", pd.Series(dtype=object)).tolist()
        if str(value).strip()
    }
    if current_signature not in existing_signatures:
        return None

    tracker_df = restore_project_tracker_internal_view(export_df).astype(object)
    current_rows = {
        _normalize_code(row.get("基金代码")): row
        for row in current_source_df.to_dict(orient="records")
        if _normalize_code(row.get("基金代码"))
    }
    active_codes = set(
        tracker_df.loc[tracker_df["当前是否在榜"].eq("是"), "基金代码"].map(_normalize_code)
    )
    if set(current_rows) != active_codes or len(current_rows) != current_final_count:
        return None
    # Membership/NAV signatures deliberately do not count a risk-parameter
    # recalculation as a new observation period. They cannot justify reusing
    # old metrics, fees, manager data or classifications, however.
    current_metrics_refreshed = False
    for index, old_row in tracker_df.iterrows():
        source_row = current_rows.get(_normalize_code(old_row.get("基金代码")))
        if source_row is None:
            continue
        for source_column, value in source_row.items():
            target_column = PROJECT_TRACKER_EXPORT_TO_INTERNAL_RENAME.get(source_column, source_column)
            if target_column not in PROJECT_TRACKER_COLUMNS or source_column == "序号":
                continue
            value = "" if pd.isna(value) else value
            previous = old_row.get(target_column, "")
            if str(previous) != str(value):
                previous_number, next_number = _safe_float(previous), _safe_float(value)
                if previous_number is None or next_number is None or previous_number != next_number:
                    current_metrics_refreshed = True
                tracker_df.at[index, target_column] = value
        # Confidence is a fact of this publication, independent of how many
        # previous periods the fund appeared in. An absent source stays absent.
        source_confidence = source_row.get("项目质量信心", "")
        source_confidence = "" if pd.isna(source_confidence) else source_confidence
        if str(old_row.get("项目质量信心", "")) != str(source_confidence):
            current_metrics_refreshed = True
        tracker_df.at[index, "项目质量信心"] = source_confidence
    process_current = process_df.copy()
    current_process_row = {
        "业务日期": current_generated_at[:10],
        "净值快照日期": current_nav_snapshot_date,
        "快照签名": current_signature,
        "项目批次": current_run_id,
        "生成时间": current_generated_at,
        "正式推荐数量": str(int(current_final_count or 0)),
        "来源文件": project_relative(settings, current_filtered_excel),
    }
    if not process_current.get("项目批次", pd.Series(dtype=object)).astype(str).eq(current_run_id).any():
        process_current = pd.concat([process_current, pd.DataFrame([current_process_row])], ignore_index=True)
    else:
        current_mask = process_current["项目批次"].astype(str).eq(current_run_id)
        for column, value in current_process_row.items():
            process_current.loc[current_mask, column] = value
    process_current = process_current.fillna("")
    process_current = process_current.sort_values(
        by=["业务日期", "生成时间", "项目批次"],
        ascending=[True, True, True],
        kind="mergesort",
    ).reset_index(drop=True)

    unique_snapshot_count = int(
        len(
            {
                _tracker_snapshot_key(row.get("净值快照日期"), row.get("快照签名"))
                for row in process_current.to_dict(orient="records")
                if str(row.get("净值快照日期") or "").strip() or str(row.get("快照签名") or "").strip()
            }
        )
    )
    latest_run = current_process_row
    duplicate_source = next(
        (
            row
            for row in process_current.to_dict(orient="records")
            if str(row.get("项目批次") or "").strip() != current_run_id
            and str(row.get("快照签名") or "").strip() == current_signature
        ),
        None,
    )
    unique_snapshots = _build_project_tracker_unique_snapshots_from_process_rows(
        settings,
        process_current,
        load_tracker_source_frame=load_tracker_source_frame,
    )
    transition_payload = _build_project_tracker_membership_change_summary(
        unique_snapshots,
        tracked_fund_count=int(len(tracker_df.index)),
        repeated_fund_count=int(pd.to_numeric(tracker_df.get("出现次数"), errors="coerce").fillna(0).ge(2).sum()) if not tracker_df.empty else 0,
    )
    summary = {
        "total_periods": int(unique_snapshot_count),
        "tracked_fund_count": int(len(tracker_df.index)),
        "repeated_fund_count": int(pd.to_numeric(tracker_df.get("出现次数"), errors="coerce").fillna(0).ge(2).sum()) if not tracker_df.empty else 0,
        "current_active_count": int((tracker_df.get("当前是否在榜", pd.Series(dtype=object)) == "是").sum()) if not tracker_df.empty else 0,
        "latest_business_date": str(process_current.get("业务日期", pd.Series(dtype=object)).astype(str).iloc[-1]) if not process_current.empty else "",
        "latest_nav_snapshot_date": current_nav_snapshot_date,
        "unique_snapshot_count": int(unique_snapshot_count),
        "processed_run_count": int(len(process_current.index)),
        "duplicate_snapshot_run_count": int(max(0, len(process_current.index) - unique_snapshot_count)),
        "latest_run_id": current_run_id,
        "latest_run_business_date": latest_run["业务日期"],
        "latest_run_nav_snapshot_date": current_nav_snapshot_date,
        "latest_run_snapshot_signature": current_signature,
        "latest_run_is_new_snapshot": False,
        "current_metrics_refreshed": current_metrics_refreshed,
        "latest_run_duplicate_of_run_id": str((duplicate_source or {}).get("项目批次") or ""),
        "latest_run_duplicate_of_business_date": str((duplicate_source or {}).get("业务日期") or ""),
        "latest_run_duplicate_of_nav_snapshot_date": str((duplicate_source or {}).get("净值快照日期") or ""),
        **transition_payload,
    }
    return tracker_df, process_current[PROJECT_TRACKER_PROCESS_COLUMNS].copy(), summary


def _load_project_report_history(
    settings: Settings,
    *,
    current_run_id: str,
    current_generated_at: str,
    current_filtered_excel: Path,
    current_final_count: int,
    current_nav_snapshot_date: str = "",
    publication_registry_builder: Callable[[Settings], dict[str, Any]] | None = None,
    history_capture: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    history: list[dict[str, Any]] = []
    current_run_order_key = (str(current_generated_at or "").strip(), str(current_run_id or "").strip())
    publication_registry_builder = publication_registry_builder or build_publication_registry
    registry = publication_registry_builder(settings)
    if history_capture is not None:
        history_capture.clear()
        history_capture.update(contract=TRACKER_HISTORY_INPUT_CONTRACT, complete=False, records=[], issues=[],
                               canonical_record_count=len(registry.get("canonical_runs") or []))
        if not isinstance(registry.get("canonical_runs"), list):
            history_capture["issues"].append("canonical_registry_unavailable")
    canonical_record_map = {
        str(item.get("run_id") or "").strip(): item
        for item in registry.get("canonical_runs") or []
        if str(item.get("run_id") or "").strip()
    }
    if history_capture is not None and len(canonical_record_map) != history_capture["canonical_record_count"]:
        history_capture["issues"].append("canonical_registry_missing_or_duplicate_run")
    for record in canonical_record_map.values():
        captured: dict[str, Any] = {}
        if history_capture is not None:
            captured = {"run_id": str(record.get("run_id") or "").strip(),
                        "publishable": bool(record.get("publishable")),
                        "report_path": str(record.get("report_path") or ""),
                        "registry_nav_snapshot_date": str((record.get("snapshot") or {}).get("candidate_date") or "").strip()}
            history_capture["records"].append(captured)

        def record_outcome(outcome: str, *, unavailable: bool = False) -> None:
            from backend.report_history import mode as history_mode
            if unavailable and history_mode(settings) == 'ledger':
                raise history_store.HistoryError('historical_tracker_input_unavailable:' + outcome)
            if history_capture is not None:
                captured["outcome"] = outcome
                if unavailable:
                    history_capture["issues"].append(f"{captured['run_id']}:{outcome}")

        if not bool(record.get("publishable")):
            record_outcome("not_publishable")
            continue
        report_path = Path(str(record.get("report_path") or ""))
        if not report_path.exists():
            record_outcome("report_missing", unavailable=True)
            continue
        try:
            report_bytes = report_path.read_bytes()
            payload = json.loads(report_bytes.decode("utf-8"))
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            record_outcome("report_unreadable", unavailable=True)
            continue
        if not isinstance(payload, dict):
            record_outcome("report_invalid", unavailable=True)
            continue
        if history_capture is not None:
            captured["report_sha256"] = hashlib.sha256(report_bytes).hexdigest()
            captured["revision"] = (payload.get("publication_control") or {}).get("revision")
        run_id = str(payload.get("run_id") or "").strip()
        if not run_id or run_id == current_run_id:
            record_outcome("current_run" if run_id else "report_run_missing", unavailable=not run_id)
            continue
        if history_capture is not None and run_id != captured["run_id"]:
            history_capture["issues"].append(f"{captured['run_id']}:report_run_mismatch")
        artifacts = payload.get("artifacts") or {}
        filtered_excel = artifacts.get("filtered_excel")
        if not filtered_excel:
            record_outcome("workbook_reference_missing", unavailable=True)
            continue
        excel_path = settings.project_root / str(filtered_excel)
        history_options=history_store.project_options(excel_path)
        if history_options and history_options[2]=='ledger' and history_store.view(excel_path,'tracker') is None:
            raise history_store.HistoryError('historical_tracker_source_not_imported:'+str(excel_path))
        if not history_store.available(excel_path, 'tracker'):
            record_outcome("workbook_missing", unavailable=True)
            continue
        generated_at = str(payload.get("generated_at") or "").strip()
        if not generated_at:
            record_outcome("report_time_missing", unavailable=True)
            continue
        if (generated_at, run_id) > current_run_order_key:
            record_outcome("after_current_run")
            continue
        final_count = int((payload.get("counts") or {}).get("final_count") or 0)
        if final_count <= 0:
            record_outcome("empty_publication")
            continue
        nav_snapshot_date = str(((record.get("snapshot") or {}).get("candidate_date") or "")).strip()
        if not nav_snapshot_date:
            record_outcome("registry_snapshot_date_missing", unavailable=True)
            continue
        if history_capture is not None:
            captured.update(generated_at=generated_at, excel_path=str(excel_path), final_count=final_count)
        record_outcome("selected")
        business_date = generated_at[:10]
        history.append(
            {
                "run_id": run_id,
                "generated_at": generated_at,
                "business_date": business_date,
                "final_count": final_count,
                "excel_path": excel_path,
                "nav_snapshot_date": nav_snapshot_date,
            }
        )

    history.append(
        {
            "run_id": current_run_id,
            "generated_at": current_generated_at,
            "business_date": current_generated_at[:10],
            "final_count": current_final_count,
            "excel_path": current_filtered_excel,
            "nav_snapshot_date": current_nav_snapshot_date,
        }
    )
    history.sort(key=lambda item: (item["business_date"], item["generated_at"], item["run_id"]))

    deduped: dict[str, dict[str, Any]] = {}
    for item in history:
        dedupe_key = str(item.get("nav_snapshot_date") or "").strip() or item["run_id"]
        existing = deduped.get(dedupe_key)
        if existing is None or (item["generated_at"], item["run_id"]) > (existing["generated_at"], existing["run_id"]):
            deduped[dedupe_key] = item
    result = sorted(
        deduped.values(),
        key=lambda value: (value["business_date"], value["generated_at"], value["run_id"]),
    )
    if history_capture is not None:
        selected_ids = [item["run_id"] for item in result if item["run_id"] != current_run_id]
        for item in history_capture["records"]:
            if item.get("outcome") == "selected" and item["run_id"] not in selected_ids:
                item["outcome"] = "superseded_snapshot"
        history_capture.update(selected_history_run_ids=selected_ids, complete=not history_capture["issues"])
    return result


def _build_project_final_tracker(
    settings: Settings,
    *,
    current_run_id: str,
    current_generated_at: str,
    current_filtered_excel: Path,
    current_final_count: int,
    current_nav_snapshot_date: str = "",
    try_reuse_project_final_tracker: Callable[..., tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]] | None] | None = None,
    load_project_report_history: Callable[..., list[dict[str, Any]]] | None = None,
    load_tracker_source_frame: Callable[[Path], tuple[pd.DataFrame, str, str]] | None = None,
    project_relative: Callable[[Settings, Path], str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    try_reuse_project_final_tracker = try_reuse_project_final_tracker or _try_reuse_project_final_tracker
    load_project_report_history = load_project_report_history or _load_project_report_history
    load_tracker_source_frame = load_tracker_source_frame or _load_tracker_source_frame
    project_relative = project_relative or _project_relative

    history = load_project_report_history(
        settings,
        current_run_id=current_run_id,
        current_generated_at=current_generated_at,
        current_filtered_excel=current_filtered_excel,
        current_final_count=current_final_count,
        current_nav_snapshot_date=current_nav_snapshot_date,
    )
    history_basis = {"history_input_contract": TRACKER_HISTORY_INPUT_CONTRACT,
                     "history_input_signature": _tracker_history_input_signature(settings, history, current_run_id)}
    previous_summary = _load_tracker_cache_summary(settings)
    if all(previous_summary.get(key) == value for key, value in history_basis.items()):
        reused = try_reuse_project_final_tracker(
            settings, current_run_id=current_run_id, current_generated_at=current_generated_at,
            current_filtered_excel=current_filtered_excel, current_final_count=current_final_count,
            current_nav_snapshot_date=current_nav_snapshot_date, load_tracker_source_frame=load_tracker_source_frame,
            project_relative=project_relative,
        )
        if reused is not None:
            reused[2].update(history_basis)
            return reused
    if not history:
        empty = pd.DataFrame(columns=PROJECT_TRACKER_COLUMNS)
        process = pd.DataFrame(columns=PROJECT_TRACKER_PROCESS_COLUMNS)
        return empty, process, {
            "total_periods": 0,
            "tracked_fund_count": 0,
            "repeated_fund_count": 0,
            "current_active_count": 0,
            "latest_business_date": "",
            **history_basis,
            **_empty_project_tracker_membership_change_summary(),
        }

    process_rows: list[dict[str, Any]] = []
    tracker_map: dict[str, dict[str, Any]] = {}
    snapshots_by_key: dict[str, dict[str, Any]] = {}

    for item in history:
        csv_df, snapshot_signature, nav_snapshot_date = load_tracker_source_frame(item["excel_path"])
        if csv_df.empty:
            continue
        snapshot_key = _tracker_snapshot_key(nav_snapshot_date, snapshot_signature)
        process_rows.append(
            {
                "业务日期": item["business_date"],
                "净值快照日期": nav_snapshot_date,
                "快照签名": snapshot_signature,
                "项目批次": item["run_id"],
                "生成时间": item["generated_at"],
                "正式推荐数量": int(len(csv_df.index)),
                "来源文件": project_relative(settings, item["excel_path"]),
            }
        )
        if not snapshot_key:
            continue
        snapshots_by_key[snapshot_key] = {
                "business_date": item["business_date"],
                "nav_snapshot_date": nav_snapshot_date,
                "snapshot_signature": snapshot_signature,
                "run_id": item["run_id"],
                "generated_at": item["generated_at"],
                "csv_df": csv_df.copy(),
        }

    unique_snapshots = sorted(snapshots_by_key.values(), key=lambda item: (item["generated_at"], item["run_id"]))

    if not unique_snapshots:
        tracker_df = pd.DataFrame(columns=PROJECT_TRACKER_COLUMNS)
        process_df = pd.DataFrame(process_rows, columns=PROJECT_TRACKER_PROCESS_COLUMNS)
        return tracker_df, process_df, {
            "total_periods": 0,
            "tracked_fund_count": 0,
            "repeated_fund_count": 0,
            "current_active_count": 0,
            "latest_business_date": "",
            "latest_nav_snapshot_date": "",
            **history_basis,
            **_empty_project_tracker_membership_change_summary(),
        }

    ordered_periods = [str(item.get("nav_snapshot_date") or item.get("business_date") or "") for item in unique_snapshots]
    latest_business_date = unique_snapshots[-1]["business_date"]
    latest_nav_snapshot_date = unique_snapshots[-1]["nav_snapshot_date"]
    latest_period = ordered_periods[-1] if ordered_periods else ""

    for item in unique_snapshots:
        csv_df = item["csv_df"]
        period_label = str(item.get("nav_snapshot_date") or item.get("business_date") or "")
        for row in csv_df.to_dict(orient="records"):
            code = _normalize_code(row.get("基金代码"))
            if not code:
                continue
            current = tracker_map.setdefault(
                code,
                {
                    "基金代码": code,
                    "基金名称": row.get("基金名称", ""),
                    "基金公司": row.get("基金公司", ""),
                    "债基风格标签": row.get("债基风格标签", ""),
                    "出现期次列表": [],
                    "最新行": {},
                },
            )
            current["基金名称"] = row.get("基金名称", "") or current["基金名称"]
            current["基金公司"] = row.get("基金公司", "") or current["基金公司"]
            current["债基风格标签"] = row.get("债基风格标签", "") or current["债基风格标签"]
            current["出现期次列表"].append(period_label)
            current["最新行"] = row

    # A rebuild may revisit old publications. Preserve already recorded
    # historical labels rather than applying today's confidence formula.
    reports_dir = Path(getattr(settings, "reports_dir", settings.project_root))
    previous_tracker = _read_project_tracker_export_sheet(reports_dir / "最终优质基金累积跟踪.xlsx", "完整数据")
    previous_confidence_by_code = {
        _normalize_code(row.get("基金代码")): row.get("项目质量信心", "")
        for row in previous_tracker.to_dict(orient="records")
        if _normalize_code(row.get("基金代码"))
    }
    tracker_rows: list[dict[str, Any]] = []
    for payload in tracker_map.values():
        appearance_periods = sorted(set(payload["出现期次列表"]))
        if not appearance_periods:
            continue
        last_period = appearance_periods[-1]
        current_active = last_period == latest_period
        last_index = ordered_periods.index(last_period)
        consecutive_absence_periods = len(ordered_periods) - 1 - last_index
        latest_row = payload["最新行"]
        occurrences = len(appearance_periods)
        confidence = latest_row.get("项目质量信心", "")
        if not current_active and payload["基金代码"] in previous_confidence_by_code:
            confidence = previous_confidence_by_code[payload["基金代码"]]
        confidence = "" if pd.isna(confidence) else confidence
        tracker_rows.append(
            {
                "基金代码": payload["基金代码"],
                "基金名称": payload["基金名称"],
                FORMAL_GROUP_LABEL: formal_group_from_row(latest_row),
                "正式投资评分": "" if _resolve_formal_score_value(latest_row) is None else _resolve_formal_score_value(latest_row),
                "出现次数": occurrences,
                "出现频率(%)": round(occurrences / len(ordered_periods) * 100, 2),
                "首次出现日期": appearance_periods[0],
                "最后出现日期": last_period,
                "连续缺席期数": consecutive_absence_periods,
                "当前是否在榜": "是" if current_active else "否",
                "基金公司": payload["基金公司"],
                "债基风格标签": payload["债基风格标签"],
                "最新基金规模(亿)": latest_row.get("基金规模(亿)", ""),
                "最新年化收益(%)": latest_row.get("1年-年化收益(%)", ""),
                "最新夏普比率": latest_row.get("1年-夏普比率", ""),
                "最新最大回撤(%)": latest_row.get("1年-最大回撤(%)", ""),
                "最新卡玛比率": latest_row.get("1年-卡玛比率", ""),
                "最新波动率": latest_row.get("1年-波动率", ""),
                "最新费率合计(%)": latest_row.get("费率合计(%)", ""),
                "最新综合加权得分": latest_row.get("综合加权得分", ""),
                "最新近期得分": latest_row.get("近期得分", ""),
                "最新长期得分": latest_row.get("长期得分", ""),
                "项目质量信心": confidence,
                "最新基金类型": latest_row.get("基金类型", ""),
                "最新风险等级": latest_row.get("风险等级", ""),
                "最新成立日期": latest_row.get("成立日期", ""),
                "最新基金经理": latest_row.get("基金经理", ""),
                "最新项目推荐结论": latest_row.get("项目推荐结论", ""),
                "最新推荐理由": latest_row.get("推荐理由", ""),
                "最新推荐备注": latest_row.get("推荐备注", ""),
                "最新持有期状态": latest_row.get("持有期状态", ""),
                "最新优选类别": latest_row.get("优选类别", ""),
                "最新是否可申购": latest_row.get("是否可申购", ""),
                "最新申购状态": latest_row.get("申购状态", ""),
                "最新基准归类": latest_row.get("基准归类", ""),
                "最新经理从业与评分状态": latest_row.get("经理从业与评分状态", ""),
                "最新经理最短从业天数": latest_row.get("经理最短从业天数", ""),
                "最新经理平均综合评分": latest_row.get("经理平均综合评分", ""),
                "最新经理评分覆盖": latest_row.get("经理评分覆盖", "未记录"),
                "最新经理从业数据覆盖": latest_row.get("经理从业数据覆盖", "未记录"),
                "最新规模数据状态": latest_row.get("规模数据状态", ""),
                "最新规模数据账龄(天)": latest_row.get("规模数据账龄(天)", ""),
                "最新是否含权益/转债增强": latest_row.get("是否含权益/转债增强", ""),
                "最新费率合计(%)": latest_row.get("费率合计(%)", ""),
                "最新近1周(%)": latest_row.get("近1周(%)", ""),
                "最新近1月(%)": latest_row.get("近1月(%)", ""),
                "最新近3月(%)": latest_row.get("近3月(%)", ""),
                "最新近6月(%)": latest_row.get("近6月(%)", ""),
                "最新近1年(%)": latest_row.get("近1年(%)", ""),
                "最新近2年(%)": latest_row.get("近2年(%)", ""),
                "最新近3年(%)": latest_row.get("近3年(%)", ""),
                "最新今年来(%)": latest_row.get("今年来(%)", ""),
                "最新成立来(%)": latest_row.get("成立来(%)", ""),
                "区间收益基准日": latest_row.get("区间收益基准日", ""),
                "区间收益获取时间": latest_row.get("区间收益获取时间", ""),
                "区间收益来源": latest_row.get("区间收益来源", ""),
                "区间收益来源SHA256": latest_row.get("区间收益来源SHA256", ""),
                "最新波动率": latest_row.get("1年-波动率", ""),
                "最新索提诺比率": latest_row.get("1年-索提诺比率", ""),
                "最新卡玛比率": latest_row.get("1年-卡玛比率", ""),
                "最新VaR95(%)": latest_row.get("1年-VaR95(%)", ""),
                "最新VaR99(%)": latest_row.get("1年-VaR99(%)", ""),
                "最新正收益比率": latest_row.get("1年-正收益比率", ""),
                "最新日增长率(%)": latest_row.get("最新日增长率(%)", ""),
                "最新净值日期": latest_row.get("最新净值日期", ""),
                "最新单位净值": latest_row.get("最新单位净值", ""),
                "最新累计净值": latest_row.get("最新累计净值", ""),
                "最新1年-最长回撤观测数": latest_row.get("1年-最长回撤观测数", ""),
                **{f"最新{column}": latest_row.get(column, "") for column in DATA_VALIDATION_COLUMNS},
                "最新AutoStock校验状态": latest_row.get("AutoStock校验状态", ""),
                "最新AutoStock校验备注": latest_row.get("AutoStock校验备注", ""),
                "最新AutoStock原始校验状态": latest_row.get("AutoStock原始校验状态", ""),
                "最新AutoStock告警原因": latest_row.get("AutoStock告警原因", ""),
                "最新长期稳定性标签": latest_row.get("长期稳定性标签", ""),
                "最新母基金名称": latest_row.get("母基金名称", ""),
                "最新关联份额数": latest_row.get("关联份额数", ""),
                "最新关联份额名称": latest_row.get("关联份额名称", ""),
                "最新关联份额代码": latest_row.get("关联份额代码", ""),
            }
        )

    tracker_df = pd.DataFrame(tracker_rows)
    if tracker_df.empty:
        tracker_df = pd.DataFrame(columns=PROJECT_TRACKER_COLUMNS)
    else:
        confidence_order = {"高信心": 3, "中信心": 2, "边界观察": 1}
        tracker_df["综合评分"] = tracker_df[FORMAL_SCORE_LABEL]
        tracker_df["_current_active_sort"] = tracker_df["当前是否在榜"].map({"是": 1, "否": 0}).fillna(0)
        tracker_df["_confidence_sort"] = tracker_df["项目质量信心"].map(confidence_order).fillna(0)
        tracker_df["_formal_score_sort"] = pd.to_numeric(tracker_df[FORMAL_SCORE_LABEL], errors="coerce").fillna(-999.0)
        tracker_df["_annual_return_sort"] = pd.to_numeric(tracker_df["最新年化收益(%)"], errors="coerce").fillna(-999.0)
        tracker_df = tracker_df.sort_values(
            by=["_current_active_sort", "_formal_score_sort", "出现次数", "_confidence_sort", "_annual_return_sort", "基金代码"],
            ascending=[False, False, False, False, False, True],
            kind="mergesort",
        )
        tracker_df = tracker_df[PROJECT_TRACKER_COLUMNS].reset_index(drop=True)

    process_df = pd.DataFrame(process_rows, columns=PROJECT_TRACKER_PROCESS_COLUMNS)
    latest_run_snapshot = process_rows[-1] if process_rows else {}
    duplicate_source = None
    if latest_run_snapshot:
        latest_nav_snapshot_date_text = str(latest_run_snapshot.get("净值快照日期") or "")
        for item in process_rows[:-1]:
            if str(item.get("净值快照日期") or "") == latest_nav_snapshot_date_text:
                duplicate_source = item
                break
        if (duplicate_source is None and latest_nav_snapshot_date_text
                and previous_summary.get("latest_nav_snapshot_date") == latest_nav_snapshot_date_text):
            duplicate_source = {"项目批次": previous_summary.get("latest_run_id"),
                                "业务日期": previous_summary.get("latest_run_business_date"),
                                "净值快照日期": previous_summary.get("latest_nav_snapshot_date")}
    summary = {
        "total_periods": int(len(unique_snapshots)),
        "tracked_fund_count": int(len(tracker_df.index)),
        "repeated_fund_count": int(pd.to_numeric(tracker_df.get("出现次数"), errors="coerce").fillna(0).ge(2).sum()) if not tracker_df.empty else 0,
        "current_active_count": int((tracker_df.get("当前是否在榜", pd.Series(dtype=object)) == "是").sum()) if not tracker_df.empty else 0,
        "latest_business_date": latest_business_date,
        "latest_nav_snapshot_date": latest_nav_snapshot_date,
        "unique_snapshot_count": int(len(unique_snapshots)),
        "processed_run_count": int(len(history)),
        "duplicate_snapshot_run_count": int(max(0, len(history) - len(unique_snapshots))),
        "latest_run_id": latest_run_snapshot.get("项目批次") if latest_run_snapshot else "",
        "latest_run_business_date": latest_run_snapshot.get("业务日期") if latest_run_snapshot else "",
        "latest_run_nav_snapshot_date": latest_run_snapshot.get("净值快照日期") if latest_run_snapshot else "",
        "latest_run_snapshot_signature": latest_run_snapshot.get("快照签名") if latest_run_snapshot else "",
        "latest_run_is_new_snapshot": duplicate_source is None,
        "latest_run_duplicate_of_run_id": duplicate_source.get("项目批次") if duplicate_source else "",
        "latest_run_duplicate_of_business_date": duplicate_source.get("业务日期") if duplicate_source else "",
        "latest_run_duplicate_of_nav_snapshot_date": duplicate_source.get("净值快照日期") if duplicate_source else "",
        **history_basis,
    }
    summary.update(
        _build_project_tracker_membership_change_summary(
            unique_snapshots,
            tracked_fund_count=summary["tracked_fund_count"],
            repeated_fund_count=summary["repeated_fund_count"],
        )
    )
    return tracker_df, process_df, summary


def _build_project_tracker_export_view(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=PROJECT_TRACKER_EXPORT_COLUMNS)

    export_df = normalize_drawdown_duration_columns(df).reset_index(drop=True)
    export_df.insert(0, "序号", range(1, len(export_df.index) + 1))
    export_df = export_df.rename(
        columns={
            "最新基金类型": "基金类型",
            "最新风险等级": "风险等级",
            "最新成立日期": "成立日期",
            "最新基金规模(亿)": "基金规模(亿)",
            "最新基金经理": "基金经理",
            "最新项目推荐结论": "项目推荐结论",
            "最新推荐理由": "推荐理由",
            "最新推荐备注": "推荐备注",
            "最新持有期状态": "持有期状态",
            "最新优选类别": "优选类别",
            "最新是否可申购": "是否可申购",
            "最新申购状态": "申购状态",
            "最新基准归类": "基准归类",
            "最新经理从业与评分状态": "经理从业与评分状态",
            "最新经理最短从业天数": "经理最短从业天数",
            "最新经理平均综合评分": "经理平均综合评分",
            "最新经理评分覆盖": "经理评分覆盖",
            "最新经理从业数据覆盖": "经理从业数据覆盖",
            "最新规模数据状态": "规模数据状态",
            "最新规模数据账龄(天)": "规模数据账龄(天)",
            "最新是否含权益/转债增强": "是否含权益/转债增强",
            "最新费率合计(%)": "费率合计(%)",
            "最新近1周(%)": "近1周(%)",
            "最新近1月(%)": "近1月(%)",
            "最新近3月(%)": "近3月(%)",
            "最新近6月(%)": "近6月(%)",
            "最新近1年(%)": "近1年(%)",
            "最新近2年(%)": "近2年(%)",
            "最新近3年(%)": "近3年(%)",
            "最新今年来(%)": "今年来(%)",
            "最新成立来(%)": "成立来(%)",
            "最新波动率": "1年-波动率",
            "最新最大回撤(%)": "1年-最大回撤(%)",
            "最新年化收益(%)": "1年-年化收益(%)",
            "最新夏普比率": "1年-夏普比率",
            "最新索提诺比率": "1年-索提诺比率",
            "最新卡玛比率": "1年-卡玛比率",
            "最新VaR95(%)": "1年-VaR95(%)",
            "最新VaR99(%)": "1年-VaR99(%)",
            "最新正收益比率": "1年-正收益比率",
            "最新日增长率(%)": "最新日增长率(%)",
            "最新净值日期": "最新净值日期",
            "最新单位净值": "最新单位净值",
            "最新累计净值": "最新累计净值",
            "最新1年-最长回撤观测数": "1年-最长回撤观测数",
            "最新综合加权得分": "综合加权得分",
            "最新近期得分": "近期得分",
            "最新长期得分": "长期得分",
            **{f"最新{column}": column for column in DATA_VALIDATION_COLUMNS},
            "最新AutoStock校验状态": "AutoStock校验状态",
            "最新AutoStock校验备注": "AutoStock校验备注",
            "最新AutoStock原始校验状态": "AutoStock原始校验状态",
            "最新AutoStock告警原因": "AutoStock告警原因",
            "最新长期稳定性标签": "长期稳定性标签",
            "最新母基金名称": "母基金名称",
            "最新关联份额数": "关联份额数",
            "最新关联份额名称": "关联份额名称",
            "最新关联份额代码": "关联份额代码",
        }
    )
    return _select_unique_columns(export_df, PROJECT_TRACKER_EXPORT_COLUMNS)


def _build_project_tracker_summary_items(summary: dict[str, Any]) -> list[tuple[str, Any]]:
    return [
        (
            label,
            summary.get(key) if summary.get(key) not in (None, "") else (0 if "count" in key or "period" in key else ""),
        )
        for label, key in PROJECT_TRACKER_SUMMARY_ITEMS
    ]


def _find_project_tracker_header_row(ws) -> int:
    max_scan_row = min(ws.max_row, 12)
    for row_idx in range(1, max_scan_row + 1):
        row_values = {
            str(ws.cell(row=row_idx, column=col_idx).value or "").strip()
            for col_idx in range(1, ws.max_column + 1)
        }
        if PROJECT_TRACKER_HEADER_REQUIRED_COLUMNS.issubset(row_values):
            return row_idx
    return 1


def _project_tracker_sheet_counts(ws) -> tuple[int | None, dict[str, int] | None]:
    """Count actual code rows, preserving an unreadable history as unknown."""
    if ws is None:
        return None, None
    header_row = _find_project_tracker_header_row(ws)
    headers = {
        str(ws.cell(header_row, column).value or "").strip(): column
        for column in range(1, ws.max_column + 1)
    }
    code_column = headers.get("基金代码")
    if code_column is None:
        return None, None
    group_column = headers.get(FORMAL_GROUP_LABEL)
    groups = {STABLE_BOND_GROUP_LABEL: 0, ENHANCED_BOND_GROUP_LABEL: 0} if group_column else None
    record_count = 0
    for row in range(header_row + 1, ws.max_row + 1):
        fund_code = _normalize_code(ws.cell(row, code_column).value)
        if len(fund_code) != 6 or not fund_code.isdigit():
            continue
        record_count += 1
        if groups is not None:
            group = str(ws.cell(row, group_column).value or "").strip()
            if group not in groups:
                groups = None
            else:
                groups[group] += 1
    return record_count, groups


def _write_project_tracker_reading_note(ws) -> None:
    record_count, _ = _project_tracker_sheet_counts(ws)
    history_sheet = ws.parent["完整数据"] if "完整数据" in ws.parent.sheetnames else None
    history_count, history_groups = _project_tracker_sheet_counts(history_sheet)
    count_text = "未知" if record_count is None else str(record_count)
    if ws.title == "完整数据":
        note = f"阅读说明：本表为历史累计（含已离榜），本表实际记录数：{count_text}。"
        note += "当前在榜及两个分组工作表仅展示当期正式在榜；历史曾入榜不代表本期推荐。"
    else:
        scope = "当期全部正式在榜" if ws.title == "当前在榜" else f"当期{ws.title}正式在榜"
        note = f"阅读说明：本表为{scope}，本表实际记录数：{count_text}；"
        if ws.title in {STABLE_BOND_GROUP_LABEL, ENHANCED_BOND_GROUP_LABEL}:
            historical = None if history_groups is None else history_groups[ws.title]
            note += f"本组历史记录数：{'未知' if historical is None else historical}。"
        else:
            historical = history_count
            note += f"历史累计记录数：{'未知' if history_count is None else history_count}。"
        note += "A:X为全表累计摘要。"
        if record_count == 0:
            note += "本期暂无符合全部发布条件的基金；历史曾入榜不代表本期推荐，可在完整数据按正式榜分组查询。"
        else:
            note += "历史曾入榜不代表本期推荐。"
        visible_scope = "当期全部正式在榜" if ws.title == "当前在榜" else f"当期{ws.title}"
        visible_count = "数量未知" if record_count is None else f"{record_count}只"
        visible_history = "历史记录数未知" if historical is None else f"历史{historical}只"
        ws.cell(2, 1, f"跟踪信息｜{visible_scope} {visible_count}（{visible_history}，详见完整数据）")

    ws.cell(1, 1).comment = Comment(note, "Fund-ANA")
    start_column = len(PROJECT_TRACKER_SUMMARY_ITEMS) * 2 + 1
    if ws.max_column < start_column:
        return  # A compact legacy workbook retains its existing column contract.
    ws.merge_cells(start_row=1, start_column=start_column, end_row=1, end_column=ws.max_column)
    cell = ws.cell(1, start_column, note)
    cell.font = Font(name="微软雅黑", size=10, color="1C4587")
    cell.fill = PatternFill(start_color="EAF2F8", end_color="EAF2F8", fill_type="solid")
    cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)


def _refresh_project_tracker_summary_row(ws, *, summary: dict[str, Any]) -> bool:
    header_row = _find_project_tracker_header_row(ws)
    if header_row != 3:
        return False

    summary_fill = PatternFill(start_color="D9EAD3", end_color="D9EAD3", fill_type="solid")
    summary_value_fill = PatternFill(start_color="F3F8ED", end_color="F3F8ED", fill_type="solid")
    summary_label_font = Font(name="微软雅黑", size=10, bold=True, color="274E13")
    summary_value_font = Font(name="微软雅黑", size=10, bold=True, color="1C4587")
    normal_font = Font(name="微软雅黑", size=10, color="000000")
    center = Alignment(horizontal="center", vertical="center")
    thin_border = Border(
        left=Side(style="thin", color="B4B4B4"),
        right=Side(style="thin", color="B4B4B4"),
        top=Side(style="thin", color="B4B4B4"),
        bottom=Side(style="thin", color="B4B4B4"),
    )

    for merged_range in list(ws.merged_cells.ranges):
        if merged_range.min_row == merged_range.max_row == 1:
            ws.unmerge_cells(str(merged_range))
    max_col = ws.max_column
    for col_idx in range(1, max_col + 1):
        cell = ws.cell(row=1, column=col_idx)
        cell.value = None
        cell.fill = PatternFill(fill_type=None)
        cell.font = normal_font
        cell.alignment = center
        cell.border = thin_border

    for idx, (label, value) in enumerate(_build_project_tracker_summary_items(summary), start=1):
        label_col = (idx - 1) * 2 + 1
        value_col = label_col + 1
        ws.cell(row=1, column=label_col, value=label)
        ws.cell(row=1, column=value_col, value=value)
        ws.cell(row=1, column=label_col).fill = summary_fill
        ws.cell(row=1, column=label_col).font = summary_label_font
        ws.cell(row=1, column=value_col).fill = summary_value_fill
        ws.cell(row=1, column=value_col).font = summary_value_font
        ws.cell(row=1, column=label_col).alignment = center
        ws.cell(row=1, column=value_col).alignment = center
        ws.cell(row=1, column=label_col).border = thin_border
        ws.cell(row=1, column=value_col).border = thin_border

    _write_project_tracker_reading_note(ws)
    ws.row_dimensions[1].height = 22
    return True


def _apply_project_tracker_workbook_presentation(
    path: Path,
    *,
    summary: dict[str, Any],
) -> None:
    wb = load_workbook(path)
    try:
        summary_fill = PatternFill(start_color="D9EAD3", end_color="D9EAD3", fill_type="solid")
        summary_value_fill = PatternFill(start_color="F3F8ED", end_color="F3F8ED", fill_type="solid")
        group_font = Font(name="微软雅黑", size=11, bold=True, color="333333")
        summary_label_font = Font(name="微软雅黑", size=10, bold=True, color="274E13")
        summary_value_font = Font(name="微软雅黑", size=10, bold=True, color="1C4587")
        header_font = Font(name="微软雅黑", size=11, bold=True, color="FFFFFF")
        normal_font = Font(name="微软雅黑", size=10, color="000000")
        positive_font = Font(name="微软雅黑", size=10, color="FF0000", bold=True)
        negative_font = Font(name="微软雅黑", size=10, color="008000", bold=True)
        header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
        top_decile_fill = PatternFill(start_color="F6DDD7", end_color="F6DDD7", fill_type="solid")
        center = Alignment(horizontal="center", vertical="center")
        left = Alignment(horizontal="left", vertical="center")
        thin_border = Border(
            left=Side(style="thin", color="B4B4B4"),
            right=Side(style="thin", color="B4B4B4"),
            top=Side(style="thin", color="B4B4B4"),
            bottom=Side(style="thin", color="B4B4B4"),
        )
        tracker_sheet_names = ["完整数据", "当前在榜", STABLE_BOND_GROUP_LABEL, ENHANCED_BOND_GROUP_LABEL]
        summary_items = _build_project_tracker_summary_items(summary)

        def _clear_existing_group_row(ws, row_idx: int) -> None:
            merged_ranges = list(ws.merged_cells.ranges)
            for merged_range in merged_ranges:
                if merged_range.min_row <= row_idx <= merged_range.max_row:
                    ws.unmerge_cells(str(merged_range))
            for col_idx in range(1, ws.max_column + 1):
                cell = ws.cell(row=row_idx, column=col_idx)
                cell.value = None
                cell.fill = PatternFill(fill_type=None)
                cell.font = normal_font
                cell.alignment = center
                cell.border = thin_border

        def _reset_conditional_formatting(ws) -> None:
            ws.conditional_formatting._cf_rules.clear()

        for sheet_name in tracker_sheet_names:
            if sheet_name not in wb.sheetnames:
                continue
            ws = wb[sheet_name]
            header_row = _find_project_tracker_header_row(ws)
            if header_row > 3:
                ws.delete_rows(1, header_row - 3)
                header_row = 3
            elif header_row < 3:
                ws.insert_rows(1, amount=3 - header_row)
                header_row = 3

            _clear_existing_group_row(ws, 2)
            max_col = ws.max_column
            max_row = ws.max_row
            data_start_row = header_row + 1

            _clear_existing_group_row(ws, 1)

            for idx, (label, value) in enumerate(summary_items, start=1):
                label_col = (idx - 1) * 2 + 1
                value_col = label_col + 1
                ws.cell(row=1, column=label_col, value=label)
                ws.cell(row=1, column=value_col, value=value)
                ws.cell(row=1, column=label_col).fill = summary_fill
                ws.cell(row=1, column=label_col).font = summary_label_font
                ws.cell(row=1, column=value_col).fill = summary_value_fill
                ws.cell(row=1, column=value_col).font = summary_value_font
                ws.cell(row=1, column=label_col).alignment = center
                ws.cell(row=1, column=value_col).alignment = center
                ws.cell(row=1, column=label_col).border = thin_border
                ws.cell(row=1, column=value_col).border = thin_border

            header_map = {
                str(ws.cell(row=header_row, column=col_idx).value or ""): col_idx
                for col_idx in range(1, max_col + 1)
            }

            left_idx = header_map.get("基金名称")
            growth_cols = [
                idx
                for header, idx in header_map.items()
                if "(%)" in str(header)
                and any(keyword in str(header) for keyword in ["增长", "收益", "年", "月", "周", "来", "回撤", "VaR"])
            ]

            for col_idx in range(1, max_col + 1):
                cell = ws.cell(row=header_row, column=col_idx)
                cell.fill = header_fill
                cell.font = header_font
                cell.alignment = center
                cell.border = thin_border

            for title, columns, fill_color in PROJECT_TRACKER_GROUP_HEADER_SCHEMES:
                matched_cols = [header_map[column] for column in columns if column in header_map]
                if not matched_cols:
                    continue
                start_col = min(matched_cols)
                end_col = max(matched_cols)
                ws.merge_cells(start_row=2, start_column=start_col, end_row=2, end_column=end_col)
                group_cell = ws.cell(row=2, column=start_col, value=title)
                group_cell.fill = PatternFill(start_color=fill_color, end_color=fill_color, fill_type="solid")
                group_cell.font = group_font
                group_cell.alignment = center
                for col_idx in range(start_col, end_col + 1):
                    ws.cell(row=2, column=col_idx).border = thin_border
            for col_idx in range(1, max_col + 1):
                if ws.cell(row=2, column=col_idx).value in (None, ""):
                    ws.cell(row=2, column=col_idx).border = thin_border
                    ws.cell(row=2, column=col_idx).alignment = center

            _write_project_tracker_reading_note(ws)
            for row_idx in range(data_start_row, max_row + 1):
                for col_idx in range(1, max_col + 1):
                    cell = ws.cell(row=row_idx, column=col_idx)
                    cell.border = thin_border
                    cell.alignment = left if left_idx and col_idx == left_idx else center
                    cell.font = normal_font
                    if col_idx in growth_cols and cell.value not in (None, ""):
                        try:
                            value = float(str(cell.value).replace("%", "").replace(",", "").strip())
                            if value > 0:
                                cell.font = positive_font
                            elif value < 0:
                                cell.font = negative_font
                        except (TypeError, ValueError):
                            cell.font = normal_font

            ws.freeze_panes = "D4"
            ws.auto_filter.ref = f"A3:{get_column_letter(max_col)}{max_row}"
            ws.auto_filter.sortState = None
            ws.row_dimensions[1].height = 22
            ws.row_dimensions[2].height = 22
            ws.row_dimensions[3].height = 22

            _reset_conditional_formatting(ws)

            if max_row < data_start_row:
                continue

            for sort_header, descending in PROJECT_TRACKER_DEFAULT_SORT_COLUMNS:
                sort_col_idx = header_map.get(sort_header)
                if sort_col_idx:
                    sort_col_letter = get_column_letter(sort_col_idx)
                    ws.auto_filter.add_sort_condition(
                        f"{sort_col_letter}{data_start_row}:{sort_col_letter}{max_row}",
                        descending=descending,
                    )

            for header in ["近1月(%)", "近3月(%)", "1年-年化收益(%)", "1年-夏普比率"]:
                col_idx = header_map.get(header)
                if col_idx:
                    col_letter = get_column_letter(col_idx)
                    ws.conditional_formatting.add(
                        f"{col_letter}{data_start_row}:{col_letter}{max_row}",
                        ColorScaleRule(
                            start_type="min",
                            start_color="63BE7B",
                            mid_type="percentile",
                            mid_value=50,
                            mid_color="FFEB84",
                            end_type="max",
                            end_color="F8696B",
                        ),
                    )

            for header in ["1年-波动率", "1年-最大回撤(%)"]:
                col_idx = header_map.get(header)
                if col_idx:
                    col_letter = get_column_letter(col_idx)
                    ws.conditional_formatting.add(
                        f"{col_letter}{data_start_row}:{col_letter}{max_row}",
                        ColorScaleRule(
                            start_type="min",
                            start_color="F8696B",
                            mid_type="percentile",
                            mid_value=50,
                            mid_color="FFEB84",
                            end_type="max",
                            end_color="63BE7B",
                        ),
                    )

            for header in ["正式投资评分", "出现次数", "出现频率(%)", "基金规模(亿)"]:
                col_idx = header_map.get(header)
                if col_idx:
                    col_letter = get_column_letter(col_idx)
                    ws.conditional_formatting.add(
                        f"{col_letter}{data_start_row}:{col_letter}{max_row}",
                        DataBarRule(start_type="min", end_type="max", color="5B9BD5", showValue=True),
                    )

            for header, direction in PROJECT_TRACKER_TOP_DECILE_RULES.items():
                col_idx = header_map.get(header)
                if not col_idx:
                    continue
                numeric_values: list[tuple[int, float]] = []
                for row_idx in range(data_start_row, max_row + 1):
                    raw = ws.cell(row=row_idx, column=col_idx).value
                    if raw in (None, ""):
                        continue
                    try:
                        value = float(str(raw).replace("%", "").replace(",", "").strip())
                    except (TypeError, ValueError):
                        continue
                    numeric_values.append((row_idx, value))
                if not numeric_values:
                    continue
                sorted_values = sorted(
                    numeric_values,
                    key=lambda item: item[1],
                    reverse=str(direction).strip().lower() == "desc",
                )
                threshold = sorted_values[max(0, math.ceil(len(sorted_values) * 0.1) - 1)][1]
                for row_idx, value in numeric_values:
                    if (
                        str(direction).strip().lower() == "desc" and value >= threshold
                    ) or (
                        str(direction).strip().lower() == "asc" and value <= threshold
                    ):
                        ws.cell(row=row_idx, column=col_idx).fill = top_decile_fill

        if "处理记录" in wb.sheetnames:
            process_ws = wb["处理记录"]
            process_ws.freeze_panes = "A2"
            process_ws.auto_filter.ref = process_ws.dimensions
    finally:
        wb.save(path)
        wb.close()


def _update_project_tracker_workbook_for_duplicate_snapshot(
    path: Path,
    *,
    process_df: pd.DataFrame,
    summary: dict[str, Any],
) -> bool:
    if not path.exists():
        return False
    wb = load_workbook(path)
    try:
        tracker_sheet_names = ["完整数据", "当前在榜", STABLE_BOND_GROUP_LABEL, ENHANCED_BOND_GROUP_LABEL]
        partial_refresh_ok = True
        for sheet_name in tracker_sheet_names:
            if sheet_name not in wb.sheetnames:
                partial_refresh_ok = False
                break
            if not _refresh_project_tracker_summary_row(wb[sheet_name], summary=summary):
                partial_refresh_ok = False
                break
        _replace_sheet_with_style(
            wb,
            title="处理记录",
            df=process_df,
            left_align_column="来源文件",
            column_widths={
                "业务日期": 12,
                "项目批次": 20,
                "生成时间": 20,
                "正式推荐数量": 12,
                "来源文件": 40,
            },
        )
        wb.save(path)
    finally:
        wb.close()
    if not partial_refresh_ok:
        _apply_project_tracker_workbook_presentation(path, summary=summary)
    return True
