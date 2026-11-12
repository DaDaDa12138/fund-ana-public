from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


_WATCHLIST_TOP_RE = re.compile(r"仅(?P<side>近期|长期)优质观察池_Top(?P<limit>\d+)_")
_FINAL_TOP_DISTRIBUTION_RE = re.compile(r"最终Top(?P<count>\d+)分布：")
_FINAL_POLICY_LOG_RE = re.compile(
    r"参数: top_n=(?P<top_n>\d+), short_weight=(?P<short>[-\d.]+), "
    r"long_weight=(?P<long>[-\d.]+), hard_bonus=(?P<hard>[-\d.]+), "
    r"soft_bonus=(?P<soft>[-\d.]+), (?P<rest>.+)"
)


def _normalize_string(value: str) -> str:
    text = str(value or "")
    if not text:
        return text

    text = _WATCHLIST_TOP_RE.sub(
        lambda match: f"仅{match.group('side')}优质观察池_{match.group('limit')}只_",
        text,
    )
    text = _FINAL_TOP_DISTRIBUTION_RE.sub(
        lambda match: f"最终主榜{match.group('count')}只分布：",
        text,
    )
    text = text.replace("TopN锚点外淘汰", "锚点分数带外淘汰")
    text = text.replace("TopN 截断", "主榜锚点截断")
    text = text.replace("TopN截断", "主榜锚点截断")
    text = text.replace("主榜采用 TopN 末位分数带扩容", "主榜采用锚点末位分数带扩容")
    text = text.replace("但因 TopN 截断未进入最终主榜", "但未达到最终主榜锚点")

    match = _FINAL_POLICY_LOG_RE.search(text)
    if match:
        top_n = int(match.group("top_n") or 0)
        anchor_limit = "None" if top_n >= 999999 else str(max(top_n, 1))
        replacement = (
            "参数: "
            f"mainlist_anchor_limit={anchor_limit}, watchlist_limit=20, "
            f"short_weight={match.group('short')}, "
            f"long_weight={match.group('long')}, "
            f"hard_bonus={match.group('hard')}, "
            f"soft_bonus={match.group('soft')}, "
            f"{match.group('rest')}"
        )
        text = f"{text[:match.start()]}{replacement}{text[match.end():]}"

    return text


def normalize_text_content(text: str) -> tuple[str, bool]:
    """归一普通文本内容中的历史口径。"""
    normalized = _normalize_string(text)
    return normalized, normalized != text


def normalize_snapshot_payload(payload: Any) -> tuple[Any, bool]:
    """递归归一状态/报告 JSON 中的历史口径字符串。"""
    changed = False

    def _walk(value: Any) -> Any:
        nonlocal changed
        if isinstance(value, dict):
            return {key: _walk(item) for key, item in value.items()}
        if isinstance(value, list):
            return [_walk(item) for item in value]
        if isinstance(value, str):
            normalized = _normalize_string(value)
            if normalized != value:
                changed = True
            return normalized
        return value

    normalized_payload = _walk(payload)
    return normalized_payload, changed


def rewrite_normalized_json_file(path: str | Path) -> bool:
    """就地重写单个 JSON 文件，清理历史口径字符串。"""
    file_path = Path(path)
    if not file_path.exists():
        return False

    try:
        payload = json.loads(file_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return False

    normalized_payload, changed = normalize_snapshot_payload(payload)
    if not changed:
        return False

    tmp_path = file_path.with_suffix(f"{file_path.suffix}.tmp")
    tmp_path.write_text(
        json.dumps(normalized_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    tmp_path.replace(file_path)
    return True


def rewrite_normalized_text_file(path: str | Path) -> bool:
    """就地重写普通文本文件中的历史口径。"""
    file_path = Path(path)
    if not file_path.exists():
        return False

    try:
        original = file_path.read_text(encoding="utf-8")
    except OSError:
        return False

    normalized, changed = normalize_text_content(original)
    if not changed:
        return False

    tmp_path = file_path.with_suffix(f"{file_path.suffix}.tmp")
    tmp_path.write_text(normalized, encoding="utf-8")
    tmp_path.replace(file_path)
    return True
