"""运行时配置覆盖加载器（P1-03 / B7.1）。

用途：允许通过运行时 overrides 动态覆盖部分"应季度审视但不适合硬编码"的参数，
例如 ``risk_free_rate``。主 ``config/project.yaml`` 保持稳定，overrides 只保存"运维周期性更新"
的数值与更新日期。

默认路径策略：
1. 首选可写共享路径 ``backend/data/runtime_overrides.yaml``，适配 Docker scheduler / web 共享运行态
2. 兼容读取旧路径 ``config/runtime_overrides.yaml``（只读配置卷场景下不会再写回）

读取优先级（`get_risk_free_rate()` 为例）：
1. runtime overrides 文件（优先 data 路径，其次 legacy config 路径）
2. 环境变量 ``BOND_FUND_RISK_FREE_RATE``（局部压测 / 冻结测试用）
3. 回退到 ``core.bond_fund_screening.config.RISK_FREE_RATE`` 常量

进程内缓存一次，读写可通过 ``refresh()`` 显式刷新——``scripts/refresh_risk_free_rate.py``
在写入 overrides 后会调用。
"""

from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover - yaml 缺失不应发生在正常部署
    yaml = None  # type: ignore


logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_DEFAULT_OVERRIDES_PATH = _PROJECT_ROOT / "backend" / "data" / "runtime_overrides.yaml"
_LEGACY_OVERRIDES_PATH = _PROJECT_ROOT / "config" / "runtime_overrides.yaml"

_LOCK = threading.Lock()
_CACHE_BY_PATH: dict[str, dict[str, Any]] = {}
_CACHED_MTIME_NS_BY_PATH: dict[str, int] = {}


@dataclass(slots=True)
class RiskFreeRateResolution:
    value: float
    source: str  # "runtime_overrides" | "env" | "default"
    updated_at: str | None


def _resolve_read_path(path: Path) -> Path:
    if path.exists():
        return path
    if path == _DEFAULT_OVERRIDES_PATH and _LEGACY_OVERRIDES_PATH.exists():
        return _LEGACY_OVERRIDES_PATH
    return path


def _load_overrides_dict(path: Path = _DEFAULT_OVERRIDES_PATH) -> dict[str, Any]:
    resolved_path = _resolve_read_path(path)
    cache_key = str(resolved_path)

    if not resolved_path.exists():
        return {}

    try:
        mtime_ns = resolved_path.stat().st_mtime_ns
    except OSError:
        return {}

    with _LOCK:
        cached_payload = _CACHE_BY_PATH.get(cache_key)
        cached_mtime_ns = _CACHED_MTIME_NS_BY_PATH.get(cache_key)
        if cached_payload is not None and cached_mtime_ns == mtime_ns:
            return dict(cached_payload)

        if yaml is None:  # pragma: no cover
            logger.warning("yaml 模块不可用，runtime_overrides 无法读取")
            return {}

        try:
            raw = yaml.safe_load(resolved_path.read_text(encoding="utf-8")) or {}
        except Exception as exc:  # pragma: no cover - 容错
            logger.error("解析 runtime_overrides 失败 %s: %s", resolved_path, exc)
            return {}

        if not isinstance(raw, dict):
            return {}

        payload = dict(raw)
        _CACHE_BY_PATH[cache_key] = payload
        _CACHED_MTIME_NS_BY_PATH[cache_key] = mtime_ns
        return dict(payload)


def refresh(path: Path = _DEFAULT_OVERRIDES_PATH) -> None:
    """显式丢弃缓存，强制下次调用时重新读取。脚本落盘后调用。"""
    resolved_path = _resolve_read_path(path)
    cache_key = str(resolved_path)
    with _LOCK:
        if path == _DEFAULT_OVERRIDES_PATH:
            _CACHE_BY_PATH.clear()
            _CACHED_MTIME_NS_BY_PATH.clear()
        else:
            _CACHE_BY_PATH.pop(cache_key, None)
            _CACHED_MTIME_NS_BY_PATH.pop(cache_key, None)


def get_risk_free_rate(
    default: float,
    *,
    overrides_path: Path = _DEFAULT_OVERRIDES_PATH,
) -> RiskFreeRateResolution:
    """按 overrides → env → default 的优先级解析风险利率。"""

    overrides = _load_overrides_dict(overrides_path)
    if "risk_free_rate" in overrides:
        rfr_node = overrides["risk_free_rate"]
        if isinstance(rfr_node, dict):
            try:
                value = float(rfr_node.get("value"))
                return RiskFreeRateResolution(
                    value=value,
                    source="runtime_overrides",
                    updated_at=str(rfr_node.get("updated_at") or "") or None,
                )
            except (TypeError, ValueError):
                pass
        else:
            try:
                return RiskFreeRateResolution(
                    value=float(rfr_node),
                    source="runtime_overrides",
                    updated_at=None,
                )
            except (TypeError, ValueError):
                pass

    env_value = os.environ.get("BOND_FUND_RISK_FREE_RATE")
    if env_value is not None:
        try:
            return RiskFreeRateResolution(
                value=float(env_value),
                source="env",
                updated_at=None,
            )
        except ValueError:
            pass

    return RiskFreeRateResolution(value=float(default), source="default", updated_at=None)


def write_risk_free_rate(
    value: float,
    *,
    updated_at: str,
    source_detail: str | None = None,
    overrides_path: Path = _DEFAULT_OVERRIDES_PATH,
) -> None:
    """刷新脚本在估算/人工更新后调用，原子写入 overrides。"""

    if yaml is None:  # pragma: no cover
        raise RuntimeError("yaml 模块不可用，无法写 runtime_overrides")

    overrides_path.parent.mkdir(parents=True, exist_ok=True)

    payload: dict[str, Any] = {}
    source_path = _resolve_read_path(overrides_path)
    if source_path.exists():
        try:
            loaded = yaml.safe_load(source_path.read_text(encoding="utf-8")) or {}
        except Exception:  # pragma: no cover
            loaded = {}
        if isinstance(loaded, dict):
            payload = dict(loaded)

    payload["risk_free_rate"] = {
        "value": float(value),
        "updated_at": str(updated_at),
        "source_detail": source_detail or "",
    }

    temp_file = overrides_path.with_suffix(f"{overrides_path.suffix}.tmp")
    temp_file.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    temp_file.replace(overrides_path)
    refresh(overrides_path)
