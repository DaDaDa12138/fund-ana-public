"""刷新 runtime overrides 中的 ``risk_free_rate``（P1-03 / B7.1）。

用例：
- 手工季度更新（推荐路径）：
    python3 scripts/refresh_risk_free_rate.py --rate 0.019 --source "manual CMT 10Y 2026Q2"
- 观察现值（dry-run）：
    python3 scripts/refresh_risk_free_rate.py --show
- 周调度自动校准（接入 ``scheduler.weekly_maintenance``）：本 CLI 会把上一次运维填写的值
  和 ``core.bond_fund_screening.config.RISK_FREE_RATE`` 基线做漂移比较，如果超过 ``--alert-bps``
  （默认 25bp）就在 stdout 输出机器可读的告警，供调度层写入维护摘要。

注意：
- 默认仍支持人工显式写入；
- ``--try-api`` 现已通过中债收益率页面的真实会话接口抓取 10Y 国债到期收益率；
- 若真实接口访问失败，本脚本只返回错误并保留现值，不会写入伪造 fallback。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import re
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
VENV_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"
if VENV_PYTHON.exists() and Path(sys.executable) != VENV_PYTHON:
    os.execv(str(VENV_PYTHON), [str(VENV_PYTHON), *sys.argv])

sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "core"))

import requests


def _import_runtime_overrides():
    from bond_fund_screening.runtime_overrides import (  # type: ignore
        get_risk_free_rate,
        refresh,
        write_risk_free_rate,
    )
    from bond_fund_screening.config import RISK_FREE_RATE  # type: ignore

    return get_risk_free_rate, refresh, write_risk_free_rate, float(RISK_FREE_RATE)


def _fetch_from_api() -> float | None:
    """从中债收益率页面抓取 10Y 国债到期收益率。"""
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://yield.chinabond.com.cn/cbweb-mn/yield_main?locale=zh_CN",
        }
    )
    base_url = "https://yield.chinabond.com.cn/cbweb-mn"
    yield_main_url = f"{base_url}/yield_main?locale=zh_CN"
    detail_url = (
        f"{base_url}/yc/ycDetail?"
        "ycDefIds=2c9081e50a2f9606010a3068cae70001&&"
        "zblx=txy&&workTime=&&dxbj=&&qxlx=&&yqqxN=&&yqqxK=&&"
        "wrjxCBFlag=0&&locale=zh_CN"
    )

    session.get(yield_main_url, timeout=20).raise_for_status()
    response = session.post(detail_url, timeout=20)
    response.raise_for_status()
    html = response.text
    match = re.search(
        r"<td>\s*10(?:\.0)?y\s*</td>\s*<td>\s*([0-9]+(?:\.[0-9]+)?)\s*</td>",
        html,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return float(match.group(1)) / 100.0


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="P1-03: 刷新无风险利率 runtime override")
    parser.add_argument("--rate", type=float, help="手工写入的年化利率（小数，如 0.019）")
    parser.add_argument("--source", default="manual", help="来源备注，写入 override 元数据")
    parser.add_argument(
        "--try-api",
        action="store_true",
        help="尝试从中债收益率页面抓取 10Y 国债收益率并写入 override",
    )
    parser.add_argument(
        "--alert-bps",
        type=float,
        default=25.0,
        help="漂移告警阈值（basis points，默认 25）",
    )
    parser.add_argument("--show", action="store_true", help="仅展示当前解析值，不写 override")
    args = parser.parse_args(argv)

    get_risk_free_rate, refresh, write_risk_free_rate, default_rate = _import_runtime_overrides()

    resolution = get_risk_free_rate(default_rate)
    baseline = default_rate

    report: dict = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "baseline": baseline,
        "current_value": resolution.value,
        "current_source": resolution.source,
        "current_updated_at": resolution.updated_at,
        "drift_bps": round(abs(resolution.value - baseline) * 10000, 2),
        "alert_bps_threshold": args.alert_bps,
        "alert_triggered": abs(resolution.value - baseline) * 10000 > args.alert_bps,
    }

    if args.show:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0

    new_value: float | None = None
    source_detail = args.source

    if args.rate is not None:
        new_value = float(args.rate)
    elif args.try_api:
        try:
            api_value = _fetch_from_api()
        except Exception as exc:
            report["error"] = "api_fetch_failed"
            report["error_detail"] = str(exc)
            print(json.dumps(report, ensure_ascii=False, indent=2))
            return 1
        if api_value is None:
            report["error"] = "api_value_unavailable"
            print(json.dumps(report, ensure_ascii=False, indent=2))
            return 1
        new_value = float(api_value)
        source_detail = "chinabond_10y_treasury_yield_curve"
    else:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0

    if new_value is None:
        report["error"] = "rate_unspecified"
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 1

    write_risk_free_rate(
        new_value,
        updated_at=datetime.now().isoformat(timespec="seconds"),
        source_detail=source_detail,
    )
    refresh()

    post = get_risk_free_rate(default_rate)
    report["post_value"] = post.value
    report["post_source"] = post.source
    report["post_updated_at"] = post.updated_at
    report["post_drift_bps"] = round(abs(post.value - baseline) * 10000, 2)
    report["post_alert_triggered"] = abs(post.value - baseline) * 10000 > args.alert_bps
    report["applied"] = True

    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
