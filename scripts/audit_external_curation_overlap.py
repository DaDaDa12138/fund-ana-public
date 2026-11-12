"""外部候选交集审计脚本（P1-09.4 / B14）。

比对"天天基金 CLTYPE=038 优质债券候选池 + 三机构 4/5 星评级"与项目 ``final_count``
的交集，输出 JSON 到 ``reports/audit/external_curation_overlap/*.json``。

该脚本只读本地 DB + 最新发布的 ``reports/latest.json``，不发任何上游 API 请求；
CLTYPE=038 和三机构评级都由 ``bond_universe_sync`` / ``supplement_fund_grade`` 之前
的采集阶段写入 ``funds.external_curation_flag`` 与 ``fund_grades`` 表。

用途：
- 作为 P1-07/08 动态阈值调整的策略对标参考。
- 帮助观察"官方认可的优质样本"有多少进入项目正式主榜，高交集说明
  项目筛选与行业口径一致；低交集提示门槛设计偏差。

CLI：
    python3 scripts/audit_external_curation_overlap.py
    python3 scripts/audit_external_curation_overlap.py --min-star 4
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent
VENV_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"
_VENV_BOOTSTRAP_ENV = "BOND_SCREENING_VENV_BOOTSTRAPPED"


def _ensure_project_venv() -> None:
    if not VENV_PYTHON.exists():
        return
    current_prefix = Path(getattr(sys, "prefix", "") or "").resolve()
    target_prefix = (PROJECT_ROOT / ".venv").resolve()
    if current_prefix == target_prefix:
        return
    attempted_prefix = os.environ.get(_VENV_BOOTSTRAP_ENV, "")
    if attempted_prefix == str(target_prefix):
        return
    if Path(sys.executable) != VENV_PYTHON:
        os.environ[_VENV_BOOTSTRAP_ENV] = str(target_prefix)
        os.execv(str(VENV_PYTHON), [str(VENV_PYTHON), *sys.argv])

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.config import load_settings  # noqa: E402
from backend.runtime_replay import resolve_runtime_artifact_path  # noqa: E402
from scripts.audit_runtime_utils import (  # noqa: E402
    build_audit_artifact_path as _build_audit_artifact_path,
    resolve_run_payload as _resolve_run_payload,
)


def _safe_int_grade(value: Any) -> int | None:
    try:
        return int(str(value or "").strip())
    except (TypeError, ValueError):
        return None


def _load_curated_codes(db_path: Path, flag: str) -> set[str]:
    conn = sqlite3.connect(str(db_path))
    try:
        # V6: funds / external_curation_flag 列不存在时返回空集合（冷启动）
        table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='funds'"
        ).fetchone()
        if not table:
            return set()
        cols = {row[1] for row in conn.execute("PRAGMA table_info(funds)").fetchall()}
        if "external_curation_flag" not in cols:
            return set()
        rows = conn.execute(
            "SELECT fcode, external_curation_flag FROM funds WHERE external_curation_flag IS NOT NULL "
            "AND TRIM(external_curation_flag) <> ''"
        ).fetchall()
    finally:
        conn.close()
    codes: set[str] = set()
    for row in rows:
        fcode = str(row[0] or "").strip()
        raw = str(row[1] or "").strip()
        flags = [item for item in raw.split("+") if item]
        if fcode and flag in flags:
            codes.add(fcode)
    return codes


def _load_high_star_codes_from_fund_grades(db_path: Path, min_star: int) -> dict[str, dict[str, Any]]:
    """取每只基金最新一期的 fund_grades，若 max(ZS, SH, JA, HT) >= min_star 则收录。

    V6: ``fund_grades`` 表不存在时优雅返回空字典（新部署场景 / supplement_fund_grade
    从未跑过），让 audit 脚本仍能输出 0 交集的审计 JSON。
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        table_exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='fund_grades'"
        ).fetchone()
        if not table_exists:
            return {}
        rows = conn.execute(
            """
            SELECT fg.fcode, fg.rdate, fg.zspj_zs, fg.szpj3, fg.japj, fg.htpj
            FROM fund_grades fg
            JOIN (
                SELECT fcode, MAX(rdate) AS latest FROM fund_grades GROUP BY fcode
            ) latest_fg ON latest_fg.fcode = fg.fcode AND latest_fg.latest = fg.rdate
            """
        ).fetchall()
    finally:
        conn.close()

    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        stars = [
            _safe_int_grade(row["zspj_zs"]),
            _safe_int_grade(row["szpj3"]),
            _safe_int_grade(row["japj"]),
            _safe_int_grade(row["htpj"]),
        ]
        meaningful = [s for s in stars if s is not None]
        if not meaningful:
            continue
        peak = max(meaningful)
        if peak < min_star:
            continue
        result[str(row["fcode"])] = {
            "rdate": str(row["rdate"] or ""),
            "zs": stars[0],
            "sh": stars[1],
            "ja": stars[2],
            "ht": stars[3],
            "peak": peak,
            "source_mode": "fund_grades_latest_max",
        }
    return result


def _load_high_star_codes_from_funds_table(db_path: Path, min_star: int) -> dict[str, dict[str, Any]]:
    """回退读取 `funds.rating_shanghai`。

    当前仓库里 `fund_grades` 可能为空，但 `funds.rating_shanghai` 已有覆盖。
    该回退不等于“三机构最大星级”，只是基于当前稳定可得字段提供单源外部评级参考，
    避免审计脚本因为空表而直接输出 0 覆盖的误导结果。
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='funds'"
        ).fetchone()
        if not table:
            return {}
        cols = {row[1] for row in conn.execute("PRAGMA table_info(funds)").fetchall()}
        if "rating_shanghai" not in cols:
            return {}
        rows = conn.execute(
            """
            SELECT fcode, rating_shanghai
            FROM funds
            WHERE rating_shanghai IS NOT NULL AND TRIM(CAST(rating_shanghai AS TEXT)) <> ''
            """
        ).fetchall()
    finally:
        conn.close()

    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        peak = _safe_int_grade(row["rating_shanghai"])
        if peak is None or peak < min_star:
            continue
        result[str(row["fcode"])] = {
            "rdate": "",
            "zs": None,
            "sh": peak,
            "ja": None,
            "ht": None,
            "peak": peak,
            "source_mode": "funds.rating_shanghai_fallback",
        }
    return result


def _load_high_star_codes(db_path: Path, min_star: int) -> tuple[dict[str, dict[str, Any]], str]:
    primary = _load_high_star_codes_from_fund_grades(db_path, min_star)
    if primary:
        return primary, "fund_grades_latest_max"
    fallback = _load_high_star_codes_from_funds_table(db_path, min_star)
    if fallback:
        return fallback, "funds.rating_shanghai_fallback"
    return {}, "unavailable"


def _resolve_project_artifact_path(path_text: Any) -> Path | None:
    text = str(path_text or "").strip()
    if not text:
        return None
    path = Path(text)
    if path.is_absolute():
        return path
    return (PROJECT_ROOT / path).resolve()


def _load_codes_from_filtered_csv(path: Path) -> set[str]:
    codes: set[str] = set()
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if not isinstance(row, dict):
                continue
            fcode = str(row.get("基金代码") or row.get("fcode") or "").strip()
            if fcode:
                codes.add(fcode)
    return codes


def _load_final_codes(
    payload: dict[str, Any],
    artifacts: dict[str, Any],
    *,
    project_root: Path,
) -> tuple[set[str], str, str, str]:
    filtered_csv_path = resolve_runtime_artifact_path(
        artifacts.get("filtered_csv"),
        project_root=project_root,
    )
    if filtered_csv_path and filtered_csv_path.exists():
        try:
            return (
                _load_codes_from_filtered_csv(filtered_csv_path),
                str(payload.get("run_id") or ""),
                "run_local_filtered_csv",
                str(filtered_csv_path),
            )
        except Exception:
            pass

    final_preview = payload.get("final_preview") or []
    codes: set[str] = set()
    for item in final_preview:
        if isinstance(item, dict):
            fc = str(item.get("基金代码") or item.get("fcode") or "").strip()
            if fc:
                codes.add(fc)
    run_id = str(payload.get("run_id") or "")
    return codes, run_id, "runtime_payload_final_preview_fallback", ""


def _build_auxiliary_scope(run_id: str, latest_canonical_run_id: str) -> tuple[str, str, str]:
    if run_id == latest_canonical_run_id:
        return (
            "same_run_current_db_state",
            "latest + 当前库状态",
            "正式主榜来自 latest canonical run；外部精选标记与评级来自当前数据库状态。当前输出可用于 latest 同期审计，但项目尚未保留 run-local 外部参考快照。",
        )
    return (
        "cross_run_current_db_state",
        "历史终榜 + 当前库状态",
        "正式主榜来自历史 run；外部精选标记与评级仍来自当前数据库状态。该结果只能视为跨期近似对比，不能当作历史精确复盘。",
    )


def _project_display_path(path: Path, project_root: Path) -> str:
    try:
        return str(path.relative_to(project_root))
    except ValueError:
        return str(path)


def _overlap_summary(label: str, source: set[str], target: set[str]) -> dict[str, Any]:
    inter = source & target
    return {
        "label": label,
        "source_count": len(source),
        "target_count": len(target),
        "intersection_count": len(inter),
        "intersection_ratio_vs_source": round(len(inter) / len(source), 4) if source else 0.0,
        "intersection_ratio_vs_target": round(len(inter) / len(target), 4) if target else 0.0,
        "source_only_sample": sorted(source - target)[:20],
        "target_only_sample": sorted(target - source)[:20],
        "intersection_sample": sorted(inter)[:20],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="P1-09.4: 外部候选交集审计")
    parser.add_argument("--run-id", default="", help="指定 run_id；默认读取 latest canonical。")
    parser.add_argument(
        "--flag",
        default="tiantian_038",
        help="要对比的 external_curation_flag 值（默认 tiantian_038）",
    )
    parser.add_argument(
        "--min-star",
        type=int,
        default=4,
        help="三机构评级最大值的下限（默认 4，表示 4 星或 5 星）",
    )
    parser.add_argument("--output", default=None, help="输出 JSON 路径（默认写到 reports/audit/external_curation_overlap/）")
    args = parser.parse_args(argv)

    settings = load_settings()
    project_root = Path(getattr(settings, "project_root", PROJECT_ROOT))
    db_path = Path(settings.database_path)
    try:
        latest_canonical_run_id, payload, artifacts, replay_precision = _resolve_run_payload(
            settings,
            project_root=project_root,
        )
        run_id, payload, artifacts, replay_precision = _resolve_run_payload(
            settings,
            run_id=args.run_id,
            project_root=project_root,
        )
    except RuntimeError as exc:
        # The audit is auxiliary and must also be usable on a cold deployment
        # before the first formal publication.  Emit a valid empty result so
        # callers can distinguish "no publication yet" from a script failure.
        if not args.run_id and "latest" in str(exc).lower():
            latest_canonical_run_id = ""
            payload = {"run_id": "", "final_preview": []}
            artifacts = {}
            replay_precision = {
                "status": "reference_only",
                "status_label": "暂无正式发布，等待首个运行",
            }
        else:
            print(
                json.dumps(
                    {
                        "passed": False,
                        "error": str(exc),
                        "latest_report_file": str(settings.latest_report_file),
                        "requested_run_id": str(args.run_id or "").strip(),
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 1

    curated = _load_curated_codes(db_path, args.flag)
    high_star_map, high_star_source_mode = _load_high_star_codes(db_path, args.min_star)
    high_star_codes = set(high_star_map.keys())
    final_codes, run_id, final_source_mode, final_source_path = _load_final_codes(
        payload,
        artifacts,
        project_root=project_root,
    )
    auxiliary_scope, auxiliary_scope_label, scope_detail = _build_auxiliary_scope(
        run_id,
        latest_canonical_run_id,
    )

    summaries = [
        _overlap_summary("curated_038_vs_final", curated, final_codes),
        _overlap_summary("high_star_vs_final", high_star_codes, final_codes),
        _overlap_summary("curated_038_vs_high_star", curated, high_star_codes),
    ]

    triangulated = curated & high_star_codes & final_codes

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "run_id": run_id,
        "latest_canonical_run_id": latest_canonical_run_id,
        "flag": args.flag,
        "min_star": int(args.min_star),
        "replay_precision": replay_precision,
        "replay_precision_status": str(replay_precision.get("status") or "").strip(),
        "replay_precision_status_label": str(replay_precision.get("status_label") or "").strip(),
        "auxiliary_scope": auxiliary_scope,
        "auxiliary_scope_label": auxiliary_scope_label,
        "run_local_external_reference_snapshot_available": False,
        "historical_exact_supported": False,
        "scope_detail": scope_detail,
        "high_star_source_mode": high_star_source_mode,
        "high_star_source_count": len(high_star_codes),
        "production_strategy_eligible": False,
        "strategy_use_warning": (
            "外部精选/评级结果仅用于审计辅助与 sanity check，不得直接驱动生产筛选阈值；"
            "当前 high_star 还可能退化为 funds.rating_shanghai 单源 fallback。"
        ),
        "final_source_mode": final_source_mode,
        "final_source_path": final_source_path,
        "final_code_count": len(final_codes),
        "summaries": summaries,
        "triangulated_count": len(triangulated),
        "triangulated_sample": sorted(triangulated)[:30],
    }

    output_path = Path(args.output) if args.output else (
        _build_audit_artifact_path(
            Path(settings.reports_dir) / "audit" / "external_curation_overlap",
            "external_curation_overlap",
            run_id=run_id,
        )
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    report["artifact_path"] = str(output_path)

    print(
        json.dumps(
            {
                "passed": True,
                "run_id": run_id,
                "artifact_path": str(output_path),
                "artifact_project_path": _project_display_path(output_path, project_root),
                "replay_precision_status": report["replay_precision_status"],
                "replay_precision_status_label": report["replay_precision_status_label"],
                "auxiliary_scope": report["auxiliary_scope"],
                "auxiliary_scope_label": report["auxiliary_scope_label"],
                "final_source_mode": report["final_source_mode"],
                "triangulated_count": report["triangulated_count"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    _ensure_project_venv()
    raise SystemExit(main())
