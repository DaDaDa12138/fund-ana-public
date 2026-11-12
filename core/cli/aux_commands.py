#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""辅助 CLI 命令实现。"""

import json
import sqlite3
from pathlib import Path

from utils import build_nav_snapshot_signature
from utils.calculation_capture import capture_call, capture_enabled, record_event
from utils.manager_collection_summary import validate_manager_collection_summary
from utils.holding_period_policy import HOLDING_PERIOD_LIMIT, combine_holding_period_names, holding_period_gate_reason


_RISK_PERIOD_DAYS = {
    "1y": 365,
    "3y": 365 * 3,
    "5y": 365 * 5,
}


def _resolve_risk_periods(config) -> list[str]:
    risk_config = (config or {}).get("risk_metrics", {}) or {}
    raw_periods = risk_config.get("periods", ["1y", "3y", "5y"]) or ["1y", "3y", "5y"]
    periods: list[str] = []
    for raw in raw_periods:
        period = str(raw or "").strip()
        if period in _RISK_PERIOD_DAYS and period not in periods:
            periods.append(period)
    return periods or ["1y", "3y", "5y"]


def _resolve_period_coverage_grace_days(config) -> int:
    risk_config = (config or {}).get("risk_metrics", {}) or {}
    try:
        return max(0, int(risk_config.get("period_coverage_grace_days", 7) or 7))
    except (TypeError, ValueError):
        return 7


def _resolve_min_data_points(config) -> int:
    risk_config = (config or {}).get("risk_metrics", {}) or {}
    try:
        return max(1, int(risk_config.get("min_data_points", 30) or 30))
    except (TypeError, ValueError):
        return 30


def _period_required_days(period: str, coverage_grace_days: int) -> int:
    return max(_RISK_PERIOD_DAYS.get(period, 0) - max(0, int(coverage_grace_days or 0)), 1)


@capture_call('database_risk.changed_input_candidates', omit=('config',))
def _resolve_changed_nav_risk_candidates(db_path, config=None, target_fcodes=None) -> list[str]:
    database_path = Path(db_path)
    if not database_path.exists():
        return []

    normalized_target_fcodes = None
    if target_fcodes is not None:
        normalized_target_fcodes = []
        seen_target_codes = set()
        for raw_code in target_fcodes:
            code = str(raw_code or "").strip()
            if not code or code in seen_target_codes:
                continue
            seen_target_codes.add(code)
            normalized_target_fcodes.append(code)
        if not normalized_target_fcodes:
            return []

    periods = _resolve_risk_periods(config)
    coverage_grace_days = _resolve_period_coverage_grace_days(config)
    min_data_points = _resolve_min_data_points(config)
    required_observations = max(min_data_points + 1, 2)
    if not periods:
        return []

    required_days_by_period = {
        period: _period_required_days(period, coverage_grace_days)
        for period in periods
    }
    try:
        holding_days_threshold = int(
            ((config or {}).get("risk_metrics", {}) or {}).get(
                "skip_risk_metrics_for_holding_days_gt",
                (config or {}).get("exclude_holding_days_gt", 90),
            ) or 90
        )
    except (TypeError, ValueError):
        holding_days_threshold = 90

    conn = sqlite3.connect(str(database_path))
    conn.row_factory = sqlite3.Row
    try:
        fund_returns_columns = {
            str(row[1] or "").strip()
            for row in conn.execute("PRAGMA table_info(fund_returns)").fetchall()
        }
        fund_columns = {
            str(row[1] or "").strip()
            for row in conn.execute("PRAGMA table_info(funds)").fetchall()
        }
        holding_metadata_fields = ("shortname", "fullname", "holding_period_source", "holding_keywords")
        holding_metadata_select = ", ".join(
            f"funds.{field} AS {field}" if field in fund_columns else f"NULL AS {field}"
            for field in holding_metadata_fields
        )
        fund_risk_columns = {
            str(row[1] or "").strip()
            for row in conn.execute("PRAGMA table_info(fund_risk_metrics)").fetchall()
        }
        snapshot_signature_fields = [
            field
            for field in [
                "latest_nav",
                "daily_growth",
                "return_1w",
                "return_1m",
                "return_3m",
                "return_6m",
                "return_1y",
                "return_2y",
                "return_3y",
                "return_5y",
                "return_ytd",
                "return_since",
                "period_return_date",
            ]
            if field in fund_returns_columns
        ]
        latest_nav_extra_select = ", ".join(f"{field} AS {field}" for field in snapshot_signature_fields)
        latest_nav_extra_select = f", {latest_nav_extra_select}" if latest_nav_extra_select else ""
        latest_nav_snapshot_select = ", ".join(f"latest_nav.{field}" for field in snapshot_signature_fields)
        latest_nav_snapshot_select = f"{latest_nav_snapshot_select}, " if latest_nav_snapshot_select else ""
        risk_signature_select = (
            "COALESCE(risk.nav_snapshot_signature, '') AS nav_snapshot_signature"
            if "nav_snapshot_signature" in fund_risk_columns
            else "'' AS nav_snapshot_signature"
        )
        risk_duration_select = (
            "risk.max_drawdown_duration" if "max_drawdown_duration" in fund_risk_columns else "NULL"
        )
        risk_reasons_select = (
            "risk.undefined_metric_reasons_json" if "undefined_metric_reasons_json" in fund_risk_columns else "NULL"
        )
        risk_contract_select = (
            "risk.formula_contract_json" if "formula_contract_json" in fund_risk_columns else "NULL"
        )
        placeholders = ",".join(["?"] * len(periods))
        latest_nav_where = "latest_nav_date IS NOT NULL AND TRIM(latest_nav_date) <> ''"
        query_params: list[str] = []
        if normalized_target_fcodes is not None:
            placeholders_target = ",".join(["?"] * len(normalized_target_fcodes))
            latest_nav_where += f" AND fcode IN ({placeholders_target})"
            query_params.extend(normalized_target_fcodes)
        query = f"""
            WITH latest_nav AS (
                SELECT fcode, COALESCE(TRIM(latest_nav_date), '') AS latest_nav_date{latest_nav_extra_select}
                FROM fund_returns
                WHERE {latest_nav_where}
            )
            """
        query += f""",
            nav_coverage AS (
                SELECT
                    latest_nav.fcode,
                    MAX(nav.nav_date) AS last_nav_date,
                    MIN(CASE WHEN nav.nav_date BETWEEN date(latest_nav.latest_nav_date, '-365 day') AND latest_nav.latest_nav_date THEN nav.nav_date END) AS first_nav_date_1y,
                    MAX(CASE WHEN nav.nav_date BETWEEN date(latest_nav.latest_nav_date, '-365 day') AND latest_nav.latest_nav_date THEN nav.nav_date END) AS last_nav_date_1y,
                    COUNT(CASE WHEN nav.nav_date BETWEEN date(latest_nav.latest_nav_date, '-365 day') AND latest_nav.latest_nav_date THEN 1 END) AS nav_rows_1y,
                    MIN(CASE WHEN nav.nav_date BETWEEN date(latest_nav.latest_nav_date, '-1095 day') AND latest_nav.latest_nav_date THEN nav.nav_date END) AS first_nav_date_3y,
                    MAX(CASE WHEN nav.nav_date BETWEEN date(latest_nav.latest_nav_date, '-1095 day') AND latest_nav.latest_nav_date THEN nav.nav_date END) AS last_nav_date_3y,
                    COUNT(CASE WHEN nav.nav_date BETWEEN date(latest_nav.latest_nav_date, '-1095 day') AND latest_nav.latest_nav_date THEN 1 END) AS nav_rows_3y,
                    MIN(CASE WHEN nav.nav_date BETWEEN date(latest_nav.latest_nav_date, '-1825 day') AND latest_nav.latest_nav_date THEN nav.nav_date END) AS first_nav_date_5y,
                    MAX(CASE WHEN nav.nav_date BETWEEN date(latest_nav.latest_nav_date, '-1825 day') AND latest_nav.latest_nav_date THEN nav.nav_date END) AS last_nav_date_5y,
                    COUNT(CASE WHEN nav.nav_date BETWEEN date(latest_nav.latest_nav_date, '-1825 day') AND latest_nav.latest_nav_date THEN 1 END) AS nav_rows_5y
                FROM latest_nav
                LEFT JOIN fund_nav_history nav ON nav.fcode = latest_nav.fcode
                GROUP BY latest_nav.fcode
            )
            SELECT
                latest_nav.fcode,
                latest_nav.latest_nav_date,
                {latest_nav_snapshot_select}
                COALESCE(nav_coverage.last_nav_date, '') AS last_nav_date,
                CAST(
                    COALESCE(
                        julianday(nav_coverage.last_nav_date_1y) - julianday(nav_coverage.first_nav_date_1y),
                        0
                    ) AS INT
                ) AS coverage_days_1y,
                COALESCE(nav_coverage.nav_rows_1y, 0) AS nav_rows_1y,
                CAST(
                    COALESCE(
                        julianday(nav_coverage.last_nav_date_3y) - julianday(nav_coverage.first_nav_date_3y),
                        0
                    ) AS INT
                ) AS coverage_days_3y,
                COALESCE(nav_coverage.nav_rows_3y, 0) AS nav_rows_3y,
                CAST(
                    COALESCE(
                        julianday(nav_coverage.last_nav_date_5y) - julianday(nav_coverage.first_nav_date_5y),
                        0
                    ) AS INT
                ) AS coverage_days_5y,
                COALESCE(nav_coverage.nav_rows_5y, 0) AS nav_rows_5y,
                COALESCE(funds.has_holding_period, 0) AS has_holding_period,
                funds.holding_days AS holding_days,
                {holding_metadata_select},
                COALESCE(risk.period, '') AS period,
                COALESCE(risk.end_date, '') AS end_date,
                {risk_signature_select},
                {risk_duration_select} AS max_drawdown_duration,
                {risk_reasons_select} AS undefined_metric_reasons_json,
                {risk_contract_select} AS formula_contract_json
            FROM latest_nav
            LEFT JOIN nav_coverage ON nav_coverage.fcode = latest_nav.fcode
            LEFT JOIN funds ON funds.fcode = latest_nav.fcode
            LEFT JOIN fund_risk_metrics risk
              ON risk.fcode = latest_nav.fcode
             AND risk.period IN ({placeholders})
             AND risk.rowid = (
                 SELECT latest_risk.rowid
                 FROM fund_risk_metrics latest_risk
                 WHERE latest_risk.fcode = latest_nav.fcode
                   AND latest_risk.period = risk.period
                 ORDER BY COALESCE(TRIM(latest_risk.end_date), '') DESC, latest_risk.rowid DESC
                 LIMIT 1
             )
            ORDER BY latest_nav.fcode, risk.period
            """
        query_params.extend(periods)
        rows = conn.execute(query, tuple(query_params)).fetchall()
        if capture_enabled():
            record_event('database_risk.changed_input_rows', {
                'rows': [dict(row) for row in rows], 'periods': periods,
                'coverage_grace_days': coverage_grace_days, 'min_data_points': min_data_points,
                'required_observations': required_observations, 'holding_days_threshold': holding_days_threshold,
            })
    finally:
        conn.close()

    nav_state_by_code: dict[str, dict[str, object]] = {}
    risk_by_code: dict[str, dict[str, dict[str, object]]] = {}
    for row in rows:
        code = str(row["fcode"] or "").strip()
        if not code:
            continue
        latest_nav_date = str(row["latest_nav_date"] or "").strip()
        last_nav_date = str(row["last_nav_date"] or "").strip()
        coverage_days = {}
        nav_rows_by_period = {}
        try:
            has_holding_period = int(row["has_holding_period"] or 0)
        except (TypeError, ValueError):
            has_holding_period = 0
        current_snapshot_signature = build_nav_snapshot_signature(
            {
                "latest_nav_date": latest_nav_date,
                **{field: row[field] for field in snapshot_signature_fields},
            }
        )
        for index, period in enumerate(periods):
            coverage_raw = row[f"coverage_days_{period}"]
            nav_rows_raw = row[f"nav_rows_{period}"]
            try:
                coverage_days[period] = max(0, int(coverage_raw or 0))
            except (TypeError, ValueError):
                coverage_days[period] = 0
            try:
                nav_rows_by_period[period] = max(0, int(nav_rows_raw or 0))
            except (TypeError, ValueError):
                nav_rows_by_period[period] = 0
        period = str(row["period"] or "").strip()
        end_date = str(row["end_date"] or "").strip()
        nav_snapshot_signature = str(row["nav_snapshot_signature"] or "").strip()

        nav_state_by_code[code] = {
            "latest_nav_date": latest_nav_date,
            "last_nav_date": last_nav_date,
            "coverage_days_by_period": coverage_days,
            "nav_rows_by_period": nav_rows_by_period,
            "has_holding_period": has_holding_period,
            "holding_days": row["holding_days"],
            **{field: row[field] for field in holding_metadata_fields},
            "current_snapshot_signature": current_snapshot_signature,
        }
        if period:
            try:
                reasons = json.loads(row['undefined_metric_reasons_json'] or '{}')
            except (TypeError, ValueError):
                reasons = {}
            duration_undefined = (
                isinstance(reasons, dict)
                and reasons.get('max_drawdown_duration') == 'invalid_observations'
            )
            risk_by_code.setdefault(code, {})[period] = {
                "end_date": end_date,
                "nav_snapshot_signature": nav_snapshot_signature,
                "duration_missing": row['max_drawdown_duration'] is None and not duration_undefined,
                "formula_contract_json": row['formula_contract_json'],
            }

    candidates: list[str] = []
    risk_signature_supported = "nav_snapshot_signature" in fund_risk_columns
    from bond_fund_screening.core.metric_formula_contract import VERSION as formula_version
    def current_contract(raw):
        try:
            contract = json.loads(raw or "null")
        except (TypeError, ValueError):
            return False
        return isinstance(contract, dict) and contract.get("version") == formula_version
    for code in sorted(nav_state_by_code):
        latest_nav_date = str(nav_state_by_code[code].get("latest_nav_date") or "").strip()
        last_nav_date = str(nav_state_by_code[code].get("last_nav_date") or "").strip()
        if not latest_nav_date or latest_nav_date != last_nav_date:
            continue
        holding_state = nav_state_by_code[code]
        if holding_period_gate_reason(
            has_holding_period=holding_state.get("has_holding_period"),
            holding_days=holding_state.get("holding_days"),
            fund_name=combine_holding_period_names(holding_state.get("shortname"), holding_state.get("fullname")),
            holding_period_source=holding_state.get("holding_period_source"),
            holding_keywords=holding_state.get("holding_keywords"),
            threshold_days=holding_days_threshold,
        ) == HOLDING_PERIOD_LIMIT:
            continue
        coverage_days_by_period = nav_state_by_code[code].get("coverage_days_by_period") or {}
        nav_rows_by_period = nav_state_by_code[code].get("nav_rows_by_period") or {}
        eligible_periods = [
            period
            for period in periods
            if int(coverage_days_by_period.get(period) or 0) >= required_days_by_period.get(period, 1)
            and int(nav_rows_by_period.get(period) or 0) >= required_observations
        ]
        if not eligible_periods:
            continue
        risk_dates = risk_by_code.get(code) or {}
        # Schema migration leaves old observations NULL. They need one refresh
        # even when the NAV date and return signature have not changed. A real
        # zero or an explicitly undefined new calculation is already complete.
        if any((risk_dates.get(period) or {}).get('duration_missing', True) for period in eligible_periods):
            candidates.append(code)
            continue
        if any(str((risk_dates.get(period) or {}).get("end_date") or "").strip() != latest_nav_date for period in eligible_periods):
            candidates.append(code)
            continue
        if "formula_contract_json" in fund_risk_columns and any(
            not current_contract((risk_dates.get(period) or {}).get("formula_contract_json"))
            for period in eligible_periods
        ):
            candidates.append(code)
            continue
        current_snapshot_signature = str(nav_state_by_code[code].get("current_snapshot_signature") or "").strip()
        if risk_signature_supported and current_snapshot_signature and any(
            str((risk_dates.get(period) or {}).get("nav_snapshot_signature") or "").strip() != current_snapshot_signature
            for period in eligible_periods
        ):
            candidates.append(code)
    return candidates


def _load_target_fcodes_from_file(path_value: str | None) -> list[str]:
    normalized_path = str(path_value or "").strip()
    if not normalized_path:
        return []

    payload = json.loads(Path(normalized_path).read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        raw_codes = payload.get("updated_fcodes") or payload.get("target_fcodes") or []
    elif isinstance(payload, list):
        raw_codes = payload
    else:
        raw_codes = []

    result = []
    seen_codes = set()
    for raw_code in raw_codes:
        code = str(raw_code or "").strip()
        if not code or code in seen_codes:
            continue
        seen_codes.add(code)
        result.append(code)
    return result


def _merge_changed_nav_risk_candidates(
    resolved_candidates: list[str],
    hinted_target_fcodes: list[str] | None = None,
) -> list[str]:
    """
    合并真实未对齐候选与前序步骤提示的基金代码。

    设计说明：
    - `target_fcodes_file` 仅表示“本轮前序链路观察到的净值更新提示”，不应成为
      changed-nav 风险重算的硬边界；
    - 若数据库中仍存在更早遗留的未对齐风险指标，自动化链路应在本轮一并自愈；
    - 因此以真实未对齐候选为主，补充保留提示集合中的代码顺序信息。
    """
    merged: list[str] = []
    seen_codes: set[str] = set()

    for raw_code in resolved_candidates or []:
        code = str(raw_code or "").strip()
        if not code or code in seen_codes:
            continue
        seen_codes.add(code)
        merged.append(code)

    for raw_code in hinted_target_fcodes or []:
        code = str(raw_code or "").strip()
        if not code or code in seen_codes:
            continue
        seen_codes.add(code)
        merged.append(code)

    return merged


def cmd_calculate_risk(
    args,
    config,
    *,
    logger,
    ensure_database_initialized,
    emit_command_error,
    analyzer_cls,
    database_manager_cls,
):
    """计算风险指标。"""
    logger.info("=" * 60)
    logger.info("计算风险指标")
    logger.info("=" * 60)

    db_path = ensure_database_initialized(config)
    if not db_path:
        return 1

    try:
        analyzer = analyzer_cls(config=config)

        if args.fcode:
            count = analyzer.calculate_fund_risk(args.fcode, force_update=args.force)
            print(f"\n✅ 基金{args.fcode}风险指标计算完成: {count}个周期")
            return 0

        if getattr(args, "changed_nav_only", False):
            target_fcodes_file = getattr(args, "target_fcodes_file", None)
            target_fcodes = _load_target_fcodes_from_file(target_fcodes_file)
            resolved_candidate_codes = _resolve_changed_nav_risk_candidates(
                db_path,
                config=config,
            )
            candidate_codes = _merge_changed_nav_risk_candidates(
                resolved_candidate_codes,
                hinted_target_fcodes=target_fcodes,
            )
            if target_fcodes_file:
                logger.info(
                    "changed-nav 风险重算提示文件基金数: %s, 当前数据库未对齐候选数: %s, 合并后执行数: %s",
                    len(target_fcodes),
                    len(resolved_candidate_codes),
                    len(candidate_codes),
                )
            logger.info("检测到净值已更新且风险指标未对齐基金数: %s", len(candidate_codes))
            if not candidate_codes:
                print("\n✅ 未发现需要同步重算风险指标的基金")
                return 0

            total_metrics = analyzer.calculate_all_funds(
                workers=args.workers,
                force_update=True,
                target_fcodes=candidate_codes,
            )
            print(
                f"\n✅ 净值已更新基金风险指标同步完成: "
                f"目标基金{len(candidate_codes)}只, "
                f"生成{total_metrics}个周期"
            )
            return 0

        logger.warning("即将计算所有基金的风险指标，这可能需要10-20分钟")

        if not args.yes:
            response = input("是否继续？(y/N): ")
            if response.lower() != 'y':
                print("已取消")
                return 0

        count = analyzer.calculate_all_funds(
            workers=args.workers,
            force_update=args.force
        )

        print(f"\n✅ 风险指标计算完成: {count}个指标")

        import os
        task_history_id = os.environ.get('TASK_HISTORY_ID')
        if task_history_id:
            try:
                from database.models import TaskHistory

                wal_mode = config.get('database', {}).get('wal_mode', True)
                db_manager = database_manager_cls(db_path=db_path, wal_mode=wal_mode)

                with db_manager.get_session() as session:
                    task_record = session.query(TaskHistory).filter_by(id=int(task_history_id)).first()
                    if task_record:
                        task_record.records_processed = count
                        task_record.records_updated = count
                        logger.info(f"已更新任务历史记录 (ID={task_history_id}): 处理 {count} 个指标")
            except Exception as e:
                logger.warning(f"更新任务历史记录失败: {e}")

        return 0
    except Exception as exc:
        return emit_command_error("计算风险指标失败", exc)


def cmd_collect_details(
    args,
    config,
    *,
    logger,
    ensure_database_initialized,
    emit_command_error,
    collector_cls,
):
    """采集基金详情。"""
    logger.info("=" * 60)
    logger.info("采集基金详情")
    logger.info("=" * 60)

    db_path = ensure_database_initialized(config)
    if not db_path:
        return 1

    try:
        collector = collector_cls(config=config)
        from utils.detail_collection_summary import (
            detail_collection_source_errors, validate_detail_collection_summary,
        )

        detail_kwargs = dict(
            workers=args.workers,
            force_update=args.force,
            bypass_empty_detail_cooldown=bool(
                getattr(args, 'bypass_empty_detail_cooldown', False)
            ),
            target_fcodes=[args.fcode] if args.fcode else None,
        )
        if bool(getattr(args, 'scale_missing_only', False)):
            detail_kwargs['scale_missing_only'] = True
        count = collector.collect_all_details(**detail_kwargs)
        evidence = collector.get_api_source_evidence()
        print("api_source_evidence=" + collector.get_api_source_evidence_json(), flush=True)
        summary = validate_detail_collection_summary(collector.detail_collection_summary, returned_count=count)
        errors = detail_collection_source_errors(summary, evidence)
        if errors:
            raise ValueError(";".join(errors))
        print("detail_collection_summary=" + json.dumps(summary, ensure_ascii=False, allow_nan=False), flush=True)
        print(f"\n基金详情处理结束：写入 {count}，不可用 {summary['unavailable_count']}，异常 {summary['failed_count']}")
        return 2 if summary["status"] == "degraded" else 0
    except Exception as exc:
        return emit_command_error("采集基金详情失败", exc)


def cmd_collect_managers(
    args,
    config,
    *,
    logger,
    ensure_database_initialized,
    emit_command_error,
    collector_cls,
):
    """采集基金经理。"""
    logger.info("=" * 60)
    logger.info("采集基金经理")
    logger.info("=" * 60)

    db_path = ensure_database_initialized(config)
    if not db_path:
        return 1

    try:
        collector = collector_cls(config=config)
        count = collector.collect_all_managers(
            workers=args.workers,
            force_update=args.force
        )

        if hasattr(collector, "get_manager_list_api_source_evidence_json"):
            print("api_source_evidence=" + collector.get_manager_list_api_source_evidence_json())
        if hasattr(collector, "get_manager_detail_api_source_evidence_json"):
            print("api_source_evidence=" + collector.get_manager_detail_api_source_evidence_json())
        summary = validate_manager_collection_summary(
            getattr(collector, "last_run_summary", None), returned_manager_count=count,
        )
        print("manager_collection_summary=" + json.dumps(summary, ensure_ascii=False), flush=True)
        if summary["status"] == "degraded":
            cooldown_text = f"，等待重试{summary['cooldown_count']}只基金" if 'cooldown_count' in summary else ''
            print(f"\n⚠️ 基金经理采集结束: {count}位经理，失败{summary['failed_count']}项，空响应{summary['empty_count']}项{cooldown_text}")
            return 2
        if summary["status"] == "skipped":
            print(f"\n基金经理采集跳过: {summary['reason']}")
        else:
            print(f"\n✅ 基金经理采集完成: {count}位经理")
        return 0
    except Exception as exc:
        return emit_command_error("采集基金经理失败", exc)


def cmd_collect_companies(
    args,
    config,
    *,
    logger,
    ensure_database_initialized,
    emit_command_error,
    collector_cls,
):
    """采集基金公司。"""
    logger.info("=" * 60)
    logger.info("采集基金公司")
    logger.info("=" * 60)

    db_path = ensure_database_initialized(config)
    if not db_path:
        return 1

    try:
        collector = collector_cls(config=config)
        count = collector.collect_all_companies(
            workers=args.workers,
            force_update=args.force
        )

        summary = collector.last_run_summary
        print("api_source_evidence=" + collector.get_api_source_evidence_json())
        print("company_collection_summary=" + json.dumps(summary, ensure_ascii=False))
        failed = int(summary["failed_count"])
        if summary["status"] == "degraded":
            print(f"\n⚠️ 基金公司采集结束: {count}家公司，失败{failed}家，冷却{summary['cooldown_count']}家")
            return 2
        if summary["status"] == "skipped":
            print(f"\n基金公司采集跳过: {summary['skip_reason']}")
        else:
            print(f"\n✅ 基金公司采集完成: {count}家公司")
        return 0
    except Exception as exc:
        return emit_command_error("采集基金公司失败", exc)
