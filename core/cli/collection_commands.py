#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""主采集链路 CLI 命令实现。"""

import json
import hashlib
import os
from datetime import datetime
from pathlib import Path


def cmd_collect_funds(
    args,
    config,
    *,
    logger,
    ensure_database_initialized,
    emit_command_error,
    fund_collector_cls,
):
    """采集基金列表。"""
    fund_type = getattr(args, 'fund_type', 'bond')
    fund_type_display = fund_type if fund_type else 'bond'

    logger.info("=" * 60)
    logger.info(f"采集{fund_type_display}型基金列表")
    logger.info("=" * 60)

    db_path = ensure_database_initialized(config)
    if not db_path:
        return 1

    try:
        collector = fund_collector_cls(config=config)

        fund_types = None
        if fund_type and fund_type != 'bond':
            if fund_type == 'all':
                fund_types = ['stock', 'index', 'hybrid', 'bond', 'money', 'qdii', 'fof', 'etf', 'lof']
            else:
                fund_types = [fund_type]

        only_buyable_arg = getattr(args, 'include_non_buyable', False)
        count = collector.collect_all(fund_types=fund_types, only_buyable=only_buyable_arg)

        print(f"\n✅ {fund_type_display}基金列表采集完成: {count}只")
        return 0
    except Exception as exc:
        return emit_command_error("采集基金列表失败", exc)


def cmd_collect_period_returns(
    args, config, *, logger, ensure_database_initialized, emit_command_error,
    fund_collector_cls,
):
    """Refresh one bounded batch and persist accepted/rejected source responses."""
    if not ensure_database_initialized(config):
        return 1
    try:
        collector = fund_collector_cls(config=config)
        codes = getattr(args, "fcodes", None)
        if codes:
            codes = list(dict.fromkeys(code.strip() for code in codes.split(",") if code.strip()))
            if any(len(code) != 6 or not code.isascii() or not code.isdecimal() for code in codes):
                raise ValueError("基金代码须为六位数字")
        kwargs = {"force_refresh": True} if getattr(args, "force_refresh", False) else {}
        summary = collector.collect_current(fcodes=codes, **kwargs)
        evidence_dir = Path(os.environ.get("FUND_ANA_SUBPROCESS_LOG_DIR") or
                            Path(__file__).resolve().parents[2] / "reports/audit/period_returns")
        evidence_dir.mkdir(parents=True, exist_ok=True)
        path = evidence_dir / f"period_return_responses_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.json"
        payload = {"summary": summary, "observations": collector.response_observations,
                   "response_scope": "decoded_API_payloads_and_request_codes; not_wire_bytes"}
        with path.open("x", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2, allow_nan=False)
            stream.write("\n")
        summary["response_artifact"] = str(path)
        summary["response_artifact_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        print("api_source_evidence=" + json.dumps(summary, ensure_ascii=False, allow_nan=False), flush=True)
        print(f"区间收益补采完成: 目标={summary['target_count']} 已落库={summary['saved_fund_count']} "
              f"失败或缺报={summary['failed_count']} 待后续处理={summary['pending_count']} "
              f"阻塞={summary['blocking_count']} 明确缺报且无旧收益={len(summary['nonblocking_missing_fcodes'])} "
              f"日期落后隔离={len(summary['quarantined_date_lag_fcodes'])} "
              f"初始化占位隔离={len(summary.get('initialization_placeholder_fcodes', []))} "
              f"旧刷新拒绝且保留已验证新快照={len(summary.get('superseded_refresh_fcodes', []))}", flush=True)
        # Exit zero means the bounded command can hand off to screening.
        # Availability remains degraded in its evidence and scheduler state;
        # an explicitly absent return is never counted as a saved snapshot.
        return 2 if summary["screening_blocked"] else 0
    except Exception as exc:
        return emit_command_error("区间收益补采失败", exc)


def cmd_collect_nav(
    args,
    config,
    *,
    logger,
    ensure_database_initialized,
    emit_command_error,
    update_collection_task_history,
    nav_collector_cls,
):
    """采集历史净值。"""
    logger.info("=" * 60)
    logger.info("采集历史净值")
    logger.info("=" * 60)

    db_path = ensure_database_initialized(config)
    if not db_path:
        return 1

    if args.incremental and args.fcode:
        print("❌ --incremental 不能与 --fcode 同时使用")
        return 1

    try:
        collector = nav_collector_cls(config=config)

        if args.incremental:
            records = collector.collect_all_incremental(workers=args.workers)
            updated_fcodes_output = getattr(args, "updated_fcodes_output", None)
            if updated_fcodes_output:
                output_path = Path(str(updated_fcodes_output)).expanduser()
                output_path.parent.mkdir(parents=True, exist_ok=True)
                payload = {
                    "generated_at": __import__("datetime").datetime.now().isoformat(timespec="seconds"),
                    "updated_fcodes": collector.get_recent_updated_nav_codes(),
                }
                output_path.write_text(
                    json.dumps(payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
            print(
                "api_source_evidence="
                + collector.get_nav_api_source_evidence_json(
                    inserted_nav_record_count=records,
                )
            )
            print(f"\n✅ 增量更新完成: {records:,}条净值记录")

            stats = collector.get_stats()
            update_collection_task_history(
                success_count=stats.get('success', 0),
                total_count=stats.get('total', 0),
                failed_count=stats.get('failed', 0),
                inserted_count=records,
            )
            return 0

        if args.fcode:
            records = collector.collect_fund_history(args.fcode, force_update=args.force)
            print(
                "api_source_evidence="
                + collector.get_nav_api_source_evidence_json(
                    inserted_nav_record_count=records,
                )
            )
            print(f"\n✅ 基金{args.fcode}采集完成: {records:,}条净值记录")

            update_collection_task_history(
                success_count=1,
                total_count=1,
                failed_count=0,
                inserted_count=records,
            )
            return 0

        logger.warning("即将开始全量采集，这可能需要3-4小时（分批进行）")

        if not args.yes:
            response = input("是否继续？(y/N): ")
            if response.lower() != 'y':
                print("已取消")
                return 0

        records = collector.collect_all_full(
            workers=args.workers,
            batch_size=args.batch_size,
            batch_interval=args.batch_interval
        )

        print(
            "api_source_evidence="
            + collector.get_nav_api_source_evidence_json(
                inserted_nav_record_count=records,
            )
        )
        print(f"\n✅ 全量采集完成: {records:,}条净值记录")

        stats = collector.get_stats()
        update_collection_task_history(
            success_count=stats.get('success', 0),
            total_count=stats.get('total', 0),
            failed_count=stats.get('failed', 0),
            inserted_count=records,
        )
        return 0
    except Exception as exc:
        return emit_command_error("采集历史净值失败", exc)


def collect_nav_with_limit(
    config,
    limit,
    *,
    logger,
    ensure_database_initialized,
    update_collection_task_history,
    nav_collector_cls,
    emit_command_error=None,
):
    """限制模式下仅采集前 N 只基金的净值。"""
    if limit is None or limit <= 0:
        print("❌ --limit 必须是大于0的整数")
        return 1

    db_path = ensure_database_initialized(config)
    if not db_path:
        return 1

    try:
        collector = nav_collector_cls(config=config)
        funds = collector._get_all_funds()
    except Exception as exc:
        if emit_command_error is not None:
            return emit_command_error("限制模式读取基金列表失败", exc)
        raise

    if not funds:
        logger.warning("没有找到基金数据，请先运行基金列表采集")
        print("\n✅ 限制模式采集完成: 0只基金，0条净值记录")

        try:
            update_collection_task_history(
                success_count=0,
                total_count=0,
                failed_count=0,
                inserted_count=0,
            )
        except Exception as exc:
            if emit_command_error is not None:
                return emit_command_error("限制模式更新任务历史失败", exc)
            raise
        return 0

    selected_funds = funds[:limit]
    total_records = 0
    success_count = 0
    failed_count = 0

    logger.info("限制模式：共发现%s只基金，本次仅采集前%s只", len(funds), len(selected_funds))

    for fcode in selected_funds:
        try:
            records = collector.collect_fund_history(fcode, force_update=False)
            total_records += records
            success_count += 1
        except Exception as exc:
            failed_count += 1
            logger.error("限制模式采集基金%s失败: %s", fcode, exc)
            try:
                collector.log_collection(
                    task_type='nav_collection',
                    fcode=fcode,
                    status='failed',
                    message=str(exc),
                )
            except Exception as log_exc:
                if emit_command_error is not None:
                    return emit_command_error("限制模式记录失败基金日志失败", log_exc)
                raise

            try:
                update_collection_task_history(
                    success_count=success_count,
                    total_count=success_count + failed_count,
                    failed_count=failed_count,
                    inserted_count=total_records,
                )
            except Exception as history_exc:
                if emit_command_error is not None:
                    return emit_command_error("限制模式更新任务历史失败", history_exc)
                raise
            return 1

    print(f"\n✅ 限制模式采集完成: {len(selected_funds)}只基金，{total_records}条净值记录")

    try:
        update_collection_task_history(
            success_count=success_count,
            total_count=success_count + failed_count,
            failed_count=failed_count,
            inserted_count=total_records,
        )
    except Exception as exc:
        if emit_command_error is not None:
            return emit_command_error("限制模式更新任务历史失败", exc)
        raise
    return 0


def cmd_collect_all(
    args,
    config,
    *,
    logger,
    get_configured_db_path,
    check_database_exists,
    init_db_cmd,
    collect_funds_cmd,
    collect_nav_cmd,
    stats_cmd,
    collect_nav_with_limit_fn,
    emit_command_error,
    namespace_cls,
):
    """一键采集所有数据。"""
    logger.info("=" * 60)
    logger.info("一键采集所有数据")
    logger.info("=" * 60)

    try:
        db_path = get_configured_db_path(config)
    except Exception as exc:
        return emit_command_error("解析数据库配置失败", exc)

    if not check_database_exists(db_path):
        logger.info("步骤1: 初始化数据库...")
        if init_db_cmd(namespace_cls(drop=False), config) != 0:
            return 1

    logger.info("\n步骤2: 采集基金列表...")
    collect_funds_result = collect_funds_cmd(
        namespace_cls(include_non_buyable=False),
        config,
    )
    if collect_funds_result != 0:
        return 1

    logger.info("\n步骤3: 采集净值历史...")

    if args.limit is not None and args.limit <= 0:
        print("❌ --limit 必须是大于0的整数")
        return 1

    if args.limit is not None:
        logger.info(f"测试模式：仅采集前{args.limit}只基金")
        nav_result = collect_nav_with_limit_fn(config, args.limit)
    else:
        nav_result = collect_nav_cmd(
            namespace_cls(
                incremental=False,
                fcode=None,
                force=False,
                workers=args.workers,
                batch_size=args.batch_size,
                batch_interval=args.batch_interval,
                yes=True,
            ),
            config,
        )
    if nav_result != 0:
        return 1

    logger.info("\n步骤4: 采集完成统计")
    if stats_cmd(namespace_cls(api=True), config) != 0:
        return 1

    return 0
