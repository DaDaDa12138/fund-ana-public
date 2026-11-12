#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""最终筛选数据库查询结果装配与批量查库辅助工具。"""

from __future__ import annotations

import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pandas as pd
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from database.nav_history_queries import (
    attach_nav_history_archive_if_available,
    build_nav_history_union_source_sql,
    detach_nav_history_archive,
    load_latest_nav_history_archive_binding,
    nav_history_archive_binding_matches_database,
)


def load_sales_fee_validation_classification_map(
    *,
    audit_dir: Path | None = None,
) -> dict[str, str]:
    """读取最新 sales_fee 重采验证分类映射，供最终摘要审计复用。"""
    base_dir = audit_dir or (Path(__file__).resolve().parents[2] / 'audit')
    candidates = sorted(base_dir.glob('sales_fee_refetch_validation_audit_*.json'))
    if not candidates:
        return {}

    payload = json.loads(candidates[-1].read_text(encoding='utf-8'))
    mapping: dict[str, str] = {}
    for row in payload.get('rows') or []:
        fcode = str(row.get('fcode') or '').strip()
        classification = str(row.get('classification') or '').strip()
        if fcode and classification:
            mapping[fcode] = classification
    return mapping


def create_project_session(*, db_path: Path):
    """创建项目数据库 Session。"""
    engine = create_engine(f"sqlite:///{db_path}")
    Session = sessionmaker(bind=engine)
    return Session(), engine


def _session_database_path(session) -> Path | None:
    try:
        bind = session.get_bind()
        database = getattr(getattr(bind, "url", None), "database", None)
    except Exception:
        return None
    if not database or str(database) == ":memory:":
        return None
    return Path(str(database)).resolve()


def _parse_nav_date(value):
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def load_fund_complete_info_maps(
    session,
    normalized_fcodes: list[str],
    *,
    fund_model,
    returns_model,
    risk_model,
    load_latest_nav_records_fn,
) -> tuple[dict, dict, dict, dict]:
    """批量加载最终主榜导出所需的基金、收益、风险与最新净值映射。"""
    fund_map = {
        row.fcode: row
        for row in session.query(fund_model).filter(fund_model.fcode.in_(normalized_fcodes)).all()
    }
    returns_map = {
        row.fcode: row
        for row in session.query(returns_model).filter(returns_model.fcode.in_(normalized_fcodes)).all()
    }
    risk_map = {
        row.fcode: row
        for row in session.query(risk_model).filter(
            risk_model.fcode.in_(normalized_fcodes),
            risk_model.period == '1y'
        ).all()
    }
    latest_nav_map = load_latest_nav_records_fn(session, normalized_fcodes)
    return fund_map, returns_map, risk_map, latest_nav_map


def load_latest_nav_records(
    session,
    normalized_fcodes: list[str],
    *,
    fund_nav_history_model,
) -> dict:
    """批量加载每只基金的最新净值记录。"""
    if not normalized_fcodes:
        return {}

    db_path = _session_database_path(session)
    if db_path is not None and db_path.exists():
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        archive_binding = load_latest_nav_history_archive_binding()
        if not nav_history_archive_binding_matches_database(archive_binding, db_path):
            archive_binding = {}
        archive_attach = attach_nav_history_archive_if_available(
            conn,
            archive_path=archive_binding.get("archive_path"),
            archive_table_name=archive_binding.get("archive_table_name") or "fund_nav_history_archive",
            archive_cutoff_date=archive_binding.get("archive_cutoff_date") or "",
        )
        try:
            nav_source_sql = build_nav_history_union_source_sql(archive_attach)
            placeholders = ",".join("?" for _ in normalized_fcodes)
            rows = conn.execute(
                f"""
                WITH nav_source AS (
                    {nav_source_sql}
                ),
                latest_nav AS (
                    SELECT fcode, MAX(nav_date) AS max_nav_date
                    FROM nav_source
                    WHERE fcode IN ({placeholders})
                    GROUP BY fcode
                )
                SELECT
                    n.fcode,
                    n.nav_date,
                    n.unit_nav,
                    n.accum_nav,
                    n.daily_growth
                FROM nav_source n
                INNER JOIN latest_nav latest
                    ON latest.fcode = n.fcode
                   AND latest.max_nav_date = n.nav_date
                """,
                tuple(normalized_fcodes),
            ).fetchall()
            return {
                str(row["fcode"]).strip(): SimpleNamespace(
                    fcode=str(row["fcode"]).strip(),
                    nav_date=_parse_nav_date(row["nav_date"]),
                    unit_nav=row["unit_nav"],
                    accum_nav=row["accum_nav"],
                    daily_growth=row["daily_growth"],
                )
                for row in rows
                if str(row["fcode"] or "").strip()
            }
        finally:
            if archive_attach.get("attached"):
                detach_nav_history_archive(
                    conn,
                    alias=str(archive_attach.get("alias") or "nav_archive_db"),
                )
            conn.close()

    latest_nav_subquery = (
        session.query(
            fund_nav_history_model.fcode.label('fcode'),
            func.max(fund_nav_history_model.nav_date).label('max_nav_date')
        )
        .filter(fund_nav_history_model.fcode.in_(normalized_fcodes))
        .group_by(fund_nav_history_model.fcode)
        .subquery()
    )

    rows = (
        session.query(fund_nav_history_model)
        .join(
            latest_nav_subquery,
            (fund_nav_history_model.fcode == latest_nav_subquery.c.fcode) &
            (fund_nav_history_model.nav_date == latest_nav_subquery.c.max_nav_date)
        )
        .all()
    )
    return {row.fcode: row for row in rows}


def _build_returns_payload(returns) -> dict:
    """构造收益字段映射。"""
    from utils.period_return_snapshot import RETURN_PUBLIC_FIELDS, has_dated_period_snapshot
    provenance = {field: getattr(returns, field, None) for field in RETURN_PUBLIC_FIELDS}
    dated = has_dated_period_snapshot(provenance)
    if returns:
        values = {
            '近1周(%)': returns.return_1w,
            '近1月(%)': returns.return_1m,
            '近3月(%)': returns.return_3m,
            '近6月(%)': returns.return_6m,
            '近1年(%)': returns.return_1y,
            '近2年(%)': returns.return_2y,
            '近3年(%)': returns.return_3y,
            '今年来(%)': returns.return_ytd,
            '成立来(%)': returns.return_since,
        }
        if not dated:
            values = {field: None for field in values}
        return {**values, **{label: provenance[field] for field, label in RETURN_PUBLIC_FIELDS.items()}}

    return {
        '近1周(%)': None,
        '近1月(%)': None,
        '近3月(%)': None,
        '近6月(%)': None,
        '近1年(%)': None,
        '近2年(%)': None,
        '近3年(%)': None,
        '今年来(%)': None,
        '成立来(%)': None,
        **{label: None for label in RETURN_PUBLIC_FIELDS.values()},
    }


def _build_risk_payload(risk_1y, *, returns, resolve_display_annual_return_1y_fn) -> dict:
    """构造 1 年风险指标字段映射。"""
    if risk_1y:
        raw_sharpe = getattr(risk_1y, 'raw_sharpe_ratio', None)
        raw_sortino = getattr(risk_1y, 'raw_sortino_ratio', None)
        return {
            '1年-波动率': risk_1y.volatility,
            '1年-最大回撤(%)': risk_1y.max_drawdown,
            '1年-年化收益(%)': resolve_display_annual_return_1y_fn(risk_1y, returns),
            '1年-夏普比率': raw_sharpe if raw_sharpe is not None else risk_1y.sharpe_ratio,
            '1年-索提诺比率': raw_sortino if raw_sortino is not None else risk_1y.sortino_ratio,
            '1年-卡玛比率': risk_1y.calmar_ratio,
            '1年-VaR95(%)': risk_1y.var_95,
            '1年-VaR99(%)': risk_1y.var_99,
            '1年-正收益比率': risk_1y.positive_ratio,
        }

    return {
        '1年-波动率': None,
        '1年-最大回撤(%)': None,
        '1年-年化收益(%)': None,
        '1年-夏普比率': None,
        '1年-索提诺比率': None,
        '1年-卡玛比率': None,
        '1年-VaR95(%)': None,
        '1年-VaR99(%)': None,
        '1年-正收益比率': None,
    }


def _build_latest_nav_payload(latest_nav) -> dict:
    """构造最新净值字段映射。"""
    if latest_nav:
        return {
            '最新日增长率(%)': latest_nav.daily_growth if latest_nav.daily_growth is not None else 0.0,
            '最新净值日期': latest_nav.nav_date.strftime('%Y-%m-%d') if latest_nav.nav_date else None,
            '最新单位净值': latest_nav.unit_nav,
            '最新累计净值': latest_nav.accum_nav,
        }

    return {
        '最新日增长率(%)': None,
        '最新净值日期': None,
        '最新单位净值': None,
        '最新累计净值': None,
    }


def build_fund_complete_info_frame(
    *,
    fcode_pairs: list[tuple[str, str | None]],
    fund_map: dict,
    returns_map: dict,
    risk_map: dict,
    latest_nav_map: dict,
    resolve_display_annual_return_1y_fn,
    print_fn,
) -> pd.DataFrame:
    """将批量查询结果装配为最终导出用 DataFrame。"""
    result_data = []

    for original_fcode, normalized_fcode in fcode_pairs:
        if not normalized_fcode:
            print_fn(f"  ⚠️ 基金 {original_fcode} 基金代码无效,跳过")
            continue

        fund = fund_map.get(normalized_fcode)
        if not fund:
            print_fn(f"  ⚠️ 基金 {normalized_fcode} 在数据库中不存在,跳过")
            continue

        returns = returns_map.get(normalized_fcode)
        risk_1y = risk_map.get(normalized_fcode)
        latest_nav = latest_nav_map.get(normalized_fcode)

        fund_data = {
            '基金代码': original_fcode,
            '基金名称': fund.shortname,
            '基金类型': fund.ftype,
            '风险等级': fund.risk_level,
            '成立日期': fund.estab_date.strftime('%Y-%m-%d') if fund.estab_date else None,
            '基金规模(亿)': fund.scale_billion,
            '基金公司': fund.company_name,
            '基金经理': fund.manager_names,
            '业绩基准': getattr(fund, 'benchmark', None),
            '管理费率': getattr(fund, 'mgr_fee_rate', None),
            '托管费率': getattr(fund, 'trust_fee_rate', None),
            '销售服务费率': getattr(fund, 'sales_fee_rate', None),
        }
        fund_data.update(_build_returns_payload(returns))
        fund_data.update(_build_risk_payload(
            risk_1y,
            returns=returns,
            resolve_display_annual_return_1y_fn=resolve_display_annual_return_1y_fn,
        ))
        fund_data.update(_build_latest_nav_payload(latest_nav))
        result_data.append(fund_data)

    df = pd.DataFrame(result_data)
    print_fn(f"  ✅ 成功从数据库获取 {len(df)} 只基金的完整信息")
    return df


def build_latest_nav_result_map(
    *,
    fcode_pairs: list[tuple[object, str | None]],
    latest_nav_map: dict,
) -> dict:
    """将批量最新净值查询结果映射回原始基金代码列表。"""
    result = {}
    for original_fcode, normalized_fcode in fcode_pairs:
        nav_row = latest_nav_map.get(normalized_fcode) if normalized_fcode else None
        result[str(original_fcode)] = {
            'daily_growth': (
                nav_row.daily_growth
                if nav_row and nav_row.daily_growth is not None
                else (0.0 if nav_row else None)
            ),
            'nav_date': nav_row.nav_date if nav_row else None,
        }
    return result


def fetch_latest_nav_data(
    fcodes: list,
    *,
    session=None,
    project_db_path_getter,
    create_project_session_fn,
    prepare_lookup_fcodes_fn,
    load_latest_nav_records_fn,
    print_fn,
) -> dict:
    """按既有语义批量查询最新净值。"""
    owns_session = session is None
    engine = None

    try:
        db_path = project_db_path_getter()
        if owns_session:
            if not db_path.exists():
                print_fn(f"  ⚠️ 数据库文件不存在: {db_path}")
                return {}
            session, engine = create_project_session_fn()

        fcode_pairs, normalized_fcodes = prepare_lookup_fcodes_fn(fcodes)
        latest_nav_map = load_latest_nav_records_fn(session, normalized_fcodes)
        return build_latest_nav_result_map(
            fcode_pairs=fcode_pairs,
            latest_nav_map=latest_nav_map,
        )

    except Exception as exc:
        print_fn(f"  ⚠️ 获取最新净值数据失败: {exc}")
        return {}
    finally:
        if owns_session and session is not None:
            session.close()
        if engine is not None:
            engine.dispose()


def fetch_fund_complete_info(
    fcodes: list,
    *,
    session=None,
    project_db_path_getter,
    create_project_session_fn,
    prepare_lookup_fcodes_fn,
    load_latest_nav_records_fn,
    fund_model,
    returns_model,
    risk_model,
    resolve_display_annual_return_1y_fn,
    print_fn,
    traceback_print_fn=None,
) -> pd.DataFrame:
    """按既有语义批量查询基金完整信息。"""
    owns_session = session is None
    engine = None

    try:
        db_path = project_db_path_getter()
        if owns_session:
            if not db_path.exists():
                print_fn(f"  ⚠️ 数据库文件不存在: {db_path}")
                return pd.DataFrame()
            session, engine = create_project_session_fn()

        fcode_pairs, normalized_fcodes = prepare_lookup_fcodes_fn(fcodes)
        if not normalized_fcodes:
            return pd.DataFrame()

        fund_map, returns_map, risk_map, latest_nav_map = load_fund_complete_info_maps(
            session,
            normalized_fcodes,
            fund_model=fund_model,
            returns_model=returns_model,
            risk_model=risk_model,
            load_latest_nav_records_fn=load_latest_nav_records_fn,
        )
        return build_fund_complete_info_frame(
            fcode_pairs=fcode_pairs,
            fund_map=fund_map,
            returns_map=returns_map,
            risk_map=risk_map,
            latest_nav_map=latest_nav_map,
            resolve_display_annual_return_1y_fn=resolve_display_annual_return_1y_fn,
            print_fn=print_fn,
        )

    except Exception as exc:
        print_fn(f"  ⚠️ 获取基金完整信息失败: {exc}")
        if traceback_print_fn is not None:
            traceback_print_fn()
        return pd.DataFrame()
    finally:
        if owns_session and session is not None:
            session.close()
        if engine is not None:
            engine.dispose()
