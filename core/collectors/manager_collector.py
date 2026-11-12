"""
基金经理采集器
采集基金经理的详细信息：简历、从业天数、管理规模、业绩等
"""

from dataclasses import replace
import json
import logging
import math
import time
from datetime import date, datetime, timedelta
from typing import Any, Optional, Dict, List, Set
from collectors.base_collector import BaseCollector
from collectors.source_evidence import ApiSourceEvidence
from database.models import Fund, FundManager
from utils import safe_float, safe_int, ProgressMonitor
from utils.manager_collection_summary import build_manager_collection_summary
from utils.tiantian_data_quality import (
    inspect_tiantian_manager_info_payload,
    inspect_tiantian_manager_info_record,
    inspect_tiantian_manager_list_payload,
)

logger = logging.getLogger(__name__)

MANAGER_DETAIL_RETRY_MAX_ATTEMPTS = 3
MANAGER_DETAIL_RETRYABLE_STATUSES = {
    'request_failed',
    'upstream_business_degraded',
}
MANAGER_DETAIL_RETRYABLE_FAILURE_KINDS = {
    'timeout',
    'connection_error',
    'invalid_json',
    'http_error',
}


def _parse_manager_netnav_to_billion(value) -> Optional[float]:
    """
    将天天基金经理详情中的 NETNAV（元）转换为数据库使用的亿元。
    """
    amount = _manager_detail_float(value)
    if amount is None:
        return None
    return amount / 1e8


def _manager_detail_float(value: Any) -> Optional[float]:
    """Unknown and invalid upstream values are not current numeric data."""
    if isinstance(value, bool):
        return None
    try:
        number = safe_float(value)
    except OverflowError:
        return None
    return number if number is not None and math.isfinite(number) else None


def _manager_detail_count(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, float) and (not math.isfinite(value) or not value.is_integer()):
        return None
    try:
        number = safe_int(value)
    except OverflowError:
        return None
    return number if number is not None and 0 <= number < 2**63 else None


def _manager_detail_text(value: Any, *, numeric_id: bool = False) -> Optional[str]:
    if numeric_id and isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    if not isinstance(value, str):
        return None
    if value.strip().lower() in {"", "-", "--", "null", "none", "nan", "n/a"}:
        return None
    return value


def _manager_detail_snapshot_values(data: Dict[str, Any]) -> Dict[str, Any]:
    """Replace the current profile from one accepted response, including NULLs.

    Keeping an older optional field would mix observations while advancing the
    profile timestamp. An omitted field is unknown in this response, not a
    request to keep an earlier score. Failed envelopes/identities never enter
    this mapping; last_updated_at remains a profile save time, not a valuation
    date for upstream performance metrics.
    """
    max_profit = _manager_detail_float(data.get('FMAXEARN1'))
    max_drawdown = _manager_detail_float(data.get('FMAXRETRA1'))
    return {
        'name': data['MGRNAME'],
        'resume': _manager_detail_text(data.get('RESUME')),
        'total_days': _manager_detail_count(data.get('TOTALDAYS')),
        'manage_scale': _parse_manager_netnav_to_billion(data.get('NETNAV')),
        'fund_count': _manager_detail_count(data.get('FCOUNT')),
        'annual_return': _manager_detail_float(data.get('YIELDSE')),
        'max_profit': _manager_detail_float(max_profit * 100) if max_profit is not None else None,
        'max_drawdown': _manager_detail_float(max_drawdown * 100) if max_drawdown is not None else None,
        'score_overall': _manager_detail_float(data.get('MGOLD')),
        'score_experience': _manager_detail_float(data.get('SDAY')),
        'score_return': _manager_detail_float(data.get('SY1')),
        'company_id': _manager_detail_text(data.get('JJGSID'), numeric_id=True),
        'company_name': _manager_detail_text(data.get('JJGS')),
        'photo_url': _manager_detail_text(data.get('NEWPHOTOURL')),
    }


def _split_manager_tokens(value) -> List[str]:
    """将接口中的经理 ID / 姓名字段拆为去重后的顺序列表。"""
    normalized = str(value or '').replace('，', ',').strip()
    if not normalized:
        return []

    tokens = []
    seen = set()
    for raw in normalized.split(','):
        token = raw.strip()
        if not token or token in seen:
            continue
        seen.add(token)
        tokens.append(token)
    return tokens


def _is_active_manager_record(item: Dict) -> bool:
    """
    判断经理列表记录是否表示当前仍在任的经理组合。

    fundMNMangerList 会同时返回现任与历史经理区间。
    已验证的当前区间使用 `LEMPDATE='--'`。缺字段或空值不足以证明
    当前在任；历史记录的 ISINOFFICE 也可能为 1，不能代替任职区间。
    """
    leave_date = str((item or {}).get('LEMPDATE') or '').strip()
    return leave_date == '--'


def _extract_manager_list_fields(datas: List[Dict]) -> Dict[str, str]:
    """
    从 fundMNMangerList 返回中提取完整的经理 ID / 姓名列表。

    文档与线上返回都可能出现两种形式：
    1. 单条记录内直接给出逗号分隔的 `MGRID` / `MGRNAME`
    2. 多条记录分别给出单个经理
    """
    active_records = [item for item in datas or [] if isinstance(item, dict) and _is_active_manager_record(item)]

    manager_ids: List[str] = []
    manager_names: List[str] = []
    seen_ids = set()
    seen_names = set()

    for item in active_records:
        if not isinstance(item, dict):
            continue

        for manager_id in _split_manager_tokens(item.get('MGRID')):
            if manager_id in seen_ids:
                continue
            seen_ids.add(manager_id)
            manager_ids.append(manager_id)

        for manager_name in _split_manager_tokens(item.get('MGRNAME')):
            if manager_name in seen_names:
                continue
            seen_names.add(manager_name)
            manager_names.append(manager_name)

    return {
        'manager_ids': ','.join(manager_ids),
        'manager_names': ','.join(manager_names),
    }


def _confirms_no_current_managers(payload: Dict, fcode: str, *, as_of: date | None = None) -> bool:
    """Only a complete, identity-matched, ended history may clear a binding."""
    records = payload.get('Datas')
    total_text = str(payload.get('TotalCount') or '').strip()
    if (not isinstance(records, list) or not records or not total_text.isascii()
            or not total_text.isdigit() or int(total_text) != len(records)):
        return False
    today = as_of or datetime.now().date()
    for item in records:
        if not isinstance(item, dict) or str(item.get('FCODE') or '').strip() != fcode:
            return False
        ids = _split_manager_tokens(item.get('MGRID'))
        names = _split_manager_tokens(item.get('MGRNAME'))
        if (not ids or len(ids) != len(names)
                or any(not value.isascii() or not value.isdigit() for value in ids)
                or any(value in {'--', '-', 'null', 'None'} for value in names)):
            return False
        try:
            start = date.fromisoformat(str(item.get('FEMPDATE') or ''))
            end = date.fromisoformat(str(item.get('LEMPDATE') or ''))
        except ValueError:
            return False
        if start > end or end >= today:
            return False
    return True


def _should_retry_manager_detail_quality(quality) -> bool:
    """仅对明显瞬时失败做有限重试，避免把永久错误拖成长尾。"""
    status = str(getattr(quality, 'status', '') or '').strip()
    failure_kind = str(getattr(quality, 'failure_kind', '') or '').strip()
    payload_type = str(getattr(quality, 'payload_type', '') or '').strip()

    if status in MANAGER_DETAIL_RETRYABLE_STATUSES:
        return failure_kind in MANAGER_DETAIL_RETRYABLE_FAILURE_KINDS or status == 'upstream_business_degraded'

    # 线上 fundMSNMangerInfo 偶发返回 Success=true 但 Datas=None，通常是瞬时降级。
    if status == 'invalid_datas_type' and payload_type == 'NoneType':
        return True

    return False


class ManagerCollector(BaseCollector):
    """基金经理采集器"""

    def __init__(self, config=None, db_manager=None, api_client=None):
        """初始化经理采集器"""
        super().__init__(config, db_manager, api_client)

        # 采集配置
        collection_config = self.config.get('collection', {})
        self.manager_config = collection_config.get('manager', {})
        self.default_workers = self.manager_config.get('workers', 5)
        self.fund_list_refresh_stale_days = max(
            1,
            int(self.manager_config.get('fund_list_refresh_stale_days', 42) or 42),
        )
        self.missing_list_retry_days = max(
            1,
            int(self.manager_config.get('missing_list_retry_days', 7) or 7),
        )
        self.list_failure_retry_hours = max(
            1, int(self.manager_config.get('list_failure_retry_hours', 24)),
        )
        self.detail_refresh_stale_days = max(
            1,
            int(self.manager_config.get('detail_refresh_stale_days', 56) or 56),
        )
        self.max_fund_targets_per_run = max(
            0,
            int(self.manager_config.get('max_fund_targets_per_run', 1500) or 0),
        )
        self.max_detail_targets_per_run = max(
            0,
            int(self.manager_config.get('max_detail_targets_per_run', 300) or 0),
        )
        self.last_run_summary = None
        self._manager_list_api_source_evidence = ApiSourceEvidence(
            task="collect-managers",
            source="fundMNMangerList",
        )
        self._manager_detail_api_source_evidence = ApiSourceEvidence(
            task="collect-managers",
            source="fundMSNMangerInfo",
        )

    def _ensure_manager_list_api_source_evidence(self) -> ApiSourceEvidence:
        if not hasattr(self, "_manager_list_api_source_evidence") or self._manager_list_api_source_evidence is None:
            self._manager_list_api_source_evidence = ApiSourceEvidence(
                task="collect-managers",
                source="fundMNMangerList",
            )
        return self._manager_list_api_source_evidence

    def _ensure_manager_detail_api_source_evidence(self) -> ApiSourceEvidence:
        if not hasattr(self, "_manager_detail_api_source_evidence") or self._manager_detail_api_source_evidence is None:
            self._manager_detail_api_source_evidence = ApiSourceEvidence(
                task="collect-managers",
                source="fundMSNMangerInfo",
            )
        return self._manager_detail_api_source_evidence

    def reset_api_source_evidence(self, **context: Any) -> None:
        self._ensure_manager_list_api_source_evidence().reset(**context)
        self._ensure_manager_detail_api_source_evidence().reset(**context)

    def get_manager_list_api_source_evidence(self, **overrides: Any) -> dict[str, Any]:
        return self._ensure_manager_list_api_source_evidence().snapshot(**overrides)

    def get_manager_detail_api_source_evidence(self, **overrides: Any) -> dict[str, Any]:
        return self._ensure_manager_detail_api_source_evidence().snapshot(**overrides)

    def get_manager_list_api_source_evidence_json(self, **overrides: Any) -> str:
        return self._ensure_manager_list_api_source_evidence().snapshot_json(**overrides)

    def get_manager_detail_api_source_evidence_json(self, **overrides: Any) -> str:
        return self._ensure_manager_detail_api_source_evidence().snapshot_json(**overrides)

    def collect_all_managers(self, workers=None, force_update=False, report_task_history=True):
        """
        采集所有基金经理信息（分两步：1.采集经理列表更新Fund表 2.采集经理详情）

        Args:
            workers: 并发数
            force_update: 强制更新
            report_task_history: 是否写入 task_history（调度器原生任务应关闭，避免双重记账）

        Returns:
            采集的经理数
        """
        workers = workers or self.default_workers
        self.last_run_summary = None
        logger.info(f"开始采集基金经理信息（并发数={workers}）...")

        # 步骤1: 采集所有基金的经理列表，更新Fund表
        logger.info("步骤1/2: 采集基金经理列表并更新基金表...")
        fund_manager_summary = self._collect_fund_managers(force_update=force_update)

        # 步骤2: 提取唯一的经理ID，采集经理详情
        logger.info("步骤2/2: 采集基金经理详细信息...")
        manager_detail_summary = self._collect_manager_details(workers, force_update)
        manager_count = manager_detail_summary['updated_manager_count']

        self.last_run_summary = build_manager_collection_summary({
            'workers': workers,
            'force_update': bool(force_update),
            'fund_manager_list': fund_manager_summary,
            'manager_details': manager_detail_summary,
        })

        self.print_stats()

        if report_task_history:
            # CLI/脚本模式下补齐复合任务的阶段统计；调度器原生任务由外层统一记账。
            from utils.task_reporter import update_task_history
            total_count = (
                fund_manager_summary['total_funds'] +
                manager_detail_summary['requested_manager_count']
            )
            success_count = (
                fund_manager_summary['success_funds'] +
                manager_detail_summary['updated_manager_count']
            )
            failed_count = (
                fund_manager_summary['failed_funds'] +
                manager_detail_summary['failed_manager_count']
            )
            update_task_history(
                success_count=success_count,
                total_count=total_count,
                failed_count=failed_count,
                status_override=self.last_run_summary['status'],
                extra_data={'stage_summary': self.last_run_summary}
            )

        return manager_count

    def _collect_fund_managers(
        self, force_update: bool = False, *, target_fcodes: Optional[List[str]] = None,
    ):
        """更新基金经理列表；自动任务按缺失/过期时间有界轮转。"""
        now = datetime.now()
        stale_cutoff = now - timedelta(days=self.fund_list_refresh_stale_days)
        missing_retry_cutoff = now - timedelta(days=self.missing_list_retry_days)
        retry_cutoff = now - timedelta(hours=self.list_failure_retry_hours)
        with self.db_manager.get_session(readonly=True) as session:
            query = session.query(
                Fund.fcode, Fund.manager_ids, Fund.manager_names,
                Fund.manager_list_updated_at, Fund.manager_list_attempted_at,
                Fund.manager_list_last_status,
            )
            if target_fcodes is not None:
                query = query.filter(Fund.fcode.in_(target_fcodes))
            rows = query.all()

        eligible = []
        fresh_count = cooldown_count = 0
        cooldown_until = []
        for row in rows:
            updated_at, attempted_at = row.manager_list_updated_at, row.manager_list_attempted_at
            missing = not row.manager_ids or not row.manager_names
            unresolved = attempted_at is not None and (
                row.manager_list_last_status not in {'ok', 'no_current_managers_confirmed'}
                or updated_at is None or attempted_at > updated_at
            )
            if not force_update:
                due = (unresolved or updated_at is None or updated_at > now
                       or updated_at < stale_cutoff
                       or (missing and updated_at < missing_retry_cutoff))
                if not due:
                    fresh_count += 1
                    continue
                if unresolved and retry_cutoff < attempted_at <= now:
                    cooldown_count += 1
                    cooldown_until.append(attempted_at + timedelta(hours=self.list_failure_retry_hours))
                    continue
            eligible.append(row)
        # A recent failed attempt moves behind never-attempted targets even
        # after its cooldown expires. Missing bindings keep their initial priority.
        eligible.sort(key=lambda row: (
            row.manager_list_attempted_at or datetime.min,
            0 if not row.manager_ids or not row.manager_names else 1,
            row.manager_list_updated_at or datetime.min,
            row.fcode,
        ))
        fcodes = [row.fcode for row in eligible]
        if not force_update and self.max_fund_targets_per_run > 0:
            fcodes = fcodes[:self.max_fund_targets_per_run]

        collection_mode = "force_full_refresh" if force_update else "bounded_missing_and_stale_refresh"
        logger.info(f"共{len(fcodes)}只基金需要采集经理信息（模式={collection_mode}）")
        self._ensure_manager_list_api_source_evidence().reset(
            stage="manager_list",
            target_count=len(fcodes),
            force_update=bool(force_update),
            collection_mode=collection_mode,
            refresh_stale_days=self.fund_list_refresh_stale_days,
            missing_retry_days=self.missing_list_retry_days,
            failure_retry_hours=self.list_failure_retry_hours,
            eligible_count=len(eligible),
            remaining_eligible_count=len(eligible) - len(fcodes),
            fresh_skipped_count=fresh_count,
            cooldown_count=cooldown_count,
            cooldown_next_retry_at=min(cooldown_until).isoformat() if cooldown_until else None,
            attempted_at_semantics='durable_attempt_start_before_api_call_not_success',
            updated_at_semantics='confirmed_assignment_saved_successfully',
            max_targets_per_run=(self.max_fund_targets_per_run or None),
        )
        print(f"经理名单刷新: 目标={len(fcodes)} 新鲜跳过={fresh_count} "
              f"失败冷却={cooldown_count} 待轮转={len(eligible) - len(fcodes)}", flush=True)
        if target_fcodes is not None:
            self._ensure_manager_list_api_source_evidence().update_context(
                target_scope='explicit_fund_codes', requested_fcodes=sorted(set(target_fcodes)),
            )

        success_count = 0
        failed_count = 0
        empty_count = 0
        no_current_count = 0
        batch_size = 500
        pending_updates = []  # 待更新的数据

        def flush_updates(updates: List[Dict]) -> None:
            nonlocal success_count, failed_count, no_current_count
            try:
                saved_codes = self._batch_update_fund_managers(updates)
            except Exception as exc:
                logger.error("采集基金经理列表批量保存失败: %s", exc)
                saved_codes = set()
                evidence = self._ensure_manager_list_api_source_evidence()
                for item in updates:
                    evidence.record_save_failure(fcode=item['fcode'], message=str(exc))
            success_count += len(saved_codes)
            failed_count += len(updates) - len(saved_codes)
            for item in updates:
                if item['fcode'] not in saved_codes:
                    self._finish_failed_manager_list_attempt(
                        item['fcode'], item['manager_list_attempted_at'], 'save_failed',
                    )
            no_current_count += sum(
                item.get('no_current_managers_confirmed') is True and item['fcode'] in saved_codes
                for item in updates
            )

        with ProgressMonitor(len(fcodes), desc="采集基金经理列表", unit="只基金") as progress:
            for fcode in fcodes:
                attempted_at = None
                try:
                    # Persist a start before consuming upstream quota. If the
                    # business write fails or the process stops, this start
                    # still enforces cooldown without renewing the old data.
                    attempted_at = self._start_manager_list_attempt(fcode)
                    # 调用API获取经理列表
                    evidence = self._ensure_manager_list_api_source_evidence()
                    evidence.record_request()
                    try:
                        result = self.api_client.get('/fundMNMangerList', params={'FCODE': fcode})
                    except Exception as exc:
                        evidence.record_transport_exception(fcode=fcode, exc=exc)
                        raise
                    records, quality = inspect_tiantian_manager_list_payload(
                        result,
                        endpoint_label=f"/fundMNMangerList FCODE={fcode}",
                    )
                    manager_fields = None
                    no_current_confirmed = False
                    if quality.accepted and quality.has_data:
                        manager_fields = _extract_manager_list_fields(records)
                        if not manager_fields['manager_ids'] or not manager_fields['manager_names']:
                            no_current_confirmed = _confirms_no_current_managers(result, fcode)
                            if not no_current_confirmed:
                                quality = replace(
                                    quality, accepted=False, has_data=False,
                                    status='manager_assignment_unverified',
                                    message='经理任职关系无法确认，保留原名单及其更新时间',
                                )
                    evidence.record_response_quality(
                        fcode=fcode,
                        quality=quality,
                        records=records,
                    )
                    if not quality.accepted:
                        logger.warning("采集基金%s经理列表失败: %s", fcode, quality.message)
                        self._finish_failed_manager_list_attempt(fcode, attempted_at, quality.status)
                        failed_count += 1
                        progress.update(1, success=False)
                        continue
                    if not quality.has_data:
                        empty_count += 1
                        failed_count += 1
                        self._finish_failed_manager_list_attempt(fcode, attempted_at, 'empty_response')
                        progress.update(1, success=False)
                        continue

                    update_item = {
                        'fcode': fcode,
                        'manager_list_updated_at': datetime.now(),
                        'manager_list_attempted_at': attempted_at,
                    }
                    if no_current_confirmed:
                        update_item.update({
                            'no_current_managers_confirmed': True,
                            'manager_ids': None,
                            'manager_names': None,
                            'assignment_source_payload': result,
                        })
                    else:
                        update_item.update(manager_fields)
                    pending_updates.append(update_item)

                    # 批量更新
                    if len(pending_updates) >= batch_size:
                        flush_updates(pending_updates)
                        pending_updates = []
                    progress.update(1, success=True)

                except Exception as e:
                    logger.error(f"采集基金{fcode}经理列表失败: {e}")
                    if attempted_at is not None:
                        self._finish_failed_manager_list_attempt(fcode, attempted_at, 'request_failed')
                    else:
                        self._ensure_manager_list_api_source_evidence().record_save_failure(
                            fcode=fcode, message=f'attempt_start_not_saved: {e}',
                        )
                    failed_count += 1
                    progress.update(1, success=False)

        # 处理剩余的更新
        if pending_updates:
            flush_updates(pending_updates)

        logger.info(f"基金经理列表采集完成: {success_count}只基金")
        self._ensure_manager_list_api_source_evidence().update_context(
            success_count=success_count,
            failed_count=failed_count,
            empty_count=empty_count,
            no_current_manager_fund_count=no_current_count,
        )
        return {
            'total_funds': len(fcodes),
            'success_funds': success_count,
            'failed_funds': failed_count,
            'empty_funds': empty_count,
            'cooldown_funds': cooldown_count,
        }

    def _start_manager_list_attempt(self, fcode: str) -> datetime:
        attempted_at = datetime.now()
        with self.db_manager.get_session(task_name='manager_list_attempt_start') as session:
            changed = session.query(Fund).filter(Fund.fcode == fcode).update({
                Fund.manager_list_attempted_at: attempted_at,
                Fund.manager_list_last_status: 'started',
                # Operational metadata must not refresh business timestamps.
                Fund.last_updated_at: Fund.last_updated_at,
            }, synchronize_session=False)
            if changed != 1:
                raise ValueError('manager_list_attempt_fund_not_found')
        return attempted_at

    def _finish_failed_manager_list_attempt(self, fcode: str, attempted_at: datetime, status: str) -> None:
        try:
            with self.db_manager.get_session(task_name='manager_list_attempt_failure') as session:
                changed = session.query(Fund).filter(
                    Fund.fcode == fcode, Fund.manager_list_attempted_at == attempted_at,
                ).update({
                    Fund.manager_list_last_status: status,
                    Fund.last_updated_at: Fund.last_updated_at,
                }, synchronize_session=False)
                if changed != 1:
                    raise ValueError('manager_list_attempt_missing_or_superseded')
        except Exception as exc:
            # A durable started state remains unresolved and in cooldown.
            logger.error('基金%s经理失败状态保存失败: %s', fcode, exc)
            self._ensure_manager_list_api_source_evidence().record_save_failure(fcode=fcode, message=str(exc))

    def _batch_update_fund_managers(self, updates: List[Dict]) -> Set[str]:
        """批量更新基金经理信息"""
        if not updates:
            return set()
        fcodes = [item.get('fcode') for item in updates if item.get('fcode')]
        if not fcodes:
            return set()

        receipts = []
        with self.db_manager.get_session(task_name='manager_bulk_update') as session:
            existing_rows = session.query(
                Fund.fcode,
                Fund.manager_ids,
                Fund.manager_names,
                Fund.manager_list_updated_at,
                Fund.manager_list_attempted_at,
                Fund.manager_list_last_status,
            ).filter(Fund.fcode.in_(fcodes)).all()
            existing_map = {
                row.fcode: row for row in existing_rows if getattr(row, 'fcode', None)
            }

            payload = []
            saved_manager_fcodes: list[str] = []
            for item in updates:
                fcode = item.get('fcode')
                existing = existing_map.get(fcode)
                if not fcode or existing is None:
                    continue
                if (existing.manager_list_attempted_at != item.get('manager_list_attempted_at')
                        or existing.manager_list_last_status != 'started'):
                    self._ensure_manager_list_api_source_evidence().record_save_failure(
                        fcode=fcode, message='manager_list_attempt_superseded',
                    )
                    continue

                normalized_ids = ','.join(_split_manager_tokens(item.get('manager_ids')))
                normalized_names = ','.join(_split_manager_tokens(item.get('manager_names')))

                mapping = {
                    'fcode': fcode,
                    'manager_list_updated_at': item['manager_list_updated_at'],
                    'manager_list_last_status': 'ok',
                }
                no_current_confirmed = item.get('no_current_managers_confirmed') is True
                if not no_current_confirmed and (not normalized_ids or not normalized_names):
                    raise ValueError('unconfirmed_manager_assignment_cannot_refresh_success_time')
                if no_current_confirmed:
                    mapping['manager_list_last_status'] = 'no_current_managers_confirmed'
                    mapping['manager_ids'] = None
                    mapping['manager_names'] = None
                    receipts.append({
                        'schema_version': 1,
                        'source': 'fundMNMangerList',
                        'fcode': fcode,
                        'status': 'no_current_managers_confirmed',
                        'retrieved_at': mapping['manager_list_updated_at'].isoformat(),
                        'previous_manager_ids': existing.manager_ids,
                        'previous_manager_names': existing.manager_names,
                        'source_payload': item['assignment_source_payload'],
                    })
                elif normalized_ids:
                    mapping['manager_ids'] = normalized_ids
                elif getattr(existing, 'manager_ids', None):
                    mapping['manager_ids'] = existing.manager_ids

                if not no_current_confirmed:
                    if normalized_names:
                        mapping['manager_names'] = normalized_names
                    elif getattr(existing, 'manager_names', None):
                        mapping['manager_names'] = existing.manager_names

                if normalized_ids or normalized_names or no_current_confirmed:
                    saved_manager_fcodes.append(str(fcode))
                payload.append(mapping)

            if payload:
                session.bulk_update_mappings(Fund, payload)

            missing_count = len(updates) - len([item for item in updates if item.get('fcode') in existing_map])
            if missing_count > 0:
                logger.warning(f"基金经理批量更新跳过{missing_count}只不存在的基金")
                evidence = self._ensure_manager_list_api_source_evidence()
                missing_codes = [
                    str(item.get('fcode') or '').strip()
                    for item in updates
                    if item.get('fcode') not in existing_map
                ]
                for fcode in missing_codes:
                    evidence.record_save_failure(fcode=fcode, message="fund_not_found")

        # get_session commits before any saved-record claim or clear receipt.
        evidence = self._ensure_manager_list_api_source_evidence()
        for fcode in saved_manager_fcodes:
            evidence.record_saved_records(fcode=fcode, record_count=1)
        for receipt in receipts:
            print('manager_assignment_evidence=' + json.dumps(receipt, ensure_ascii=False), flush=True)
        return {item['fcode'] for item in payload}

    def _collect_manager_details(self, workers: int, force_update: bool) -> Dict[str, int]:
        """
        采集经理详细信息

        Args:
            workers: 并发数
            force_update: 强制更新

        Returns:
            采集的经理数
        """
        # 获取所有唯一的经理ID
        with self.db_manager.get_session(readonly=True) as session:
            # 直接查询manager_ids字段，避免detached对象问题
            manager_ids_list = [
                f.manager_ids for f in
                session.query(Fund.manager_ids).filter(Fund.manager_ids.isnot(None)).all()
            ]

        all_mgr_ids: Set[str] = set()
        for manager_ids in manager_ids_list:
            if manager_ids:
                # manager_ids可能是"30678620"或"30075269,30073143"
                ids = [mid.strip() for mid in manager_ids.split(',')]
                all_mgr_ids.update(ids)

        mgr_ids = sorted(all_mgr_ids)
        logger.info(f"共{len(mgr_ids)}位唯一经理需要采集详情")

        if not force_update:
            # 缺失优先，已有详情按独立更新时间轮转刷新。
            with self.db_manager.get_session(readonly=True) as session:
                existing_rows = session.query(
                    FundManager.mgr_id,
                    FundManager.last_updated_at,
                ).filter(FundManager.mgr_id.in_(mgr_ids)).all()
                existing_by_id = {
                    row.mgr_id: row.last_updated_at
                    for row in existing_rows
                    if row.mgr_id
                }
            stale_cutoff = datetime.now() - timedelta(days=self.detail_refresh_stale_days)
            candidates = [
                mid
                for mid in mgr_ids
                if mid not in existing_by_id
                or existing_by_id.get(mid) is None
                or existing_by_id[mid] < stale_cutoff
            ]
            candidates.sort(
                key=lambda mid: (
                    0 if mid not in existing_by_id else 1,
                    existing_by_id.get(mid) or datetime.min,
                    mid,
                )
            )
            if self.max_detail_targets_per_run > 0:
                candidates = candidates[:self.max_detail_targets_per_run]
            mgr_ids = candidates

            logger.info(f"缺失/过期过滤后剩余{len(mgr_ids)}位经理需要采集")

        self._ensure_manager_detail_api_source_evidence().reset(
            stage="manager_detail",
            force_update=bool(force_update),
            target_count=len(mgr_ids),
            collection_mode=("force_full_refresh" if force_update else "bounded_missing_and_stale_refresh"),
            refresh_stale_days=self.detail_refresh_stale_days,
            max_targets_per_run=(self.max_detail_targets_per_run or None),
        )

        if not mgr_ids:
            logger.info("所有经理详情已采集完成")
            self._ensure_manager_detail_api_source_evidence().update_context(
                success_count=0, failed_count=0, skipped_count=0,
            )
            return {
                'requested_manager_count': 0,
                'updated_manager_count': 0,
                'failed_manager_count': 0,
                'skipped_manager_count': 0,
            }

        # 使用通用并发采集方法
        before_failed = self.stats.get('failed', 0)
        before_skipped = self.stats.get('skipped', 0)
        updated_count = self.concurrent_collect(
            items=mgr_ids,
            collect_func=self.collect_manager_detail,
            desc="采集经理详情",
            unit="位经理",
            workers=workers
        )
        failed_count = max(self.stats.get('failed', 0) - before_failed, 0)
        skipped_count = max(self.stats.get('skipped', 0) - before_skipped, 0)
        self._ensure_manager_detail_api_source_evidence().update_context(
            success_count=updated_count, failed_count=failed_count, skipped_count=skipped_count,
        )
        return {
            'requested_manager_count': len(mgr_ids),
            'updated_manager_count': updated_count,
            'failed_manager_count': failed_count,
            'skipped_manager_count': skipped_count,
        }

    def collect_manager_detail(self, mgr_id: str) -> bool:
        """
        采集单个经理的详细信息

        Args:
            mgr_id: 经理ID

        Returns:
            是否成功
        """
        mgr_id = str(mgr_id or '').strip()
        endpoint_label = f"/fundMSNMangerInfo FCODE={mgr_id}"
        evidence = self._ensure_manager_detail_api_source_evidence()

        for attempt in range(1, MANAGER_DETAIL_RETRY_MAX_ATTEMPTS + 1):
            evidence.record_request()
            try:
                detail = self.api.get_manager_info(mgr_id)
            except Exception as exc:
                evidence.record_transport_exception(fcode=mgr_id, exc=exc)
                raise
            detail_payload, quality = inspect_tiantian_manager_info_payload(
                detail,
                endpoint_label=endpoint_label,
                expected_manager_id=mgr_id,
            )
            evidence.record_response_quality(
                fcode=mgr_id,
                quality=quality,
                records=[detail_payload] if isinstance(detail_payload, dict) else [],
            )
            if quality.accepted and quality.has_data and isinstance(detail_payload, dict):
                saved = self._save_manager_detail(mgr_id, detail_payload)
                if saved:
                    evidence.record_saved_records(fcode=mgr_id, record_count=1)
                else:
                    evidence.record_save_failure(fcode=mgr_id, message="manager_detail_save_returned_false")
                return saved

            should_retry = (
                attempt < MANAGER_DETAIL_RETRY_MAX_ATTEMPTS
                and _should_retry_manager_detail_quality(quality)
            )
            if should_retry:
                wait_seconds = 0.5 * attempt
                logger.warning(
                    "获取经理%s详情失败，准备重试(%s/%s): %s",
                    mgr_id,
                    attempt,
                    MANAGER_DETAIL_RETRY_MAX_ATTEMPTS,
                    quality.message,
                )
                time.sleep(wait_seconds)
                continue

            if not quality.accepted:
                logger.warning(f"获取经理{mgr_id}详情失败: {quality.message}")
            else:
                logger.warning(f"获取经理{mgr_id}详情失败或无数据: {quality.message}")
            return False

        return False

    def _save_manager_detail(self, mgr_id: str, data: Dict) -> bool:
        """
        保存经理详情到数据库

        Args:
            mgr_id: 经理ID
            data: 详情数据

        Returns:
            是否成功
        """
        try:
            mgr_id = str(mgr_id or '').strip()
            # Also protect direct save callers: a valid envelope/name alone
            # must never allow another manager's data to replace this row.
            _, quality = inspect_tiantian_manager_info_record(data, expected_manager_id=mgr_id)
            if not quality.accepted or not quality.has_data:
                logger.warning("经理%s详情未保存: %s", mgr_id, quality.message)
                return False

            values = _manager_detail_snapshot_values(data)

            with self.db_manager.get_session() as session:
                # 查找或创建经理记录（使用merge避免并发冲突）
                manager = session.query(FundManager).filter_by(mgr_id=mgr_id).first()

                if not manager:
                    # 创建新记录时立即设置必需的name字段
                    manager = FundManager(mgr_id=mgr_id, name=data['MGRNAME'])
                    session.add(manager)
                    # 立即flush以获取可能的并发冲突
                    try:
                        session.flush()
                    except Exception:
                        # 如果flush失败（可能是并发插入），回滚并重新查询
                        session.rollback()
                        manager = session.query(FundManager).filter_by(mgr_id=mgr_id).first()
                        if not manager:
                            raise

                # One current response, rather than newer values merged with
                # optional fields retained from unrelated old observations.
                for field, value in values.items():
                    setattr(manager, field, value)

                manager.last_updated_at = datetime.now()

                # commit 由 get_session() 上下文管理器自动处理
                logger.debug(f"经理{mgr_id}({manager.name})详情保存成功")
                return True

        except Exception as e:
            logger.error(f"保存经理{mgr_id}详情失败: {e}")
            return False
