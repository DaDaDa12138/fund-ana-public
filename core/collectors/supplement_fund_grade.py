"""基金三机构评级补全采集器（P1-09.3 / B14）。

目的：
- 拉取 tiantian ``/fundGradeDetail`` 的 ``RDATE / ZSPJ / SZPJ3 / JAPJ / HTPJ`` 最近一期，
  落到 ``fund_grades`` 表（历史保留）+ ``funds.rating_multi_source`` 聚合字段。
- 恢复当前 ``funds.rating`` 大多为 ``'-'`` 的空洞，供审计脚本
  ``scripts/audit_external_curation_overlap.py`` 与 P2-02 推荐主表字段使用。

约束：
- 主链路只对**主榜候选池**（或调用方传入的 fcodes 集合）执行，避免 7800+ 只基金全量扫描
  导致 API 消耗过大。
- 逐只调用（tiantian 无批量），默认并发由调用方控制，默认 ``workers=5``。
- 幂等：已有 ``(fcode, rdate)`` 记录直接更新不插入。
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from collectors.base_collector import BaseCollector
from database.models import Fund, FundGrade
from utils import ProgressMonitor

logger = logging.getLogger(__name__)


_PLACEHOLDER_VALUES = {"", "--", "---", "N/A", "null", "NULL", "暂无", "-"}


def _normalize_grade_value(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    if not text or text in _PLACEHOLDER_VALUES:
        return None
    return text


def _parse_rdate(value: Any) -> Optional[date]:
    text = str(value or "").strip()
    if not text or text in _PLACEHOLDER_VALUES:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _aggregate_rating_multi_source(
    zspj_zs: Optional[str],
    szpj3: Optional[str],
    japj: Optional[str],
    htpj: Optional[str],
) -> Optional[str]:
    """把四家机构的最近一期评级聚合成一个可阅读字符串。

    格式：``ZS:5|SH:4|JA:-|HT:-``；全空返回 None（funds.rating_multi_source 保持空）。
    """
    parts = [
        f"ZS:{zspj_zs or '-'}",
        f"SH:{szpj3 or '-'}",
        f"JA:{japj or '-'}",
        f"HT:{htpj or '-'}",
    ]
    meaningful = [p for p in parts if not p.endswith(":-")]
    if not meaningful:
        return None
    return "|".join(parts)


class FundGradeCollector(BaseCollector):
    """基金三机构评级采集器（P1-09.3）。"""

    def __init__(self, config=None, db_manager=None, api_client=None):
        super().__init__(config, db_manager, api_client)

    def supplement_fcodes(
        self,
        fcodes: Iterable[str],
        *,
        workers: int = 5,
    ) -> Dict[str, Any]:
        """对传入的 ``fcodes`` 集合拉取 /fundGradeDetail 最近一期。

        Returns:
            {"processed_total": N, "updated_total": M, "failed_total": K,
             "errors_sample": [...]}
        """
        normalized_fcodes = sorted({str(fc).strip() for fc in fcodes if str(fc).strip()})
        if not normalized_fcodes:
            return {"processed_total": 0, "updated_total": 0, "failed_total": 0, "errors_sample": []}

        grade_payloads: List[Dict[str, Any]] = []
        failed_fcodes: List[Dict[str, Any]] = []

        get_fund_grade = getattr(self.api, "get_fund_grade", None)
        if not callable(get_fund_grade):
            return {
                "processed_total": 0,
                "updated_total": 0,
                "failed_total": 0,
                "errors_sample": [{"reason": "endpoint_missing"}],
            }

        with ProgressMonitor(len(normalized_fcodes), desc="采集基金评级", unit="只") as progress:
            with ThreadPoolExecutor(max_workers=max(1, int(workers))) as pool:
                future_to_code = {
                    pool.submit(self._fetch_single, get_fund_grade, fc): fc
                    for fc in normalized_fcodes
                }
                for future in as_completed(future_to_code):
                    fc = future_to_code[future]
                    try:
                        item = future.result()
                    except Exception as exc:  # pragma: no cover - defensive
                        failed_fcodes.append({"fcode": fc, "reason": str(exc)})
                        progress.update(1, success=False)
                        continue
                    if item is None:
                        failed_fcodes.append({"fcode": fc, "reason": "empty_or_invalid_payload"})
                        progress.update(1, success=False)
                        continue
                    grade_payloads.append(item)
                    progress.update(1, success=True)

        updated_total = self._persist_grades(grade_payloads)
        return {
            "processed_total": len(normalized_fcodes),
            "updated_total": updated_total,
            "failed_total": len(failed_fcodes),
            "errors_sample": failed_fcodes[:10],
        }

    def _fetch_single(self, get_fund_grade, fcode: str) -> Optional[Dict[str, Any]]:
        """拉取单只基金最近一期评级。"""
        try:
            payload = get_fund_grade(fcode, page_index=1, page_size=1)
        except Exception as exc:
            logger.debug("基金%s 评级拉取失败: %s", fcode, exc)
            return None
        if not isinstance(payload, dict):
            return None
        datas = payload.get("Datas")
        if not isinstance(datas, list) or not datas:
            return None
        row = datas[0]
        if not isinstance(row, dict):
            return None
        rdate = _parse_rdate(row.get("RDATE"))
        if rdate is None:
            return None
        return {
            "fcode": fcode,
            "rdate": rdate,
            "zspj_zs": _normalize_grade_value(row.get("ZSPJ")),
            "szpj3": _normalize_grade_value(row.get("SZPJ3")),
            "japj": _normalize_grade_value(row.get("JAPJ")),
            "htpj": _normalize_grade_value(row.get("HTPJ")),
        }

    def _persist_grades(self, records: List[Dict[str, Any]]) -> int:
        if not records:
            return 0

        updated = 0
        with self.db_manager.get_session(task_name="fund_grade_supplement") as session:
            for item in records:
                stmt = sqlite_insert(FundGrade).values(
                    fcode=item["fcode"],
                    rdate=item["rdate"],
                    zspj_zs=item["zspj_zs"],
                    szpj3=item["szpj3"],
                    japj=item["japj"],
                    htpj=item["htpj"],
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=[FundGrade.fcode, FundGrade.rdate],
                    set_={
                        "zspj_zs": stmt.excluded.zspj_zs,
                        "szpj3": stmt.excluded.szpj3,
                        "japj": stmt.excluded.japj,
                        "htpj": stmt.excluded.htpj,
                    },
                )
                session.execute(stmt)

                aggregated = _aggregate_rating_multi_source(
                    item["zspj_zs"], item["szpj3"], item["japj"], item["htpj"]
                )
                session.query(Fund).filter_by(fcode=item["fcode"]).update(
                    {
                        Fund.rating_multi_source: aggregated,
                        Fund.last_updated_at: datetime.now(),
                    },
                    synchronize_session=False,
                )
                updated += 1

        return updated


def run_fund_grade_supplement(
    config: Dict[str, Any],
    db_manager,
    api_client,
    *,
    fcodes: Iterable[str],
    workers: int = 5,
) -> Dict[str, Any]:
    """独立入口：供 scheduler 或 CLI 调用。"""

    collector = FundGradeCollector(config=config, db_manager=db_manager, api_client=api_client)
    return collector.supplement_fcodes(fcodes, workers=workers)
