"""
基础采集器类
"""

import logging
import threading
from typing import Optional, Dict, Any, Callable, List
from concurrent.futures import ThreadPoolExecutor, as_completed, CancelledError
from database.connection import get_db_manager
from database.models import CollectionLog
from api.client import APIClient
from api.endpoints import FundAPIEndpoints
from utils import ProgressMonitor, chunks

logger = logging.getLogger(__name__)


class BaseCollector:
    """基础采集器"""

    def __init__(self, config=None, db_manager=None, api_client=None):
        """
        初始化采集器

        Args:
            config: 配置字典
            db_manager: 数据库管理器
            api_client: API客户端
        """
        self.config = config or {}
        self.db_manager = db_manager or get_db_manager()

        # API客户端
        if api_client:
            self.api_client = api_client
        else:
            self.api_client = APIClient(self.config)

        # API端点
        self.api = FundAPIEndpoints(self.api_client)

        # 统计信息
        self.stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'skipped': 0,
        }
        # v1.9.28修复：stats 字典被多线程并发更新，+= 操作非原子，需加锁保护
        self._stats_lock = threading.Lock()
        self._recent_outcomes: dict[str, dict[str, Any]] = {}
        self._recent_outcomes_lock = threading.Lock()

        logger.info(f"{self.__class__.__name__} 初始化完成")

    def _resolve_submit_batch_size(self, workers: int, total_items: int) -> int:
        """限制单轮提交的 future 数量，降低大任务调度和内存开销。"""
        try:
            normalized_workers = int(workers)
        except (TypeError, ValueError):
            normalized_workers = 1
        try:
            normalized_total = int(total_items)
        except (TypeError, ValueError):
            normalized_total = 0

        normalized_workers = max(1, normalized_workers)
        submit_batch_size = max(normalized_workers * 4, 200)
        if normalized_total > 0:
            return min(normalized_total, submit_batch_size)
        return submit_batch_size

    def concurrent_collect(
        self,
        items: List,
        collect_func: Callable,
        desc: str,
        unit: str,
        workers: int = 5,
        id_field: str = None
    ) -> int:
        """
        通用并发采集方法

        Args:
            items: 待采集项列表
            collect_func: 采集函数 (item) -> bool
            desc: 进度条描述
            unit: 进度条单位
            workers: 并发数
            id_field: 用于日志显示的ID字段名（可选）

        Returns:
            成功采集数
        """
        success_count = 0
        failed_items = []  # 记录失败的项目

        with ProgressMonitor(len(items), desc=desc, unit=unit) as progress:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                submit_batch_size = self._resolve_submit_batch_size(workers, len(items))

                for item_batch in chunks(items, submit_batch_size):
                    futures = {executor.submit(collect_func, item): item for item in item_batch}

                    for future in as_completed(futures):
                        item = futures[future]
                        try:
                            result = future.result()
                            if result:
                                success_count += 1
                                self.update_stats(success=True)
                            else:
                                self.update_stats(success=False, skipped=True)

                            progress.update(1, success=result)
                            # 设置进度条后缀
                            item_id = getattr(item, id_field, item) if id_field and hasattr(item, id_field) else item
                            progress.set_postfix(current=str(item_id)[:10], total=f"{success_count}")

                        except CancelledError:
                            # concurrent.futures.CancelledError 是 Exception 子类，
                            # 需放在 except Exception 之前以区分处理
                            item_id = getattr(item, id_field, item) if id_field and hasattr(item, id_field) else item
                            logger.warning(f"采集被取消: {item_id}")
                            self.update_stats(success=False)
                            failed_items.append((item, 'CancelledError'))
                            progress.update(1, success=False)

                        except Exception as e:
                            item_id = getattr(item, id_field, item) if id_field and hasattr(item, id_field) else item
                            logger.error(f"采集失败: {item_id} - {e}")
                            self.update_stats(success=False)
                            failed_items.append((item, str(e)))
                            progress.update(1, success=False)

        # 将失败项持久化到 collection_log 表
        if failed_items:
            for failed_item, error_msg in failed_items:
                fcode = getattr(failed_item, 'fcode', None)
                if not fcode and id_field:
                    fcode = getattr(failed_item, id_field, None)
                fcode_str = str(fcode) if fcode else str(failed_item)[:50]
                self.log_collection(
                    task_type=desc,
                    fcode=fcode_str,
                    status='failed',
                    message=error_msg[:500]
                )
            logger.warning(f"采集汇总: 总计={len(items)}, 成功={success_count}, "
                          f"失败={len(failed_items)}, 跳过={len(items) - success_count - len(failed_items)}")

        return success_count

    def log_collection(self, task_type, fcode=None, status='success', message=None):
        """
        记录采集日志

        Args:
            task_type: 任务类型
            fcode: 基金代码
            status: 状态
            message: 消息
        """
        try:
            with self.db_manager.get_session() as session:
                log = CollectionLog(
                    task_type=task_type,
                    fcode=fcode,
                    status=status,
                    message=message
                )
                session.add(log)
        except Exception as e:
            logger.error(f"记录采集日志失败: {e}")

    def update_stats(self, success=True, skipped=False):
        """
        更新统计信息

        Args:
            success: 是否成功
            skipped: 是否跳过
        """
        with self._stats_lock:
            self.stats['total'] += 1

            if skipped:
                self.stats['skipped'] += 1
            elif success:
                self.stats['success'] += 1
            else:
                self.stats['failed'] += 1

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        with self._stats_lock:
            stats = {**self.stats}

        if stats['total'] > 0:
            stats['success_rate'] = (stats['success'] / stats['total']) * 100
        else:
            stats['success_rate'] = 0

        return stats

    def print_stats(self):
        """打印统计信息"""
        stats = self.get_stats()

        print(f"\n{'='*50}")
        print(f"{self.__class__.__name__} 统计")
        print(f"{'='*50}")
        print(f"总数:     {stats['total']:,}")
        print(f"成功:     {stats['success']:,}")
        print(f"失败:     {stats['failed']:,}")
        print(f"跳过:     {stats['skipped']:,}")
        print(f"成功率:   {stats['success_rate']:.2f}%")
        print(f"{'='*50}\n")

    def reset_stats(self):
        """重置统计信息"""
        with self._stats_lock:
            self.stats = {
                'total': 0,
                'success': 0,
                'failed': 0,
                'skipped': 0,
            }

    def record_recent_outcome(
        self,
        key: str,
        *,
        success: bool,
        status: str,
        message: str | None = None,
    ) -> None:
        normalized_key = str(key or "").strip()
        if not normalized_key:
            return
        with self._recent_outcomes_lock:
            self._recent_outcomes[normalized_key] = {
                'success': bool(success),
                'status': str(status or '').strip() or ('ok' if success else 'failed'),
                'message': str(message or '').strip(),
            }

    def consume_recent_outcome(self, key: str) -> Optional[Dict[str, Any]]:
        normalized_key = str(key or "").strip()
        if not normalized_key:
            return None
        with self._recent_outcomes_lock:
            return self._recent_outcomes.pop(normalized_key, None)

    def snapshot_recent_outcomes(self) -> Dict[str, Dict[str, Any]]:
        with self._recent_outcomes_lock:
            return {
                key: dict(value)
                for key, value in self._recent_outcomes.items()
            }
