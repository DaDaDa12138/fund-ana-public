"""
进度监控工具：基于tqdm的进度条
"""

from tqdm import tqdm
import sys
import time
import threading
from typing import Optional, Callable


class ProgressMonitor:
    """进度监控器"""

    def __init__(
        self,
        total,
        desc="处理中",
        unit="个",
        ncols=100,
        show_completion_summary=True,
        heartbeat_interval_seconds=30,
    ):
        """
        初始化进度监控器

        Args:
            total: 总任务数
            desc: 描述
            unit: 单位
            ncols: 进度条宽度
            show_completion_summary: 关闭时仅关闭默认完成摘要打印
            heartbeat_interval_seconds: 非交互式环境下的进度心跳间隔
        """
        self.total = total
        self.desc = desc
        self.unit = unit
        self.show_completion_summary = bool(show_completion_summary)
        self.heartbeat_interval_seconds = max(float(heartbeat_interval_seconds or 0), 0.0)
        self._lock = threading.Lock()
        self._interactive = hasattr(sys.stderr, 'isatty') and sys.stderr.isatty()

        self.pbar = tqdm(
            total=total,
            desc=desc,
            unit=unit,
            ncols=ncols,
            bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]',
            disable=not self._interactive,
        )

        self.start_time = time.time()
        self._last_heartbeat_at = self.start_time
        self.success_count = 0
        self.failure_count = 0
        self.skipped_count = 0

    def update(self, n=1, success=True, skipped=False, desc=None):
        """
        更新进度

        Args:
            n: 增加的数量
            success: 是否成功
            skipped: 是否跳过
            desc: 新的描述
        """
        with self._lock:
            if skipped:
                self.skipped_count += n
            elif success:
                self.success_count += n
            else:
                self.failure_count += n

            if desc:
                self.pbar.set_description(desc)

            self.pbar.update(n)
            self._emit_non_interactive_heartbeat_locked()

    def _emit_non_interactive_heartbeat_locked(self):
        """在日志管道环境中定期输出进度，避免长任务被误判为无输出卡死。"""
        if self._interactive or self.heartbeat_interval_seconds <= 0:
            return
        now = time.time()
        if now - self._last_heartbeat_at < self.heartbeat_interval_seconds:
            return
        total_processed = self.success_count + self.failure_count + self.skipped_count
        elapsed = now - self.start_time
        rate = total_processed / elapsed if elapsed > 0 and total_processed > 0 else 0.0
        print(
            f"[progress] {self.desc}: processed={total_processed:,}/{self.total:,} "
            f"success={self.success_count:,} skipped={self.skipped_count:,} "
            f"failed={self.failure_count:,} elapsed={elapsed:.1f}s "
            f"rate={rate:.2f} {self.unit}/s",
            flush=True,
        )
        self._last_heartbeat_at = now

    def set_postfix(self, **kwargs):
        """设置后缀信息"""
        if not self._interactive:
            return
        with self._lock:
            self.pbar.set_postfix(**kwargs)

    def close(self):
        """关闭进度条"""
        self.pbar.close()

        if not self.show_completion_summary:
            return

        # 计算统计信息
        elapsed = time.time() - self.start_time
        total_processed = self.success_count + self.failure_count + self.skipped_count

        print(f"\n{'='*60}")
        print(f"任务完成统计")
        print(f"{'='*60}")
        print(f"总任务数:    {self.total:,}")
        print(f"已处理:      {total_processed:,}")
        print(f"成功:        {self.success_count:,}")
        print(f"跳过:        {self.skipped_count:,}")
        print(f"失败:        {self.failure_count:,}")
        if total_processed > 0:
            print(f"成功率:      {self.success_count/total_processed*100:.2f}%")
        print(f"总耗时:      {elapsed:.1f}秒")
        if total_processed > 0 and elapsed > 0:
            print(f"平均速度:    {total_processed/elapsed:.2f} {self.unit}/秒")
        print(f"{'='*60}\n")

    def __enter__(self):
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出"""
        self.close()


class BatchProgress:
    """批处理进度监控"""

    def __init__(self, total_batches, batch_size, desc="批处理"):
        """
        初始化批处理进度

        Args:
            total_batches: 总批次数
            batch_size: 每批大小
            desc: 描述
        """
        self.total_batches = total_batches
        self.batch_size = batch_size
        self.current_batch = 0

        self.batch_pbar = tqdm(
            total=total_batches,
            desc=f"{desc} (批次)",
            unit="批",
            position=0
        )

        self.item_pbar = tqdm(
            total=batch_size,
            desc=f"{desc} (当前批)",
            unit="个",
            position=1
        )

    def next_batch(self, batch_num=None):
        """开始下一批"""
        self.current_batch += 1
        if batch_num is not None:
            self.current_batch = batch_num

        self.batch_pbar.update(1)
        self.item_pbar.reset()

    def update_item(self, n=1):
        """更新当前批次的项目进度"""
        self.item_pbar.update(n)

    def close(self):
        """关闭进度条"""
        self.batch_pbar.close()
        self.item_pbar.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def track_progress(iterable, desc="处理中", total=None, callback: Optional[Callable] = None):
    """
    跟踪可迭代对象的进度

    Args:
        iterable: 可迭代对象
        desc: 描述
        total: 总数（如果可迭代对象不支持len）
        callback: 每次迭代后的回调函数

    Yields:
        迭代项
    """
    if total is None:
        try:
            total = len(iterable)
        except TypeError:
            total = None

    with tqdm(iterable, desc=desc, total=total, unit="个") as pbar:
        for item in pbar:
            yield item
            if callback:
                callback(item)
