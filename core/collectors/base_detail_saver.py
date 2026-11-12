"""
通用详情保存器 - 处理并发安全的数据库更新

本模块提供了一个通用的详情保存器类，用于处理基金详情、经理信息、公司信息等数据的保存。
主要解决了以下问题：
1. 并发安全的数据库更新（使用 get_or_create 模式）
2. 字段映射和类型转换的统一处理
3. 减少重复代码，提高可维护性

设计原则：
- SOLID: 单一职责（只负责数据保存）
- DRY: 消除三个采集器中的重复代码
- KISS: 简单的API设计，易于使用
"""
from typing import Dict, Any, Callable, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


def safe_int(value) -> Optional[int]:
    """
    安全转换为整数

    Args:
        value: 待转换的值（可能包含逗号、空格等）

    Returns:
        转换后的整数，失败返回None
    """
    if value is None:
        return None
    try:
        return int(float(str(value).replace(',', '').replace(' ', '')))
    except (ValueError, TypeError):
        return None


def safe_float(value) -> Optional[float]:
    """
    安全转换为浮点数

    Args:
        value: 待转换的值（可能包含逗号、百分号等）

    Returns:
        转换后的浮点数，失败返回None
    """
    if value is None:
        return None
    try:
        return float(str(value).replace(',', '').replace('%', '').replace(' ', ''))
    except (ValueError, TypeError):
        return None


def safe_str(value) -> Optional[str]:
    """
    安全转换为字符串

    Args:
        value: 待转换的值

    Returns:
        转换后的字符串，空值返回None
    """
    if value is None or value == '':
        return None
    return str(value).strip()


class BaseDetailSaver:
    """
    通用详情保存器

    提供并发安全的数据库更新功能，支持字段映射和类型转换。

    使用示例：
        saver = BaseDetailSaver(db_manager, FundManager, 'mgr_id')

        field_mapping = {
            'name': ('name', safe_str),
            'star': ('star', safe_int),
            'workTime': ('work_time', safe_str)
        }

        success = saver.save_detail('001', api_data, field_mapping)
    """

    def __init__(self, db_manager, model_class, id_field_name: str = 'id'):
        """
        初始化保存器

        Args:
            db_manager: 数据库管理器实例
            model_class: SQLAlchemy模型类
            id_field_name: 主键字段名（默认'id'）
        """
        self.db_manager = db_manager
        self.model_class = model_class
        self.id_field_name = id_field_name

    def save_detail(
        self,
        id_value: str,
        data: Dict[str, Any],
        field_mapping: Dict[str, Tuple[Callable, ...]]
    ) -> bool:
        """
        保存详情数据到数据库

        Args:
            id_value: 记录ID（如基金代码、经理ID等）
            data: API返回的原始数据字典
            field_mapping: 字段映射字典
                格式: {api_field: (model_field, parser_func)}
                示例: {'name': ('name', safe_str), 'star': ('star', safe_int)}

        Returns:
            bool: 保存是否成功

        注意:
            - 使用get_or_create模式确保并发安全
            - 解析失败的字段会被跳过，不影响其他字段
            - 所有数据库操作在事务中执行
        """
        try:
            with self.db_manager.get_session() as session:
                # 获取或创建记录（并发安全）
                obj = self._get_or_create(session, id_value)

                # 遍历字段映射，更新数据
                for api_field, (model_field, parser_func) in field_mapping.items():
                    if api_field in data and data[api_field] is not None:
                        try:
                            # 解析并设置字段值
                            value = parser_func(data[api_field])
                            if value is not None:  # 只设置非空值
                                setattr(obj, model_field, value)
                        except (ValueError, TypeError) as e:
                            # 单个字段解析失败不影响其他字段
                            logger.debug(f"字段{api_field}解析失败: {e}")
                            continue

                # 提交事务
                session.commit()
                return True

        except Exception as e:
            logger.error(f"保存{self.model_class.__name__}失败 (ID={id_value}): {e}")
            return False

    def _get_or_create(self, session, id_value):
        """
        获取或创建记录（并发安全）

        Args:
            session: 数据库会话
            id_value: 记录ID

        Returns:
            模型实例

        实现说明:
            1. 先尝试查询现有记录
            2. 如果不存在，创建新记录并flush
            3. 如果flush失败（并发冲突），回滚后重新查询
            4. 这种模式确保在高并发环境下的数据一致性
        """
        filter_kwargs = {self.id_field_name: id_value}
        obj = session.query(self.model_class).filter_by(**filter_kwargs).first()

        if not obj:
            # 创建新记录
            obj = self.model_class(**filter_kwargs)
            session.add(obj)
            try:
                session.flush()  # 立即写入，检测冲突
            except Exception:
                # 并发冲突：其他线程已创建，回滚后重新查询
                session.rollback()
                obj = session.query(self.model_class).filter_by(**filter_kwargs).first()
                if not obj:
                    # 极端情况：重新查询仍失败，抛出异常
                    raise

        return obj


# 导出常用的转换函数
__all__ = ['BaseDetailSaver', 'safe_int', 'safe_float', 'safe_str']
