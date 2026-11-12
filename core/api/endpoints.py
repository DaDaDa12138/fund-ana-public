"""
API端点封装：提供便捷的方法调用天天基金API
"""

import logging
from typing import List, Dict, Optional, Tuple
from config.api_config import APIEndpoints, FundType, SortColumn, TimeRange
from utils.tiantian_data_quality import inspect_tiantian_search_payload

logger = logging.getLogger(__name__)

# Tiantian /fundMNRank 的 FundType 参数与项目内部 type_code 存在少量差异。
# 保持内部 type_code/数据库口径不变，仅在实际调用排行接口时做最小映射。
RANK_FUND_TYPE_PARAM_MAP = {
    FundType.QDII: '6',
    FundType.FOF: '15',
}


class FundAPIEndpoints:
    """基金API端点封装"""

    def __init__(self, client):
        """
        初始化

        Args:
            client: APIClient实例
        """
        self.client = client

    def get_bond_fund_rank(self, fund_type=FundType.BOND, sort_column=SortColumn.RETURN_1Y,
                            page_index=1, page_size=30, only_buyable=False, **kwargs) -> Optional[Dict]:
        """
        获取债券型基金排行

        Args:
            fund_type: 基金类型（31=债券型, 32=定开债）
            sort_column: 排序字段
            page_index: 页码
            page_size: 每页数量
            only_buyable: 仅可购基金
            **kwargs: 其他筛选参数

        Returns:
            {
                'Datas': [...],  # 基金列表
                'TotalCount': 1234  # 总数
            }
        """
        rank_fund_type = RANK_FUND_TYPE_PARAM_MAP.get(fund_type, fund_type)
        params = {
            'FundType': rank_fund_type,
            'SortColumn': sort_column,
            'Sort': 'desc',
            'pageIndex': page_index,
            'pageSize': page_size,
            'ISABNORMAL': 'true',
        }

        if only_buyable:
            params['BUY'] = 'true'

        # 合并额外参数
        params.update(kwargs)
        if int(params['pageIndex']) < 1 or not 1 <= int(params['pageSize']) <= 30:
            raise ValueError('/fundMNRank requires pageIndex >= 1 and pageSize in 1..30')

        result = self.client.get(APIEndpoints.FUND_RANK, params=params)
        return result

    def get_fund_detail(self, fcode: str) -> Optional[Dict]:
        """
        获取基金详细信息

        Args:
            fcode: 基金代码

        Returns:
            基金详情数据
        """
        params = {'FCODE': fcode}
        result = self.client.get(APIEndpoints.FUND_DETAIL, params=params)
        return result

    def get_fund_stop_watch(self, fcode: str) -> Optional[Dict]:
        """
        获取基金简介 /fundMNStopWatch（P1-09.1 / B14）。

        用途：当 /fundMNDetailInformation 上游业务 fail 或返回空 `Datas` 时，
        作为轻量兜底采集基金类型元数据（FTYPE / FEATURE / BFUNDTYPE / ESTABDATE）。
        ⚠️ StopWatch 返回字段**不包含 CYCLE / INVSTRA / INVTGT**，所以它不能替代
        持有期识别（那仍然只能靠 Detail 或 supplement_holding_period）。

        Args:
            fcode: 基金代码

        Returns:
            简介数据
        """
        params = {'FCODE': fcode}
        result = self.client.get(APIEndpoints.FUND_STOP_WATCH, params=params)
        return result

    def get_fund_valuation(self, fcode: str) -> Optional[Dict]:
        """
        获取基金估值详情。

        Args:
            fcode: 基金代码

        Returns:
            基金估值数据
        """
        params = {'FCODE': fcode}
        result = self.client.get(APIEndpoints.FUND_VALUATION, params=params)
        return result

    def get_nav_history(self, fcode: str, page_index=1, page_size=100) -> Optional[Dict]:
        """
        获取基金历史净值

        Args:
            fcode: 基金代码
            page_index: 页码
            page_size: 每页数量

        Returns:
            {
                'Datas': [...],  # 净值列表
                'TotalCount': 1250  # 总记录数
            }
        """
        # P0-04.1 / B12: 天天基金文档对 /fundMNHisNetList 明写 `pagesize`（全小写），
        # `pageIndex` 驼峰。此前项目误用 `pageSize` 驼峰，自建代理宽容未暴露问题；
        # 一旦上游或代理收紧，会静默回退默认分页（~10-20），导致历史净值抓不全。
        params = {
            'FCODE': fcode,
            'pageIndex': page_index,
            'pagesize': page_size,
        }
        result = self.client.get(APIEndpoints.FUND_NAV_HISTORY, params=params)
        return result

    def get_nav_diagram(self, fcode: str, time_range=TimeRange.YEAR_5, point_count=500) -> Optional[Dict]:
        """
        获取净值走势图数据

        Args:
            fcode: 基金代码
            time_range: 时间范围（1n/3n/5n等）
            point_count: 数据点数量

        Returns:
            净值走势数据
        """
        params = {
            'FCODE': fcode,
            'RANGE': time_range,
            'POINTCOUNT': point_count
        }
        result = self.client.get(APIEndpoints.FUND_NAV_DIAGRAM, params=params)
        return result

    def get_manager_info(self, mgr_id: str) -> Optional[Dict]:
        """
        获取基金经理详细信息

        Args:
            mgr_id: 基金经理ID

        Returns:
            经理详情数据
        """
        params = {'FCODE': mgr_id}
        result = self.client.get(APIEndpoints.MANAGER_INFO, params=params)
        return result

    def get_company_info(self, company_id: str, action='fundcompanybaseinfo') -> Optional[Dict]:
        """
        获取基金公司信息

        Args:
            company_id: 公司ID
            action: 操作类型

        Returns:
            公司信息数据
        """
        params = {
            'cc': company_id,
            'action': action
        }
        result = self.client.get(APIEndpoints.COMPANY_API, params=params)
        return result

    def search_funds(
        self,
        keyword: str,
        search_type='1',
        *,
        return_quality: bool = False,
    ) -> Optional[List[Dict]] | Tuple[List[Dict], object]:
        """
        搜索基金

        Args:
            keyword: 关键字
            search_type: 搜索类型（1=搜基金, 3=按字母, 7=搜经理, 8=搜公司）

        Returns:
            搜索结果列表
        """
        params = {
            'm': search_type,
            'key': keyword
        }
        result = self.client.get(APIEndpoints.FUND_SEARCH, params=params)
        datas, quality = inspect_tiantian_search_payload(
            result,
            endpoint_label=f"{APIEndpoints.FUND_SEARCH} key={keyword}",
            data_key='Datas',
            error_code_field='ErrCode',
            error_message_fields=['ErrMsg', 'Message', 'ErrorMessage', 'firstError'],
        )
        if not quality.accepted:
            raise RuntimeError(quality.message)
        if return_quality:
            return datas, quality
        return datas

    def search_funds_by_name(
        self,
        keyword: str,
        order_type: str = '2',
        page_index: int = 1,
        page_size: int = 10,
    ) -> Optional[List[Dict]]:
        """
        按基金名称搜索。

        Args:
            keyword: 基金名称关键词
            order_type: 搜索排序方式（默认 2）
            page_index: 页码
            page_size: 每页数量

        Returns:
            搜索结果列表
        """
        params = {
            'key': keyword,
            'orderType': order_type,
            'pageindex': page_index,
            'pagesize': page_size,
        }
        result = self.client.get(APIEndpoints.FUND_SEARCH_BY_NAME, params=params)
        datas, quality = inspect_tiantian_search_payload(
            result,
            endpoint_label=f"{APIEndpoints.FUND_SEARCH_BY_NAME} key={keyword}",
            data_key='data',
            success_field='success',
            error_code_field='errorCode',
            error_message_fields=['firstError', 'message', 'msg'],
        )
        if not quality.accepted:
            raise RuntimeError(quality.message)
        return datas

    # ========== P0-R2新增API方法 ==========

    def get_period_increase(self, fcode: str, range_value: Optional[str] = None) -> Optional[Dict]:
        """
        获取基金阶段/年度涨幅

        Args:
            fcode: 基金代码
            range_value: RANGE 参数；None 表示阶段涨幅，'n' 表示年度涨幅，
                其他取值按上游接口文档透传。

        Returns:
            {
                'Datas': [
                    {'title': '2021年度', 'syl': '5.23', 'rank': '150', 'sc': '2000'},
                    {'title': '2022年度', 'syl': '3.45', 'rank': '200', 'sc': '2100'},
                    ...
                ]
            }
        """
        params = {'FCODE': fcode}
        if range_value:
            params['RANGE'] = range_value
        result = self.client.get(APIEndpoints.FUND_PERIOD_INCREASE, params=params)
        return result

    def get_rank_diagram(self, fcode: str) -> Optional[Dict]:
        """
        获取基金同类排名走势

        Args:
            fcode: 基金代码

        Returns:
            {
                'Datas': [
                    {'x': '2024-01', 'y': 150, 'sc': 2000},
                    {'x': '2024-02', 'y': 145, 'sc': 2010},
                    ...
                ]
            }
        """
        params = {'FCODE': fcode}
        result = self.client.get(APIEndpoints.FUND_RANK_DIAGRAM, params=params)
        return result

    def get_fund_grade(
        self,
        fcode: str,
        *,
        page_index: int = 1,
        page_size: int = 1,
    ) -> Optional[Dict]:
        """
        获取基金评级 /fundGradeDetail（P1-09.3 / B14）。

        返回 ``Datas`` 列表，每条包含 ``RDATE / ZSPJ / SZPJ3 / JAPJ / HTPJ``（
        招商证券 / 上海证券 / 济安金信 / 海通证券 三-四机构评级，多为 1-5 分）。
        默认 ``page_size=1`` 取最近一期；历史扫描可传更大值。

        Args:
            fcode: 基金 code
            page_index: 页码，1 开始
            page_size: 每页条数

        Returns:
            {"Datas": [{"RDATE":"2026-03-31", "ZSPJ":"5", "SZPJ3":"4",
                        "JAPJ":"", "HTPJ":"-"}, ...]}
        """
        params = {
            'FCODE': fcode,
            'pageIndex': page_index,
            'pageSize': page_size,
        }
        result = self.client.get(APIEndpoints.FUND_GRADE, params=params)
        return result
