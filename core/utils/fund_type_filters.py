#!/usr/bin/env python3
"""
统一的基金类型过滤辅助。
"""

import re
from typing import Optional

from sqlalchemy import and_, not_, or_

from database.models import Fund
from utils.fund_exclusion_rules import build_sqlalchemy_exclusion_filter
from utils.negotiable_cd_policy import infer_high_confidence_negotiable_cd_type_fields


BOND_TYPE_CODES = ("31", "32")
BOND_FTYPE_CODES = ("003", "008")
WEALTH_MANAGEMENT_FTYPE_CODES = ("009", "2949")
INDEX_TYPE_CODES = ("26", "039", "043")
INDEX_FTYPE_CODES = ("007", "025", "026", "039", "043")
BROAD_BUCKET_TYPE_CODES = ("25", "26", "27", "35", "36", "37")
BROAD_BUCKET_TYPE_CATEGORIES = ("equity", "hybrid", "money_market", "alternative", "multi_asset")
BOND_TEXT_PREFIXES = ("债券", "定开债")
WEALTH_MANAGEMENT_TEXT_PREFIXES = ("理财", "短期理财")
QDII_INDEX_TEXT_PREFIXES = ("指数型-海外",)
PRODUCT_FORM_TYPE_CODES = ("3", "4")
NON_BOND_TYPE_MAPPINGS = (
    ("FOF", "37", "multi_asset"),
    ("股票", "25", "equity"),
    ("混合", "27", "hybrid"),
    ("货币", "35", "money_market"),
    ("QDII", "36", "alternative"),
)
QDII_COMMODITY_SIGNALS = ("原油", "黄金", "贵金属", "商品")
QDII_COMMODITY_BENCHMARK_SIGNALS = (
    "WTI原油",
    "布伦特原油",
    "黄金现货",
    "上海黄金交易所",
    "上海期货交易所",
    "大连商品交易所",
    "白银期货",
    "有色金属期货",
    "豆粕期货",
    "Au99.99",
    "AU99.99",
    "Au9999",
    "SHAU",
)
QDII_INDEX_SIGNALS = ("指数", "ETF", "联接")
QDII_SAFE_NAME_INDEX_SIGNALS = (
    "标普",
    "纳斯达克",
    "MSCI",
    "日经",
    "道琼斯",
    "恒生科技",
    "恒生国企",
    "恒生中国央企",
    "恒指",
    "恒生ETF",
    "中证中美互联网",
)
QDII_BOND_SIGNALS = (
    "高收益债",
    "中高收益债",
    "收益债券",
    "信用债",
    "中短期美元债",
    "新兴市场",
)
QDII_RUNTIME_FAMILY_SUFFIX_PATTERNS = (
    r"美元现汇A?$",
    r"美元现钞A?$",
    r"美元现汇C$",
    r"美元现钞C$",
    r"美元A$",
    r"美元C$",
    r"A人民币$",
    r"C人民币$",
    r"D人民币$",
    r"E人民币$",
    r"人民币A$",
    r"人民币C$",
    r"人民币D$",
    r"人民币E$",
    r"人民币$",
    r"\(QDII\)$",
    r"\(QDII\)A$",
    r"\(QDII\)C$",
    r"\(QDII\)D$",
    r"\(QDII\)E$",
    r"\(QDII-FOF\)$",
    r"\(QDII-FOF\)A$",
    r"\(QDII-FOF\)C$",
    r"\(QDII-FOF\)D$",
    r"\(QDII-FOF-LOF\)$",
    r"\(QDII-FOF-LOF\)A$",
    r"\(QDII-FOF-LOF\)C$",
    r"\(QDII-FOF-LOF\)D$",
    r"A美元现汇$",
    r"A美元现钞$",
    r"C美元现汇$",
    r"C美元现钞$",
    r"美元现汇$",
    r"美元现钞$",
    r"美元$",
    r"人民币份额$",
    r"美元现汇份额$",
    r"美元现钞份额$",
    r"\(人民币\)$",
    r"\(美元现汇\)$",
    r"\(美元现钞\)$",
    r"\(人民币份额\)$",
    r"\(美元现汇份额\)$",
    r"\(美元现钞份额\)$",
    r"美元现汇\(QDII\)$",
    r"美元现钞\(QDII\)$",
    r"人民币\(QDII\)$",
    r"A$",
    r"C$",
    r"D$",
    r"E$",
)
QDII_CURATED_FAMILY_GROUPS = {
    "QDII-混合偏股": (
        "万家全球成长一年持有期混合",
        "东方红医疗创新混合",
        "中欧港股数字经济混合发起",
        "中欧港股消费混合发起",
        "交银环球精选混合(QDII)",
        "光大保德信阳光香港精选混合",
        "创金合信港股互联网3个月持有期混合",
        "华夏全球科技先锋混合(QDII)",
        "华夏港股前沿经济混合",
        "华宝海外中国成长混合",
        "华宝致远混合",
        "华泰柏瑞亚洲领导企业混合",
        "南方中国新兴经济9个月持有期混合",
        "南方港股医药行业混合发起",
        "南方港股数字经济混合发起",
        "国富全球科技互联混合(QDII)",
        "大成中国优势混合",
        "大成港股恒信混合(QDII)",
        "大成港股精选混合",
        "天弘全球高端制造混合",
        "富国中国中小盘混合(QDII)",
        "富国全球健康生活主题混合(QDII)",
        "富国全球消费精选混合(QDII)",
        "富国红利精选混合(QDII)",
        "富国医药精选混合",
        "富国港股精选混合(QDII)",
        "广发全球科技三个月定开混合(QDII)",
        "建信新兴市场混合",
        "摩根中国生物医药混合",
        "摩根亚太优势混合",
        "摩根全球天然资源混合",
        "国富大中华精选混合",
        "易方达全球成长精选混合(QDII)",
        "易方达全球优质企业混合(QDII)",
        "易方达全球医药行业混合发起式(QDII)",
        "景顺长城大中华混合",
        "汇添富全球医疗混合(QDII)",
        "汇添富全球汽车产业升级混合(QDII)",
        "汇添富全球消费混合(QDII)",
        "汇添富香港优势精选混合",
        "工银印度基金",
        "摩根全球新兴市场混合(QDII)",
        "易方达优质精选混合(QDII)",
        "海富通中国海外混合",
        "嘉实海外中国股票混合",
        "长盛环球行业混合(QDII)",
        "嘉实全球产业精选混合发起式(QDII)",
        "银华海外数字经济量化选股混合发起式",
        "长城港股医疗保健精选混合发起",
    ),
    "QDII-混合灵活": (
        "华夏大中华混合(QDII)",
        "华夏新时代混合",
        "华夏移动互联混合",
        "工银新经济混合(QDII)",
        "南方香港成长灵活配置混合",
        "招商普盛全球配置(QDII)",
        "招商普盛全球配置(QDII-FOF)",
        "摩根中国世纪混合(QDII)",
        "易方达全球配置混合(QDII)",
        "汇添富全球移动互联混合(QDII)",
        "融通核心价值混合",
    ),
    "QDII-混合债": (
        "华安全球美元收益债",
        "华安全球美元票息债",
        "国富美元债一年持有期债券(QDII)",
        "富国全球债券(QDII)",
        "广发全球稳健配置混合(QDII)",
        "融通中国概念债券",
        "鹏华全球中短债(QDII)",
    ),
    "QDII-纯债": (
        "中银亚太精选债券(QDII)",
        "中银美元债债券(QDII)",
        "大成全球美元债(QDII)",
        "工银全球美元债",
        "汇添富美元债债券(QDII)",
        "长信全球债券",
        "银华美元债精选债券(QDII)",
    ),
    "QDII-混合平衡": (
        "华夏海外聚享混合发起式",
        "摩根全球多元配置(QDII-FOF)",
    ),
    "QDII-FOF": (
        "中银全球策略",
        "天弘标普500发起",
        "摩根海外稳健配置混合(QDII-FOF)",
    ),
    "QDII-商品": (
        "博时抗通胀增强回报",
        "银华抗通胀主题(QDII-FOF-LOF)",
    ),
    "QDII-普通股票": (
        "工银香港中小盘",
        "博时大中华亚太精选",
        "华宝纳斯达克精选股票发起式(QDII)",
        "浦银安盛全球智能科技(QDII)",
    ),
    "指数型-海外股票": (
        "华宝标普油气上游股票",
        "华泰柏瑞恒生消费ETF发起式联接",
        "广发全球医疗保健",
    ),
}
def _normalize_qdii_runtime_family_name(name: Optional[str]) -> str:
    """归一化 QDII 短名称，便于匹配同一家族的不同份额/币种后缀。"""
    normalized = str(name or "").strip()
    changed = True
    while changed and normalized:
        changed = False
        for pattern in QDII_RUNTIME_FAMILY_SUFFIX_PATTERNS:
            updated = re.sub(pattern, "", normalized)
            if updated != normalized:
                normalized = updated
                changed = True
    return re.sub(r"\s+", "", normalized)


QDII_CURATED_FAMILY_MAP = {
    _normalize_qdii_runtime_family_name(family_name): semantic_ftype
    for semantic_ftype, family_names in QDII_CURATED_FAMILY_GROUPS.items()
    for family_name in family_names
}


def is_explicit_non_bond_type(ftype: Optional[str]) -> bool:
    """显式的非债券基金类型应优先覆盖历史脏分类字段。"""
    if not ftype:
        return False

    ftype = str(ftype).strip()
    if not ftype:
        return False

    if ftype == "指数型" or (ftype.startswith("指数型-") and ftype != "指数型-固收"):
        return True

    return any(ftype.startswith(prefix) for prefix, _, _ in NON_BOND_TYPE_MAPPINGS)


def is_index_fund_type(
    ftype: Optional[str],
    type_code: Optional[str] = None,
    type_category: Optional[str] = None,
) -> bool:
    """统一判定“指数基金”口径，供估算接口支持性等场景复用。"""
    del type_category

    normalized_ftype = str(ftype or "").strip()
    normalized_type_code = str(type_code or "").strip()

    if normalized_type_code in INDEX_TYPE_CODES:
        return True

    if normalized_ftype in INDEX_FTYPE_CODES:
        return True

    if not normalized_ftype:
        return False

    return "指数" in normalized_ftype


def supports_tiantian_valuation_detail(
    ftype: Optional[str],
    type_code: Optional[str] = None,
    type_category: Optional[str] = None,
) -> bool:
    """
    判断是否应调用天天基金 `fundVarietieValuationDetail`。

    文档与实测都表明该接口仅对指数类基金返回估值数据：
    - 已明确识别为指数基金：允许请求
    - 已明确识别为非指数基金：直接跳过
    - 类型仍模糊（例如仅有 ETF/LOF 产品形态占位）：保守允许请求，避免误杀真实指数基金
    """
    if is_index_fund_type(
        ftype,
        type_code=type_code,
        type_category=type_category,
    ):
        return True

    normalized_ftype = str(ftype or "").strip()
    normalized_type_code = str(type_code or "").strip()
    normalized_type_category = str(type_category or "").strip()

    if not normalized_ftype and not normalized_type_code and not normalized_type_category:
        return True

    if is_low_confidence_product_form_state(
        normalized_ftype or None,
        normalized_type_code or None,
    ):
        return True

    return False


def is_semantic_ftype(ftype: Optional[str]) -> bool:
    """判断 ftype 是否已经是可直接理解的语义化文本。"""
    normalized = str(ftype or "").strip()
    return bool(normalized) and not normalized.isdigit()


def is_low_confidence_product_form_state(
    ftype: Optional[str],
    type_code: Optional[str],
) -> bool:
    """
    判断当前类型状态是否只是 ETF/LOF 这类“产品形态”占位。

    当列表接口只返回 ETF/LOF 入口类型 + 数字 ftype 时，无法安全推出真实资产 bucket，
    此时不应覆盖已有的语义化结构类型。
    """
    normalized_type_code = str(type_code or "").strip()
    if normalized_type_code not in PRODUCT_FORM_TYPE_CODES:
        return False
    return not is_semantic_ftype(ftype)


def is_low_confidence_generic_numeric_state(
    ftype: Optional[str],
    type_code: Optional[str],
    type_category: Optional[str],
) -> bool:
    """
    判断当前状态是否属于“只有宽泛 bucket、缺少语义文本”的数值占位。

    这类状态常见于 `fundMNRank` 的低信息量返回，例如：
    - `002 + 27/hybrid`
    - `007 + 26/equity`
    - 其它仅给出宽泛 bucket、但缺少可读语义文本的数值类型

    对这类状态，如果本地已经存在由详情链路写入的更具体语义类型，应优先保留本地语义。
    """
    normalized_ftype = str(ftype or "").strip()
    if not normalized_ftype or not normalized_ftype.isdigit():
        return False

    normalized_type_code = str(type_code or "").strip()
    normalized_type_category = str(type_category or "").strip()
    return (
        normalized_type_code in BROAD_BUCKET_TYPE_CODES
        or normalized_type_category in BROAD_BUCKET_TYPE_CATEGORIES
    )


def has_specific_structured_type_state(
    ftype: Optional[str],
    type_code: Optional[str],
    type_category: Optional[str],
) -> bool:
    """
    判断当前状态是否已经携带比 ETF/LOF 产品形态更具体的资产分类信息。
    """
    normalized_type_code = str(type_code or "").strip()
    normalized_type_category = str(type_category or "").strip()

    if is_semantic_ftype(ftype):
        return True
    if normalized_type_code and normalized_type_code not in PRODUCT_FORM_TYPE_CODES:
        return True
    if normalized_type_category and normalized_type_category not in {"equity", "unknown"}:
        return True
    return False


def is_qdii_type(
    type_code: Optional[str],
    type_category: Optional[str],
) -> bool:
    """统一判定结构化字段是否已经明确标记为 QDII。"""
    return str(type_code or "").strip() == "36" or str(type_category or "").strip() == "alternative"


def is_ambiguous_qdii_index_text(ftype: Optional[str]) -> bool:
    """
    判断是否属于“泛化指数文本”，无法单靠文本安全地把现有 QDII 改写成普通指数基金。

    部分真实 QDII 海外指数基金的 detail 会返回 `指数型-股票`，此时应优先保留
    现有结构化 QDII bucket，而不是直接降级成普通指数基金。
    """
    normalized = str(ftype or "").strip()
    if not normalized:
        return False
    if normalized.startswith("QDII"):
        return False
    if normalized.startswith(QDII_INDEX_TEXT_PREFIXES):
        return False
    return normalized == "指数型" or normalized.startswith("指数型-")


def should_preserve_existing_qdii_bucket(
    ftype: Optional[str],
    type_code: Optional[str],
    type_category: Optional[str],
) -> bool:
    """对已确认 QDII 的基金，遇到歧义指数文本时保留原有 QDII bucket。"""
    return is_qdii_type(type_code, type_category) and is_ambiguous_qdii_index_text(ftype)


def preserve_existing_qdii_semantic_ftype(
    incoming_ftype: Optional[str],
    existing_ftype: Optional[str],
    type_code: Optional[str],
    type_category: Optional[str],
) -> Optional[str]:
    """
    对已确认 QDII 的基金，若新详情只返回泛化指数文本，则优先保留已有更具体的海外指数语义。
    """
    normalized_incoming = str(incoming_ftype or "").strip()
    normalized_existing = str(existing_ftype or "").strip()

    if not normalized_incoming:
        return None
    if not should_preserve_existing_qdii_bucket(normalized_incoming, type_code, type_category):
        return normalized_incoming
    if normalized_existing.startswith(QDII_INDEX_TEXT_PREFIXES):
        return normalized_existing
    return normalized_incoming


def is_bond_type(
    ftype: Optional[str],
    type_code: Optional[str],
    type_category: Optional[str],
) -> bool:
    """统一判定债券基金口径，兼容新旧字段。"""
    if is_explicit_non_bond_type(ftype):
        return False

    if type_category == "fixed_income":
        return True

    if type_code in BOND_TYPE_CODES:
        return True

    if not ftype:
        return False

    ftype = str(ftype).strip()
    if ftype in BOND_FTYPE_CODES or ftype in WEALTH_MANAGEMENT_FTYPE_CODES:
        return True

    if ftype == "指数型-固收":
        return True

    return ftype.startswith(BOND_TEXT_PREFIXES + WEALTH_MANAGEMENT_TEXT_PREFIXES)


def build_bond_filter():
    """构建债券基金查询过滤条件。"""
    explicit_non_bond_filter = or_(
        Fund.ftype == "指数型",
        and_(Fund.ftype.like("指数型-%"), Fund.ftype != "指数型-固收"),
        Fund.ftype.like("FOF%"),
        Fund.ftype.like("股票%"),
        Fund.ftype.like("混合%"),
        Fund.ftype.like("货币%"),
        Fund.ftype.like("QDII%"),
    )

    runtime_exclusion_filter = build_sqlalchemy_exclusion_filter(Fund)

    return and_(
        or_(
            Fund.type_category == "fixed_income",
            Fund.type_code.in_(BOND_TYPE_CODES),
            Fund.ftype.in_(BOND_FTYPE_CODES + WEALTH_MANAGEMENT_FTYPE_CODES),
            Fund.ftype == "指数型-固收",
            Fund.ftype.like("债券%"),
            Fund.ftype.like("定开债%"),
            Fund.ftype.like("理财%"),
            Fund.ftype.like("短期理财%"),
        ),
        or_(
            Fund.ftype.is_(None),
            not_(explicit_non_bond_filter),
        ),
        runtime_exclusion_filter,
    )


def normalize_fund_type_fields(
    ftype: Optional[str],
    type_code: Optional[str],
    type_category: Optional[str],
    allow_bond_text_fallback: bool = True,
):
    """按基金类型文本纠正 type_code/type_category。"""
    normalized_code = str(type_code) if type_code is not None else None
    normalized_category = type_category

    if not ftype:
        return normalized_code, normalized_category

    ftype = str(ftype).strip()
    if ftype == "008" or ftype.startswith("定开债"):
        return "32", "fixed_income"

    if ftype in WEALTH_MANAGEMENT_FTYPE_CODES or ftype.startswith(WEALTH_MANAGEMENT_TEXT_PREFIXES):
        if normalized_code not in BOND_TYPE_CODES:
            normalized_code = "31"
        return normalized_code, "fixed_income"

    if ftype == "指数型-固收":
        return "31", "fixed_income"

    if ftype.startswith(QDII_INDEX_TEXT_PREFIXES):
        return "36", "alternative"

    if ftype in {"商品", "商品型", "商品（黄金等）"} or ftype.startswith("商品"):
        return "26", "equity"

    if ftype == "指数型" or ftype.startswith("指数型-"):
        return "26", "equity"

    for prefix, normalized_code_value, normalized_category_value in NON_BOND_TYPE_MAPPINGS:
        if ftype.startswith(prefix):
            return normalized_code_value, normalized_category_value

    if allow_bond_text_fallback and (ftype == "003" or ftype.startswith("债券")):
        if normalized_code not in BOND_TYPE_CODES:
            normalized_code = "31"
        return normalized_code, "fixed_income"

    return normalized_code, normalized_category


def normalize_fixed_income_type_fields(
    ftype: Optional[str],
    type_code: Optional[str],
    type_category: Optional[str],
):
    """兼容旧接口，按基金类型文本纠正 type_code/type_category。"""
    return normalize_fund_type_fields(
        ftype,
        type_code,
        type_category,
        allow_bond_text_fallback=True,
    )


def infer_high_confidence_qdii_semantic_ftype(
    shortname: Optional[str],
    fullname: Optional[str] = None,
    benchmark: Optional[str] = None,
) -> Optional[str]:
    """
    基于本地名称/benchmark 为 QDII 运行时补充高置信语义。

    约束：
    - 只覆盖已验证为 100% 精度的高置信子集；
    - 无法稳定判定的样本继续保留 `QDII-未分类`。
    """
    name_text = " ".join(
        part.strip()
        for part in (str(shortname or ""), str(fullname or ""))
        if str(part or "").strip()
    )
    benchmark_text = str(benchmark or "").strip()
    if not name_text and not benchmark_text:
        return None

    if (
        any(token in name_text for token in QDII_COMMODITY_SIGNALS)
        or any(token in benchmark_text for token in QDII_COMMODITY_BENCHMARK_SIGNALS)
    ):
        return "QDII-商品"

    if "不动产" in name_text or (
        "房地产" in name_text and not any(token in name_text for token in (*QDII_INDEX_SIGNALS, "REIT"))
    ):
        return "QDII-REITs"

    curated_family_ftype = QDII_CURATED_FAMILY_MAP.get(
        _normalize_qdii_runtime_family_name(shortname)
    )
    if curated_family_ftype:
        return curated_family_ftype

    if any(token in name_text for token in QDII_BOND_SIGNALS) and "混合" not in name_text:
        return "QDII-纯债"

    if (
        "FOF" not in name_text
        and "混合" not in name_text
        and "股票" not in name_text
        and "债" not in name_text
        and (
            "指数" in name_text
            or (
                "QDII" in name_text
                and any(token in name_text for token in ("ETF", "联接"))
            )
            or any(token in name_text for token in QDII_SAFE_NAME_INDEX_SIGNALS)
        )
    ):
        return "指数型-海外股票"

    if (
        "股票" in name_text
        and "混合" not in name_text
        and not any(
            token in name_text
            for token in (
                *QDII_COMMODITY_SIGNALS,
                "油气",
                *QDII_INDEX_SIGNALS,
                "标普",
                "纳斯达克",
                "恒生",
                "道琼斯",
                "富时",
                "MSCI",
                "日经",
                "H股",
            )
        )
    ):
        return "QDII-普通股票"

    return None
