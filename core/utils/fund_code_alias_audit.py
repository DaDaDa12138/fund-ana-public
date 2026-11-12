#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
97xxxx 基金代码 alias 审计与 dry-run 预览公共方法。
"""

from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from database.connection import resolve_db_path
from utils.project_runtime_config import get_project_config_path


ALIAS_CANDIDATE_SQL = """
SELECT
    fcode,
    shortname,
    fullname,
    ftype,
    type_code,
    type_category,
    data_source,
    first_collected_at,
    last_updated_at
FROM funds
WHERE fcode LIKE '97%'
ORDER BY type_category, fcode
"""

DEFAULT_AUDIT_DIR = "audit"
AUDIT_JSON_PATTERN = "fund_code_alias_audit_*.json"
PREVIEW_JSON_PATTERN = "fund_code_alias_cleanup_preview_*.json"
DEFAULT_ALIAS_REVIEW_OVERRIDE_PATH = PROJECT_ROOT / "config" / "fund_code_alias_review_overrides.json"
DEFAULT_PROJECT_CONFIG_PATH = get_project_config_path()
DEFAULT_CONTAINER_PROJECT_CONFIG_PATH = "/app/config/project.yaml"


def open_readonly_db(db_path: Optional[str] = None) -> sqlite3.Connection:
    """以只读方式打开 SQLite 数据库。"""
    resolved = resolve_db_path(db_path or "database/bond_funds.db")
    conn = sqlite3.connect(f"file:{resolved}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_alias_candidates(db_path: Optional[str] = None) -> List[dict]:
    """读取数据库中的全部 97xxxx 基金候选。"""
    with open_readonly_db(db_path) as conn:
        rows = conn.execute(ALIAS_CANDIDATE_SQL).fetchall()
    return [dict(row) for row in rows]


def _query_rows_by_shortnames(
    conn: sqlite3.Connection,
    shortnames: Sequence[str],
) -> Dict[str, List[dict]]:
    """按基金简称读取本地非 97 主代码同名记录。"""
    normalized = sorted({str(name).strip() for name in shortnames if str(name).strip()})
    if not normalized:
        return {}

    placeholders = ",".join("?" for _ in normalized)
    sql = f"""
    SELECT
        fcode,
        shortname,
        fullname,
        ftype,
        type_code,
        type_category,
        data_source,
        last_updated_at
    FROM funds
    WHERE shortname IN ({placeholders})
      AND fcode NOT LIKE '97%'
    ORDER BY shortname, fcode
    """
    mapping: Dict[str, List[dict]] = defaultdict(list)
    for row in conn.execute(sql, normalized).fetchall():
        item = dict(row)
        mapping[item["shortname"]].append(item)
    return dict(mapping)


def _discover_fcode_tables(conn: sqlite3.Connection) -> List[str]:
    """发现包含 fcode 字段的表，用于评估后续清理影响范围。"""
    tables = []
    table_rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()

    for row in table_rows:
        table_name = row["name"]
        columns = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        if any(str(column["name"]).strip() == "fcode" for column in columns):
            tables.append(table_name)
    return tables


def _build_reference_count_map(
    conn: sqlite3.Connection,
    codes: Sequence[str],
) -> Dict[str, Dict[str, int]]:
    """统计各 alias code 在每张 fcode 表中的引用数量。"""
    normalized_codes = sorted({str(code).strip() for code in codes if str(code).strip()})
    if not normalized_codes:
        return {}

    placeholders = ",".join("?" for _ in normalized_codes)
    tables = _discover_fcode_tables(conn)
    reference_map: Dict[str, Dict[str, int]] = {code: {} for code in normalized_codes}

    for table_name in tables:
        sql = (
            f"SELECT fcode, COUNT(*) AS row_count FROM {table_name} "
            f"WHERE fcode IN ({placeholders}) GROUP BY fcode"
        )
        for row in conn.execute(sql, normalized_codes).fetchall():
            code = row["fcode"]
            reference_map.setdefault(code, {})[table_name] = int(row["row_count"] or 0)

    return reference_map


def _dedupe_codes(codes: Optional[Iterable[str]]) -> List[str]:
    """对代码列表做稳定去重。"""
    seen = set()
    result = []
    for code in codes or []:
        normalized = str(code or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def _collect_type_mismatches(record: dict, canonical_row: Optional[dict]) -> List[str]:
    """收集来源记录与候选 canonical 之间的类型口径冲突。"""
    canonical_row = canonical_row or {}
    mismatches = []

    source_type_code = str(record.get("type_code") or "").strip()
    canonical_type_code = str(canonical_row.get("type_code") or "").strip()
    if source_type_code and canonical_type_code and source_type_code != canonical_type_code:
        mismatches.append(f"type_code:{source_type_code}!={canonical_type_code}")

    source_type_category = str(record.get("type_category") or "").strip()
    canonical_type_category = str(canonical_row.get("type_category") or "").strip()
    if (
        source_type_category
        and canonical_type_category
        and source_type_category != canonical_type_category
    ):
        mismatches.append(f"type_category:{source_type_category}!={canonical_type_category}")

    return mismatches


def load_alias_review_override_map(
    override_path: Optional[Path] = None,
) -> Dict[str, dict]:
    """读取人工复核确认后的 alias 阻断规则。"""
    path = Path(override_path or DEFAULT_ALIAS_REVIEW_OVERRIDE_PATH)
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    if not path.exists():
        return {}

    payload = json.loads(path.read_text(encoding="utf-8"))
    mapping = {}
    for item in payload.get("items") or []:
        source_fcode = str(item.get("source_fcode") or "").strip()
        if not source_fcode:
            continue
        mapping[source_fcode] = dict(item)
    return mapping


def apply_alias_review_override(classification: dict, review_override: Optional[dict]) -> dict:
    """将人工复核确认结果覆盖到自动审计分类上。"""
    if not review_override:
        return dict(classification or {})

    applied = dict(classification or {})
    canonical_fcode = str(review_override.get("canonical_fcode") or applied.get("canonical_fcode") or "").strip()
    reason = str(review_override.get("reason") or "").strip()
    note = str(review_override.get("note") or "").strip()
    suffix = f"（{note}）" if note else ""
    applied.update({
        "classification": str(review_override.get("classification") or "blocked_false_alias").strip(),
        "confidence": "high",
        "canonical_fcode": canonical_fcode or None,
        "canonical_source": "manual_review_override",
        "reason": (reason + suffix) if reason else applied.get("reason"),
    })
    return applied


def classify_alias_row(
    record: dict,
    local_matches: Optional[List[dict]] = None,
    evidence: Optional[dict] = None,
) -> dict:
    """基于本地重复记录与 API 证据对 97xxxx 记录做只读分类。"""
    local_matches = list(local_matches or [])
    evidence = evidence or {}

    current_code = str(record.get("fcode") or "").strip()
    code_search = evidence.get("code_search") or {}
    name_search = evidence.get("name_search") or {}

    local_non97_codes = _dedupe_codes(match.get("fcode") for match in local_matches)
    exact_name_codes = _dedupe_codes(name_search.get("exact_name_codes"))
    exact_name_non97_codes = [code for code in exact_name_codes if not code.startswith("97")]
    unique_name_code = str(name_search.get("unique_canonical_fcode") or "").strip()
    unique_name_code = unique_name_code if unique_name_code and unique_name_code != current_code else ""

    if code_search.get("exact_match_found"):
        return {
            "classification": "keep_primary_97",
            "confidence": "high",
            "canonical_fcode": current_code,
            "canonical_source": "code_search_exact",
            "reason": "fundSearch 对当前 97 代码存在精确命中，暂不视为 alias",
        }

    if unique_name_code and not unique_name_code.startswith("97"):
        matched_local_row = next(
            (match for match in local_matches if str(match.get("fcode") or "").strip() == unique_name_code),
            None,
        )
        type_mismatches = _collect_type_mismatches(record, matched_local_row)
        type_category_mismatches = [item for item in type_mismatches if item.startswith("type_category:")]
        if type_category_mismatches:
            return {
                "classification": "type_mismatch_name_match",
                "confidence": "low",
                "canonical_fcode": unique_name_code,
                "canonical_source": "name_search_exact",
                "reason": (
                    "名称搜索返回唯一非 97 主代码，但本地类型口径不一致："
                    + ", ".join(type_category_mismatches)
                ),
            }
        return {
            "classification": (
                "alias_to_existing_canonical"
                if unique_name_code in local_non97_codes
                else "alias_to_remote_canonical"
            ),
            "confidence": "high" if unique_name_code in local_non97_codes else "medium",
            "canonical_fcode": unique_name_code,
            "canonical_source": "name_search_exact",
            "reason": "代码搜索未精确命中，但名称搜索返回唯一非 97 主代码",
        }

    if len(set(exact_name_non97_codes)) > 1:
        return {
            "classification": "ambiguous_name_match",
            "confidence": "low",
            "canonical_fcode": None,
            "canonical_source": "name_search_exact",
            "reason": "名称搜索命中了多个非 97 主代码，无法安全确定唯一主键",
        }

    if len(local_non97_codes) == 1:
        return {
            "classification": "local_duplicate_without_api_confirmation",
            "confidence": "medium",
            "canonical_fcode": local_non97_codes[0],
            "canonical_source": "local_same_name",
            "reason": "本地数据库已存在同名非 97 主代码，但当前 API 证据仍不足以直接执行清理",
        }

    if len(local_non97_codes) > 1:
        return {
            "classification": "ambiguous_local_duplicate",
            "confidence": "low",
            "canonical_fcode": None,
            "canonical_source": "local_same_name",
            "reason": "本地数据库存在多个同名非 97 主代码，存在歧义",
        }

    return {
        "classification": "no_evidence",
        "confidence": "low",
        "canonical_fcode": None,
        "canonical_source": None,
        "reason": "当前没有足够证据判定该 97 代码应归并到其他主代码",
    }


def enrich_alias_rows(
    alias_rows: Sequence[dict],
    db_path: Optional[str] = None,
    evidence_map: Optional[Dict[str, dict]] = None,
    review_override_map: Optional[Dict[str, dict]] = None,
) -> List[dict]:
    """将本地重复信息、引用计数和 API 证据合并到 alias 审计结果中。"""
    evidence_map = evidence_map or {}
    review_override_map = review_override_map or load_alias_review_override_map()
    codes = [str(row.get("fcode") or "").strip() for row in alias_rows]
    shortnames = [str(row.get("shortname") or "").strip() for row in alias_rows]

    with open_readonly_db(db_path) as conn:
        local_match_map = _query_rows_by_shortnames(conn, shortnames)
        reference_count_map = _build_reference_count_map(conn, codes)

    enriched = []
    for row in alias_rows:
        fcode = str(row.get("fcode") or "").strip()
        shortname = str(row.get("shortname") or "").strip()
        local_matches = local_match_map.get(shortname, [])
        evidence = evidence_map.get(fcode) or {}
        classification = classify_alias_row(row, local_matches=local_matches, evidence=evidence)
        review_override = review_override_map.get(fcode)
        classification = apply_alias_review_override(classification, review_override)
        reference_counts = reference_count_map.get(fcode, {})
        canonical_fcode = classification.get("canonical_fcode")

        enriched.append({
            **row,
            "local_same_name_non97_rows": local_matches,
            "local_same_name_non97_codes": _dedupe_codes(match.get("fcode") for match in local_matches),
            "local_same_name_non97_count": len(local_matches),
            "canonical_fcode": canonical_fcode,
            "canonical_exists_locally": bool(
                canonical_fcode and canonical_fcode in _dedupe_codes(match.get("fcode") for match in local_matches)
            ),
            "classification": classification["classification"],
            "confidence": classification["confidence"],
            "canonical_source": classification.get("canonical_source"),
            "reason": classification["reason"],
            "evidence": evidence,
            "review_override": review_override,
            "reference_counts": reference_counts,
            "reference_total": sum(reference_counts.values()),
            "affected_tables": sorted(reference_counts.keys()),
        })

    return enriched


def summarize_alias_audit(rows: Sequence[dict]) -> dict:
    """汇总 alias 审计结果。"""
    rows = list(rows or [])
    by_type_category = Counter(str(row.get("type_category") or "unknown") for row in rows)
    by_classification = Counter(str(row.get("classification") or "unknown") for row in rows)
    preview_rows = build_alias_cleanup_preview(rows)
    by_proposed_action = Counter(str(row.get("proposed_action") or "unknown") for row in preview_rows)

    return {
        "total_97_codes": len(rows),
        "with_local_same_name_non97": sum(1 for row in rows if row.get("local_same_name_non97_count")),
        "with_canonical_exists_locally": sum(1 for row in rows if row.get("canonical_exists_locally")),
        "with_reference_rows": sum(1 for row in rows if row.get("reference_total", 0) > 0),
        "reference_row_total": sum(int(row.get("reference_total") or 0) for row in rows),
        "by_type_category": dict(sorted(by_type_category.items())),
        "by_classification": dict(sorted(by_classification.items())),
        "by_proposed_action": dict(sorted(by_proposed_action.items())),
    }


def build_alias_cleanup_preview(rows: Sequence[dict]) -> List[dict]:
    """基于审计结果生成 dry-run 清理预览。"""
    preview_rows = []
    for row in rows or []:
        classification = str(row.get("classification") or "").strip()
        canonical_fcode = str(row.get("canonical_fcode") or "").strip() or None
        canonical_exists_locally = bool(row.get("canonical_exists_locally"))

        proposed_action = "manual_review"
        preview_confidence = row.get("confidence")
        preview_reason = row.get("reason")

        if classification == "keep_primary_97":
            proposed_action = "keep"
        elif classification == "alias_to_existing_canonical":
            proposed_action = "merge_into_existing_canonical"
        elif classification == "alias_to_remote_canonical":
            proposed_action = "rekey_alias_to_canonical"
        elif classification == "blocked_false_alias":
            proposed_action = "blocked_false_alias"
        elif classification == "local_duplicate_without_api_confirmation":
            proposed_action = "manual_review"
        elif classification in {
            "ambiguous_name_match",
            "ambiguous_local_duplicate",
            "no_evidence",
            "type_mismatch_name_match",
        }:
            proposed_action = "manual_review"

        preview_rows.append({
            "fcode": row.get("fcode"),
            "shortname": row.get("shortname"),
            "ftype": row.get("ftype"),
            "type_code": row.get("type_code"),
            "type_category": row.get("type_category"),
            "classification": classification,
            "confidence": preview_confidence,
            "canonical_fcode": canonical_fcode,
            "canonical_exists_locally": canonical_exists_locally,
            "proposed_action": proposed_action,
            "reference_total": row.get("reference_total", 0),
            "affected_tables": row.get("affected_tables", []),
            "reason": preview_reason,
        })

    preview_rows.sort(
        key=lambda item: (
            {
                "merge_into_existing_canonical": 0,
                "rekey_alias_to_canonical": 1,
                "blocked_false_alias": 2,
                "manual_review": 3,
                "keep": 4,
            }.get(
                item.get("proposed_action"), 9
            ),
            str(item.get("type_category") or ""),
            str(item.get("fcode") or ""),
        )
    )
    return preview_rows


def fetch_alias_evidence_via_docker(
    rows: Sequence[dict],
    container_name: str = "bond-fund-tracker",
) -> Dict[str, dict]:
    """在 Docker 容器内批量拉取 fundSearch + 名称搜索证据。"""
    payload_rows = [
        {
            "fcode": str(row.get("fcode") or "").strip(),
            "shortname": str(row.get("shortname") or "").strip(),
        }
        for row in rows or []
        if str(row.get("fcode") or "").strip() and str(row.get("shortname") or "").strip()
    ]
    if not payload_rows:
        return {}

    pycode = build_alias_evidence_pycode()

    result = subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            container_name,
            "python3",
            "-c",
            pycode,
            json.dumps(payload_rows, ensure_ascii=False),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        raise RuntimeError(
            f"docker exec 拉取 alias 审计证据失败: container={container_name}, error={stderr or 'unknown'}"
        )
    stdout = (result.stdout or "").strip()
    return json.loads(stdout.splitlines()[-1] if stdout else "{}")


def build_alias_evidence_pycode(
    config_path: str = DEFAULT_CONTAINER_PROJECT_CONFIG_PATH,
) -> str:
    """构造容器内 alias 审计证据抓取脚本。"""
    return """
import json
import sys
from pathlib import Path
import yaml

sys.path.insert(0, '/app')

from api.client import APIClient
from api.endpoints import FundAPIEndpoints

def _ensure_config_dict(config_data):
    return config_data if isinstance(config_data, dict) else {}

def _extract_code(item):
    if not isinstance(item, dict):
        return ''
    base_info = item.get('FundBaseInfo') or {}
    candidates = [
        base_info.get('FCODE'),
        item.get('fcode'),
        item.get('FCODE'),
        item.get('code'),
        item.get('CODE'),
        item.get('_id'),
    ]
    for candidate in candidates:
        value = str(candidate or '').strip()
        if value:
            return value
    return ''


def _extract_shortname(item):
    if not isinstance(item, dict):
        return ''
    base_info = item.get('FundBaseInfo') or {}
    candidates = [
        base_info.get('SHORTNAME'),
        item.get('shortname'),
        item.get('SHORTNAME'),
        item.get('name'),
        item.get('NAME'),
    ]
    for candidate in candidates:
        value = str(candidate or '').strip()
        if value:
            return value
    return ''


records = json.loads(sys.argv[1])
config_path = Path("__CONFIG_PATH__")
cfg = {}
if config_path.exists():
    with config_path.open('r', encoding='utf-8') as f:
        cfg = _ensure_config_dict(yaml.safe_load(f))

client = APIClient(cfg)
api = FundAPIEndpoints(client)
payload = {}

for record in records:
    fcode = str(record.get('fcode') or '').strip()
    shortname = str(record.get('shortname') or '').strip()
    item_payload = {
        'code_search': {},
        'name_search': {},
    }

    try:
        code_results = api.search_funds(fcode, search_type='1') or []
        exact_match = None
        for item in code_results:
            if _extract_code(item) == fcode:
                exact_match = item
                break

        base_info = {}
        if isinstance(exact_match, dict):
            base_info = exact_match.get('FundBaseInfo') or {}

        item_payload['code_search'] = {
            'result_count': len(code_results),
            'exact_match_found': bool(exact_match),
            'category_desc': (exact_match or {}).get('CATEGORYDESC') if isinstance(exact_match, dict) else None,
            'has_fund_base_info': bool(base_info),
            'search_fcode': _extract_code(exact_match),
            'search_shortname': _extract_shortname(exact_match),
            'search_ftype': base_info.get('FTYPE') if isinstance(base_info, dict) else None,
        }
    except Exception as exc:
        item_payload['code_search'] = {
            'error': str(exc),
            'result_count': 0,
            'exact_match_found': False,
            'has_fund_base_info': False,
        }

    try:
        name_results = api.search_funds_by_name(shortname, page_index=1, page_size=20) or []
        exact_name_matches = []
        for item in name_results:
            candidate_name = _extract_shortname(item)
            candidate_code = _extract_code(item)
            if candidate_name == shortname and candidate_code:
                exact_name_matches.append({
                    'fcode': candidate_code,
                    'shortname': candidate_name,
                })

        deduped = []
        seen_codes = set()
        for item in exact_name_matches:
            code = item['fcode']
            if code in seen_codes:
                continue
            seen_codes.add(code)
            deduped.append(item)

        exact_codes = [item['fcode'] for item in deduped]
        non97_codes = [code for code in exact_codes if not code.startswith('97')]
        unique_non97 = non97_codes[0] if len(non97_codes) == 1 else None
        search_source = 'fundSearchInfoByName'
        fallback_general_result_count = 0

        if not deduped:
            general_name_results = api.search_funds(shortname, search_type='1') or []
            fallback_general_result_count = len(general_name_results)
            general_exact_matches = []
            for item in general_name_results:
                candidate_name = _extract_shortname(item)
                candidate_code = _extract_code(item)
                if candidate_name == shortname and candidate_code:
                    general_exact_matches.append({
                        'fcode': candidate_code,
                        'shortname': candidate_name,
                    })

            deduped = []
            seen_codes = set()
            for item in general_exact_matches:
                code = item['fcode']
                if code in seen_codes:
                    continue
                seen_codes.add(code)
                deduped.append(item)

            exact_codes = [item['fcode'] for item in deduped]
            non97_codes = [code for code in exact_codes if not code.startswith('97')]
            unique_non97 = non97_codes[0] if len(non97_codes) == 1 else None
            if deduped:
                search_source = 'fundSearch'

        item_payload['name_search'] = {
            'result_count': len(name_results),
            'exact_name_match_count': len(deduped),
            'exact_name_codes': exact_codes,
            'exact_name_matches': deduped,
            'unique_canonical_fcode': unique_non97,
            'search_source': search_source,
            'fallback_general_result_count': fallback_general_result_count,
        }
    except Exception as exc:
        item_payload['name_search'] = {
            'error': str(exc),
            'result_count': 0,
            'exact_name_match_count': 0,
            'exact_name_codes': [],
            'exact_name_matches': [],
            'unique_canonical_fcode': None,
        }

    payload[fcode] = item_payload

print(json.dumps(payload, ensure_ascii=False))
""".replace("__CONFIG_PATH__", repr(config_path))


def _parse_artifact_generated_at(value: object) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    candidates = [text]
    if text.endswith("Z"):
        candidates.append(text[:-1] + "+00:00")
    for candidate in candidates:
        try:
            return datetime.fromisoformat(candidate).timestamp()
        except (TypeError, ValueError, OSError):
            continue
    return None


def _artifact_generated_timestamp(path: Path) -> float | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    return _parse_artifact_generated_at(payload.get("generated_at"))


def _artifact_latest_sort_key(path: Path) -> tuple[float, float, str]:
    generated_timestamp = _artifact_generated_timestamp(path)
    try:
        modified_timestamp = path.stat().st_mtime
    except OSError:
        modified_timestamp = 0.0
    return (
        generated_timestamp if generated_timestamp is not None else modified_timestamp,
        modified_timestamp,
        path.name,
    )


def _resolve_artifact_dir(output_dir: str | Path) -> Path:
    artifact_dir = Path(output_dir)
    if artifact_dir.is_absolute():
        return artifact_dir
    if artifact_dir.parts and artifact_dir.parts[0] == "reports":
        return (PROJECT_ROOT.parent / artifact_dir).resolve()
    return (PROJECT_ROOT / artifact_dir).resolve()


def resolve_latest_artifact_path(pattern: str, output_dir: str = DEFAULT_AUDIT_DIR) -> Path:
    """解析最新的审计/预览产物路径。"""
    artifact_dir = _resolve_artifact_dir(output_dir)
    candidates = sorted(artifact_dir.glob(pattern), key=_artifact_latest_sort_key)
    if not candidates:
        raise FileNotFoundError(f"未找到匹配产物: {artifact_dir / pattern}")
    return candidates[-1]


def load_latest_json_artifact(pattern: str, output_dir: str = DEFAULT_AUDIT_DIR) -> Tuple[Path, dict]:
    """读取最新 JSON 产物。"""
    artifact_path = resolve_latest_artifact_path(pattern=pattern, output_dir=output_dir)
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    return artifact_path, payload


def load_latest_alias_audit_payload(output_dir: str = DEFAULT_AUDIT_DIR) -> Tuple[Path, dict]:
    """读取最新 alias 审计结果。"""
    return load_latest_json_artifact(pattern=AUDIT_JSON_PATTERN, output_dir=output_dir)


def load_latest_alias_cleanup_preview_payload(output_dir: str = DEFAULT_AUDIT_DIR) -> Tuple[Path, dict]:
    """读取最新 alias cleanup dry-run 预览结果。"""
    return load_latest_json_artifact(pattern=PREVIEW_JSON_PATTERN, output_dir=output_dir)
