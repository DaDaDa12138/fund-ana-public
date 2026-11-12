#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一版本号管理

所有模块应从此文件导入版本号，确保版本号的单一来源。

使用方式:
    from version import __version__, VERSION_INFO, CHANGELOG
"""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import re
from typing import Dict, List, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python 3.8 环境兼容兜底
    ZoneInfo = None

# 基础回退版本号（当 docs/CHANGELOG_v*.md 不可用时使用）
_FALLBACK_VERSION = "1.9.794"
_SHANGHAI_TZ = ZoneInfo('Asia/Shanghai') if ZoneInfo else timezone(timedelta(hours=8))

# 版本主数据优先来自 docs/CHANGELOG_v*.md
# 这里只保留最小回退摘要，避免再次维护第二份历史 changelog。
_FALLBACK_CHANGELOG = [
    {
        'version': _FALLBACK_VERSION,
        'date': '2026-11-08',
        'released_at': '2026-11-08',
        'title': '收益异常隔离与历史回放类型保真',
        'changes': [
            '严格识别回到成立日的收益占位响应，保留历史事实与原始拒绝证据，逐基金屏蔽异常输入。',
            '采集持续记录降级，普通失败与日期冲突仍阻断；限定代码的人工重试不改变金融门禁。',
            '账本嵌入证明使用原类型编码，独立验证日期与缺失状态摘要，不补写旧冻结证据。',
        ],
    }
]

_DOCS_DIR = Path(__file__).resolve().parent / 'docs'
_DOC_CHANGELOG_PATTERN = re.compile(r'^CHANGELOG_v(?P<version>\d+\.\d+\.\d+)\.md$')
_DOC_HEADING_PATTERN = re.compile(
    r'^#\s*(?:CHANGELOG\s+)?v?(?P<version>\d+\.\d+\.\d+)(?:\s*[—-]\s*(?P<title>.+))?$'
)
_DOC_RELEASE_DATE_PATTERN = re.compile(r'^\*\*发布日期\*\*:\s*(?P<date>\d{4}-\d{2}-\d{2})')
_DOC_RELEASED_AT_PATTERN = re.compile(
    r'^\*\*(?:发布时间|推出时间|上线时间)\*\*:\s*(?P<ts>\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2}(?::\d{2})?)?)'
)
_DOC_RELEASE_STATUS_PATTERN = re.compile(r'^\*\*发布状态\*\*:\s*(?P<status>.+)$')
_DOC_BULLET_PATTERN = re.compile(r'^(?P<indent>\s*)(?:-\s+|\d+\.\s+)(?P<text>.+)$')
_DOC_DRAFT_COMMENT_PATTERN = re.compile(r'<!--\s*draft\s*:\s*(?:true|1|yes)\s*-->', re.IGNORECASE)
_DOC_PLACEHOLDER_MARKERS = (
    '待补充版本主题',
    '待补充本轮更新摘要',
    '待补充验证结果',
)
_PREFERRED_CHANGE_SECTIONS = (
    '本轮修改',
    '本轮确认并修复的问题',
    '核心修复',
    '问题修复',
    '修复内容',
    '功能更新',
    '前端更新',
    '后端更新',
    '策略修复',
    '策略更新',
    '架构修复',
    '本轮修复',
    '本轮调整',
    '接口调整',
    '可观测性增强',
)
_SKIP_CHANGE_SECTIONS = (
    '本轮目标',
    '验证情况',
    '回归验证',
    '运行验证',
    '配置与运维同步',
    '当前结论',
    '架构收益',
    '审计结论',
    '影响范围',
    '对剩余',
    '当前仍需继续关注',
    '下一步修改计划',
)


def _version_key(version: str) -> Tuple[int, int, int]:
    """将 x.y.z 版本号转为可排序元组。"""
    parts = str(version).split('.')
    padded = (parts + ['0', '0', '0'])[:3]
    return tuple(int(p) for p in padded)


def _clean_markdown_text(text: str) -> str:
    """移除常见 Markdown 标记，保留网页可读文本。"""
    cleaned = re.sub(r'`([^`]+)`', r'\1', text)
    cleaned = re.sub(r'\*\*([^*]+)\*\*', r'\1', cleaned)
    cleaned = re.sub(r'\[(.*?)\]\(.*?\)', r'\1', cleaned)
    return cleaned.strip()


def _is_top_level_bullet(raw_line: str) -> bool:
    """仅保留一级 bullet，避免前端展示实现细节级子项。"""
    bullet_match = _DOC_BULLET_PATTERN.match(raw_line.rstrip())
    if bullet_match is None:
        return False
    return not bullet_match.group('indent')


def _collect_nested_bullet_summaries(lines: List[str], start_index: int) -> Tuple[List[str], int]:
    """收敛一级 bullet 下的嵌套子项，避免网页日志出现残缺说明。"""
    nested_items: List[str] = []
    index = start_index + 1

    while index < len(lines):
        raw_line = lines[index].rstrip()
        stripped = raw_line.strip()

        if not stripped:
            break
        if stripped.startswith('#'):
            break

        bullet_match = _DOC_BULLET_PATTERN.match(raw_line)
        if bullet_match is None:
            break
        if not bullet_match.group('indent'):
            break

        text = _clean_markdown_text(bullet_match.group('text'))
        if text and not _is_path_only_change(text):
            nested_items.append(text.rstrip('；;。'))
        index += 1

    return nested_items, index


def _merge_change_with_nested_items(text: str, nested_items: List[str]) -> str:
    """把嵌套子项压平成一条摘要，保留关键信息并减少前端杂乱度。"""
    base_text = str(text or '').strip()
    if not nested_items:
        return base_text

    nested_summary = '；'.join(item for item in nested_items if item)
    if not nested_summary:
        return base_text

    if base_text.endswith(('：', ':', '，', ',')):
        return f'{base_text}{nested_summary}'
    if base_text.endswith(('。', '；', ';')):
        return f'{base_text} {nested_summary}'
    return f'{base_text}：{nested_summary}'


def _is_path_only_change(text: str) -> bool:
    """过滤纯文件路径条目，避免网页更新项过于杂乱。"""
    normalized = str(text or '').strip()
    if not normalized or ' ' in normalized:
        return False
    if '/' in normalized:
        return True
    return bool(re.match(r'^[A-Za-z0-9_.-]+\.(py|md|json|html|js|ts|tsx|jsx|css|ya?ml)$', normalized))


def _format_doc_mtime(doc_path: Path) -> str:
    """用文档修改时间兜底生成发布时间。"""
    try:
        return datetime.fromtimestamp(doc_path.stat().st_mtime, tz=_SHANGHAI_TZ).strftime('%Y-%m-%d %H:%M')
    except OSError:
        return ''


def _is_draft_release_status(status: str) -> bool:
    """识别 changelog 发布状态中的草稿语义。"""
    normalized = str(status or '').strip().lower()
    if not normalized:
        return False
    draft_keywords = ('草稿', 'draft', '占位', '未发布', 'pending')
    return any(keyword in normalized for keyword in draft_keywords)


def _has_placeholder_release_content(title: str, changes: List[str], raw_text: str) -> bool:
    """识别尚未补完的 changelog 占位模板。"""
    content = str(raw_text or '')
    if any(marker in content for marker in _DOC_PLACEHOLDER_MARKERS):
        return True

    normalized_title = str(title or '').strip()
    if normalized_title.startswith('待补充') or normalized_title.upper().startswith(('TODO', 'TBD')):
        return True

    for item in changes:
        normalized_item = str(item or '').strip()
        if not normalized_item:
            continue
        if normalized_item.startswith('待补充') or normalized_item.upper().startswith(('TODO', 'TBD')):
            return True

    return False


def _is_preferred_change_section(section_name: str) -> bool:
    """判断是否属于网页更新日志优先展示的变更段落。"""
    section = str(section_name or '').strip()
    return any(keyword in section for keyword in _PREFERRED_CHANGE_SECTIONS)


def _parse_doc_changelog(doc_path: Path) -> Optional[Dict]:
    """从 docs/CHANGELOG_v*.md 解析单个版本条目。"""
    match = _DOC_CHANGELOG_PATTERN.match(doc_path.name)
    if match is None:
        return None

    try:
        lines = doc_path.read_text(encoding='utf-8').splitlines()
    except OSError:
        return None

    version = match.group('version')
    title = ''
    release_date = ''
    released_at = ''
    release_status = ''
    current_section = ''
    changes: List[str] = []
    seen_changes = set()
    raw_text = '\n'.join(lines)
    has_draft_comment = bool(_DOC_DRAFT_COMMENT_PATTERN.search(raw_text))
    sections = [
        raw_line.strip()[3:].strip()
        for raw_line in lines
        if raw_line.strip().startswith('## ')
    ]
    has_preferred_sections = any(_is_preferred_change_section(section) for section in sections)

    line_index = 0
    while line_index < len(lines):
        raw_line = lines[line_index]
        line = raw_line.strip()
        if not line:
            line_index += 1
            continue

        if line.startswith('# '):
            heading_match = _DOC_HEADING_PATTERN.match(line)
            if heading_match:
                version = heading_match.group('version') or version
                title = (heading_match.group('title') or '').strip()
            else:
                title = line.lstrip('#').strip()
            line_index += 1
            continue

        date_match = _DOC_RELEASE_DATE_PATTERN.match(line)
        if date_match:
            release_date = date_match.group('date')
            line_index += 1
            continue

        released_at_match = _DOC_RELEASED_AT_PATTERN.match(line)
        if released_at_match:
            released_at = released_at_match.group('ts')
            line_index += 1
            continue

        release_status_match = _DOC_RELEASE_STATUS_PATTERN.match(line)
        if release_status_match:
            release_status = _clean_markdown_text(release_status_match.group('status'))
            line_index += 1
            continue

        if line.startswith('## '):
            current_section = line[3:].strip()
            line_index += 1
            continue

        if any(skip in current_section for skip in _SKIP_CHANGE_SECTIONS):
            line_index += 1
            continue

        if has_preferred_sections and not _is_preferred_change_section(current_section):
            line_index += 1
            continue

        bullet_match = _DOC_BULLET_PATTERN.match(raw_line.rstrip())
        if bullet_match:
            if not _is_top_level_bullet(raw_line):
                line_index += 1
                continue
            text = _clean_markdown_text(bullet_match.group('text'))
            nested_items, next_index = _collect_nested_bullet_summaries(lines, line_index)
            if _is_path_only_change(text):
                line_index = next_index
                continue
            text = _merge_change_with_nested_items(text, nested_items)
            if text and text not in seen_changes:
                seen_changes.add(text)
                changes.append(text)
            line_index = next_index
            continue

        line_index += 1

    if not title:
        title = f'版本 {version} 更新'

    if not release_date and released_at:
        release_date = released_at.split()[0]
    if not release_date:
        release_date = _format_doc_mtime(doc_path)[:10]
    if not released_at:
        released_at = _format_doc_mtime(doc_path)

    has_placeholder_content = _has_placeholder_release_content(title, changes, raw_text)
    is_draft = has_draft_comment or _is_draft_release_status(release_status) or has_placeholder_content

    return {
        'version': version,
        'date': release_date,
        'released_at': released_at,
        'title': title,
        'changes': changes or [title],
        'release_status': release_status,
        'is_draft': is_draft,
        'has_placeholder_content': has_placeholder_content,
    }


def _merge_changelog_from_docs(
    base_changelog: List[Dict],
    docs_dir: Optional[Path] = None,
    *,
    include_drafts: bool = False,
) -> List[Dict]:
    """将 docs 中的 changelog 作为更高优先级数据源并入基础列表。"""
    merged: Dict[str, Dict] = {}

    for item in base_changelog:
        version = item.get('version')
        if not version:
            continue
        merged[version] = deepcopy(item)

    scan_dir = docs_dir or _DOCS_DIR
    if scan_dir.exists():
        for doc_path in scan_dir.glob('CHANGELOG_v*.md'):
            parsed = _parse_doc_changelog(doc_path)
            if parsed is None:
                continue
            if parsed.get('is_draft') and not include_drafts:
                continue

            existing = merged.get(parsed['version'], {})
            merged[parsed['version']] = {
                **existing,
                **parsed,
                'date': parsed.get('date') or existing.get('date', ''),
                'released_at': parsed.get('released_at') or existing.get('released_at', ''),
                'title': parsed.get('title') or existing.get('title', ''),
                'changes': parsed.get('changes') or existing.get('changes', []),
            }

    return [
        merged[version]
        for version in sorted(merged.keys(), key=_version_key, reverse=True)
    ]


def _build_version_state(
    base_changelog: List[Dict],
    docs_dir: Optional[Path] = None,
    fallback_version: str = _FALLBACK_VERSION,
) -> Tuple[str, List[Dict], Dict]:
    """构建最终版本号、更新日志与完整版本信息。"""
    changelog = _merge_changelog_from_docs(base_changelog, docs_dir=docs_dir)
    version = changelog[0]['version'] if changelog else fallback_version
    info = {
        'version': version,
        'name': 'Bond Fund Tracker',
        'description': '债券基金数据采集与智能筛选系统',
        'title': changelog[0].get('title', '') if changelog else '',
        'release_date': changelog[0]['date'] if changelog else '2026-02-06',
        'released_at': changelog[0].get('released_at', '') if changelog else '2026-02-06 00:00',
        'changelog': deepcopy(changelog),
    }
    return version, changelog, info


__version__, CHANGELOG, VERSION_INFO = _build_version_state(_FALLBACK_CHANGELOG)


def get_version():
    """获取版本号字符串"""
    return __version__


def get_version_info():
    """获取完整版本信息"""
    return {
        **VERSION_INFO,
        'changelog': deepcopy(VERSION_INFO.get('changelog', [])),
    }


def get_changelog():
    """获取更新日志"""
    return deepcopy(CHANGELOG)
