#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
外部服务连通性检查工具
"""

from __future__ import annotations

import os
import shlex
import time
from ipaddress import ip_address
from pathlib import Path
from typing import Tuple, Optional, List, Callable, Dict, Any
from urllib.parse import urlencode, urlparse

import requests
import yaml
from utils.project_runtime_config import PROJECT_ROOT, load_project_config_payload

DEFAULT_CONTAINER_NAME = 'bond-fund-tracker'
DEFAULT_PROJECT_ROOT = PROJECT_ROOT
TIANTIAN_RUNTIME_HEALTH_FOCUS_ENDPOINTS = (
    'fundVarietieValuationDetail',
    'fundMNDetailInformation',
    'fundMNHisNetList',
    'fundMNRank',
)
TIANTIAN_READINESS_PROBES = (
    {
        'label': 'health',
        'path': '/health',
        'params': None,
        'success': lambda status: 200 <= status < 300,
    },
    {
        'label': 'fundMNRank',
        'path': '/fundMNRank',
        'params': {
            'FundType': '31',
            'pageIndex': '1',
            'pageSize': '1',
        },
        'success': lambda status: 200 <= status < 300,
    },
)


def _normalize_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    value = str(url).strip()
    if not value:
        return None
    return value.rstrip('/')


def _build_probe_candidates(base_url: Optional[str]) -> List[str]:
    # 顺序按“显式配置优先、兼容兜底”组织，避免环境差异导致误判。
    candidates: List[str] = []
    project_config = load_project_config_payload()
    project_api_url = str(
        ((project_config.get('apis') or {}).get('tiantian_base_url') or '')
    ).strip()

    def add(url: Optional[str]):
        normalized = _normalize_url(url)
        if normalized and normalized not in candidates:
            candidates.append(normalized)

    add(base_url)
    add(project_api_url)
    add(os.environ.get('TIANTIAN_API_URL'))
    add(os.environ.get('SERVICE_CHECK_TIANTIAN_API_URL'))
    add(os.environ.get('DOCKER_CONTAINER_TIANTIAN_API') and f"http://{os.environ.get('DOCKER_CONTAINER_TIANTIAN_API')}:3000")

    if os.environ.get('DOTNET_RUNNING_IN_CONTAINER') or os.path.exists('/.dockerenv'):
        # 单容器部署优先探测当前实际容器名，同时保留历史别名兜底。
        add(f'http://{DEFAULT_CONTAINER_NAME}:3000')
        add('http://bond-fund-tiantianapi:3000')

    return candidates


def _build_probe_url(base_url: str, path: str, params: Optional[Dict[str, Any]] = None) -> str:
    normalized = _normalize_url(base_url) or str(base_url or '').strip()
    path_value = path if path.startswith('/') else f'/{path}'
    url = f"{normalized}{path_value}"
    if not params:
        return url
    return f"{url}?{urlencode(params)}"


def _is_running_in_container() -> bool:
    return bool(os.environ.get('DOTNET_RUNNING_IN_CONTAINER') or os.path.exists('/.dockerenv'))


def _is_host_local_private_tiantian_url(base_url: Optional[str]) -> bool:
    """判断当前地址是否是宿主机侧使用的本地 Tiantian 3000 端口。"""
    if _is_running_in_container():
        return False

    normalized = _normalize_url(base_url)
    if not normalized:
        return False

    try:
        parsed = urlparse(normalized)
    except Exception:
        return False

    host = (parsed.hostname or '').lower()
    port = parsed.port or (443 if parsed.scheme == 'https' else 80)
    return _is_local_or_wildcard_host(host) and port == 3000


def _is_local_or_wildcard_host(host: str) -> bool:
    normalized = str(host or '').strip().lower()
    if not normalized:
        return False
    if normalized == 'localhost':
        return True
    try:
        return ip_address(normalized).is_loopback or ip_address(normalized).is_unspecified
    except ValueError:
        return False


def _is_tiantian_host_port_mapping(value: str) -> bool:
    normalized = str(value or '').strip().strip('"').strip("'")
    if not normalized:
        return False
    if normalized.endswith('/tcp'):
        normalized = normalized[:-4]

    parts = [part.strip() for part in normalized.split(':')]
    if len(parts) == 2:
        published, target = parts
        return published == '3000' and target == '3000'
    if len(parts) == 3:
        host_ip, published, target = parts
        return _is_local_or_wildcard_host(host_ip) and published == '3000' and target == '3000'
    return False


def is_host_tiantian_port_published(
    project_root: Optional[os.PathLike | str] = None,
    service_name: str = DEFAULT_CONTAINER_NAME,
) -> bool:
    """判断当前 compose 是否已向宿主机发布 Tiantian 3000 端口。"""
    compose_path = Path(project_root or DEFAULT_PROJECT_ROOT) / 'docker-compose.yml'
    if not compose_path.exists():
        return False

    try:
        payload = yaml.safe_load(compose_path.read_text(encoding='utf-8')) or {}
    except Exception:
        return False

    service = ((payload.get('services') or {}).get(service_name) or {})
    ports = service.get('ports') or []

    for item in ports:
        if isinstance(item, str):
            if _is_tiantian_host_port_mapping(item):
                return True
        elif isinstance(item, dict):
            target = item.get('target')
            published = item.get('published')
            if int(target or 0) != 3000 or int(published or 0) != 3000:
                continue
            host_ip = str(item.get('host_ip') or '').strip()
            if not host_ip or _is_local_or_wildcard_host(host_ip):
                return True

    return False


def _resolve_container_script_relpath(
    script_path: Optional[os.PathLike | str],
    project_root: Optional[os.PathLike | str] = None,
) -> Optional[str]:
    """把宿主机脚本路径映射为容器内 /app 下的相对脚本路径。"""
    if not script_path:
        return None

    try:
        script = Path(script_path).resolve()
    except Exception:
        script = Path(str(script_path))

    if project_root:
        try:
            rel = script.relative_to(Path(project_root).resolve())
            return str(rel).replace(os.sep, '/')
        except Exception:
            pass

    parts = script.parts
    if 'scripts' in parts:
        idx = parts.index('scripts')
        return '/'.join(parts[idx:])

    return script.name


def _build_docker_exec_command(
    script_path: Optional[os.PathLike | str],
    project_root: Optional[os.PathLike | str] = None,
    argv: Optional[List[str]] = None,
    container_name: str = DEFAULT_CONTAINER_NAME,
) -> Optional[str]:
    """构造推荐的 docker exec 执行命令。"""
    script_relpath = _resolve_container_script_relpath(
        script_path=script_path,
        project_root=project_root,
    )
    if not script_relpath:
        return None

    quoted_args = [shlex.quote(str(arg)) for arg in (argv or []) if str(arg).strip()]
    inner_command = ' '.join([
        'cd /app && python3',
        script_relpath,
        *quoted_args,
    ]).strip()
    return f"docker exec {shlex.quote(container_name)} sh -lc {shlex.quote(inner_command)}"


def build_tiantian_host_runtime_guidance(
    base_url: Optional[str],
    script_path: Optional[os.PathLike | str] = None,
    project_root: Optional[os.PathLike | str] = None,
    argv: Optional[List[str]] = None,
    container_name: str = DEFAULT_CONTAINER_NAME,
) -> List[str]:
    """为宿主机使用本地 Tiantian 3000 端口的脚本输出统一修复建议。"""
    if not _is_host_local_private_tiantian_url(base_url):
        return []

    port_published = is_host_tiantian_port_published(project_root=project_root)
    command = _build_docker_exec_command(
        script_path=script_path,
        project_root=project_root,
        argv=argv,
        container_name=container_name,
    )

    if port_published:
        guidance = [
            "提示: 当前部署已配置宿主机直连 Tiantian 本地 3000 端口。",
            "请先检查 `docker compose ps`、容器健康状态，并确认 Tiantian 3000 端口映射仍存在。",
        ]
        if command:
            guidance.append(f"若需绕过宿主机网络层，也可改为容器内执行: {command}")
        return guidance

    guidance = [
        "提示: 当前部署可能未向宿主机发布 Tiantian 3000 端口。",
    ]
    if command:
        guidance.append(f"建议改为容器内执行: {command}")
    guidance.append("或在 `docker-compose.yml` 中发布 Tiantian 3000 端口，再继续使用宿主机脚本。")
    return guidance


def emit_tiantian_host_runtime_guidance(
    base_url: Optional[str],
    script_path: Optional[os.PathLike | str] = None,
    project_root: Optional[os.PathLike | str] = None,
    argv: Optional[List[str]] = None,
    container_name: str = DEFAULT_CONTAINER_NAME,
    printer: Callable[[str], None] = print,
) -> None:
    """输出宿主机脚本运行建议，默认打印到 stdout。"""
    for line in build_tiantian_host_runtime_guidance(
        base_url=base_url,
        script_path=script_path,
        project_root=project_root,
        argv=argv,
        container_name=container_name,
    ):
        printer(line)


def _build_host_internal_only_hint(candidates: List[str], errors: List[str]) -> str:
    """为宿主机误探测容器内私有端口的场景补充明确提示。"""
    if _is_running_in_container():
        return ''

    local_candidates = [
        candidate
        for candidate in candidates
        if _is_host_local_private_tiantian_url(candidate)
    ]
    if not local_candidates:
        return ''

    error_text = ' '.join(errors).lower()
    transient_markers = (
        'connection refused',
        'failed to establish a new connection',
        'max retries exceeded',
        'error=',
    )
    if not any(marker in error_text for marker in transient_markers):
        return ''

    if is_host_tiantian_port_published():
        return (
            " 提示: 当前部署预期允许宿主机访问本地 Tiantian 3000 端口；"
            "请优先检查 `docker compose ps`、容器健康状态，以及对应端口映射是否仍存在。"
        )

    return (
        " 提示: 当前部署可能未向宿主机发布 Tiantian 3000 端口；"
        "宿主机直接运行脚本时，可优先使用 `docker exec bond-fund-tracker ...`，"
        "或先在 `docker-compose.yml` 中发布 Tiantian 3000 端口。"
    )


def _load_response_json(resp: requests.Response) -> Tuple[Optional[Any], Optional[str]]:
    """安全解析响应 JSON，避免业务探针被异常 payload 干扰。"""
    if not hasattr(resp, 'json'):
        return None, 'response_has_no_json_decoder'

    try:
        return resp.json(), None
    except ValueError as exc:
        return None, str(exc) or exc.__class__.__name__
    except Exception as exc:  # pragma: no cover - requests.Response.json 通常抛 ValueError
        return None, str(exc) or exc.__class__.__name__


def inspect_tiantian_fund_mn_rank_business_response(resp: requests.Response) -> Tuple[bool, Dict[str, Any]]:
    """校验 fundMNRank 是否返回了可供采集链路消费的有效业务数据。"""
    payload, json_error = _load_response_json(resp)
    if json_error:
        return False, {
            'business_status': 'invalid_json',
            'detail_parts': [
                'business=invalid_json',
                f'json_error={json_error}',
            ],
            'json_error': json_error,
            'payload_success': None,
            'datas_count': None,
            'total_count': None,
            'err_code': None,
            'err_msg': None,
        }

    if not isinstance(payload, dict):
        return False, {
            'business_status': 'invalid_payload',
            'detail_parts': [
                'business=invalid_payload',
                f'payload_type={type(payload).__name__}',
            ],
            'json_error': None,
            'payload_success': None,
            'datas_count': None,
            'total_count': None,
            'err_code': None,
            'err_msg': None,
        }

    payload_success = payload.get('Success')
    payload_success_text = (
        'true' if payload_success is True else
        'false' if payload_success is False else
        str(payload_success)
    )
    if payload_success is not True:
        details = [
            'business=upstream_degraded',
            f'payload_success={payload_success_text}',
        ]
        err_code = payload.get('ErrCode')
        err_msg = str(payload.get('ErrMsg') or '').strip()
        if err_code not in (None, ''):
            details.append(f'err_code={err_code}')
        if err_msg:
            details.append(f'err_msg={err_msg}')
        return False, {
            'business_status': 'upstream_degraded',
            'detail_parts': details,
            'json_error': None,
            'payload_success': payload_success,
            'datas_count': None,
            'total_count': payload.get('TotalCount'),
            'err_code': err_code,
            'err_msg': err_msg or None,
        }

    datas = payload.get('Datas')
    if not isinstance(datas, list):
        return False, {
            'business_status': 'invalid_payload',
            'detail_parts': [
                'business=invalid_payload',
                f'datas_type={type(datas).__name__}',
            ],
            'json_error': None,
            'payload_success': payload_success,
            'datas_count': None,
            'total_count': payload.get('TotalCount'),
            'err_code': payload.get('ErrCode'),
            'err_msg': str(payload.get('ErrMsg') or '').strip() or None,
        }

    if not datas:
        return False, {
            'business_status': 'empty_datas',
            'detail_parts': [
                'business=empty_datas',
                'payload_success=true',
                'datas_count=0',
            ],
            'json_error': None,
            'payload_success': payload_success,
            'datas_count': 0,
            'total_count': payload.get('TotalCount'),
            'err_code': payload.get('ErrCode'),
            'err_msg': str(payload.get('ErrMsg') or '').strip() or None,
        }

    details = [
        'business=ok',
        'payload_success=true',
        f'datas_count={len(datas)}',
    ]
    total_count = payload.get('TotalCount')
    if total_count not in (None, ''):
        details.append(f'total_count={total_count}')
    return True, {
        'business_status': 'ok',
        'detail_parts': details,
        'json_error': None,
        'payload_success': payload_success,
        'datas_count': len(datas),
        'total_count': total_count,
        'err_code': payload.get('ErrCode'),
        'err_msg': str(payload.get('ErrMsg') or '').strip() or None,
    }


def _has_structured_tiantian_business_failure_payload(resp: requests.Response) -> bool:
    """判断响应体是否是可提取业务语义的结构化失败 JSON。"""
    payload, json_error = _load_response_json(resp)
    if json_error or not isinstance(payload, dict):
        return False

    business_keys = {
        'Success',
        'ErrCode',
        'ErrMsg',
        'success',
        'error',
    }
    return any(key in payload for key in business_keys)


def probe_tiantian_api_business_status(
    base_url: Optional[str] = None,
    timeout_seconds: float = 2.0,
) -> Dict[str, Any]:
    """探测 Tiantian fundMNRank 业务状态，区分代理可达与上游业务降级。"""
    candidates = _build_probe_candidates(base_url)
    if not candidates:
        return {
            'ok': False,
            'status': 'unreachable',
            'message': 'API业务未就绪: 未提供可用探测地址',
            'url': None,
            'base_url': None,
            'http_status': None,
            'payload_success': None,
            'datas_count': None,
            'total_count': None,
            'err_code': None,
            'err_msg': None,
            'json_error': None,
            'errors': [],
        }

    errors: List[str] = []
    representative_failure: Optional[Dict[str, Any]] = None

    for url in candidates:
        normalized_url = _normalize_url(url)
        probe_url = _build_probe_url(
            url,
            '/fundMNRank',
            {
                'FundType': '31',
                'pageIndex': '1',
                'pageSize': '1',
            },
        )
        try:
            resp = requests.get(
                f"{normalized_url}/fundMNRank",
                params={
                    'FundType': '31',
                    'pageIndex': '1',
                    'pageSize': '1',
                },
                timeout=timeout_seconds,
            )
        except requests.RequestException as exc:
            errors.append(f"{probe_url} error={exc} (probe=fundMNRank)")
            if representative_failure is None:
                representative_failure = {
                    'ok': False,
                    'status': 'request_error',
                    'message': '',
                    'url': probe_url,
                    'base_url': normalized_url,
                    'http_status': None,
                    'payload_success': None,
                    'datas_count': None,
                    'total_count': None,
                    'err_code': None,
                    'err_msg': None,
                    'json_error': None,
                    'errors': errors,
                }
            continue

        if not (200 <= resp.status_code < 300):
            if _has_structured_tiantian_business_failure_payload(resp):
                business_ok, business_meta = inspect_tiantian_fund_mn_rank_business_response(resp)
                if not business_ok:
                    detail_parts = [
                        'probe=fundMNRank',
                        f'status={resp.status_code}',
                        *(business_meta.get('detail_parts') or []),
                    ]
                    result = {
                        'ok': False,
                        'status': business_meta.get('business_status'),
                        'message': '',
                        'url': probe_url,
                        'base_url': normalized_url,
                        'http_status': resp.status_code,
                        'payload_success': business_meta.get('payload_success'),
                        'datas_count': business_meta.get('datas_count'),
                        'total_count': business_meta.get('total_count'),
                        'err_code': business_meta.get('err_code'),
                        'err_msg': business_meta.get('err_msg'),
                        'json_error': business_meta.get('json_error'),
                        'errors': errors,
                    }
                    errors.append(f"{probe_url} ({', '.join(detail_parts)})")
                    if representative_failure is None or representative_failure.get('status') in {
                        'request_error',
                        f'http_{resp.status_code}',
                    }:
                        representative_failure = result
                    continue

            errors.append(f"{probe_url} status={resp.status_code} (probe=fundMNRank)")
            if representative_failure is None:
                representative_failure = {
                    'ok': False,
                    'status': f'http_{resp.status_code}',
                    'message': '',
                    'url': probe_url,
                    'base_url': normalized_url,
                    'http_status': resp.status_code,
                    'payload_success': None,
                    'datas_count': None,
                    'total_count': None,
                    'err_code': None,
                    'err_msg': None,
                    'json_error': None,
                    'errors': errors,
                }
            continue

        business_ok, business_meta = inspect_tiantian_fund_mn_rank_business_response(resp)
        detail_parts = [
            'probe=fundMNRank',
            f'status={resp.status_code}',
            *(business_meta.get('detail_parts') or []),
        ]
        message = f"API业务{'可用' if business_ok else '未就绪'}: {probe_url} ({', '.join(detail_parts)})"
        result = {
            'ok': business_ok,
            'status': business_meta.get('business_status'),
            'message': message,
            'url': probe_url,
            'base_url': normalized_url,
            'http_status': resp.status_code,
            'payload_success': business_meta.get('payload_success'),
            'datas_count': business_meta.get('datas_count'),
            'total_count': business_meta.get('total_count'),
            'err_code': business_meta.get('err_code'),
            'err_msg': business_meta.get('err_msg'),
            'json_error': business_meta.get('json_error'),
            'errors': errors,
        }
        if business_ok:
            return result

        errors.append(f"{probe_url} ({', '.join(detail_parts)})")
        if representative_failure is None or representative_failure.get('status') == 'request_error':
            representative_failure = result

    hint = _build_host_internal_only_hint(candidates, errors)
    if representative_failure is None:
        representative_failure = {
            'ok': False,
            'status': 'unreachable',
            'url': None,
            'base_url': None,
            'http_status': None,
            'payload_success': None,
            'datas_count': None,
            'total_count': None,
            'err_code': None,
            'err_msg': None,
            'json_error': None,
            'errors': errors,
        }
    representative_failure['message'] = 'API业务未就绪: ' + ' | '.join(errors) + hint
    representative_failure['errors'] = list(errors)
    return representative_failure


def summarize_tiantian_runtime_health(
    base_url: Optional[str] = None,
    timeout_seconds: float = 0.6,
) -> Dict[str, Any]:
    """读取 Tiantian /health 运行摘要，供 Web 信息接口和审计脚本复用。"""
    def _attach_business_probe(summary: Dict[str, Any]) -> Dict[str, Any]:
        business_probe = probe_tiantian_api_business_status(
            base_url=base_url,
            timeout_seconds=timeout_seconds,
        )
        summary.update({
            'business_ready': business_probe.get('ok'),
            'business_status': business_probe.get('status'),
            'business_message': business_probe.get('message'),
            'business_http_status': business_probe.get('http_status'),
            'business_payload_success': business_probe.get('payload_success'),
            'business_datas_count': business_probe.get('datas_count'),
            'business_total_count': business_probe.get('total_count'),
            'business_err_code': business_probe.get('err_code'),
            'business_err_msg': business_probe.get('err_msg'),
            'business_json_error': business_probe.get('json_error'),
        })
        return summary

    candidates = _build_probe_candidates(base_url)
    if not candidates:
        return _attach_business_probe({
            'available': False,
            'verdict': 'down',
            'status': 'unreachable',
            'message': 'Tiantian 运行时摘要不可用: 未提供可用探测地址',
            'base_url': None,
            'health_url': None,
            'http_status': None,
            'metrics_available': False,
            'has_any_degraded_endpoint': False,
            'degraded_endpoints': [],
            'empty_hotspots': [],
            'focus_metrics': {},
            'errors': [],
        })

    errors: List[str] = []
    for url in candidates:
        normalized_url = _normalize_url(url)
        probe_url = _build_probe_url(url, '/health')
        try:
            resp = requests.get(
                f"{normalized_url}/health",
                timeout=timeout_seconds,
            )
        except requests.RequestException as exc:
            errors.append(f"{probe_url} error={exc}")
            continue

        if not (200 <= resp.status_code < 300):
            errors.append(f"{probe_url} status={resp.status_code}")
            continue

        payload, json_error = _load_response_json(resp)
        if json_error:
            return _attach_business_probe({
                'available': True,
                'verdict': 'warn',
                'status': 'invalid_json',
                'message': f'Tiantian /health 返回了无法解析的 JSON: {probe_url}',
                'base_url': normalized_url,
                'health_url': probe_url,
                'http_status': resp.status_code,
                'metrics_available': False,
                'has_any_degraded_endpoint': False,
                'degraded_endpoints': [],
                'empty_hotspots': [],
                'focus_metrics': {},
                'errors': errors,
                'json_error': json_error,
            })

        if not isinstance(payload, dict):
            return _attach_business_probe({
                'available': True,
                'verdict': 'warn',
                'status': 'invalid_payload',
                'message': f'Tiantian /health 返回了非对象 payload: {probe_url}',
                'base_url': normalized_url,
                'health_url': probe_url,
                'http_status': resp.status_code,
                'metrics_available': False,
                'has_any_degraded_endpoint': False,
                'degraded_endpoints': [],
                'empty_hotspots': [],
                'focus_metrics': {},
                'errors': errors,
                'payload_type': type(payload).__name__,
            })

        endpoint_metrics = payload.get('endpointMetrics')
        endpoint_summary = payload.get('endpointSummary')
        metrics_available = isinstance(endpoint_metrics, dict) and isinstance(endpoint_summary, dict)
        focus_metrics = {
            name: endpoint_metrics.get(name)
            for name in TIANTIAN_RUNTIME_HEALTH_FOCUS_ENDPOINTS
            if isinstance(endpoint_metrics, dict) and name in endpoint_metrics
        }
        degraded_endpoints = endpoint_summary.get('degradedEndpoints') if isinstance(endpoint_summary, dict) else []
        empty_hotspots = endpoint_summary.get('emptyHotspots') if isinstance(endpoint_summary, dict) else []
        degraded_endpoints = degraded_endpoints if isinstance(degraded_endpoints, list) else []
        empty_hotspots = empty_hotspots if isinstance(empty_hotspots, list) else []
        has_any_degraded_endpoint = bool(endpoint_summary.get('hasAnyDegradedEndpoint')) if isinstance(endpoint_summary, dict) else False
        if degraded_endpoints:
            has_any_degraded_endpoint = True

        if not metrics_available:
            verdict = 'warn'
            status = 'metrics_missing'
            message = f'Tiantian /health 已可达，但尚未暴露 endpointMetrics/endpointSummary: {probe_url}'
        elif has_any_degraded_endpoint:
            verdict = 'warn'
            status = 'degraded'
            message = f'Tiantian /health 报告存在降级端点: {probe_url}'
        elif empty_hotspots:
            verdict = 'warn'
            status = 'empty_hotspot'
            message = f'Tiantian /health 报告存在空热点端点: {probe_url}'
        else:
            verdict = 'ok'
            status = 'ok'
            message = f'Tiantian /health 运行摘要正常: {probe_url}'

        return _attach_business_probe({
            'available': True,
            'verdict': verdict,
            'status': status,
            'message': message,
            'base_url': normalized_url,
            'health_url': probe_url,
            'http_status': resp.status_code,
            'service': payload.get('service'),
            'version': payload.get('version'),
            'route_count': payload.get('routeCount'),
            'metrics_available': metrics_available,
            'has_any_degraded_endpoint': has_any_degraded_endpoint,
            'degraded_endpoints': degraded_endpoints,
            'empty_hotspots': empty_hotspots,
            'focus_metrics': focus_metrics,
            'errors': errors,
        })

    hint = _build_host_internal_only_hint(candidates, errors)
    return _attach_business_probe({
        'available': False,
        'verdict': 'down',
        'status': 'unreachable',
        'message': 'Tiantian 运行时摘要不可用: ' + ' | '.join(errors) + hint,
        'base_url': None,
        'health_url': None,
        'http_status': None,
        'metrics_available': False,
        'has_any_degraded_endpoint': False,
        'degraded_endpoints': [],
        'empty_hotspots': [],
        'focus_metrics': {},
        'errors': errors,
    })


def check_tiantian_api_ready(
    base_url: Optional[str] = None,
    timeout_seconds: float = 2.0,
    require_business_success: bool = False,
) -> Tuple[bool, str]:
    """
    检查天天基金代理 API 是否可达。

    Args:
        base_url: API 基础地址；未显式传入时优先按项目级 `config/project.yaml` 解析
        timeout_seconds: 请求超时时间
        require_business_success: 是否要求 `fundMNRank` 返回有效业务数据

    Returns:
        (is_ready, message)
    """
    if require_business_success:
        business_probe = probe_tiantian_api_business_status(
            base_url=base_url,
            timeout_seconds=timeout_seconds,
        )
        return bool(business_probe.get('ok')), str(business_probe.get('message') or '')

    candidates = _build_probe_candidates(base_url)
    if not candidates:
        return False, (
            "API不可达: 未提供可用探测地址"
        )

    errors = []
    success_prefix = "API业务可用" if require_business_success else "API可达"
    failure_prefix = "API业务未就绪" if require_business_success else "API不可达"
    for url in candidates:
        candidate_statuses: Dict[str, Optional[int]] = {}
        for probe in TIANTIAN_READINESS_PROBES:
            probe_url = _build_probe_url(url, probe['path'], probe.get('params'))
            try:
                resp = requests.get(
                    f"{_normalize_url(url)}{probe['path']}",
                    params=probe.get('params'),
                    timeout=timeout_seconds,
                )
                candidate_statuses[probe['label']] = resp.status_code
                if not probe['success'](resp.status_code):
                    errors.append(
                        f"{probe_url} status={resp.status_code} (probe={probe['label']})"
                    )
                    continue

                detail_parts = [
                    f"probe={probe['label']}",
                    f"status={resp.status_code}",
                ]

                # 兼容旧运行时：部分历史 Tiantian 容器没有实现 /health，
                # 但真实数据接口 fundMNRank 仍可正常返回。
                if (
                    probe['label'] == 'fundMNRank'
                    and candidate_statuses.get('health') == 404
                ):
                    detail_parts.append("compat=legacy_runtime_without_health")
                    detail_parts.append("health_status=404")
                return (
                    True,
                    f"{success_prefix}: {probe_url} "
                    f"({', '.join(detail_parts)})"
                )
            except requests.RequestException as e:
                candidate_statuses[probe['label']] = None
                errors.append(f"{probe_url} error={e} (probe={probe['label']})")

    hint = _build_host_internal_only_hint(candidates, errors)
    return False, failure_prefix + ": " + " | ".join(errors) + hint


def check_tiantian_api_business_ready(
    base_url: Optional[str] = None,
    timeout_seconds: float = 2.0,
) -> Tuple[bool, str]:
    """
    检查天天基金代理是否返回了真实可消费的 fundMNRank 业务数据。
    """
    business_probe = probe_tiantian_api_business_status(
        base_url=base_url,
        timeout_seconds=timeout_seconds,
    )
    return bool(business_probe.get('ok')), str(business_probe.get('message') or '')


def wait_tiantian_api_ready(
    base_url: Optional[str] = None,
    timeout_seconds: float = 60.0,
    probe_interval: float = 2.0,
    single_probe_timeout: float = 2.0,
) -> Tuple[bool, str]:
    """
    在给定时限内轮询天天基金代理可达性。

    Returns:
        (is_ready, message)
    """
    deadline = time.time() + max(timeout_seconds, 0.0)
    last_msg = "API不可达: 未执行探测"

    while True:
        ok, msg = check_tiantian_api_ready(
            base_url=base_url,
            timeout_seconds=single_probe_timeout,
        )
        if ok:
            return True, msg

        last_msg = msg
        now = time.time()
        if now >= deadline:
            return False, last_msg

        sleep_seconds = min(max(probe_interval, 0.0), max(deadline - now, 0.0))
        if sleep_seconds <= 0:
            return False, last_msg
        time.sleep(sleep_seconds)
