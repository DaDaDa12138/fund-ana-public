from __future__ import annotations

import hashlib
import json
from io import BytesIO
import mimetypes
import sqlite3
from pathlib import Path
from typing import Any, Callable
from zlib import adler32

import pandas as pd
from flask import Flask, Response, jsonify, request, send_file, send_from_directory

from backend.metric_units import format_metric_display_text, is_decimal_ratio_percent_field
from backend.storage_lifecycle import artifact_location
from core.utils.core_csv_storage import has_stored_csv, read_stored_csv
from backend.task_audit_storage import (
    is_legacy_task_audit_path, is_task_log_path, read_migrated_task_audit, read_migrated_task_log,
    is_maintenance_audit_path, read_migrated_maintenance_audit,
    is_legacy_audit_csv_path, read_migrated_audit_csv,
)


def register_reports_routes(
    app: Flask,
    *,
    ensure_scheduler: Callable[[], None],
    manager_status: Callable[[], dict[str, Any]],
    workflow_status_summary: Callable[[], dict[str, Any]],
    manual_operations_enabled: Callable[[], bool],
    run_collection_default: Callable[[], bool],
    run_screening_default: Callable[[], bool],
    start_workflow: Callable[[dict[str, bool]], str],
    list_reports: Callable[[], list[dict[str, Any]]],
    request_arg_truthy: Callable[..., bool],
    build_reports_compact_response: Callable[[dict[str, Any] | None], dict[str, Any]],
    load_publication_registry: Callable[[], dict[str, Any]],
    load_current_published_report: Callable[[], dict[str, Any] | None],
    load_latest_report: Callable[[], dict[str, Any] | None],
    build_publication_summary_payload: Callable[..., dict[str, Any]],
    build_latest_report_source_metadata: Callable[..., dict[str, Any]],
    build_excel_preview_source_metadata: Callable[..., dict[str, Any]],
    build_project_file_source_headers: Callable[..., dict[str, str]],
    manager_latest_report: Callable[[], dict[str, Any] | None],
    resolve_project_file: Callable[[str], Path],
    deduplicate_headers: Callable[[list[str]], list[str]],
) -> None:
    def offline_response(path: Path):
        if path.exists():
            # Immutable manifests remain useful even while the ledger is unavailable.
            root = resolve_project_file('reports').parent
            for parent in path.parents:
                if not parent.is_relative_to(root):break
                manifest=parent/'validated.json'
                if manifest.is_file():
                    try:
                        expected=json.loads(manifest.read_text()).get('files',{}).get(str(path.relative_to(parent)))
                        if expected:
                            with path.open('rb') as stream:actual=hashlib.file_digest(stream,'sha256').hexdigest()
                            if actual!=expected:
                                return jsonify({'error':'原报表SHA不匹配','status':'corrupt'}),409
                    except (ValueError,OSError) as exc:
                        return jsonify({'error':str(exc),'status':'corrupt'}),409
                    break
            return None
        root = resolve_project_file('reports').parent
        location = artifact_location(root, path)
        if location['status'] in ('archived_offline', 'original_retired'):
            from backend.report_history_retention import CONTROL, original_state
            receipt=root/CONTROL/'retirements'/f'{location["unit_id"]}.json'
            if receipt.is_file():
                from backend.config import load_settings
                from backend.report_history_retention import owners
                try:
                    original_state(root,path)
                    settings=load_settings()
                    records=owners(settings,str(path.relative_to(root)))
                    return jsonify({'error':'历史报表原件已暂存或退役，可查看账本或重新生成导出',**location,
                        'alternatives':[{'run_id':r['run_id'],'revision':r['revision'],
                            'view_url':f'/history?run_id={r["run_id"]}&revision={r["revision"]}',
                            'api_url':f'/api/reports/history/{r["run_id"]}?revision={r["revision"]}'} for r in records]}),410
                except (ValueError,OSError,sqlite3.Error) as exc:
                    corrupt=isinstance(exc,ValueError) and 'unavailable' not in str(exc)
                    corrupt=corrupt or getattr(exc,'sqlite_errorcode',0) & 255 in (sqlite3.SQLITE_CORRUPT,sqlite3.SQLITE_NOTADB)
                    return jsonify({'error':str(exc),'status':'corrupt' if corrupt else 'history_unavailable'}),409 if corrupt else 503
            return jsonify({'error': '历史非正式材料已离线归档；请通过运维命令恢复', **location}), 410
        if location['status'] in ('storage_unavailable', 'storage_catalog_corrupt', 'corrupt'):
            return jsonify({'error': '存储材料或索引未通过完整性验证', **location}), 409
        return None

    @app.get("/api/status", endpoint="status")
    def status() -> Response:
        ensure_scheduler()
        return jsonify(manager_status())

    @app.get("/api/status/summary", endpoint="status_summary")
    def status_summary() -> Response:
        ensure_scheduler()
        return jsonify(workflow_status_summary())

    @app.post("/api/run", endpoint="run_workflow")
    def run_workflow() -> Response:
        ensure_scheduler()
        if not manual_operations_enabled():
            return jsonify(
                {
                    "error": "当前环境已切换为自动运行模式，普通页面不开放手动执行",
                    "status": "manual_operations_disabled",
                }
            ), 403

        payload = request.get_json(silent=True) or {}
        options = {
            "run_collection": bool(payload.get("run_collection", run_collection_default())),
            "run_screening": bool(payload.get("run_screening", run_screening_default())),
            "reuse_existing_outputs": bool(payload.get("reuse_existing_outputs", False)),
        }
        try:
            run_id = start_workflow(options)
        except RuntimeError as exc:
            return jsonify({"error": str(exc)}), 409
        return jsonify({"run_id": run_id, "status": "started", "options": options})

    @app.get("/api/reports", endpoint="reports")
    def reports() -> Response:
        ensure_scheduler()
        if request_arg_truthy("full", "detail"):
            return jsonify(list_reports())
        return jsonify(build_reports_compact_response(load_publication_registry()))

    @app.get("/api/reports/latest", endpoint="latest_report")
    def latest_report() -> Response:
        ensure_scheduler()
        payload = load_current_published_report()
        payload_source = "current_published_report"
        if not payload:
            return jsonify(
                {
                    "error": "当前没有可用的正式发布",
                    "status": "publication_unavailable",
                    "publication_truth": False,
                }
            ), 503
        publication = load_publication_registry()
        response_payload = dict(payload)
        response_payload["automation_layers"] = publication.get("automation_layers") or {}
        tracker_summary = (
            (response_payload.get("project_publish_summary") or {}).get("project_tracker_summary") or {}
        )
        response_payload["publication_summary"] = build_publication_summary_payload(
            publication,
            tracker_summary=tracker_summary,
            run_id=str(response_payload.get("run_id") or "").strip() or None,
        )
        response_payload["publication_status_summary"] = response_payload["publication_summary"]["status_summary"]
        response_payload.update(
            build_latest_report_source_metadata(
                response_payload,
                publication=publication,
                payload_source=payload_source,
            )
        )
        return jsonify(response_payload)

    @app.get("/api/reports/latest/markdown", endpoint="latest_report_markdown")
    def latest_report_markdown() -> Response:
        ensure_scheduler()
        payload = load_current_published_report()
        if not payload:
            return jsonify(
                {
                    "error": "当前没有可用的正式发布",
                    "status": "publication_unavailable",
                    "publication_truth": False,
                }
            ), 503
        report_md = payload.get("artifacts", {}).get("report_markdown")
        if not report_md:
            return jsonify({"error": "报告文件不存在"}), 404
        path = Path(report_md)
        if not path.exists():
            return jsonify({"error": "报告文件不存在"}), 404
        return Response(path.read_text(encoding="utf-8"), mimetype="text/markdown; charset=utf-8")

    @app.get("/api/preview-excel", endpoint="preview_excel")
    def preview_excel() -> Response:
        ensure_scheduler()
        relative_path = (request.args.get("path") or "").strip()
        selected_sheet = (request.args.get("sheet") or "").strip()
        if not relative_path:
            return jsonify({"error": "缺少文件路径"}), 400

        try:
            file_path = resolve_project_file(relative_path)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400

        if not file_path.exists() or not file_path.is_file():
            archived = offline_response(file_path)
            if archived is not None:
                return archived
            return jsonify({"error": "文件不存在"}), 404

        if file_path.suffix.lower() not in {".xlsx", ".xls", ".xlsm"}:
            return jsonify({"error": "仅支持 Excel 文件预览"}), 400

        try:
            workbook = pd.read_excel(file_path, sheet_name=None, dtype=str)
        except Exception as exc:  # pragma: no cover - runtime file parsing path
            return jsonify({"error": f"Excel 解析失败: {exc}"}), 500

        target_items = workbook.items()
        if selected_sheet:
            if selected_sheet not in workbook:
                return jsonify({"error": "指定工作表不存在"}), 404
            target_items = [(selected_sheet, workbook[selected_sheet])]

        sheets: list[dict[str, Any]] = []
        for sheet_name, df in target_items:
            preview_df = df.copy()
            preview_df = preview_df.where(pd.notna(preview_df), "")
            raw_headers = [str(column or "") for column in preview_df.columns.tolist()]
            headers = deduplicate_headers(raw_headers)
            rows = []
            for raw_row in preview_df.astype(str).values.tolist():
                rows.append(
                    [
                        format_metric_display_text(header, value, suffix=True)
                        if is_decimal_ratio_percent_field(header)
                        else value
                        for header, value in zip(raw_headers, raw_row)
                    ]
                )
            sheets.append(
                {
                    "name": str(sheet_name),
                    "row_count": int(len(preview_df.index)),
                    "column_count": int(len(headers)),
                    "columns": headers,
                    "rows": rows,
                }
            )

        response_payload = {
            "path": relative_path,
            "filename": file_path.name,
            "sheet_count": len(sheets),
            "selected_sheet": selected_sheet or None,
            "sheets": sheets,
        }
        response_payload.update(
            build_excel_preview_source_metadata(
                file_path,
                relative_path=relative_path,
                selected_sheet=selected_sheet or None,
                sheets=sheets,
                workbook_sheet_names=list(workbook.keys()),
            )
        )
        return jsonify(response_payload)

    @app.get("/api/files/<path:relative_path>", endpoint="project_file")
    def project_file(relative_path: str) -> Response:
        ensure_scheduler()
        try:
            file_path = resolve_project_file(relative_path)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        download = (request.args.get("download") or "").strip().lower() in {"1", "true", "yes"}
        archived = offline_response(file_path)
        if archived is not None:
            return archived
        if not file_path.exists() and has_stored_csv(file_path):
            try:
                original, receipt = read_stored_csv(file_path)
            except (OSError, ValueError):
                return jsonify({"error": "历史核心 CSV 压缩文件校验失败"}), 409
            original_info = receipt["original_identity"]
            # Match Werkzeug's ordinary path ETag in this deployment namespace.
            # Preserve the original float mtime as well as nanoseconds.
            check = adler32(str(file_path.absolute()).encode()) & 0xFFFFFFFF
            etag = f"{original_info['mtime']}-{original_info['size']}-{check}"
            response = send_file(
                BytesIO(original), mimetype="text/csv", as_attachment=download,
                download_name=file_path.name, conditional=True, etag=etag,
                last_modified=original_info["mtime"],
            )
            response.headers.update(build_project_file_source_headers(
                file_path, relative_path=relative_path, download=download,
            ))
            response.headers.update({
                "X-Fund-ANA-Storage-Codec": "zstd",
                "X-Fund-ANA-Original-SHA256": receipt["original_sha256"],
                "X-Fund-ANA-Stored-SHA256": receipt["stored_sha256"],
            })
            return response
        if not file_path.exists() and is_task_log_path(file_path):
            try:
                original, _, receipt = read_migrated_task_log(file_path)
            except FileNotFoundError:
                return jsonify({"error": "文件不存在"}), 404
            except (OSError, ValueError):
                return jsonify({"error": "历史任务日志压缩文件校验失败"}), 409
            response = send_file(
                BytesIO(original), mimetype=mimetypes.guess_type(file_path.name)[0] or "application/octet-stream",
                as_attachment=download, download_name=file_path.name, conditional=True, max_age=0,
                etag=receipt["original_sha256"],
                last_modified=receipt["original_identity"]["mtime_ns"] / 1_000_000_000,
            )
            response.headers.update(build_project_file_source_headers(
                file_path, relative_path=relative_path, download=download,
            ))
            response.headers.update({
                "X-Fund-ANA-Storage-Codec": "gzip",
                "X-Fund-ANA-Original-SHA256": receipt["original_sha256"],
                "X-Fund-ANA-Stored-SHA256": receipt["stored_sha256"],
            })
            return response
        legacy_gzip_request = file_path.name.endswith((".json.legacy.gz", ".csv.legacy.gz"))
        logical_path = (file_path.with_name(file_path.name.removesuffix(".legacy.gz"))
                        if legacy_gzip_request else file_path)
        maintenance_audit = is_maintenance_audit_path(logical_path)
        legacy_audit_csv = is_legacy_audit_csv_path(logical_path)
        if ((not file_path.exists() and is_legacy_task_audit_path(file_path))
                or ((maintenance_audit or legacy_audit_csv) and (legacy_gzip_request or not file_path.exists()))):
            try:
                reader = (read_migrated_audit_csv if legacy_audit_csv else read_migrated_maintenance_audit
                          if maintenance_audit else read_migrated_task_audit)
                original, stored, receipt = reader(logical_path)
            except FileNotFoundError:
                return jsonify({"error": "文件不存在"}), 404
            except (OSError, ValueError):
                return jsonify({"error": "历史审计压缩文件校验失败"}), 409
            transferred = stored if legacy_gzip_request else original
            response = send_file(BytesIO(transferred),
                                 mimetype=("application/gzip" if legacy_gzip_request else "text/csv"
                                           if legacy_audit_csv else "application/json"),
                                 as_attachment=download,
                                 download_name=file_path.name, conditional=True,
                                 etag=hashlib.sha256(transferred).hexdigest(), max_age=0,
                                 last_modified=(receipt["original_identity"]["mtime_ns"] / 1_000_000_000
                                                if legacy_audit_csv else None))
            response.headers.update({
                "X-Fund-ANA-Source": "task_audit_original_bytes",
                "X-Fund-ANA-Truth-Policy": "verified_original_artifact_bytes_only",
                "X-Fund-ANA-Missing-Value-Policy": "preserve_missing_semantics",
                "X-Fund-ANA-Business-Value-Synthesis": "forbidden",
                "X-Fund-ANA-Official-Publication-Truth": "false_binary_transfer_only",
                "X-Fund-ANA-Storage-Codec": "gzip",
                "X-Fund-ANA-Original-SHA256": receipt["original_sha256"],
                "X-Fund-ANA-Stored-SHA256": receipt["stored_sha256"],
            })
            if maintenance_audit or legacy_audit_csv:
                response.headers.update(build_project_file_source_headers(
                    file_path, relative_path=relative_path, download=download,
                ))
            return response
        if not file_path.exists() or not file_path.is_file():
            return jsonify({"error": "文件不存在"}), 404

        response = send_from_directory(
            file_path.parent,
            file_path.name,
            as_attachment=download,
            download_name=file_path.name,
        )
        for header, value in build_project_file_source_headers(
            file_path,
            relative_path=relative_path,
            download=download,
        ).items():
            response.headers[header] = value
        return response
