from __future__ import annotations

import hashlib
import json
import math
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from openpyxl import load_workbook
from openpyxl.comments import Comment
from openpyxl.formatting.rule import ColorScaleRule, DataBarRule
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

from backend.config import Settings
from backend.manager_metrics import MANAGER_COLUMN_DESCRIPTIONS
from backend.metric_units import (
    DRAWDOWN_OBSERVATION_FIELDS,
    apply_metric_display_units_to_frame,
    is_decimal_ratio_percent_field,
    metric_excel_number_format,
    parse_metric_number,
)
from core.utils.drawdown_duration import DRAWDOWN_OBSERVATION_DEFINITION

WORKBOOK_CONTENT_SIGNATURE_VERSION = 5
LIGHTWEIGHT_STYLE_ROW_THRESHOLD = 3000
LIGHTWEIGHT_STYLE_CELL_THRESHOLD = 120000


def _read_json_file(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return {}
    file_path = Path(path)
    if not file_path.exists() or not file_path.is_file():
        return {}
    try:
        payload = json.loads(file_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _stringify_frame(df: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
    safe_df = df.copy().astype(object)
    return safe_df.where(safe_df.notna(), "").astype(str)


def _excel_decimal_ratio_value(value: object) -> float | str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    if not text or text.endswith("%"):
        return text
    number = parse_metric_number(value)
    return float(number) if number is not None else text


def _project_relative(settings: Settings, path: str | Path | None) -> str | None:
    if not path:
        return None
    candidate = Path(path).resolve()
    try:
        return str(candidate.relative_to(settings.project_root.resolve()))
    except ValueError:
        return str(candidate)


def _resolve_project_path(settings: Settings, path: str | Path | None) -> Path | None:
    if not path:
        return None
    candidate = Path(path)
    return candidate if candidate.is_absolute() else (settings.project_root / candidate).resolve()


def _write_excel_meta(
    *,
    settings: Settings,
    meta_path: Path,
    output_file: Path,
    report_type: str,
    report_label: str,
    run_id: str,
    source_files: list[str | Path | None],
    counts: dict[str, Any],
    content_signature: str | None = None,
    reused_from: dict[str, Any] | None = None,
) -> None:
    payload = {
        "report_type": report_type,
        "report_label": report_label,
        "fund_type": "bond",
        "fund_type_name": "债券型基金",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "primary_run_id": run_id,
        "source_run_ids": [run_id],
        "output_file": _project_relative(settings, output_file),
        "derived_from_files": [
            _project_relative(settings, source_file)
            for source_file in source_files
            if source_file
        ],
        "counts": counts,
    }
    if content_signature:
        payload["content_signature"] = content_signature
    if reused_from:
        payload["reused_from"] = reused_from
    meta_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _frame_content_signature(df: pd.DataFrame) -> str:
    text_df = _stringify_frame(df)
    payload = {
        "columns": list(text_df.columns),
        "rows": text_df.values.tolist(),
    }
    return hashlib.sha1(json.dumps(payload, ensure_ascii=False, sort_keys=False).encode("utf-8")).hexdigest()


def _build_multisheet_workbook_signature(
    *,
    main_sheet_name: str,
    main_df: pd.DataFrame,
    extra_sheets: list[dict[str, Any]] | None = None,
    left_align_column: str | None = None,
    highlight_fill_color: str | None = None,
    column_widths: dict[str, int] | None = None,
    top_decile_rules: dict[str, str] | None = None,
) -> str:
    payload = {
        "signature_version": WORKBOOK_CONTENT_SIGNATURE_VERSION,
        "main_sheet_name": str(main_sheet_name or ""),
        "main": {
            "left_align_column": str(left_align_column or ""),
            "highlight_fill_color": str(highlight_fill_color or ""),
            "column_widths": dict(column_widths or {}),
            "top_decile_rules": dict(top_decile_rules or {}),
            "df_signature": _frame_content_signature(main_df),
        },
        "extra_sheets": [
            {
                "title": str(sheet.get("title") or ""),
                "left_align_column": str(sheet.get("left_align_column") or ""),
                "highlight_fill_color": str(sheet.get("highlight_fill_color") or ""),
                "column_widths": dict(sheet.get("column_widths") or {}),
                "top_decile_rules": dict(sheet.get("top_decile_rules") or {}),
                "df_signature": _frame_content_signature(
                    sheet.get("df") if isinstance(sheet.get("df"), pd.DataFrame) else pd.DataFrame()
                ),
            }
            for sheet in (extra_sheets or [])
        ],
    }
    return hashlib.sha1(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def _should_use_lightweight_sheet_style(text_df: pd.DataFrame) -> bool:
    row_count = int(len(text_df.index))
    col_count = int(len(text_df.columns))
    cell_count = row_count * max(1, col_count)
    return row_count >= LIGHTWEIGHT_STYLE_ROW_THRESHOLD or cell_count >= LIGHTWEIGHT_STYLE_CELL_THRESHOLD


def _try_reuse_previous_workbook_artifact(
    *,
    settings: Settings,
    current_signature: str,
    output_file: Path,
    artifact_key: str,
    artifact_meta_key: str,
    current_snapshot_date: str = "",
    csv_output_file: Path | None = None,
    csv_artifact_key: str | None = None,
) -> dict[str, Any] | None:
    latest_report = _read_json_file(settings.reports_dir / "latest.json")
    if not latest_report:
        return None
    latest_snapshot_date = str(
        ((((latest_report.get("database_freshness") or {}).get("effective_nav_snapshot") or {}).get("candidate_date") or ""))
    ).strip()
    if current_snapshot_date and latest_snapshot_date and current_snapshot_date != latest_snapshot_date:
        return None

    artifacts = latest_report.get("artifacts") or {}
    previous_output = _resolve_project_path(settings, artifacts.get(artifact_key))
    previous_meta = _resolve_project_path(settings, artifacts.get(artifact_meta_key))
    if previous_output is None or previous_meta is None or not previous_output.exists() or not previous_meta.exists():
        return None

    meta_payload = _read_json_file(previous_meta)
    if str(meta_payload.get("content_signature") or "").strip() != current_signature:
        return None

    if previous_output.resolve() == output_file.resolve():
        previous_csv_path = None
        if csv_output_file is not None and csv_artifact_key:
            previous_csv_path = _resolve_project_path(settings, artifacts.get(csv_artifact_key))
        if previous_csv_path is not None and csv_output_file is not None and previous_csv_path.exists():
            if previous_csv_path.resolve() != csv_output_file.resolve():
                csv_output_file.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(previous_csv_path, csv_output_file)
        return {
            "run_id": str(latest_report.get("run_id") or "").strip(),
            "output_file": _project_relative(settings, previous_output),
            "csv_file": _project_relative(settings, previous_csv_path) if previous_csv_path else None,
            "meta_file": _project_relative(settings, previous_meta),
        }

    output_file.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(previous_output, output_file)

    previous_csv_path: Path | None = None
    if csv_output_file is not None and csv_artifact_key:
        previous_csv_path = _resolve_project_path(settings, artifacts.get(csv_artifact_key))
        if previous_csv_path and previous_csv_path.exists():
            csv_output_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(previous_csv_path, csv_output_file)

    return {
        "run_id": str(latest_report.get("run_id") or "").strip(),
        "output_file": _project_relative(settings, previous_output),
        "csv_file": _project_relative(settings, previous_csv_path) if previous_csv_path else None,
        "meta_file": _project_relative(settings, previous_meta),
    }


def _materialize_multisheet_workbook(
    *,
    settings: Settings,
    output_file: Path,
    artifact_key: str,
    artifact_meta_key: str,
    main_sheet_name: str,
    main_df: pd.DataFrame,
    extra_sheets: list[dict[str, Any]] | None = None,
    left_align_column: str | None = None,
    highlight_fill_color: str | None = None,
    column_widths: dict[str, int] | None = None,
    top_decile_rules: dict[str, str] | None = None,
    current_snapshot_date: str = "",
    csv_output_file: Path | None = None,
    csv_artifact_key: str | None = None,
    csv_df: pd.DataFrame | None = None,
) -> dict[str, Any]:
    content_signature = _build_multisheet_workbook_signature(
        main_sheet_name=main_sheet_name,
        main_df=main_df,
        extra_sheets=extra_sheets,
        left_align_column=left_align_column,
        highlight_fill_color=highlight_fill_color,
        column_widths=column_widths,
        top_decile_rules=top_decile_rules,
    )
    reused_from = _try_reuse_previous_workbook_artifact(
        settings=settings,
        current_signature=content_signature,
        output_file=output_file,
        artifact_key=artifact_key,
        artifact_meta_key=artifact_meta_key,
        current_snapshot_date=current_snapshot_date,
        csv_output_file=csv_output_file if csv_df is None else None,
        csv_artifact_key=csv_artifact_key if csv_df is None else None,
    )
    if reused_from is None:
        _write_multisheet_workbook(
            output_file,
            main_sheet_name=main_sheet_name,
            main_df=main_df,
            extra_sheets=extra_sheets,
            left_align_column=left_align_column,
            highlight_fill_color=highlight_fill_color,
            column_widths=column_widths,
            top_decile_rules=top_decile_rules,
        )
    # CSV-only provenance and schema are not covered by the workbook signature.
    # Reuse the workbook independently and export the current CSV inputs each time.
    if csv_output_file is not None and csv_df is not None:
        csv_output_file.parent.mkdir(parents=True, exist_ok=True)
        apply_metric_display_units_to_frame(csv_df).to_csv(
            csv_output_file,
            index=False,
            encoding="utf-8-sig",
        )
    return {
        "content_signature": content_signature,
        "reused_from": reused_from,
    }


def _add_sheet_with_style(
    wb,
    *,
    title: str,
    df: pd.DataFrame,
    index: int | None = None,
    left_align_column: str | None = None,
    highlight_fill_color: str | None = None,
    column_widths: dict[str, int] | None = None,
    top_decile_rules: dict[str, str] | None = None,
) -> None:
    ws = wb.create_sheet(title, index=index) if index is not None else wb.create_sheet(title)
    if df.empty:
        df = pd.DataFrame(columns=df.columns if len(df.columns) else ["说明"])
        if "说明" in df.columns and df.empty:
            df.loc[0] = ["暂无数据"]

    raw_df = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
    text_df = _stringify_frame(df)
    lightweight_style = _should_use_lightweight_sheet_style(text_df)
    header_values = list(text_df.columns)
    header_texts = [str(value or "") for value in header_values]
    decimal_ratio_column_indexes = {
        idx
        for idx, header in enumerate(header_texts)
        if is_decimal_ratio_percent_field(header) or header in DRAWDOWN_OBSERVATION_FIELDS
    }
    ws.append(header_values)
    for row_index, row in enumerate(text_df.itertuples(index=False, name=None)):
        output_row = []
        raw_row = raw_df.iloc[row_index] if row_index < len(raw_df.index) else None
        for column_index, value in enumerate(row):
            if column_index in decimal_ratio_column_indexes and raw_row is not None:
                output_row.append(_excel_decimal_ratio_value(raw_row.iloc[column_index]))
            else:
                output_row.append(value)
        ws.append(output_row)
    max_col = max(1, len(header_values))
    max_row = max(1, len(text_df.index) + 1)

    header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
    header_font = Font(name="微软雅黑", size=11, bold=True, color="FFFFFF")
    border = Border(
        left=Side(style="thin", color="B4B4B4"),
        right=Side(style="thin", color="B4B4B4"),
        top=Side(style="thin", color="B4B4B4"),
        bottom=Side(style="thin", color="B4B4B4"),
    )
    center = Alignment(horizontal="center", vertical="center")
    left = Alignment(horizontal="left", vertical="center")
    highlight_fill = None
    if highlight_fill_color and not lightweight_style:
        highlight_fill = PatternFill(start_color=highlight_fill_color, end_color=highlight_fill_color, fill_type="solid")
    positive_font = Font(name="微软雅黑", size=10, color="FF0000", bold=True)
    negative_font = Font(name="微软雅黑", size=10, color="008000", bold=True)
    normal_font = Font(name="微软雅黑", size=10, color="000000")
    top_decile_fill = PatternFill(start_color="F6DDD7", end_color="F6DDD7", fill_type="solid")

    headers = {header: idx for idx, header in enumerate(header_texts, start=1)}
    left_idx = headers.get(left_align_column) if left_align_column else None
    growth_value_maps: dict[int, dict[int, float]] = {}
    if not lightweight_style:
        growth_cols = [
            idx
            for header, idx in headers.items()
            if "(%)" in str(header)
            and any(keyword in str(header) for keyword in ["增长", "收益", "年", "月", "周", "来", "回撤", "VaR"])
        ]
        for col_idx in growth_cols:
            numeric_series = pd.to_numeric(
                text_df.iloc[:, col_idx - 1]
                .str.replace("%", "", regex=False)
                .str.replace(",", "", regex=False)
                .str.strip(),
                errors="coerce",
            )
            growth_value_maps[col_idx] = {
                row_idx: float(value)
                for row_idx, value in zip(range(2, len(text_df.index) + 2), numeric_series.tolist())
                if pd.notna(value)
            }

    for idx in range(1, max_col + 1):
        cell = ws.cell(row=1, column=idx)
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = center
        cell.border = border

        description = MANAGER_COLUMN_DESCRIPTIONS.get(header_texts[idx - 1])
        if header_texts[idx - 1] in DRAWDOWN_OBSERVATION_FIELDS:
            description = DRAWDOWN_OBSERVATION_DEFINITION
        if description:
            cell.comment = Comment(description, "Fund-ANA")

    if lightweight_style:
        if left_idx:
            for row_idx in range(2, max_row + 1):
                ws.cell(row=row_idx, column=left_idx).alignment = left
    else:
        for row_idx in range(2, max_row + 1):
            for col_idx in range(1, max_col + 1):
                cell = ws.cell(row=row_idx, column=col_idx)
                cell.border = border
                cell.alignment = left if left_idx and col_idx == left_idx else center
                cell.font = normal_font

                if highlight_fill is not None:
                    cell.fill = highlight_fill

                growth_value = growth_value_maps.get(col_idx, {}).get(row_idx)
                if growth_value is not None:
                    if growth_value > 0:
                        cell.font = positive_font
                    elif growth_value < 0:
                        cell.font = negative_font

    for col_idx, header in enumerate(header_texts, start=1):
        number_format = metric_excel_number_format(header)
        if not number_format:
            continue
        for row_idx in range(2, max_row + 1):
            cell = ws.cell(row=row_idx, column=col_idx)
            if isinstance(cell.value, (int, float)) and not isinstance(cell.value, bool):
                cell.number_format = number_format

    if top_decile_rules and not lightweight_style:
        for header, direction in top_decile_rules.items():
            col_idx = headers.get(header)
            if not col_idx or max_row <= 1:
                continue
            numeric_series = pd.to_numeric(
                text_df.iloc[:, col_idx - 1]
                .str.replace("%", "", regex=False)
                .str.replace(",", "", regex=False)
                .str.strip(),
                errors="coerce",
            )
            numeric_values = [
                (row_idx, float(value))
                for row_idx, value in zip(range(2, len(text_df.index) + 2), numeric_series.tolist())
                if pd.notna(value)
            ]
            if not numeric_values:
                continue
            sorted_values = sorted(
                numeric_values,
                key=lambda item: item[1],
                reverse=str(direction).strip().lower() == "desc",
            )
            cutoff_index = max(0, math.ceil(len(sorted_values) * 0.1) - 1)
            threshold = sorted_values[cutoff_index][1]
            for row_idx, value in numeric_values:
                if (
                    str(direction).strip().lower() == "desc" and value >= threshold
                ) or (
                    str(direction).strip().lower() == "asc" and value <= threshold
                ):
                    ws.cell(row=row_idx, column=col_idx).fill = top_decile_fill

    for col_idx in range(1, max_col + 1):
        header = header_texts[col_idx - 1]
        if column_widths and header in column_widths:
            ws.column_dimensions[get_column_letter(col_idx)].width = column_widths[header]
            continue
        if "(%)" in header:
            ws.column_dimensions[get_column_letter(col_idx)].width = 14
            continue
        column_lengths = text_df.iloc[:, col_idx - 1].str.len() if not text_df.empty else pd.Series(dtype=int)
        max_length = max(8, len(header), int(column_lengths.max()) if not column_lengths.empty else 0)
        ws.column_dimensions[get_column_letter(col_idx)].width = min(max_length + 2, 30)

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:{get_column_letter(max_col)}{max_row}"


def _write_multisheet_workbook(
    path: Path,
    *,
    main_sheet_name: str,
    main_df: pd.DataFrame,
    extra_sheets: list[dict[str, Any]] | None = None,
    left_align_column: str | None = None,
    highlight_fill_color: str | None = None,
    column_widths: dict[str, int] | None = None,
    top_decile_rules: dict[str, str] | None = None,
) -> None:
    from openpyxl import Workbook

    wb = Workbook()
    wb.remove(wb.active)
    _add_sheet_with_style(
        wb,
        title=main_sheet_name,
        df=main_df,
        left_align_column=left_align_column,
        highlight_fill_color=highlight_fill_color,
        column_widths=column_widths,
        top_decile_rules=top_decile_rules,
    )
    for sheet in extra_sheets or []:
        _add_sheet_with_style(
            wb,
            title=str(sheet.get("title") or "补充信息"),
            df=sheet.get("df") if isinstance(sheet.get("df"), pd.DataFrame) else pd.DataFrame(),
            left_align_column=sheet.get("left_align_column"),
            highlight_fill_color=sheet.get("highlight_fill_color"),
            column_widths=sheet.get("column_widths"),
            top_decile_rules=sheet.get("top_decile_rules"),
        )
    wb.save(path)


def _replace_sheet_with_style(
    wb,
    *,
    title: str,
    df: pd.DataFrame,
    left_align_column: str | None = None,
    highlight_fill_color: str | None = None,
    column_widths: dict[str, int] | None = None,
    top_decile_rules: dict[str, str] | None = None,
) -> None:
    insert_index = wb.sheetnames.index(title) if title in wb.sheetnames else len(wb.sheetnames)
    if title in wb.sheetnames:
        ws = wb[title]
        wb.remove(ws)
    _add_sheet_with_style(
        wb,
        title=title,
        index=insert_index,
        df=df,
        left_align_column=left_align_column,
        highlight_fill_color=highlight_fill_color,
        column_widths=column_widths,
        top_decile_rules=top_decile_rules,
    )


def _style_workbook(
    path: Path,
    *,
    left_align_column: str | None = None,
    highlight_fill_color: str | None = None,
    column_widths: dict[str, int] | None = None,
) -> None:
    wb = load_workbook(path)
    try:
        ws = wb.active
        header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
        header_font = Font(name="微软雅黑", size=11, bold=True, color="FFFFFF")
        border = Border(
            left=Side(style="thin", color="B4B4B4"),
            right=Side(style="thin", color="B4B4B4"),
            top=Side(style="thin", color="B4B4B4"),
            bottom=Side(style="thin", color="B4B4B4"),
        )
        center = Alignment(horizontal="center", vertical="center")
        left = Alignment(horizontal="left", vertical="center")
        highlight_fill = (
            PatternFill(start_color=highlight_fill_color, end_color=highlight_fill_color, fill_type="solid")
            if highlight_fill_color
            else None
        )
        positive_font = Font(name="微软雅黑", size=10, color="FF0000", bold=True)
        negative_font = Font(name="微软雅黑", size=10, color="008000", bold=True)
        normal_font = Font(name="微软雅黑", size=10, color="000000")

        headers = {ws.cell(row=1, column=idx).value: idx for idx in range(1, ws.max_column + 1)}
        left_idx = headers.get(left_align_column) if left_align_column else None
        growth_cols = [
            idx
            for header, idx in headers.items()
            if "(%)" in str(header)
            and any(keyword in str(header) for keyword in ["增长", "收益", "年", "月", "周", "来", "回撤", "VaR"])
        ]

        for idx in range(1, ws.max_column + 1):
            cell = ws.cell(row=1, column=idx)
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = center
            cell.border = border

        for row_idx in range(2, ws.max_row + 1):
            for col_idx in range(1, ws.max_column + 1):
                cell = ws.cell(row=row_idx, column=col_idx)
                cell.border = border
                cell.alignment = left if left_idx and col_idx == left_idx else center
                cell.font = normal_font

                if highlight_fill is not None:
                    cell.fill = highlight_fill

                if col_idx in growth_cols and cell.value not in (None, ""):
                    try:
                        value = float(cell.value)
                        if value > 0:
                            cell.font = positive_font
                        elif value < 0:
                            cell.font = negative_font
                    except (TypeError, ValueError):
                        cell.font = normal_font

        for col_idx in range(1, ws.max_column + 1):
            header = str(ws.cell(row=1, column=col_idx).value or "")
            if column_widths and header in column_widths:
                ws.column_dimensions[get_column_letter(col_idx)].width = column_widths[header]
                continue
            if "(%)" in header:
                ws.column_dimensions[get_column_letter(col_idx)].width = 14
                continue
            max_length = 8
            for row_idx in range(1, ws.max_row + 1):
                value = ws.cell(row=row_idx, column=col_idx).value
                max_length = max(max_length, len(str(value or "")))
            ws.column_dimensions[get_column_letter(col_idx)].width = min(max_length + 2, 30)

        ws.freeze_panes = "A2"
        wb.save(path)
    finally:
        wb.close()
