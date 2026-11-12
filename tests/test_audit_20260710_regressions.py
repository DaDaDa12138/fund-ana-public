from __future__ import annotations

import csv
import json
from pathlib import Path
from types import SimpleNamespace
import sys

import pandas as pd
from flask import Flask

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.api_routes.recommendations import register_recommendations_routes
from backend.fee_semantics import evaluate_fee_semantics
from backend.project_publish_decisions import append_project_decision_columns
from backend.publication import load_current_published_report_payload
from backend import report_workbook


def test_formal_publish_blocks_unresolved_fee_components() -> None:
    df = pd.DataFrame(
        [
            {
                "fcode": "010653",
                "基金代码": "010653",
                "基金名称": "费率缺失债基",
                "ftype": "债券型-长债",
                "is_buyable": 1,
                "has_holding_period": 0,
                "holding_days": 0,
                "mgr_fee_rate": 0.3,
                "trust_fee_rate": 0.1,
                "sales_fee_rate": None,
                "latest_nav_date": pd.Timestamp.now().date().isoformat(),
                "综合加权得分": 80.0,
                "近期得分": 78.0,
                "长期得分": 82.0,
                "1年-年化收益(%)": 4.0,
                "1年-夏普比率": 1.5,
                "1年-最大回撤(%)": 0.01,
            }
        ]
    )

    row = append_project_decision_columns(df, decision_map={}, soft_warning_map={}).iloc[0]

    assert bool(row["基础门禁通过"]) is False
    assert "fee_data_missing" in row["基础推荐原因代码"]
    assert row["费率口径"] == "sales_fee_missing_unresolved"
    assert pd.isna(row["费率合计(%)"])


def test_fee_lower_bound_is_not_rendered_as_exact_total() -> None:
    result = evaluate_fee_semantics(
        fcode="000001",
        mgr_fee_rate=0.3,
        trust_fee_rate=0.1,
        sales_fee_rate=None,
        validation_map={"000001": "official_placeholder_confirmed"},
    )

    assert result["screening_ready"] is True
    assert result["screening_value"] == 0.4
    assert result["lower_bound"] == 0.4
    assert result["exact_total"] is None
    assert result["uses_floor"] is True


def test_csv_percent_headers_write_percentage_points(tmp_path: Path, monkeypatch) -> None:
    output_file = tmp_path / "formal.xlsx"
    csv_file = tmp_path / "formal.csv"
    df = pd.DataFrame(
        [{"基金代码": "000001", "1年-最大回撤(%)": 0.002697, "1年-波动率": 0.005041}]
    )
    monkeypatch.setattr(report_workbook, "_try_reuse_previous_workbook_artifact", lambda **_: None)
    monkeypatch.setattr(report_workbook, "_write_multisheet_workbook", lambda *_, **__: None)

    report_workbook._materialize_multisheet_workbook(
        settings=SimpleNamespace(),
        output_file=output_file,
        artifact_key="formal",
        artifact_meta_key="formal_meta",
        main_sheet_name="正式推荐",
        main_df=df,
        csv_output_file=csv_file,
        csv_artifact_key="formal_csv",
        csv_df=df,
    )

    with csv_file.open(encoding="utf-8-sig", newline="") as handle:
        row = next(csv.DictReader(handle))
    assert row["1年-最大回撤(%)"] == "0.27"
    assert row["1年-波动率"] == "0.5"


def test_current_published_loader_does_not_fallback_to_latest_canonical(
    tmp_path: Path,
    monkeypatch,
) -> None:
    report_file = tmp_path / "latest.json"
    report_file.write_text(json.dumps({"run_id": "diagnostic_run"}), encoding="utf-8")
    monkeypatch.setattr(
        "backend.publication.load_publication_registry",
        lambda _settings: {
            "current_published_run_id": "",
            "latest_canonical_run_id": "diagnostic_run",
            "canonical_runs": [
                {
                    "run_id": "diagnostic_run",
                    "published": False,
                    "report_path": str(report_file),
                }
            ],
        },
    )

    assert load_current_published_report_payload(SimpleNamespace(project_root=tmp_path)) is None


def test_recommendations_endpoint_does_not_fail_open_to_unpublished_fallback() -> None:
    app = Flask(__name__)
    fallback_calls = {"latest": 0, "snapshot": 0, "manager": 0}

    def _fallback(name: str):
        def _load():
            fallback_calls[name] += 1
            return {"run_id": "unpublished"}

        return _load

    register_recommendations_routes(
        app,
        ensure_scheduler=lambda: None,
        is_test_context=lambda: False,
        load_recommendations_payload_for_test=lambda: {},
        load_current_published_report=lambda: None,
        load_latest_report=_fallback("latest"),
        load_latest_report_snapshot=_fallback("snapshot"),
        load_manager_latest_report=_fallback("manager"),
        build_recommendations_summary_response=lambda payload: payload,
        build_recommendations_detail_response=lambda payload, *_, **__: payload,
        build_recommendations_export_workbook=lambda payload: (payload, "unused.xlsx"),
    )

    response = app.test_client().get("/api/recommendations/latest")

    assert response.status_code == 503
    assert response.get_json()["status"] == "publication_unavailable"
    assert fallback_calls == {"latest": 0, "snapshot": 0, "manager": 0}
