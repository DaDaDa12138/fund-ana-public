"""Capture integrity uses synthetic contracts; real-window checks use frozen API data."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta
import base64
import gzip
import hashlib
import importlib
import json
import multiprocessing
import os
from pathlib import Path
import sys
import shutil
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "core"))
from utils import calculation_capture as capture
from backend import calculation_input_capture as binding


def _wire(value):
    return json.loads(json.dumps(capture._encode(value), allow_nan=False))


def _frames():
    values = pd.DataFrame({
        "missing": pd.Series([None, float("nan"), pd.NA, pd.NaT], dtype=object),
        "number": [0.0, -0.0, float("inf"), -float("inf")],
        "count": pd.Series([0, None, 91, 2], dtype="Int64"),
        "flag": pd.Series([True, False, None, True], dtype="boolean"),
        "date": pd.to_datetime(["2026-09-01", None, "2026-09-03", "2026-09-04"], utc=True),
        "duration": pd.to_timedelta([1, 2, None, 5], unit="ns"),
        "period": pd.period_range("2026-01", periods=4, freq="M"),
        "label": pd.Series(["a", None, "b", "a"], dtype=pd.CategoricalDtype(["b", "a", "unused"], ordered=True)),
        "text": pd.Series(["000001", None, "", "真实零"], dtype="string"),
    })
    values.index = pd.Index([7, 2, 9, 0], name="original_row")
    empty = pd.DataFrame({"x": pd.Series(dtype="Float64"), "s": pd.Series(dtype="string")})
    calendar = pd.DataFrame({"x": [0.0, np.nan, 1.0]}, index=pd.date_range("2026-01-01", periods=3, freq="D", tz="Asia/Shanghai"))
    category = pd.DataFrame({"x": [1, 0]}, index=pd.CategoricalIndex(["b", "a"], categories=["unused", "a", "b"], ordered=True))
    arrow = pd.DataFrame({"s": pd.Series(["x", None, ""], dtype="string[pyarrow]")})
    return [values, empty, calendar, category, arrow]


@pytest.mark.parametrize("frame", _frames())
def test_typed_frames_preserve_missing_zero_schema_and_original_index(frame):
    wire = _wire(frame)
    restored = capture.decode(wire)
    pd.testing.assert_frame_equal(restored, frame, check_exact=True)
    assert _wire(restored) == wire
    if "missing" in frame:
        assert restored.iloc[0]["missing"] is None
        assert np.isnan(restored.iloc[1]["missing"])
        assert restored.iloc[2]["missing"] is pd.NA
        assert restored.iloc[3]["missing"] is pd.NaT
        assert np.signbit(restored.iloc[1]["number"])


@pytest.fixture
def capture_environment(tmp_path, monkeypatch):
    directory = tmp_path / "captures"
    directory.mkdir()
    monkeypatch.setenv(capture.ENV_DIRECTORY, str(directory))
    monkeypatch.setenv("FUND_ANA_WORKFLOW_RUN_ID", "synthetic_capture_run")
    monkeypatch.setattr(capture, "_ACTIVE", None)
    writer_type = capture.CaptureWriter

    def writer_factory(directory, kind, context):
        return writer_type(directory, kind, context, code_paths=[ROOT / "core/utils/calculation_capture.py"])

    monkeypatch.setattr(capture, "CaptureWriter", writer_factory)
    return directory, writer_type


def _validated(directory):
    manifests = list(directory.glob("*.manifest.json"))
    assert len(manifests) == 1
    checked = capture.validate_journal(manifests[0], expected_run_id="synthetic_capture_run")
    rows = list(capture.iter_journal(directory / checked["manifest"]["file"]))
    return checked, rows


def test_capture_deduplicates_actual_frame_and_retains_thread_identity(capture_environment):
    directory, _ = capture_environment
    frame = pd.DataFrame({"nav": [1.0, 1.1, None], "growth": [None, 10.0, 0.0]})

    @capture.capture_call("metrics")
    def metric(nav):
        return {"zero": 0.0, "missing": None, "nav": nav}

    @capture.capture_call("fund")
    def fund(code):
        with capture.capture_context(fcode=code):
            return metric(frame.copy(deep=True))

    @capture.capture_session("synthetic")
    def run():
        with ThreadPoolExecutor(max_workers=3) as pool:
            return list(pool.map(fund, ["test_a", "test_b", "test_c"]))

    assert len(run()) == 3
    checked, rows = _validated(directory)
    assert checked["manifest"]["object_count"] == 1
    funds = {row["call_id"] for row in rows if row["record"] == "call" and row["kind"] == "fund"}
    calls = [row for row in rows if row["record"] == "call" and row["kind"] == "metrics"]
    assert {capture.decode(row["tags"])["fcode"] for row in calls} == {"test_a", "test_b", "test_c"}
    assert all(row["parent_id"] in funds for row in calls)
    assert checked["manifest"]["calls"] == {"fund": 3, "metrics": 3}


def test_input_is_frozen_before_inplace_changes(capture_environment):
    directory, _ = capture_environment

    @capture.capture_session("synthetic")
    @capture.capture_call("mutating_calculation")
    def run(frame):
        frame.loc[0, "x"] = None
        return frame

    frame = pd.DataFrame({"x": [0.0, 1.0]})
    run(frame)
    _, rows = _validated(directory)
    objects = {row["sha256"]: row["value"] for row in rows if row["record"] == "object"}
    call = next(row for row in rows if row["record"] == "call")
    returned = next(row for row in rows if row["record"] == "return")
    original = capture.decode(call["arguments"], objects.__getitem__)["frame"]
    output = capture.decode(returned["value"], objects.__getitem__)
    assert original.loc[0, "x"] == 0.0 and pd.isna(output.loc[0, "x"])
    assert len(objects) == 2


def test_caught_business_exception_is_recorded_without_faking_success(capture_environment):
    directory, _ = capture_environment

    @capture.capture_call("failed_business_call")
    def failed():
        raise ValueError("synthetic failure")

    @capture.capture_session("synthetic")
    def run():
        try:
            failed()
        except ValueError:
            return None

    assert run() is None
    checked, _ = _validated(directory)
    assert checked["manifest"]["exceptions"] == {"failed_business_call": 1}
    assert checked["manifest"]["operation_outcome"] == "returned"


def test_caught_capture_failure_still_prevents_complete_manifest(capture_environment):
    directory, _ = capture_environment

    @capture.capture_call("unsupported")
    def bad(value):
        return value

    @capture.capture_session("synthetic")
    def run():
        try:
            bad(object())
        except capture.CalculationCaptureError:
            pass

    with pytest.raises(capture.CalculationCaptureError, match="unsupported_capture_value"):
        run()
    assert not list(directory.glob("*.manifest.json"))
    assert list(directory.glob("*.partial"))
    assert capture._ACTIVE is None


@pytest.mark.parametrize("limit,value", [("MIN_FREE_BYTES", 10**30), ("MAX_RECORDS", 1), ("MAX_COMPRESSED_BYTES", 1)])
def test_budget_failures_keep_partial_and_close_handles(capture_environment, monkeypatch, limit, value):
    directory, writer_type = capture_environment
    monkeypatch.setattr(capture, limit, value)
    writer = writer_type.__new__(writer_type)
    with pytest.raises(capture.CalculationCaptureError, match="budget_exhausted"):
        writer_type.__init__(writer, directory, "synthetic", {}, code_paths=[ROOT / "core/utils/calculation_capture.py"])
        writer.finish("returned")
    assert writer.raw.closed
    assert not list(directory.glob("*.manifest.json"))


def test_total_write_budget_stops_later_journal_before_directory_exceeds_limit(capture_environment, monkeypatch):
    directory, writer_type = capture_environment
    first = writer_type(directory, "synthetic", {}, code_paths=[ROOT / "core/utils/calculation_capture.py"])
    first.finish("returned")
    manifest = next(directory.glob("*.manifest.json"))
    original = manifest.read_bytes()
    before = sum(path.stat().st_size for path in directory.iterdir())
    limit = before + before // 2
    monkeypatch.setattr(capture, "MAX_DIRECTORY_BYTES", limit)
    second = writer_type.__new__(writer_type)
    with pytest.raises(capture.CalculationCaptureError, match="directory_budget_exhausted"):
        writer_type.__init__(second, directory, "synthetic", {}, code_paths=[ROOT / "core/utils/calculation_capture.py"])
        second.finish("returned")
    assert second.raw.closed
    assert sum(path.stat().st_size for path in directory.iterdir()) <= limit
    assert manifest.read_bytes() == original and len(list(directory.glob("*.manifest.json"))) == 1


def test_concurrent_processes_share_one_compressed_write_budget(tmp_path, monkeypatch):
    directory = tmp_path / "captures"
    directory.mkdir()
    source = tmp_path / "source.py"
    source.write_text("# synthetic concurrent storage test\n")
    monkeypatch.setattr(capture, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(capture, "MAX_DIRECTORY_BYTES", 80 * 1024)
    monkeypatch.setenv("FUND_ANA_WORKFLOW_RUN_ID", "synthetic_concurrent_budget")
    context = multiprocessing.get_context("fork")
    start, output = context.Event(), context.Queue()

    def write():
        writer = capture.CaptureWriter(directory, "synthetic", {}, code_paths=[source])
        start.wait(5)
        try:
            writer.event("random_bytes", os.urandom(60 * 1024))
            writer.finish("returned")
            output.put("complete")
        except capture.CalculationCaptureError:
            if not writer.closed:
                try:
                    writer._close()
                except capture.CalculationCaptureError:
                    pass
            output.put("budget_exhausted")

    workers = [context.Process(target=write) for _ in range(2)]
    try:
        for worker in workers:
            worker.start()
        start.set()
        for worker in workers:
            worker.join(15)
            assert worker.exitcode == 0
        outcomes = [output.get(timeout=2) for _ in workers]
        assert "budget_exhausted" in outcomes
        assert sum(path.stat().st_size for path in directory.iterdir()) <= 80 * 1024
        assert len(list(directory.glob("*.manifest.json"))) == outcomes.count("complete")
    finally:
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
                worker.join(5)
        output.close()


def test_sources_changed_during_calculation_prevent_a_complete_capture(capture_environment, monkeypatch, tmp_path):
    directory, writer_type = capture_environment
    source = tmp_path / "source.py"
    source.write_text("# original\n")
    monkeypatch.setattr(capture, "PROJECT_ROOT", tmp_path)
    writer = writer_type(directory, "synthetic", {}, code_paths=[source])
    source.write_text("# changed\n")
    with pytest.raises(capture.CalculationCaptureError, match="source_changed"):
        writer.finish("returned")
    assert writer.raw.closed and not list(directory.glob("*.manifest.json"))


def test_runtime_directories_are_excluded_from_source_capture(capture_environment, tmp_path, monkeypatch):
    directory, writer_type = capture_environment
    monkeypatch.setattr(capture, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(capture, "__file__", str(tmp_path / "core/utils/calculation_capture.py"))
    for name in ("core/main.py", "core/utils/calculation_capture.py", "backend/module.py", "backend/data/private.py"):
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# source fixture\n")
    writer = writer_type(directory, "synthetic", {})
    assert "backend/data/private.py" not in writer.sources
    assert "backend/module.py" in writer.sources
    writer.finish("returned")


@pytest.mark.parametrize("raw,reason", [(b'{"record":1,"record":2}\n', "duplicate_capture_json_key"),
                                        (b'{"record":NaN}\n', "untyped_nonfinite")])
def test_reader_rejects_ambiguous_json(tmp_path, raw, reason):
    path = tmp_path / "invalid.jsonl.gz"
    path.write_bytes(gzip.compress(raw, mtime=0))
    with pytest.raises(capture.CalculationCaptureError, match=reason):
        list(capture.iter_journal(path))


@pytest.mark.parametrize("mutation", ["source", "reference", "missing_return", "duplicate_terminal", "wrong_run"])
def test_full_reader_rejects_semantic_corruption_after_physical_rehash(capture_environment, mutation):
    directory, _ = capture_environment

    @capture.capture_session("synthetic")
    @capture.capture_call("calculate")
    def run(frame):
        return frame

    run(pd.DataFrame({"x": [0.0, None]}))
    checked, rows = _validated(directory)
    if mutation == "source":
        next(row for row in rows if row["record"] == "source")["bytes_base64"] = base64.b64encode(b"# changed").decode()
    elif mutation == "reference":
        next(row for row in rows if row["record"] == "return")["value"]["sha256"] = "0" * 64
    elif mutation == "missing_return":
        rows = [row for row in rows if row["record"] != "return"]
    elif mutation == "duplicate_terminal":
        rows.append(rows[-1])
    else:
        rows[0]["run_id"] = "different_run"
    raw = b"".join(capture._json(row) + b"\n" for row in rows)
    path = directory / checked["manifest"]["file"]
    path.write_bytes(gzip.compress(raw, mtime=0))
    manifest = checked["manifest"]
    manifest.update(bytes=path.stat().st_size, sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                    uncompressed_bytes=len(raw), records=len(rows))
    manifest_path = directory / (manifest["session_id"] + ".manifest.json")
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(capture.CalculationCaptureError):
        capture.validate_journal(manifest_path, expected_run_id="synthetic_capture_run")


@pytest.fixture
def workflow_capture(tmp_path, monkeypatch):
    root = tmp_path / "project"
    run_id = "synthetic_workflow"
    settings = SimpleNamespace(project_root=root, reports_dir=root / "reports")
    directory = binding.prepare_capture_directory(settings, run_id)
    log_dir = root / "reports/runs" / run_id / "logs/subprocess"
    log_dir.mkdir(parents=True)
    modules = [*binding.PERIOD_MODULES.values(), "bond_fund_screening.tasks.daily_final_screening"]
    sources = [root / "core/main.py", *(root / "core" / (module.replace(".", "/") + ".py") for module in modules)]
    for path in sources:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# synthetic subprocess contract, no business data\n")
    monkeypatch.setattr(capture, "PROJECT_ROOT", root)
    monkeypatch.setenv("FUND_ANA_WORKFLOW_RUN_ID", run_id)
    items = []

    def add(task, *, days=None, cwd_at_root=False):
        if task == "calculate-risk":
            kind, source, command = "database_risk", sources[0], [sys.executable, "main.py", "calculate-risk", "--yes", "--force"]
            argv = command[1:]
        else:
            kind = "final_screening" if task == "任务3: 最终筛选" else "screening_period"
            module = "bond_fund_screening.tasks.daily_final_screening" if kind == "final_screening" else binding.PERIOD_MODULES[task]
            source = root / "core" / (module.replace(".", "/") + ".py")
            command = [sys.executable, "-m", module]
            argv = [str(source)]
        cwd = root if cwd_at_root else root / "core"
        monkeypatch.chdir(cwd)
        monkeypatch.setattr(sys, "argv", argv)
        started = (datetime.now() - timedelta(seconds=1)).isoformat(timespec="seconds")
        writer = capture.CaptureWriter(directory, kind, {"period_days": days} if days else {}, code_paths=sources)
        writer.finish("returned")
        meta = {"task_name": task, "command": command, "cwd": str(cwd), "return_code": 0,
                "success": True, "timed_out": False, "started_at": started,
                "finished_at": (datetime.now() + timedelta(seconds=1)).isoformat(timespec="seconds")}
        meta_path = log_dir / f"child_{len(items)}.meta.json"
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2) + "\n")
        items.append({**meta, "meta_path_relative": str(meta_path.relative_to(root))})

    def index():
        path = log_dir / "subprocess_index.json"
        result = {"run_id": run_id, "log_dir_relative": str(log_dir.relative_to(root)),
                  "index_path_relative": str(path.relative_to(root)), "items": items}
        path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
        return result

    return SimpleNamespace(root=root, run_id=run_id, settings=settings, directory=directory, add=add, index=index)


def test_binding_handles_module_import_from_core_with_a_different_cwd_and_later_audit_append(workflow_capture):
    fixture = workflow_capture
    fixture.add("calculate-risk")
    for task, days in binding.PERIOD_TASKS.items():
        fixture.add(task, days=days, cwd_at_root=True)
    fixture.add("任务3: 最终筛选", cwd_at_root=True)
    original = fixture.index()
    path = binding.build_calculation_input_index(fixture.settings, fixture.directory, run_id=fixture.run_id,
        planned_tasks=["calculate-risk", "daily-screening-master"], subprocess_index=original)
    validated = binding.validate_calculation_input_index(path, full_journals=True)
    assert validated["period_days"] == [7, 30, 90, 180, 365, 730, 1095, 1460, 1825]
    assert len(validated["sessions"]) == 11
    (fixture.root / original["index_path_relative"]).write_text(json.dumps({**original, "items": [*original["items"], {"task_name": "later_report_audit"}]}))
    reference = binding.capture_index_reference(fixture.settings, str(path))
    assert reference["capture_complete"] and reference["scientific_reproduction_status"] == "not_verified"
    assert binding.validate_calculation_input_index(path, full_journals=True) == validated


def test_shared_pack_index_binds_all_children_and_survives_offline_archival(workflow_capture, monkeypatch):
    fixture = workflow_capture
    pool = fixture.root / "reports/.calculation_input_pool"
    monkeypatch.setenv("FUND_ANA_CALCULATION_SHARED_DIR", str(pool))
    fixture.add("calculate-risk")
    for task, days in binding.PERIOD_TASKS.items():
        fixture.add(task, days=days)
    fixture.add("任务3: 最终筛选")
    path = binding.build_calculation_input_index(fixture.settings, fixture.directory, run_id=fixture.run_id,
        planned_tasks=["calculate-risk", "daily-screening-master"], subprocess_index=fixture.index())
    expected = binding.validate_calculation_input_index(path, full_journals=True)
    assert all(session["schema_version"] == 3 for session in expected["sessions"])
    assert len(list(path.parent.glob("*.pack"))) == 1
    archived = fixture.root / "reports/archive/superseded_runs" / fixture.run_id
    archived.parent.mkdir(parents=True)
    fixture.directory.parent.rename(archived)
    shutil.rmtree(pool)
    actual = binding.capture_index_reference(fixture.settings, str(path))
    restored_path = fixture.root / actual["path"]
    assert binding.validate_calculation_input_index(restored_path, full_journals=True) == expected
    extra = restored_path.parent / ("0" * 32 + ".pack")
    extra.write_bytes(b"unbound synthetic extra member")
    with pytest.raises(binding.CalculationCaptureError, match="incomplete_calculation_capture_members"):
        binding.validate_calculation_input_index(restored_path)


@pytest.mark.parametrize("failure", ["missing_period", "wrong_period", "partial", "failed_child", "wrong_run", "wrong_cwd"])
def test_binding_rejects_incomplete_or_wrong_actual_children(workflow_capture, failure):
    fixture = workflow_capture
    if failure in {"missing_period", "wrong_period"}:
        for task, days in binding.PERIOD_TASKS.items():
            if failure == "missing_period" and task == "short-1week":
                continue
            fixture.add(task, days=8 if failure == "wrong_period" and task == "short-1week" else days)
        fixture.add("任务3: 最终筛选")
        tasks = ["daily-screening-master"]
    else:
        fixture.add("calculate-risk")
        tasks = ["calculate-risk"]
    original = fixture.index()
    if failure == "partial":
        (fixture.directory / "interrupted.jsonl.gz.partial").write_bytes(b"unfinished")
    elif failure == "failed_child":
        meta_path = fixture.root / original["items"][0]["meta_path_relative"]
        data = json.loads(meta_path.read_text())
        data.update(success=False, return_code=2)
        meta_path.write_text(json.dumps(data))
        original["items"][0].update(success=False, return_code=2)
        (fixture.root / original["index_path_relative"]).write_text(json.dumps(original))
    elif failure == "wrong_run":
        original["run_id"] = "other"
    elif failure == "wrong_cwd":
        meta_path = fixture.root / original["items"][0]["meta_path_relative"]
        data = json.loads(meta_path.read_text())
        data["cwd"] = str(fixture.root)
        meta_path.write_text(json.dumps(data))
    with pytest.raises(binding.CalculationCaptureError):
        binding.build_calculation_input_index(fixture.settings, fixture.directory, run_id=fixture.run_id,
            planned_tasks=tasks, subprocess_index=original)
    assert not list(fixture.directory.glob("calculation_input_index_*.json"))


def test_captured_real_window_preserves_both_calculators_outputs(capture_environment):
    from analyzers.risk_analyzer import RiskAnalyzer
    from bond_fund_screening.core.risk_calculator import RiskCalculator

    directory, _ = capture_environment
    fixture = json.loads(gzip.decompress((ROOT / "tests/fixtures/risk_window_first_growth_missing_20260912.json.gz").read_bytes()))
    snapshot = fixture["snapshots"][0]
    case, params = next(c for c in snapshot["cases"] if c["fcode"] == "025229"), snapshot["parameters"]
    frame = pd.DataFrame({"daily_growth": case["daily_growth"]}, index=pd.to_datetime(case["dates"]))
    persisted = RiskAnalyzer(config={"risk_metrics": {"risk_free_rate": params["risk_free_rate"],
        "mar": params["mar"], "trading_days_per_year": params["trading_days"]}}, db_manager=object())
    screening = RiskCalculator(risk_free_rate=params["risk_free_rate"], mar=params["mar"], trading_days_per_year=params["trading_days"])

    def calculate():
        return (persisted._calculate_period_metrics(frame, "1y", api_return=case["api_return"],
                 api_return_date=case["api_return_date"], api_snapshot_date=case["dates"][-1]),
                screening.calculate_all_metrics(frame.reset_index().rename(columns={"index": "nav_date"})))

    before = calculate()
    after = capture.capture_session("real_window")(calculate)()
    assert before == after
    checked, _ = _validated(directory)
    assert checked["manifest"]["calls"]["database_risk.period"] == 1
    assert checked["manifest"]["calls"]["screening_risk.metrics"] == 1
    assert checked["manifest"]["calls"]["risk.standard_metrics"] == 2


def test_independent_arithmetic_checks_drawdown_and_undefined_ratios():
    from backend.calculation_input_audit import independent_standard_metrics

    result = independent_standard_metrics([0.1, -0.1], [1.0, 1.1, 0.99],
        annual_return=0.05, periods_per_year=252, risk_free_rate=0.02, mar=0.0)
    assert result["max_drawdown"] == pytest.approx(0.1)
    assert result["max_drawdown_duration"] == 1
    assert result["volatility"] == pytest.approx((0.02 * 252) ** 0.5)
    assert result["raw_sharpe_ratio"] == pytest.approx(0.03 / result["volatility"])
    zero = independent_standard_metrics([0.0, 0.0], [1.0, 1.0, 1.0],
        annual_return=0.0, periods_per_year=252, risk_free_rate=0.02, mar=0.0)
    assert zero["max_drawdown"] == zero["volatility"] == 0.0
    assert zero["max_drawdown_duration"] == 0
    assert zero["raw_sharpe_ratio"] is zero["raw_sortino_ratio"] is None
    assert zero["sharpe_ratio_cap_hit"] is zero["sortino_ratio_cap_hit"] is None
    all_positive = independent_standard_metrics([0.01, 0.02], [1.0, 1.01, 1.0302],
        annual_return=0.04, periods_per_year=252, risk_free_rate=0.02, mar=0.0)
    assert all_positive["raw_sortino_ratio"] is None
    assert all_positive["undefined_metric_reasons"]["sortino_ratio"] == "zero_or_negligible_downside_deviation"


@pytest.mark.parametrize("nav,expected", [
    ([1, 0.9, 0.8, 1, 0.7], 2),
    ([1, 0.9, 1, 0.9, 0.8, 0.7], 3),
    ([1, 0.9, 1, 0.9, 1], 1),
    ([1, 1, 1], 0), ([1, 2, 3], 0),
    ([], None), ([1], None), ([1, 0, 1], None),
    ([1, float("nan"), 1], None), ([1, float("inf"), 1], None),
])
def test_independent_duration_handles_peak_resets_terminal_spells_and_invalid_input(nav, expected):
    from backend.calculation_input_audit import independent_standard_metrics

    result = independent_standard_metrics([0.1, -0.1], nav,
        annual_return=0.05, periods_per_year=252, risk_free_rate=0.02, mar=0.0)
    assert result["max_drawdown_duration"] == expected
    if expected is None:
        assert result["undefined_metric_reasons"]["max_drawdown_duration"] == "invalid_observations"
    else:
        assert "max_drawdown_duration" not in result["undefined_metric_reasons"]


def test_replay_uses_captured_freshness_clock_and_never_queries_database(monkeypatch):
    import sqlite3
    from analyzers.risk_analyzer import RiskAnalyzer
    risk_analyzer = importlib.import_module(RiskAnalyzer.__module__)
    from backend.calculation_input_audit import _replay, _differences

    fixture = json.loads(gzip.decompress((ROOT / "tests/fixtures/risk_window_first_growth_missing_20260912.json.gz").read_bytes()))
    snapshot = fixture["snapshots"][0]
    case, params = next(c for c in snapshot["cases"] if c["fcode"] == "025229"), snapshot["parameters"]
    frame = pd.DataFrame({"daily_growth": case["daily_growth"]}, index=pd.to_datetime(case["dates"]))
    analyzer = risk_analyzer.RiskAnalyzer(config={"risk_metrics": {"risk_free_rate": params["risk_free_rate"],
        "mar": params["mar"], "trading_days_per_year": params["trading_days"]}}, db_manager=object())
    args = {"nav_data": frame, "period": "1y", "api_return": case["api_return"],
            "api_return_date": case["api_return_date"], "api_snapshot_date": case["dates"][-1]}
    expected = analyzer._calculate_period_metrics(**args)
    context = {name: getattr(analyzer, name) for name in risk_analyzer._RISK_CAPTURE_FIELDS if hasattr(analyzer, name)}
    context["max_metric_nav_age_days"] = 0
    captured_time = datetime.fromisoformat(case["dates"][-1][:10] + "T21:00:00")
    original = risk_analyzer.datetime

    def forbidden(*args, **kwargs):
        raise AssertionError("database access is forbidden during frozen replay")

    monkeypatch.setattr(sqlite3, "connect", forbidden)
    actual = _replay("database_risk.period", args, context, {"database_risk.freshness_clock": {"evaluated_at": captured_time}})
    assert actual is not None and _differences(expected, actual) == []
    assert risk_analyzer.datetime is original


def test_capture_binding_survives_whole_run_archival(workflow_capture):
    fixture = workflow_capture
    fixture.add("calculate-risk")
    path = binding.build_calculation_input_index(fixture.settings, fixture.directory, run_id=fixture.run_id,
        planned_tasks=["calculate-risk"], subprocess_index=fixture.index())
    run = fixture.directory.parent
    archive = fixture.settings.reports_dir / "archive/superseded_runs" / fixture.run_id
    archive.parent.mkdir(parents=True)
    shutil.move(run, archive)
    result = binding.capture_index_reference(fixture.settings, str(path))
    assert result["source_workflow_run_id"] == fixture.run_id
    assert "/archive/superseded_runs/" in result["path"]


def test_offline_capture_validation_does_not_require_operational_write_lock(workflow_capture):
    fixture = workflow_capture
    fixture.add("calculate-risk")
    path = binding.build_calculation_input_index(fixture.settings, fixture.directory, run_id=fixture.run_id,
        planned_tasks=["calculate-risk"], subprocess_index=fixture.index())
    before = binding.validate_calculation_input_index(path, full_journals=True)
    (fixture.directory / capture.WRITE_LOCK_NAME).unlink()
    assert binding.validate_calculation_input_index(path, full_journals=True) == before


def test_report_rebuild_keeps_original_capture_run_identity(workflow_capture, monkeypatch):
    from backend.config import load_settings
    from backend import screening_replay_snapshot as snapshot

    fixture = workflow_capture
    fixture.add("calculate-risk")
    path = binding.build_calculation_input_index(fixture.settings, fixture.directory, run_id=fixture.run_id,
        planned_tasks=["calculate-risk"], subprocess_index=fixture.index())
    settings = replace(load_settings(), project_root=fixture.root, reports_dir=fixture.settings.reports_dir,
                       runs_dir=fixture.settings.reports_dir / "runs")
    report_dir = fixture.root / "reports/runs/synthetic_rebuild/revisions/one"
    report_dir.mkdir(parents=True)
    frame = pd.DataFrame({"synthetic_value": [0.0, None]})
    filters = {key: frame.copy() for key in ("candidate_df", "dynamic_publish_official_df", "final_selection_tracker_features_df",
               "final_selection_contraction_df", "dynamic_official_df")}
    filters.update(style_thresholds={}, global_thresholds={}, style_meta={}, effective_nav_candidate_date="2026-09-11")
    summary = {"run_id": "synthetic_rebuild", "generated_at": "2026-09-13T01:00:00",
               "artifacts": {"report_json": str((report_dir / "report.json").relative_to(fixture.root)),
                             "core": {"calculation_input_index_file": str(path)}}}
    monkeypatch.setattr(snapshot, "source_code_hashes", lambda root: {"synthetic": "fixture"})
    stored = snapshot.write_screening_input_snapshot(settings, summary, universe=frame, filters=filters,
        materialized={"official_df": frame}, report_inputs={"decision_map": {}, "soft_warning_map": {}})
    loaded = snapshot.load_screening_input_snapshot(stored)
    reference = loaded["inputs"]["calculation_input_capture"]
    assert loaded["run_id"] == "synthetic_rebuild"
    assert reference["source_workflow_run_id"] == fixture.run_id
    assert reference["scientific_reproduction_status"] == "not_verified"


def test_complete_journal_audit_replays_frozen_fixture_without_database(tmp_path, monkeypatch):
    """Exercise writer, child binding, object tape and replay as one isolated contract.

    The NAV fixture is real frozen data. Child metadata represents this test;
    it is not evidence of an actual production workflow execution.
    """
    import sqlite3
    from analyzers.risk_analyzer import RiskAnalyzer
    from bond_fund_screening.core.data_loader import DataLoader
    from bond_fund_screening.core.risk_calculator import RiskCalculator
    from backend.calculation_input_audit import audit_calculation_inputs

    fixture = json.loads(gzip.decompress((ROOT / "tests/fixtures/risk_window_first_growth_missing_20260912.json.gz").read_bytes()))
    snapshot = fixture["snapshots"][0]
    case, params = next(c for c in snapshot["cases"] if c["fcode"] == "025229"), snapshot["parameters"]
    frame = pd.DataFrame({"daily_growth": case["daily_growth"]}, index=pd.to_datetime(case["dates"]))
    nav = frame.reset_index().rename(columns={"index": "nav_date"})
    persisted = RiskAnalyzer(config={"risk_metrics": {"risk_free_rate": params["risk_free_rate"],
        "mar": params["mar"], "trading_days_per_year": params["trading_days"]}}, db_manager=object())
    screening = RiskCalculator(risk_free_rate=params["risk_free_rate"], mar=params["mar"],
                               trading_days_per_year=params["trading_days"])
    run_id = "isolated_frozen_fixture_audit"
    settings = SimpleNamespace(project_root=ROOT, reports_dir=tmp_path / "reports")
    directory = binding.prepare_capture_directory(settings, run_id)
    log_dir = directory.parent / "logs/subprocess"
    log_dir.mkdir(parents=True)
    monkeypatch.chdir(ROOT / "core")
    monkeypatch.setattr(sys, "argv", ["main.py", "calculate-risk", "--yes", "--force"])
    monkeypatch.setenv(capture.ENV_DIRECTORY, str(directory))
    monkeypatch.setenv("FUND_ANA_WORKFLOW_RUN_ID", run_id)
    monkeypatch.setattr(capture, "_ACTIVE", None)
    started = datetime.now().astimezone().isoformat()

    @capture.capture_session("database_risk")
    def calculate():
        persisted._calculate_period_metrics(frame, "1y", api_return=case["api_return"],
            api_return_date=case["api_return_date"], api_snapshot_date=case["dates"][-1])
        assert persisted._calculate_period_metrics(frame, "5y") is None
        screening.calculate_all_metrics(nav)
        loader = DataLoader.__new__(DataLoader)
        loader._calc_return_from_nav(nav, days=365)
        assert loader._calc_return_from_nav(nav.iloc[:1], days=365) is None

    calculate()
    meta = {"task_name": "calculate-risk", "command": [sys.executable, *sys.argv],
            "cwd": str(ROOT / "core"), "return_code": 0, "success": True, "timed_out": False,
            "started_at": started, "finished_at": datetime.now().astimezone().isoformat()}
    meta_path = log_dir / "isolated_contract.meta.json"
    meta_path.write_text(json.dumps(meta))
    index_path = log_dir / "subprocess_index.json"
    subprocess_index = {"run_id": run_id, "log_dir_relative": str(log_dir.relative_to(ROOT)),
        "index_path_relative": str(index_path.relative_to(ROOT)),
        "items": [{**meta, "meta_path_relative": str(meta_path.relative_to(ROOT))}]}
    index_path.write_text(json.dumps(subprocess_index))
    index = binding.build_calculation_input_index(settings, directory, run_id=run_id,
        planned_tasks=["calculate-risk"], subprocess_index=subprocess_index)

    def forbidden(*args, **kwargs):
        raise AssertionError("database access is forbidden during frozen journal audit")

    monkeypatch.setattr(sqlite3, "connect", forbidden)
    result = audit_calculation_inputs(index, tmp_path / "audit", project_root=ROOT)
    assert result["passed"] and result["counts"] == {
        "independent_arithmetic": 2, "mismatches": 0, "frozen_input_replay": 5,
    }
    assert result["by_kind"]["screening.return_from_nav"]["checked"] == 2
    assert result["sessions"][0]["counts"]["null_results"] == 2
    rows = [json.loads(line) for line in gzip.decompress(Path(result["checks_path"]).read_bytes()).splitlines()]
    assert len(rows) == 7 and all(row["passed"] for row in rows)
