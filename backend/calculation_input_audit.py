"""Read frozen inputs only: replay actual calls and independently check risk arithmetic."""
from __future__ import annotations

from collections import Counter, OrderedDict
from datetime import datetime
import gzip
import hashlib
import importlib
import json
import math
import os
from pathlib import Path
import resource
import statistics
import tempfile
import time
from typing import Any

from backend.calculation_input_capture import validate_calculation_input_index
from core.utils.calculation_capture import (
    CalculationCaptureError, MAX_STREAM_BYTES, MIN_FREE_BYTES, _json, decode, validate_journal,
)

REPLAY_KINDS = {"database_risk.period", "screening_risk.metrics", "screening.return_from_nav"}
STANDARD_KIND = "risk.standard_metrics"
REQUIRED_CALCULATION_SOURCES = {
    "core/analyzers/risk_analyzer.py",
    "core/bond_fund_screening/core/risk_calculator.py",
    "core/bond_fund_screening/core/data_loader.py",
    "core/bond_fund_screening/core/standard_risk_metrics.py",
}


def independent_standard_metrics(returns, nav_values, *, annual_return, periods_per_year, risk_free_rate, mar) -> dict:
    """Scalar arithmetic for the recorded policy, preserving undefined ratios.

    This deliberately does not import a production calculator. Agreement is
    arithmetic reproduction, not validation of the policy's economic merit.
    """
    values, nav = list(map(float, returns)), list(map(float, nav_values))
    empty = {
        "volatility": None, "max_drawdown": None, "max_drawdown_duration": None,
        "raw_sharpe_ratio": None, "sharpe_ratio": None,
        "sharpe_ratio_cap_hit": None, "raw_sortino_ratio": None, "sortino_ratio": None,
        "sortino_ratio_cap_hit": None,
        "undefined_metric_reasons": {"sharpe_ratio": "invalid_observations", "sortino_ratio": "invalid_observations",
                                     "max_drawdown_duration": "invalid_observations"},
    }
    if (len(values) < 2 or len(nav) < 2 or periods_per_year <= 0
            or not all(math.isfinite(value) for value in [*values, *nav]) or min(nav) <= 0):
        return empty
    volatility = statistics.stdev(values) * math.sqrt(periods_per_year)
    peak, drawdown = nav[0], 0.0
    peak_positions = []
    for position, value in enumerate(nav):
        if value >= peak:
            peak_positions.append(position)
        peak = max(peak, value)
        drawdown = max(drawdown, (peak - value) / peak)
    # Independently count gaps between peak observations. The terminal boundary
    # includes an unrecovered spell; an equal peak starts a new gap.
    duration = max(right - left - 1 for left, right in
                   zip(peak_positions, [*peak_positions[1:], len(nav)]))
    sharpe = None if abs(volatility) < 1e-10 else (annual_return - risk_free_rate) / volatility
    downside_variance = math.fsum(min(value - mar / periods_per_year, 0.0) ** 2 for value in values) / len(values)
    downside = math.sqrt(downside_variance) * math.sqrt(periods_per_year)
    sortino = None if downside_variance < 1e-20 or abs(downside) < 1e-10 else (annual_return - mar) / downside
    reasons = {}
    if sharpe is None:
        reasons["sharpe_ratio"] = "zero_or_negligible_volatility"
    if sortino is None:
        reasons["sortino_ratio"] = "zero_or_negligible_downside_deviation"
    return {
        "volatility": volatility, "max_drawdown": drawdown, "max_drawdown_duration": duration,
        "standard_calmar_ratio": None if drawdown == 0.0 else annual_return / drawdown,
        "raw_sharpe_ratio": sharpe,
        "sharpe_ratio": None if sharpe is None else min(5.0, sharpe),
        "sharpe_ratio_cap_hit": None if sharpe is None else sharpe > 5.0,
        "raw_sortino_ratio": sortino, "sortino_ratio": None if sortino is None else max(-10.0, min(10.0, sortino)),
        "sortino_ratio_cap_hit": None if sortino is None else abs(sortino) > 10.0,
        "undefined_metric_reasons": reasons,
    }


def _differences(expected: Any, actual: Any, path="") -> list[str]:
    if expected is None or actual is None:
        return [] if expected is None and actual is None else [path or "/"]
    if isinstance(expected, dict) and isinstance(actual, dict):
        if expected.keys() != actual.keys():
            return [path + "/keys"]
        return [difference for key in expected for difference in _differences(expected[key], actual[key], path + "/" + str(key))]
    if isinstance(expected, (tuple, list)) and isinstance(actual, (tuple, list)):
        if len(expected) != len(actual):
            return [path + "/length"]
        return [difference for index, (left, right) in enumerate(zip(expected, actual))
                for difference in _differences(left, right, path + "/" + str(index))]
    if isinstance(expected, bool) or isinstance(actual, bool):
        return [] if isinstance(expected, bool) and isinstance(actual, bool) and expected == actual else [path]
    if isinstance(expected, (int, float)) and isinstance(actual, (int, float)):
        if math.isnan(expected) or math.isnan(actual):
            return [] if math.isnan(expected) and math.isnan(actual) else [path]
        return [] if math.isclose(expected, actual, rel_tol=1e-9, abs_tol=1e-10) else [path]
    return [] if expected == actual else [path]


class _ObjectTape:
    """Disk offsets plus a two-frame cache avoid keeping every NAV frame in RAM."""
    def __init__(self, directory: Path):
        self.stream = tempfile.TemporaryFile(dir=directory)
        self.offsets = {}
        self.cache = OrderedDict()

    def put(self, row: dict) -> None:
        raw = _json(row["value"])
        self.stream.seek(0, os.SEEK_END)
        position = self.stream.tell()
        fs = os.fstatvfs(self.stream.fileno())
        if position + len(raw) > MAX_STREAM_BYTES or fs.f_bavail * fs.f_frsize < MIN_FREE_BYTES + len(raw):
            raise CalculationCaptureError("capture_audit_scratch_budget_exhausted")
        self.stream.write(raw)
        self.offsets[row["sha256"]] = position, len(raw)

    def get(self, digest: str) -> Any:
        if digest in self.cache:
            self.cache.move_to_end(digest)
            return self.cache[digest]
        position, size = self.offsets[digest]
        self.stream.seek(position)
        raw = self.stream.read(size)
        if len(raw) != size or hashlib.sha256(raw).hexdigest() != digest:
            raise CalculationCaptureError("capture_audit_scratch_corrupted")
        result = json.loads(raw)
        self.cache[digest] = result
        while len(self.cache) > 2:
            self.cache.popitem(last=False)
        return result

    def close(self) -> None:
        self.stream.close()
        self.cache.clear()


def _field(value: dict, name: str):
    for key, item in value.get("items", []):
        if key == name:
            return decode(item)
    return None


def _replay(kind: str, arguments: dict, context: dict, events: dict):
    if kind == "screening_risk.metrics":
        from bond_fund_screening.core.risk_calculator import RiskCalculator
        calculator = RiskCalculator(risk_free_rate=context["risk_free_rate"], mar=context["mar"],
                                    trading_days_per_year=context["trading_days_per_year"])
        return calculator.calculate_all_metrics(**arguments)
    if kind == "screening.return_from_nav":
        from bond_fund_screening.core.data_loader import DataLoader
        return DataLoader.__new__(DataLoader)._calc_return_from_nav(**arguments)
    if kind != "database_risk.period":
        raise CalculationCaptureError("unsupported_frozen_calculation_replay")
    from analyzers.risk_analyzer import RiskAnalyzer
    # Some entry points import the project's compatibility bridge. The clock
    # belongs to the implementation module, not necessarily the import alias.
    risk_analyzer = importlib.import_module(RiskAnalyzer.__module__)
    calculator = RiskAnalyzer(config={"risk_metrics": {"risk_free_rate": context["risk_free_rate"]}}, db_manager=object())
    for key, value in context.items():
        setattr(calculator, key, value)
    original_datetime = risk_analyzer.datetime
    clock = events.get("database_risk.freshness_clock", {}).get("evaluated_at")
    if clock is not None:
        class CapturedClock(datetime):
            @classmethod
            def now(cls, tz=None):
                return clock if tz is None else clock.astimezone(tz)
        risk_analyzer.datetime = CapturedClock
    try:
        return calculator._calculate_period_metrics(**arguments)
    finally:
        risk_analyzer.datetime = original_datetime


def audit_calculation_inputs(index_path: Path, output_dir: Path, *, project_root: Path, log=print) -> dict:
    started = time.monotonic()
    index_path, output_dir, project_root = Path(index_path).absolute(), Path(output_dir).absolute(), Path(project_root).absolute()
    if output_dir.is_relative_to(index_path.parent):
        raise CalculationCaptureError("audit_output_must_not_modify_capture_directory")
    index = validate_calculation_input_index(index_path)
    if (project_root != Path(__file__).resolve().parents[1]
            or not REQUIRED_CALCULATION_SOURCES.issubset(index["code_sha256"])):
        raise CalculationCaptureError("replay_requires_complete_current_calculation_sources")
    for relative, expected in index["code_sha256"].items():
        path = project_root / relative
        if Path(relative).is_absolute() or ".." in Path(relative).parts or path.is_symlink():
            raise CalculationCaptureError("invalid_captured_replay_source_path")
        with path.open("rb") as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() != expected:
                raise CalculationCaptureError(f"frozen_replay_source_changed:{relative}")
    output_dir.mkdir(parents=True, exist_ok=False)
    checks_path = output_dir / "calculation_checks.jsonl.gz"
    counts, by_kind, sessions = Counter(), {}, []
    with checks_path.open("xb") as raw_checks, gzip.GzipFile(fileobj=raw_checks, mode="wb", filename="", mtime=0) as checks:
        for session in index["sessions"]:
            pending, session_counts = {}, Counter()
            tape = _ObjectTape(output_dir)

            def observe(row):
                record = row["record"]
                if record == "object":
                    tape.put(row)
                elif record == "call":
                    parent = pending.get(row.get("parent_id"), {})
                    item = {"kind": row["kind"], "fcode": _field(row["arguments"], "fcode") or _field(row["tags"], "fcode") or parent.get("fcode"),
                            "period": _field(row["arguments"], "period") or parent.get("period"), "events": {}}
                    if row["kind"] in REPLAY_KINDS | {STANDARD_KIND}:
                        item.update(arguments=row["arguments"], context=row["context"])
                    pending[row["call_id"]] = item
                elif record == "event" and row.get("parent_id") in pending and row["kind"] == "database_risk.freshness_clock":
                    pending[row["parent_id"]]["events"][row["kind"]] = decode(row["value"], tape.get)
                elif record in {"return", "exception"}:
                    item = pending.pop(row["call_id"])
                    kind = item["kind"]
                    if kind not in REPLAY_KINDS | {STANDARD_KIND}:
                        return
                    arguments = decode(item["arguments"], tape.get)
                    context = decode(item["context"], tape.get)
                    mode = "independent_arithmetic" if kind == STANDARD_KIND else "frozen_input_replay"
                    actual = error = None
                    try:
                        actual = (independent_standard_metrics(**arguments) if kind == STANDARD_KIND
                                  else _replay(kind, arguments, context, item["events"]))
                    except Exception as exc:
                        error = type(exc).__name__
                    if record == "exception":
                        differences = [] if error == row["type"] else ["/exception_type"]
                    else:
                        expected = decode(row["value"], tape.get)
                        differences = ["/unexpected_exception:" + error] if error else _differences(expected, actual)
                        session_counts["null_results"] += int(expected is None)
                        if isinstance(expected, dict):
                            session_counts["null_metric_values"] += sum(value is None for value in expected.values())
                    passed = not differences
                    counts[mode] += 1
                    counts["mismatches"] += int(not passed)
                    session_counts[mode] += 1
                    session_counts["mismatches"] += int(not passed)
                    kind_counts = by_kind.setdefault(kind, {"checked": 0, "mismatches": 0})
                    kind_counts["checked"] += 1
                    kind_counts["mismatches"] += int(not passed)
                    checks.write(_json({"call_id": row["call_id"], "session_id": session["session_id"],
                        "kind": kind, "mode": mode, "fcode": item["fcode"], "period": item["period"],
                        "scope_period_days": decode(session["context"]).get("period_days"),
                        "recorded_outcome": record, "passed": passed, "differences": differences[:50]}) + b"\n")

            try:
                validation = validate_journal(index_path.parent / session["manifest"], expected_run_id=index["run_id"], on_record=observe)
                if validation["header"]["code_sha256"] != index["code_sha256"]:
                    raise CalculationCaptureError("frozen_replay_session_source_mismatch")
                result = {"session_id": session["session_id"], "kind": session["kind"],
                          "period_days": decode(session["context"]).get("period_days"),
                          "capture_valid": True, "counts": dict(session_counts),
                          "typed_objects": validation["typed_objects_roundtripped"]}
                sessions.append(result)
                log(json.dumps(result, ensure_ascii=False), flush=True)
            finally:
                tape.close()
    result = {"schema_version": 1, "run_id": index["run_id"], "generated_at": datetime.now().astimezone().isoformat(),
              "index_path": str(index_path), "index_sha256": hashlib.sha256(index_path.read_bytes()).hexdigest(),
              "source_verified_for_replay": True, "capture_complete": True, "passed": counts["mismatches"] == 0,
              "counts": dict(counts), "by_kind": by_kind, "sessions": sessions,
              "checks_path": str(checks_path), "checks_sha256": hashlib.sha256(checks_path.read_bytes()).hexdigest(),
              "checks_bytes": checks_path.stat().st_size, "elapsed_seconds": round(time.monotonic() - started, 3),
              "max_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
              "scope": "All recorded persisted-period, screening-risk and NAV-return calls replayed from frozen inputs. Standard risk operands independently recomputed with scalar arithmetic. No current DB/API lookup.",
              "limitations": "Agreement does not validate the annual-return numerator policy, undefined-downside policy, style floors, fund gates or investment effectiveness. Uncalled/reused old rows do not acquire historical provenance."}
    with (output_dir / "summary.json").open("x") as stream:
        json.dump(result, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")
    return result
