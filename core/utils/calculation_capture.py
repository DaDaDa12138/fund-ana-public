"""Freeze actual calculation inputs and outcomes in bounded, typed journals.

An object is written once per process journal and referenced by its content SHA.
Closing a journal proves capture completeness, not scientific correctness or a
successful business outcome. No database or API is consulted by the reader.
"""
from __future__ import annotations

import base64
from collections import Counter
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from functools import wraps
import fcntl
import gzip
import hashlib
import inspect
import json
import math
import os
from pathlib import Path
import re
import stat
import sys
import threading
import time
from typing import Any, Iterator
from uuid import uuid4

import numpy as np
import pandas as pd

SCHEMA_VERSION = 1
ENV_DIRECTORY = "FUND_ANA_CALCULATION_INPUT_DIR"
ENV_CAPTURE_RUN_ID = "FUND_ANA_CALCULATION_RUN_ID"
ENV_CAPTURE_SCOPE = "FUND_ANA_CALCULATION_SCOPE"
MAX_LINE_BYTES = 128 * 1024**2
MAX_STREAM_BYTES = 8 * 1024**3
MAX_COMPRESSED_BYTES = 1024**3
MAX_DIRECTORY_BYTES = 2 * 1024**3
WRITE_LOCK_NAME = ".capture_write.lock"
MAX_RECORDS = 1_000_000
MIN_FREE_BYTES = 512 * 1024**2
PROJECT_ROOT = Path(__file__).resolve().parents[2]
_ACTIVE = None
_CALL = ContextVar("fund_ana_calculation_call", default=None)
_TAGS = ContextVar("fund_ana_calculation_tags", default=None)


class CalculationCaptureError(RuntimeError):
    pass


def _json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _hash(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _safe(path: Path) -> Path:
    path = Path(os.path.abspath(path))
    if any(parent.is_symlink() for parent in (path, *path.parents)):
        raise CalculationCaptureError("capture_symlink_path")
    return path


def _sync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


@contextmanager
def capture_storage_write(directory: Path, incoming_bytes: int):
    """Serialize size checks with writes across all child processes.

    Count inodes once while immutable files are linked into place. The empty
    lock is operational state, not a required input of an offline replay.
    """
    directory = _safe(directory)
    lock_path = _safe(directory / WRITE_LOCK_NAME)
    with lock_path.open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            seen, allocated = set(), 0
            for path in directory.iterdir():
                info = path.lstat()
                if not stat.S_ISREG(info.st_mode):
                    raise CalculationCaptureError("invalid_capture_storage_member")
                identity = (info.st_dev, info.st_ino)
                if identity not in seen:
                    seen.add(identity)
                    allocated += info.st_size
            if allocated + incoming_bytes > MAX_DIRECTORY_BYTES:
                raise CalculationCaptureError("capture_directory_budget_exhausted")
            fs = os.statvfs(directory)
            if fs.f_bavail * fs.f_frsize < MIN_FREE_BYTES + incoming_bytes:
                raise CalculationCaptureError("capture_disk_budget_exhausted")
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


class _BudgetedFile:
    """Check actual compressed bytes before an unbuffered file write."""

    def __init__(self, stream, directory: Path):
        self.stream = stream
        self.directory = directory

    def write(self, data):
        if self.stream.tell() + len(data) > MAX_COMPRESSED_BYTES:
            raise CalculationCaptureError("capture_disk_budget_exhausted")
        with capture_storage_write(self.directory, len(data)):
            written = self.stream.write(data)
            if written != len(data):
                raise CalculationCaptureError("capture_short_write")
            return written

    def __getattr__(self, name):
        return getattr(self.stream, name)


def _encode(value: Any, frame_store=None, sequence_store=None) -> Any:
    if sequence_store is not None and (isinstance(value, np.ndarray)
            or isinstance(value, (list, tuple)) and len(value) >= 128):
        reference = sequence_store(value)
        if reference is not None:
            return reference
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if value is pd.NA:
        return {"t": "pd_na"}
    if value is pd.NaT:
        return {"t": "pd_nat"}
    if isinstance(value, pd.DataFrame):
        if frame_store is not None:
            return frame_store(value)
        if not value.columns.is_unique or isinstance(value.index, pd.MultiIndex) or isinstance(value.columns, pd.MultiIndex):
            raise CalculationCaptureError("unsupported_frame_index_or_duplicate_columns")
        index = ({"range": [value.index.start, value.index.stop, value.index.step]}
                 if isinstance(value.index, pd.RangeIndex) else {"values": _encode(value.index.tolist())})
        return {"t": "frame", "columns": _encode(value.columns.tolist()),
                "columns_name": _encode(value.columns.name),
                "dtypes": [str(dtype) for dtype in value.dtypes],
                "string_storage": {str(index): dtype.storage for index, dtype in enumerate(value.dtypes)
                                   if isinstance(dtype, pd.StringDtype)},
                "categories": {str(index): {"values": _encode(dtype.categories.tolist()), "ordered": dtype.ordered}
                               for index, dtype in enumerate(value.dtypes) if isinstance(dtype, pd.CategoricalDtype)},
                "rows": _encode(value.astype(object).values.tolist()),
                "index": index, "index_name": _encode(value.index.name), "index_dtype": str(value.index.dtype),
                "index_frequency": getattr(value.index, "freqstr", None),
                "index_category": ({"values": _encode(value.index.categories.tolist()),
                                    "ordered": value.index.ordered}
                                   if isinstance(value.index, pd.CategoricalIndex) else None)}
    if isinstance(value, pd.Series):
        return {"t": "series", "name": _encode(value.name), "frame": _encode(value.to_frame(), frame_store, sequence_store)}
    if isinstance(value, pd.Timestamp):
        return {"t": "timestamp", "v": value.isoformat()}
    if isinstance(value, pd.Timedelta):
        return {"t": "pandas_timedelta", "v": value.isoformat()}
    if isinstance(value, pd.Period):
        return {"t": "pandas_period", "v": str(value), "frequency": value.freqstr}
    if isinstance(value, datetime):
        return {"t": "datetime", "v": value.isoformat(), "fold": value.fold}
    if isinstance(value, date):
        return {"t": "date", "v": value.isoformat()}
    if isinstance(value, timedelta):
        return {"t": "timedelta", "days": value.days, "seconds": value.seconds, "microseconds": value.microseconds}
    if isinstance(value, np.ndarray):
        return {"t": "array", "dtype": str(value.dtype), "shape": list(value.shape), "values": _encode(value.tolist(), frame_store, sequence_store)}
    if isinstance(value, np.generic):
        if isinstance(value, np.datetime64):
            return {"t": "numpy_datetime", "dtype": str(value.dtype), "v": str(value)}
        return _encode(value.item(), frame_store, sequence_store)
    if isinstance(value, float):
        return value if math.isfinite(value) else {"t": "float", "v": "nan" if math.isnan(value) else "inf" if value > 0 else "-inf"}
    if isinstance(value, dict):
        return {"t": "mapping", "items": [[_encode(key), _encode(item, frame_store, sequence_store)] for key, item in value.items()]}
    if isinstance(value, (list, tuple)):
        values = [_encode(item, frame_store, sequence_store) for item in value]
        return values if isinstance(value, list) else {"t": "tuple", "items": values}
    if isinstance(value, (set, frozenset)):
        return {"t": "frozenset" if isinstance(value, frozenset) else "set",
                "items": sorted((_encode(item, frame_store, sequence_store) for item in value), key=_json)}
    if isinstance(value, Decimal):
        return {"t": "decimal", "v": str(value)}
    if isinstance(value, Path):
        return {"t": "path", "v": str(value)}
    if isinstance(value, bytes):
        return {"t": "bytes", "v": base64.b64encode(value).decode("ascii")}
    if isinstance(value, Enum):
        return {"t": "enum", "class": type(value).__module__ + "." + type(value).__qualname__,
                "name": value.name, "value": _encode(value.value)}
    raise CalculationCaptureError(f"unsupported_capture_value:{type(value).__module__}.{type(value).__qualname__}")


def decode(value: Any, object_loader=None) -> Any:
    """Decode data only; enum descriptions never trigger imports or execution."""
    if isinstance(value, list):
        return [decode(item, object_loader) for item in value]
    if not isinstance(value, dict):
        return value
    kind = value.get("t")
    if kind == "ref":
        if object_loader is None:
            raise CalculationCaptureError("capture_object_loader_required")
        return decode(object_loader(value["sha256"]), object_loader)
    if kind == "frame":
        columns = decode(value["columns"], object_loader)
        if len(columns) != len(value["dtypes"]) or len(set(columns)) != len(columns):
            raise CalculationCaptureError("invalid_captured_frame_columns")
        frame = pd.DataFrame(decode(value["rows"], object_loader), columns=columns, dtype=object)
        for index_number, (column, dtype) in enumerate(zip(columns, value["dtypes"])):
            category = value.get("categories", {}).get(str(index_number))
            if category is not None:
                dtype = pd.CategoricalDtype(decode(category["values"], object_loader), ordered=category["ordered"])
            elif str(index_number) in value.get("string_storage", {}):
                dtype = pd.StringDtype(storage=value["string_storage"][str(index_number)])
            frame[column] = frame[column].astype(dtype)
        index = value["index"]
        index_dtype = value["index_dtype"]
        if value.get("index_category") is not None:
            category = value["index_category"]
            index_dtype = pd.CategoricalDtype(decode(category["values"], object_loader), ordered=category["ordered"])
        frame.index = (pd.RangeIndex(*index["range"]) if "range" in index else
                       pd.Index(decode(index["values"], object_loader), dtype=index_dtype))
        frame.index.name = decode(value["index_name"], object_loader)
        if value.get("index_frequency"):
            frame.index.freq = value["index_frequency"]
        frame.columns.name = decode(value.get("columns_name"), object_loader)
        return frame
    if kind == "series":
        result = decode(value["frame"], object_loader).iloc[:, 0]
        result.name = decode(value["name"], object_loader)
        return result
    if kind == "mapping":
        return {decode(key, object_loader): decode(item, object_loader) for key, item in value["items"]}
    if kind in {"tuple", "set", "frozenset"}:
        return {"tuple": tuple, "set": set, "frozenset": frozenset}[kind](decode(value["items"], object_loader))
    if kind == "timestamp":
        return pd.Timestamp(value["v"])
    if kind == "pandas_timedelta":
        return pd.Timedelta(value["v"])
    if kind == "pandas_period":
        return pd.Period(value["v"], freq=value["frequency"])
    if kind == "datetime":
        return datetime.fromisoformat(value["v"]).replace(fold=value["fold"])
    if kind == "date":
        return date.fromisoformat(value["v"])
    if kind == "timedelta":
        return timedelta(days=value["days"], seconds=value["seconds"], microseconds=value["microseconds"])
    if kind == "numpy_datetime":
        return np.array(value["v"], dtype=value["dtype"])[()]
    if kind == "array":
        return np.array(decode(value["values"], object_loader), dtype=value["dtype"]).reshape(value["shape"])
    if kind == "pd_na":
        return pd.NA
    if kind == "pd_nat":
        return pd.NaT
    if kind == "float":
        return float(value["v"])
    if kind == "decimal":
        return Decimal(value["v"])
    if kind == "bytes":
        return base64.b64decode(value["v"], validate=True)
    if kind == "path":
        return Path(value["v"])
    if kind == "enum":
        return {"enum_class": value["class"], "name": value["name"], "value": decode(value["value"], object_loader)}
    raise CalculationCaptureError(f"unknown_capture_type:{kind}")


def _context(instance, attributes: tuple[str, ...]) -> dict:
    return {name: getattr(instance, name) for name in attributes if hasattr(instance, name)}


class CaptureWriter:
    def __init__(self, directory: Path, kind: str, context: dict, code_paths: list[Path] | None = None):
        self.directory = _safe(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.session_id = uuid4().hex
        self.kind = kind
        self.workflow_run_id = os.environ.get("FUND_ANA_WORKFLOW_RUN_ID", "").strip() or None
        self.run_id = os.environ.get(ENV_CAPTURE_RUN_ID, "").strip() or self.workflow_run_id or ""
        self.capture_scope = os.environ.get(ENV_CAPTURE_SCOPE, "workflow").strip()
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,120}", self.run_id):
            raise CalculationCaptureError("capture_workflow_run_id_required")
        if (self.capture_scope not in {"workflow", "collection", "standalone"}
                or (self.capture_scope == "workflow" and self.workflow_run_id != self.run_id)
                or (self.capture_scope in {"collection", "standalone"} and self.workflow_run_id is not None)):
            raise CalculationCaptureError("capture_scope_identity_mismatch")
        self.lock = threading.RLock()
        self.objects: dict[str, int] = {}
        self.open_calls: set[str] = set()
        self.calls = Counter()
        self.exceptions = Counter()
        self.records = self.bytes_written = self.stored_bytes_written = 0
        self.schema_version = SCHEMA_VERSION
        self.pack_store = None
        self.failure = None
        self.closed = False
        self.started = time.monotonic()
        self.partial = self.directory / (self.session_id + ".jsonl.gz.partial")
        self.path = self.directory / (self.session_id + ".jsonl.gz")
        self.raw = self.partial.open("xb", buffering=0)
        self.compressed = None
        try:
            shared_directory = os.environ.get("FUND_ANA_CALCULATION_SHARED_DIR", "").strip()
            if shared_directory:
                from .calculation_pack_storage import CapturePackStore

                self.pack_store = CapturePackStore(self.directory, Path(shared_directory))
                self.schema_version = 3
            os.chmod(self.partial, 0o600)
            self.compressed = gzip.GzipFile(fileobj=_BudgetedFile(self.raw, self.directory),
                                           filename="", mode="wb", compresslevel=6, mtime=0)
            self._initialize_sources(context, code_paths)
        except BaseException:
            self._close(sync=False)
            raise

    def _initialize_sources(self, context: dict, code_paths: list[Path] | None) -> None:
        # Core calculation, configuration and query modules are frozen as actual
        # source bytes; API credentials and the runtime YAML are never exported.
        if code_paths is None:
            roots = [PROJECT_ROOT / "core/analyzers", PROJECT_ROOT / "core/bond_fund_screening",
                     PROJECT_ROOT / "core/database", PROJECT_ROOT / "core/config", PROJECT_ROOT / "core/cli",
                     PROJECT_ROOT / "core/utils", PROJECT_ROOT / "backend"]
            paths = {Path(__file__), PROJECT_ROOT / "core/main.py"}
            excluded = {"__pycache__", "logs", "reports", "audit", "data", "output", ".cache", ".git"}
            for root in roots:
                for parent, directories, files in os.walk(_safe(root), followlinks=False):
                    directories[:] = sorted(name for name in directories if name not in excluded
                                             and not (Path(parent) / name).is_symlink())
                    paths.update(Path(parent) / name for name in files if name.endswith(".py"))
            code_paths = sorted(paths)
        self.source_paths = [_safe(path) for path in code_paths]
        if (not self.source_paths or len(self.source_paths) > 1024
                or len(set(self.source_paths)) != len(self.source_paths)
                or any(not path.is_relative_to(PROJECT_ROOT) or not stat.S_ISREG(path.stat().st_mode)
                       for path in self.source_paths)
                or sum(path.stat().st_size for path in self.source_paths) > MAX_LINE_BYTES):
            raise CalculationCaptureError("invalid_or_oversized_capture_sources")
        self.sources = {str(path.relative_to(PROJECT_ROOT)): _hash(path) for path in self.source_paths}
        entrypoint = Path(os.path.abspath(sys.argv[0])) if sys.argv else None
        entrypoint_source = (str(entrypoint.relative_to(PROJECT_ROOT))
                             if entrypoint is not None and entrypoint.is_relative_to(PROJECT_ROOT) else None)
        self._write({"record": "header", "schema_version": self.schema_version, "session_id": self.session_id,
                     "run_id": self.run_id, "kind": self.kind, "context": _encode(context),
                     "capture_scope": self.capture_scope, "workflow_run_id": self.workflow_run_id,
                     "argv": sys.argv, "cwd": str(Path.cwd()), "project_root": str(PROJECT_ROOT),
                     "entrypoint_source": entrypoint_source,
                     "pid": os.getpid(), "started_at": datetime.now().astimezone().isoformat(),
                     "python": sys.version, "numpy": np.__version__, "pandas": pd.__version__,
                     "code_sha256": self.sources,
                     "source_policy": "actual_call_inputs_and_outputs_no_later_database_or_api_reconstruction"})
        for path in self.source_paths:
            self._write({"record": "source", "path": str(path.relative_to(PROJECT_ROOT)),
                         "sha256": self.sources[str(path.relative_to(PROJECT_ROOT))],
                         "bytes_base64": base64.b64encode(path.read_bytes()).decode("ascii")})

    def _write(self, record: dict) -> None:
        if self.closed:
            raise CalculationCaptureError("capture_writer_already_closed")
        if self.failure:
            raise CalculationCaptureError(self.failure)
        try:
            raw = _json(record) + b"\n"
            if len(raw) > MAX_LINE_BYTES or self.bytes_written + len(raw) > MAX_STREAM_BYTES or self.records >= MAX_RECORDS:
                raise CalculationCaptureError("capture_resource_budget_exhausted")
            fs = os.statvfs(self.directory)
            if fs.f_bavail * fs.f_frsize < MIN_FREE_BYTES + len(raw) or self.raw.tell() > MAX_COMPRESSED_BYTES:
                raise CalculationCaptureError("capture_disk_budget_exhausted")
            stored = raw
            if self.pack_store is not None and record.get("record") in {"source", "object"}:
                kind = record["record"]
                field = "value" if kind == "object" else "bytes_base64"
                blob = _json(record[field]) if kind == "object" else base64.b64decode(record[field], validate=True)
                reference = self.pack_store.put(kind, blob, record["sha256"])
                stored = _json({**{key: value for key, value in record.items() if key != field},
                                "packed": reference}) + b"\n"
            if len(stored) > MAX_LINE_BYTES or self.stored_bytes_written + len(stored) > MAX_STREAM_BYTES:
                raise CalculationCaptureError("capture_resource_budget_exhausted")
            self.compressed.write(stored)
            self.bytes_written += len(raw)
            self.stored_bytes_written += len(stored)
            self.records += 1
        except Exception as exc:
            self.failure = f"{type(exc).__name__}:{exc}"
            raise

    def _frame(self, frame: pd.DataFrame) -> dict:
        value = _encode(frame)
        return self._store_object(value, _json(value))

    def _sequence(self, sequence: Any) -> dict | None:
        # Flat scalar lists (including code lists) and arrays are common actual
        # operands. Keep nested tabular containers on the existing incremental
        # frame path rather than materializing a whole collection of frames.
        scalars = (str, bool, int, float, np.generic, date, timedelta, Decimal, Path, bytes)
        if isinstance(sequence, (list, tuple)) and not all(
                item is None or item is pd.NA or item is pd.NaT
                or isinstance(item, scalars) and not isinstance(item, Enum)
                for item in sequence):
            return None
        value = _encode(sequence)
        raw = _json(value)
        if len(raw) < 4096:
            return None
        # Some supported inline descriptions (for example enum descriptions)
        # deliberately decode to data rather than constructing original types.
        # They retain the original inline contract, without claiming a typed
        # object round trip that cannot be proven.
        try:
            if _json(_encode(decode(value))) != raw:
                return None
        except (CalculationCaptureError, TypeError, ValueError, OverflowError):
            return None
        return self._store_object(value, raw)

    def _store_object(self, value: Any, raw: bytes) -> dict:
        digest = hashlib.sha256(raw).hexdigest()
        if digest not in self.objects:
            self._write({"record": "object", "sha256": digest, "bytes": len(raw), "value": value})
            self.objects[digest] = len(raw)
        return {"t": "ref", "sha256": digest}

    def pack(self, value: Any) -> Any:
        try:
            return _encode(value, self._frame, self._sequence if self.schema_version == 3 else None)
        except Exception as exc:
            self.failure = f"{type(exc).__name__}:{exc}"
            raise

    def event(self, kind: str, value: Any) -> None:
        with self.lock:
            self._write({"record": "event", "kind": kind, "parent_id": _CALL.get(),
                         "value": self.pack(value), "at": datetime.now().astimezone().isoformat()})

    def _close(self, *, sync: bool = True) -> None:
        try:
            if self.compressed is not None:
                self.compressed.close()
            if sync:
                self.raw.flush()
                os.fsync(self.raw.fileno())
        finally:
            self.raw.close()
            if self.pack_store is not None:
                self.pack_store.close()
            self.closed = True

    def finish(self, outcome: str) -> dict:
        with self.lock:
            if self.closed or outcome not in {"returned", "raised"}:
                raise CalculationCaptureError("invalid_capture_finish")
            try:
                if self.failure or self.open_calls:
                    raise CalculationCaptureError(self.failure or "unfinished_captured_calls")
                if any(_hash(path) != self.sources[str(path.relative_to(PROJECT_ROOT))] for path in self.source_paths):
                    raise CalculationCaptureError("calculation_source_changed_during_capture")
                storage = self.pack_store.finish() if self.pack_store is not None else None
                terminal = {"record": "terminal", "session_id": self.session_id, "run_id": self.run_id,
                            "operation_outcome": outcome, "records_before_terminal": self.records,
                            "object_count": len(self.objects), "calls": dict(self.calls), "exceptions": dict(self.exceptions),
                            "finished_at": datetime.now().astimezone().isoformat(),
                            "elapsed_seconds": round(time.monotonic() - self.started, 6)}
                self._write(terminal)
            finally:
                self._close()
            if self.partial.stat().st_size > MAX_COMPRESSED_BYTES:
                raise CalculationCaptureError("capture_disk_budget_exhausted")
            with capture_storage_write(self.directory, 0):
                os.link(self.partial, self.path)
                self.partial.unlink()
            manifest = {"schema_version": self.schema_version, "session_id": self.session_id, "run_id": self.run_id,
                        "capture_scope": self.capture_scope, "workflow_run_id": self.workflow_run_id,
                        "kind": self.kind, "file": self.path.name, "sha256": _hash(self.path),
                        "bytes": self.path.stat().st_size, "uncompressed_bytes": self.bytes_written,
                        "capture_complete": True, "operation_outcome": outcome, "records": self.records,
                        "object_count": len(self.objects), "calls": dict(self.calls), "exceptions": dict(self.exceptions)}
            if storage is not None:
                manifest.update(storage=storage, stored_uncompressed_bytes=self.stored_bytes_written)
            target = self.directory / (self.session_id + ".manifest.json")
            temporary = target.with_name(target.name + ".partial")
            raw = _json(manifest) + b"\n"
            with capture_storage_write(self.directory, len(raw)):
                with temporary.open("xb") as stream:
                    stream.write(raw)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.link(temporary, target)
                temporary.unlink()
            _sync_directory(self.directory)
            return manifest


def record_event(kind: str, value: Any) -> None:
    if _ACTIVE is not None:
        _ACTIVE.event(kind, value)


def capture_enabled() -> bool:
    return _ACTIVE is not None


@contextmanager
def capture_context(**tags):
    token = _TAGS.set({**(_TAGS.get() or {}), **tags})
    try:
        yield
    finally:
        _TAGS.reset(token)


def capture_call(kind: str, *, attributes: tuple[str, ...] = (), omit: tuple[str, ...] = ()):
    def decorate(function):
        signature = inspect.signature(function)

        @wraps(function)
        def wrapped(*args, **kwargs):
            writer = _ACTIVE
            if writer is None:
                return function(*args, **kwargs)
            bound = signature.bind(*args, **kwargs)
            bound.apply_defaults()
            instance = bound.arguments.get("self")
            arguments = {key: value for key, value in bound.arguments.items() if key not in {"self", "cls", *omit}}
            call_id = uuid4().hex
            with writer.lock:
                writer._write({"record": "call", "kind": kind, "call_id": call_id, "parent_id": _CALL.get(),
                               "arguments": writer.pack(arguments), "context": writer.pack(_context(instance, attributes)),
                               "tags": writer.pack(_TAGS.get() or {}),
                               "started_at": datetime.now().astimezone().isoformat()})
                writer.open_calls.add(call_id)
                writer.calls[kind] += 1
            token = _CALL.set(call_id)
            try:
                result = function(*args, **kwargs)
                with writer.lock:
                    writer._write({"record": "return", "call_id": call_id, "value": writer.pack(result),
                                   "finished_at": datetime.now().astimezone().isoformat()})
                return result
            except BaseException as exc:
                if not writer.failure:
                    with writer.lock:
                        writer._write({"record": "exception", "call_id": call_id,
                                       "type": type(exc).__name__, "message": str(exc)})
                        writer.exceptions[kind] += 1
                raise
            finally:
                with writer.lock:
                    writer.open_calls.discard(call_id)
                _CALL.reset(token)
        return wrapped
    return decorate


def capture_session(kind: str, *, attributes: tuple[str, ...] = ()):
    def decorate(function):
        @wraps(function)
        def wrapped(*args, **kwargs):
            global _ACTIVE
            directory = os.environ.get(ENV_DIRECTORY, "").strip()
            if not directory or _ACTIVE is not None:
                return function(*args, **kwargs)
            writer = CaptureWriter(Path(directory), kind, _context(args[0] if args else None, attributes))
            _ACTIVE = writer
            outcome = "raised"
            try:
                result = function(*args, **kwargs)
                outcome = "returned"
                return result
            except SystemExit as exc:
                if exc.code in (None, 0):
                    outcome = "returned"
                raise
            finally:
                _ACTIVE = None
                writer.finish(outcome)
        return wrapped
    return decorate


def iter_journal(path: Path, *, storage: dict | None = None, statistics: dict | None = None) -> Iterator[dict]:
    """Read old inline journals or hydrate strictly bound local pack references."""
    total = records = hydrated_bytes = 0
    reader = None
    schema = None
    if storage is not None:
        from .calculation_pack_storage import PackReader

        reader = PackReader(path.parent, storage)
    with gzip.open(_safe(path), "rb") as stream:
        while raw := stream.readline(MAX_LINE_BYTES + 1):
            total += len(raw)
            records += 1
            if len(raw) > MAX_LINE_BYTES or total > MAX_STREAM_BYTES or records > MAX_RECORDS or not raw.endswith(b"\n"):
                raise CalculationCaptureError("invalid_or_oversized_capture_record")
            value = json.loads(raw, object_pairs_hook=_unique_pairs, parse_constant=_reject_constant)
            if not isinstance(value, dict):
                raise CalculationCaptureError("capture_record_not_an_object")
            if records == 1:
                schema = value.get("schema_version")
            if "packed" in value:
                from .calculation_pack_storage import PackReader

                if schema not in (2, 3):
                    raise CalculationCaptureError("packed_record_requires_storage_schema")
                if schema == 2 and isinstance(value["packed"], dict) and "chunks" in value["packed"]:
                    raise CalculationCaptureError("chunked_record_requires_schema_three")
                kind = value.get("record")
                expected = ({"record", "sha256", "bytes", "packed"} if kind == "object"
                            else {"record", "sha256", "path", "packed"} if kind == "source" else set())
                if set(value) != expected:
                    raise CalculationCaptureError("invalid_packed_capture_record")
                if reader is None:
                    descriptor = _safe(path.with_name(path.name.removesuffix(".jsonl.gz") + ".manifest.json"))
                    if descriptor.stat().st_size > 1024**2:
                        raise CalculationCaptureError("capture_manifest_too_large")
                    payload = json.loads(descriptor.read_bytes(), object_pairs_hook=_unique_pairs, parse_constant=_reject_constant)
                    reader = PackReader(path.parent, payload.get("storage"))
                blob = reader.get(value.pop("packed"), value["sha256"])
                if kind == "object":
                    value["value"] = json.loads(blob, object_pairs_hook=_unique_pairs, parse_constant=_reject_constant)
                else:
                    value["bytes_base64"] = base64.b64encode(blob).decode("ascii")
                raw = _json(value) + b"\n"
            hydrated_bytes += len(raw)
            if len(raw) > MAX_LINE_BYTES or hydrated_bytes > MAX_STREAM_BYTES:
                raise CalculationCaptureError("hydrated_capture_resource_budget_exhausted")
            yield value
    if reader is not None and reader.used != set(reader.members):
        raise CalculationCaptureError("unused_capture_pack_manifest_members")
    if statistics is not None:
        statistics.update(stored_uncompressed_bytes=total)


def _unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise CalculationCaptureError("duplicate_capture_json_key")
        result[key] = value
    return result


def _reject_constant(value):
    raise CalculationCaptureError(f"untyped_nonfinite_capture_value:{value}")


def _verify_references(value: Any, objects: set[str]) -> None:
    if isinstance(value, dict):
        if value.get("t") == "ref":
            if set(value) != {"t", "sha256"} or value.get("sha256") not in objects:
                raise CalculationCaptureError("missing_or_invalid_capture_object")
        for child in value.values():
            _verify_references(child, objects)
    elif isinstance(value, list):
        for child in value:
            _verify_references(child, objects)


def validate_journal(manifest_path: Path, *, expected_run_id: str, on_record=None) -> dict:
    """Verify the entire journal, all sources/objects and every call terminal."""
    manifest_path = _safe(manifest_path)
    if manifest_path.stat().st_size > 1024**2:
        raise CalculationCaptureError("capture_manifest_too_large")
    manifest = json.loads(manifest_path.read_bytes(), object_pairs_hook=_unique_pairs, parse_constant=_reject_constant)
    identifier = manifest.get("session_id", "")
    if (not isinstance(identifier, str) or not re.fullmatch(r"[0-9a-f]{32}", identifier)
            or manifest_path.name != identifier + ".manifest.json"
            or manifest.get("file") != identifier + ".jsonl.gz"
            or manifest.get("schema_version") not in (1, 2, 3)
            or manifest.get("capture_complete") is not True or manifest.get("run_id") != expected_run_id):
        raise CalculationCaptureError("capture_manifest_identity_mismatch")
    if (manifest["schema_version"] == 1 and "storage" in manifest
            or manifest["schema_version"] in (2, 3) and not isinstance(manifest.get("storage"), dict)):
        raise CalculationCaptureError("capture_manifest_storage_mismatch")
    path = _safe(manifest_path.parent / manifest["file"])
    if path.stat().st_size != manifest.get("bytes") or path.stat().st_size > MAX_COMPRESSED_BYTES or _hash(path) != manifest.get("sha256"):
        raise CalculationCaptureError("capture_journal_physical_hash_mismatch")
    objects, sources, all_calls, pending = set(), set(), set(), {}
    calls, exceptions = Counter(), Counter()
    header = terminal = None
    count = raw_bytes = 0
    stream_statistics = {}
    for row in iter_journal(path, storage=manifest.get("storage"), statistics=stream_statistics):
        index = count
        count += 1
        raw_bytes += len(_json(row)) + 1
        kind = row.get("record")
        if terminal is not None:
            raise CalculationCaptureError("records_after_capture_terminal")
        if index == 0:
            if kind != "header" or row.get("schema_version") != manifest["schema_version"] or row.get("session_id") != identifier or row.get("run_id") != expected_run_id:
                raise CalculationCaptureError("capture_header_identity_mismatch")
            if row.get("kind") != manifest.get("kind") or not isinstance(row.get("code_sha256"), dict):
                raise CalculationCaptureError("capture_header_kind_or_sources_mismatch")
            scope = row.get("capture_scope", "workflow")
            if (scope != manifest.get("capture_scope", "workflow")
                    or row.get("workflow_run_id", expected_run_id) != manifest.get("workflow_run_id", expected_run_id)
                    or scope not in {"workflow", "collection", "standalone"}
                    or (scope == "workflow" and row.get("workflow_run_id", expected_run_id) != expected_run_id)
                    or (scope in {"collection", "standalone"} and row.get("workflow_run_id") is not None)):
                raise CalculationCaptureError("capture_header_scope_mismatch")
            header = row
            if on_record is not None:
                on_record(row)
            continue
        if kind == "source":
            name = row.get("path")
            if (not isinstance(name, str) or name in sources or Path(name).is_absolute() or ".." in Path(name).parts
                    or header["code_sha256"].get(name) != row.get("sha256")):
                raise CalculationCaptureError("capture_source_identity_mismatch")
            source_bytes = base64.b64decode(row["bytes_base64"], validate=True)
            if hashlib.sha256(source_bytes).hexdigest() != row["sha256"]:
                raise CalculationCaptureError("capture_source_bytes_mismatch")
            sources.add(name)
        elif kind == "object":
            digest = row.get("sha256")
            raw = _json(row.get("value"))
            if digest in objects or digest != hashlib.sha256(raw).hexdigest() or row.get("bytes") != len(raw):
                raise CalculationCaptureError("capture_object_hash_mismatch")
            value = row["value"]
            frame = isinstance(value, dict) and value.get("t") == "frame"
            sequence = (manifest["schema_version"] == 3 and (isinstance(value, list)
                        or isinstance(value, dict) and value.get("t") in {"array", "tuple"}))
            if not frame and not sequence:
                raise CalculationCaptureError("unsupported_capture_object_type")
            # Preserve identical typed values, including missing values, signed
            # zero, array shape, and the original frame index/order.
            if _json(_encode(decode(value))) != raw:
                raise CalculationCaptureError("captured_frame_does_not_roundtrip" if frame
                                              else "captured_sequence_does_not_roundtrip")
            objects.add(digest)
        elif kind == "call":
            call_id = row.get("call_id")
            if not isinstance(call_id, str) or call_id in all_calls or row.get("parent_id") not in {None, *pending}:
                raise CalculationCaptureError("invalid_capture_call_identity")
            _verify_references(row, objects)
            all_calls.add(call_id)
            pending[call_id] = row["kind"]
            calls[row["kind"]] += 1
        elif kind in {"return", "exception"}:
            call_id = row.get("call_id")
            if call_id not in pending:
                raise CalculationCaptureError("unknown_or_duplicate_capture_call_terminal")
            _verify_references(row, objects)
            if kind == "exception":
                exceptions[pending[call_id]] += 1
            pending.pop(call_id)
        elif kind == "event":
            if row.get("parent_id") not in {None, *pending}:
                raise CalculationCaptureError("event_outside_capture_call")
            _verify_references(row, objects)
        elif kind == "terminal":
            if (row.get("session_id") != identifier or row.get("run_id") != expected_run_id
                    or row.get("records_before_terminal") != index or pending
                    or row.get("object_count") != len(objects) or row.get("calls") != dict(calls)
                    or row.get("exceptions") != dict(exceptions)
                    or row.get("operation_outcome") != manifest.get("operation_outcome")):
                raise CalculationCaptureError("incomplete_capture_terminal")
            terminal = row
        else:
            raise CalculationCaptureError(f"unknown_capture_record:{kind}")
        if on_record is not None:
            on_record(row)
    if (header is None or terminal is None or sources != set(header["code_sha256"])
            or manifest.get("records") != count or manifest.get("uncompressed_bytes") != raw_bytes
            or (manifest["schema_version"] in (2, 3) and manifest.get("stored_uncompressed_bytes")
                != stream_statistics.get("stored_uncompressed_bytes"))
            or manifest.get("object_count") != len(objects) or manifest.get("calls") != dict(calls)
            or manifest.get("exceptions") != dict(exceptions)):
        raise CalculationCaptureError("incomplete_capture_journal")
    return {"manifest": manifest, "manifest_sha256": _hash(manifest_path), "header": header,
            "terminal": terminal, "typed_objects_roundtripped": len(objects), "complete": True}
