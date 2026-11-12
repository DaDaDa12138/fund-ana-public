"""Versioned, nullable report facts. Publication visibility is owned by the head.

This module has no Flask or scheduler dependency and is also used by core tasks.
Read operations never create a database, change a journal, or infer a publication.
"""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from datetime import date, datetime
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
from typing import Any

import pandas as pd
import yaml
from functools import wraps

CONTRACT = 'report-history-frame-v1'
SCHEMA = 1
_READS = ContextVar('report_history_reads', default=None)


def capture_reads(function):
    @wraps(function)
    def wrapped(*args,**kwargs):
        token=_READS.set({})
        try:return function(*args,**kwargs)
        finally:_READS.reset(token)
    return wrapped


def captured_reads() -> list[dict]:
    """Return observed immutable identities, without retaining decoded tables.

    The database locator is process-local only. Freezing resolves each digest
    into revision-owned objects before a report may be committed.
    """
    return list((_READS.get() or {}).values())


class HistoryError(ValueError):
    pass


def encode(value: Any) -> Any:
    if value is pd.NA:
        return {'$history_type': 'NA'}
    if value is pd.NaT:
        return {'$history_type': 'NaT'}
    if isinstance(value, (datetime, date)):
        return {'$history_type': type(value).__name__, 'value': value.isoformat()}
    if isinstance(value, float) and not math.isfinite(value):
        return {'$history_type': 'float', 'value': repr(value)}
    if isinstance(value, dict):
        return {str(k): encode(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [encode(v) for v in value]
    if value is not None and hasattr(value, 'item'):
        return encode(value.item())
    return value


def decode(value: Any) -> Any:
    if isinstance(value, list):
        return [decode(v) for v in value]
    if isinstance(value, dict):
        tag = value.get('$history_type')
        if tag == 'NA': return pd.NA
        if tag == 'NaT': return pd.NaT
        if tag == 'float': return float(value['value'])
        if tag == 'date': return date.fromisoformat(value['value'])
        if tag == 'datetime': return datetime.fromisoformat(value['value'])
        if tag == 'Timestamp': return pd.Timestamp(value['value'])
        return {k: decode(v) for k, v in value.items()}
    return value


def canonical(value: Any) -> bytes:
    return json.dumps(encode(value), ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False).encode('utf-8')


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def frame_payload(frame: pd.DataFrame) -> dict:
    return {'contract': CONTRACT, 'columns': list(frame.columns),
            'dtypes': [str(t) for t in frame.dtypes], 'index': list(frame.index),
            'index_name': frame.index.name, 'rows': frame.to_numpy(dtype=object).tolist()}


def payload_frame(payload: dict) -> pd.DataFrame:
    if payload.get('contract') != CONTRACT:
        raise HistoryError('history_frame_contract_invalid')
    frame = pd.DataFrame(payload['rows'], columns=payload['columns'], index=payload['index'])
    for i, dtype in enumerate(payload['dtypes']):
        frame.isetitem(i, frame.iloc[:, i].astype(dtype))
    frame.index.name = payload['index_name']
    return frame


def project_options(path: Path) -> tuple[Path, Path, str] | None:
    path = Path(path).absolute().resolve()
    for root in (path, *path.parents):
        config = root / 'config/project.yaml'
        if not config.is_file():
            continue
        location = path.relative_to(root)
        if location.parts and location.parts[0] not in ('reports', 'core', 'backend'):
            return None
        value = yaml.safe_load(config.read_text(encoding='utf-8')) or {}
        options = value.get('report_history') or {}
        mode = os.environ.get('FUND_ANA_REPORT_HISTORY_MODE', options.get('read_mode', 'legacy'))
        if mode not in ('legacy', 'shadow', 'ledger'):
            raise HistoryError('history_read_mode_invalid')
        db = Path((value.get('paths') or {}).get('report_history_database', 'backend/data/report_history.sqlite'))
        if not db.is_absolute(): db = root / db
        if not db.resolve().is_relative_to(root):
            raise HistoryError('history_database_outside_project')
        return root, db, mode
    return None


def relative(root: Path, path: Path) -> str:
    path = Path(path)
    if str(path).startswith('/app/') and str(root) != '/app':
        path = root / str(path)[5:]
    if not path.is_absolute(): path = root / path
    path = path.resolve()
    if not path.is_relative_to(root.resolve()):
        raise HistoryError('history_source_outside_project')
    return str(path.relative_to(root.resolve()))


def file_identity(path: Path) -> list[int]:
    s = path.stat()
    return [s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns]


@contextmanager
def connection(db: Path, *, write: bool = False):
    if write:
        db.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(db, timeout=2)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=FULL')
        conn.executescript('''
            CREATE TABLE IF NOT EXISTS objects(digest TEXT PRIMARY KEY, payload BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS sources(path TEXT NOT NULL, sha256 TEXT NOT NULL,
                sha1 TEXT NOT NULL, identity TEXT NOT NULL, observed_at TEXT NOT NULL,
                PRIMARY KEY(path,sha256));
            CREATE TABLE IF NOT EXISTS views(path TEXT NOT NULL, sha256 TEXT NOT NULL,
                kind TEXT NOT NULL, digest TEXT NOT NULL REFERENCES objects(digest),
                PRIMARY KEY(path,sha256,kind));
            CREATE TABLE IF NOT EXISTS reports(run_id TEXT NOT NULL, revision TEXT NOT NULL,
                path TEXT NOT NULL, sha256 TEXT NOT NULL, semantic TEXT NOT NULL,
                digest TEXT NOT NULL REFERENCES objects(digest), PRIMARY KEY(run_id,revision));
            CREATE TABLE IF NOT EXISTS report_sources(run_id TEXT NOT NULL, revision TEXT NOT NULL,
                path TEXT NOT NULL, PRIMARY KEY(run_id,revision,path));
            CREATE INDEX IF NOT EXISTS report_sources_path ON report_sources(path);
            CREATE TABLE IF NOT EXISTS member_views(view_id INTEGER PRIMARY KEY,
                path TEXT NOT NULL, sha256 TEXT NOT NULL, kind TEXT NOT NULL,
                digest TEXT NOT NULL REFERENCES objects(digest), UNIQUE(path,sha256,kind));
            CREATE TABLE IF NOT EXISTS members(view_id INTEGER NOT NULL REFERENCES member_views(view_id),
                ordinal INTEGER NOT NULL, code TEXT NOT NULL, group_name TEXT,
                PRIMARY KEY(view_id,ordinal));
            CREATE INDEX IF NOT EXISTS members_code ON members(code,view_id);
            CREATE TABLE IF NOT EXISTS validations(path TEXT NOT NULL, sha256 TEXT NOT NULL,
                kind TEXT NOT NULL, digest TEXT NOT NULL, verified_at TEXT NOT NULL,
                PRIMARY KEY(path,sha256,kind,digest));
            CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        ''')
        version = conn.execute('PRAGMA user_version').fetchone()[0]
        if version not in (0, SCHEMA):
            conn.close(); raise HistoryError('history_schema_invalid')
        conn.execute(f'PRAGMA user_version={SCHEMA}')
    else:
        if not db.is_file(): raise FileNotFoundError('history_database_unavailable')
        conn = sqlite3.connect(db.as_uri() + '?mode=ro', uri=True, timeout=2)
        if conn.execute('PRAGMA user_version').fetchone()[0] != SCHEMA:
            conn.close(); raise HistoryError('history_schema_invalid')
    conn.execute('PRAGMA foreign_keys=ON')
    try:
        yield conn
        if write: conn.commit()
    except BaseException:
        if write: conn.rollback()
        raise
    finally:
        conn.close()


def put_object(conn: sqlite3.Connection, value: Any) -> str:
    raw = canonical(value); key = hashlib.sha256(raw).hexdigest()
    conn.execute('INSERT OR IGNORE INTO objects VALUES(?,?)', (key, gzip.compress(raw, mtime=0)))
    return key


def index_members(conn: sqlite3.Connection, path: str, sha: str, kind: str, key: str, value: dict) -> None:
    """Index identity/position only; metrics remain in the shared, immutable payload."""
    if kind not in ('formal','tracker','watch','core_raw'):
        return
    frame = value.get('frame', value)
    if not isinstance(frame, dict) or frame.get('contract') != CONTRACT:
        return
    columns = frame['columns']
    code_column = next((c for c in ('基金代码', 'fcode', 'code') if c in columns), None)
    if code_column is None:
        return
    ci = columns.index(code_column)
    group_column = next((c for c in ('正式榜分组', '榜单分组', '分组') if c in columns), None)
    gi = columns.index(group_column) if group_column else None
    conn.execute('INSERT OR IGNORE INTO member_views(path,sha256,kind,digest) VALUES(?,?,?,?)',
                 (path,sha,kind,key))
    view_id=conn.execute('SELECT view_id FROM member_views WHERE path=? AND sha256=? AND kind=?',
                         (path,sha,kind)).fetchone()[0]
    rows = []
    for ordinal, row in enumerate(frame['rows']):
        raw = row[ci]
        if raw is None or pd.isna(raw):
            continue
        code = str(raw).strip().removesuffix('.0')
        if not code.isdigit() or len(code) > 6:
            continue
        group = row[gi] if gi is not None else None
        rows.append((view_id,ordinal,code.zfill(6),None if group is None or pd.isna(group) else str(group)))
    conn.executemany('INSERT OR IGNORE INTO members VALUES(?,?,?,?)', rows)


def get_object(conn: sqlite3.Connection, key: str) -> Any:
    row = conn.execute('SELECT payload FROM objects WHERE digest=?', (key,)).fetchone()
    if not row: raise HistoryError('history_object_missing')
    try: raw = gzip.decompress(row[0])
    except (OSError, EOFError) as exc: raise HistoryError('history_object_corrupt') from exc
    if hashlib.sha256(raw).hexdigest() != key: raise HistoryError('history_object_digest_mismatch')
    return decode(json.loads(raw))


def source_info(root: Path, db: Path, path: Path, *, kind: str | None = None) -> dict | None:
    key = relative(root, path)
    with connection(db) as conn:
        query = ('SELECT s.sha256,s.sha1,s.identity,v.digest FROM sources s JOIN views v '
                 'ON s.path=v.path AND s.sha256=v.sha256 WHERE s.path=?')
        args = [key]
        if kind:
            query += ' AND v.kind=?'; args.append(kind)
        rows = conn.execute(query, args).fetchall()
        if path.exists():
            current = file_identity(path)
            matching = [r for r in rows if json.loads(r[2]) == current]
            if rows and not matching:
                current_sha = original_hash(path)
                matching = [r for r in rows if r[0] == current_sha]
            rows = matching
        hashes = {r[0] for r in rows}
        if not hashes: return None
        if len(hashes) != 1: raise HistoryError('history_source_identity_ambiguous:' + key)
        row = rows[0]
        return {'path': key, 'sha256': row[0], 'sha1': row[1], 'identity': json.loads(row[2]),
                'digest': row[3], 'contract': CONTRACT}


def put_views(root: Path, db: Path, path: Path, views: dict[str, Any], *, expected_identity: list | None = None) -> dict:
    before = file_identity(path)
    if expected_identity is not None and before != expected_identity:
        raise HistoryError('history_source_changed_before_write')
    raw_hash = hashlib.sha256(); old_hash = hashlib.sha1()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            raw_hash.update(chunk); old_hash.update(chunk)
    if file_identity(path) != before: raise HistoryError('history_source_changed')
    key = relative(root, path); sha = raw_hash.hexdigest()
    with connection(db, write=True) as conn:
        conn.execute('INSERT INTO sources VALUES(?,?,?,?,?) ON CONFLICT(path,sha256) DO UPDATE SET identity=excluded.identity',
                     (key, sha, old_hash.hexdigest(), json.dumps(before), datetime.now().astimezone().isoformat()))
        for kind, value in views.items():
            obj = put_object(conn, value)
            existing = conn.execute('SELECT digest FROM views WHERE path=? AND sha256=? AND kind=?',
                                    (key, sha, kind)).fetchone()
            if existing and existing[0] != obj: raise HistoryError('history_view_changed_for_identical_source')
            conn.execute('INSERT OR IGNORE INTO views VALUES(?,?,?,?)', (key, sha, kind, obj))
            if digest(get_object(conn,obj)) != digest(value):
                raise HistoryError('history_round_trip_mismatch')
            index_members(conn, key, sha, kind, obj, value)
        if file_identity(path) != before: raise HistoryError('history_source_changed')
    return {'path': key, 'sha256': sha, 'views': sorted(views)}


def view(path: Path, kind: str) -> tuple[Any, dict] | None:
    options = project_options(path)
    if options is None or options[2] == 'legacy': return None
    root, db, mode = options
    if not db.exists():
        if mode == 'ledger': raise HistoryError('history_database_unavailable')
        return None
    info = source_info(root, db, path, kind=kind)
    if info is None: return None  # Newly generated candidate, not a historical source.
    with connection(db) as conn:
        value = get_object(conn, info['digest'])
    proof={**info,'kind':kind,'payload':value}
    if _READS.get() is not None:
        _READS.get()[(proof['path'],proof['sha256'],kind)]={
            **{key:item for key,item in proof.items() if key!='payload'},
            '_database_path':str(db),
        }
    return value, proof


def original_hash(path: Path, algorithm: str = 'sha256') -> str:
    if path.exists():
        with path.open('rb') as stream: return hashlib.file_digest(stream, algorithm).hexdigest()
    options = project_options(path)
    if options is None: raise FileNotFoundError(path)
    info = source_info(options[0], options[1], path)
    if info is None: raise FileNotFoundError(path)
    return info[algorithm]


def available(path: Path, kind: str) -> bool:
    if path.exists(): return True
    options=project_options(path)
    if not options: return False
    from backend.report_history_retention import original_state
    if original_state(options[0],path)['status'] not in ('archived_offline','original_retired'):return False
    return view(path, kind) is not None


def read_frame(path: Path, kind: str, reader, *, observations: list | None = None) -> pd.DataFrame:
    found = view(path, kind)
    options = project_options(path)
    if found is not None:
        if not path.exists() and not available(path,kind):
            raise HistoryError('history_original_missing_without_retirement')
        frame = payload_frame(found[0])
        if options[2] == 'shadow':
            actual = reader()
            if digest(frame_payload(actual)) != digest(found[0]):
                raise HistoryError('history_shadow_mismatch:' + str(path) + ':' + kind)
            return actual
        if observations is not None:
            observations.append({'path': str(path), 'role': 'history_' + kind, 'status': 'read',
                                 'sha256': found[1]['digest'], 'history_record': {**found[1], 'path': str(path)}})
        return frame
    return reader()


def verify_embedded(record: dict) -> None:
    if record.get('contract') != CONTRACT or digest(record['payload']) != record['digest']:
        raise HistoryError('history_frozen_record_invalid')


def archived_core_paths(directory: Path, pattern: str) -> list[tuple[Path, float]]:
    options = project_options(directory)
    if not options or options[2] != 'ledger': return []
    root, db, _ = options
    if not db.exists(): raise HistoryError('history_database_unavailable')
    import fnmatch
    parent = relative(root, directory)
    with connection(db) as conn:
        rows = conn.execute("SELECT s.path,s.identity FROM sources s JOIN views v ON s.path=v.path AND s.sha256=v.sha256 WHERE v.kind='core_raw'").fetchall()
    return [(root/p, json.loads(identity)[3]/1e9) for p,identity in rows
            if str(Path(p).parent) == parent and fnmatch.fnmatch(Path(p).name,pattern)]
