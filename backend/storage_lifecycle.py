"""Conservative, recoverable offline storage. Business artifacts are never rewritten.

Plans contain immutable file identities and hashes. Only explicitly recognized
families are candidates; unknown materials remain online. The catalog is separate
from publication manifests and doubles as a write-ahead journal for rename/recovery.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from bisect import bisect_left
from contextlib import contextmanager, ExitStack
from datetime import datetime, timedelta
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import time
from typing import Any

SCHEMA = 1
RETENTION_DAYS = 14
DEFAULT_ARCHIVE = Path('/mnt/Project-Archives/Fund-ANA')
CATALOG = 'reports/.audit/storage_lifecycle/catalog.sqlite'
MAX_SECONDS, MAX_FILES, MAX_BYTES = 60, 2000, 8 * 1024**3


def stamp() -> str:
    return datetime.now().astimezone().isoformat()


def safe(root: Path, relative: str, *, allow_leaf_symlink: bool = False) -> Path:
    p = Path(relative)
    if p.is_absolute() or not p.parts or '..' in p.parts:
        raise ValueError('invalid_lifecycle_relative_path')
    target = root / p
    checked = target.parents if allow_leaf_symlink else (target, *target.parents)
    if any(x.is_symlink() for x in checked):
        raise ValueError('lifecycle_symlink_forbidden:' + relative)
    return target


def atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.tmp')
    with temporary.open('w', encoding='utf-8') as stream:
        json.dump(value, stream, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    sync_dir(path.parent)


def sync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def identity(path: Path) -> dict:
    s = path.lstat()
    kind = ('file' if stat.S_ISREG(s.st_mode) else 'directory' if stat.S_ISDIR(s.st_mode)
            else 'symlink' if stat.S_ISLNK(s.st_mode) else None)
    if kind is None:
        raise ValueError('lifecycle_non_regular_entry:' + str(path))
    return dict(kind=kind, device=s.st_dev, inode=s.st_ino, size=s.st_size,
                mtime_ns=s.st_mtime_ns, mode=stat.S_IMODE(s.st_mode), uid=s.st_uid,
                gid=s.st_gid, nlink=s.st_nlink, allocated=s.st_blocks * 512,
                link_target=os.readlink(path) if kind == 'symlink' else None)


def same(actual: dict, expected: dict) -> bool:
    keys = ('kind', 'device', 'inode', 'mtime_ns', 'mode', 'uid', 'gid', 'link_target')
    return all(actual[k] == expected[k] for k in keys) and (
        actual['kind'] == 'directory' or actual['size'] == expected['size'])


def sha(path: Path) -> str:
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def read_json(path: Path) -> dict:
    value = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(value, dict):
        raise ValueError('lifecycle_object_required:' + str(path))
    return value


def references(value: Any, root: Path):
    if isinstance(value, dict):
        for item in value.values():
            yield from references(item, root)
    elif isinstance(value, list):
        for item in value:
            yield from references(item, root)
    elif isinstance(value, str):
        text = value
        if text.startswith('/app/'):
            text = text[5:]
        elif text.startswith(str(root) + '/'):
            text = text[len(str(root)) + 1:]
        if text.startswith(('reports/', 'backend/data/', 'core/')) and '\n' not in text and '..' not in Path(text).parts:
            yield text


def old_name(path: Path, cutoff: datetime) -> bool:
    match = re.search(r'(?<!\d)(20\d{2}-?\d{2}-?\d{2})(?:_?(\d{6}))?', path.name)
    if not match:
        return False
    try:
        date = datetime.strptime(match[1].replace('-', '') + (match[2] or '235959'), '%Y%m%d%H%M%S').astimezone()
        return date < cutoff
    except ValueError:
        return False


def protections(settings: Any, *, now: datetime | None = None) -> dict[str, str]:
    root = Path(settings.project_root).absolute()
    cutoff = (now or datetime.now().astimezone()) - timedelta(days=RETENTION_DAYS)
    protected: dict[str, str] = {}

    def keep(path: Path, reason: str):
        # Resolve aliases only within the project. Formal links must remain valid.
        path = path.resolve()
        if path.is_relative_to(root) and path.exists():
            protected[str(path.relative_to(root))] = reason

    reports = root / 'reports'
    head = reports / '.publication_current/publication_state.json'
    if head.exists():
        state = read_json(head)
        keep(head, 'publication_control')
        for entry in state.get('committed_runs', {}).values():
            for key in ('report_path', 'manifest_path'):
                keep(root / entry[key], 'formal_report_control')
            manifest_path = safe(root, entry['manifest_path'])
            manifest = read_json(manifest_path)
            for name in manifest['files']:
                keep(manifest_path.parent / name, 'formal_manifest')
    # A run can have several committed generations. The current head retains
    # only its latest revision; previous-generation controls are permanent
    # online history too. A prepared state is conservatively retained as well.
    for base in (reports / 'runs', reports / 'archive/superseded_runs'):
        for state_path in base.glob('*/revisions/*/publication_state.json'):
            keep(state_path.parent, 'formal_generation_history')

    # Legacy canonical artifacts, tracker and forward-history references also count.
    registry = reports / 'publication_registry.json'
    if registry.exists():
        for run in read_json(registry).get('canonical_runs', []):
            for ref in references(run, root):
                if '/calculation_inputs/' not in ref and (root / ref).is_file():
                    keep(root / ref, 'canonical_history_reference')
    for path in reports.glob('*.xlsx'):
        keep(path, 'online_tracker')
    output = Path(getattr(settings, 'core_output_dir', reports / 'core_output'))
    # A same-business-date rerun rebuilds the cumulative tracker from its
    # historical daily workbooks. Keeping only the tracker itself is insufficient.
    # Retain the complete daily-result family, including lineage sidecars: the
    # tracker also discovers the latest result for each date by filename.
    for pattern in ('*当日最终优质基金_*', '*累积跟踪*'):
        for path in output.glob(pattern):
            if path.is_file():
                keep(path, 'online_tracker' if '累积跟踪' in path.name else 'cumulative_tracker_rebuild_dependency')
    for path in output.glob('*累积跟踪*_report_meta.json'):
        for name in read_json(path).get('processed_result_files', []):
            if isinstance(name, str) and name and Path(name).name == name:
                source = output / name
                keep(source, 'cumulative_tracker_processed_result')
                keep(source.with_name(source.stem + '_report_meta.json'),
                     'cumulative_tracker_processed_result_lineage')
    for directory in (reports / 'runs/.forward_validation_cache', reports / '.forward_validation_cache',
                      reports / '.forward_validation_preview_cache'):
        for path in directory.glob('*.json'):
            if path.stat().st_size < 8 * 1024**2:
                payload = read_json(path)
                for ref in references(payload, root):
                    keep(root / ref, 'forward_validation_dependency')
                raw = (payload.get('excel_reference') or {}).get('reports_relative_path')
                if isinstance(raw, str) and not Path(raw).is_absolute() and '..' not in Path(raw).parts:
                    keep(reports / raw, 'forward_validation_workbook_reference')

    # Recent runs retain their complete parent collection even across the cutoff.
    pending: list[Path] = []
    for run in (reports / 'runs').glob('*'):
        if run.name.startswith('.'):
            continue
        if run.is_dir() and not old_name(run, cutoff):
            keep(run, 'recent_run_14_days')
            pending.extend(run.glob('bond_analysis_report_*.json'))
            pending.extend(run.glob('revisions/*/bond_analysis_report_*.json'))
            pending.extend(run.glob('calculation_inputs/*index*.json'))
    seen = set()
    while pending:
        path = pending.pop()
        if path in seen or not path.is_file() or path.stat().st_size > 32 * 1024**2:
            continue
        seen.add(path)
        for ref in references(read_json(path), root):
            parts = Path(ref).parts
            if 'collection_calculations' in parts:
                i = parts.index('collection_calculations')
                if len(parts) > i + 2:
                    parent = root.joinpath(*parts[:i + 3])
                    keep(parent, 'recent_run_parent_collection')
                    pending.extend(parent.glob('*.json'))
            elif '/calculation_inputs/' in ref:
                parent = (root / ref).parent
                keep(parent, 'recent_capture_dependency')

    # Active cold database bindings and configuration are runtime dependencies.
    for family in ('nav_history_archive', 'rank_history_archive', 'screening_history_archive'):
        directory = reports / 'audit' / family
        if directory.exists():
            keep(directory, 'active_cold_binding_and_recovery_evidence')
            # Query readers bind the newest successful audit. Keep its recovery
            # references too; older historical audits may refer to offline units.
            for pattern in ('*apply_runner_*.json', '*maintenance_readiness_audit_*.json'):
                paths = sorted(directory.glob(pattern), reverse=True)
                for path in paths:
                    payload = read_json(path)
                    if payload.get('status') in ('failed', 'blocked', 'error'):
                        continue
                    for ref in references(payload, root):
                        if (root / ref).is_file():
                            keep(root / ref, 'current_maintenance_recovery_binding')
                    break
    for path in (root / 'config').glob('*.json'):
        for ref in references(read_json(path), root):
            keep(root / ref, 'configuration_dependency')
    history_backup = reports / '.audit/report_history/latest_backup.json'
    if history_backup.is_file():
        value=read_json(history_backup)
        for ref in references({k:value.get(k) for k in ('artifact_path','verification_manifest_path')},root):
            keep(root/ref,'latest_history_ledger_recovery_point')
    # Ordinary intermediate-export policies must not bypass the separate
    # acceptance/receipt policy for originals already bound to the ledger.
    from backend import report_history
    if report_history.mode(settings) in ('shadow', 'ledger'):
        from core.utils import report_history_store
        with report_history_store.connection(report_history.database(settings)) as conn:
            registered = [row[0] for row in conn.execute('SELECT DISTINCT path FROM sources')]
        for relative in registered:
            source = root / report_history_store.relative(root, Path(relative))
            for path in (source, source.with_name(source.stem + '_report_meta.json')):
                if str(path.relative_to(root)) not in protected:
                    keep(path, 'report_history_registered_source')
    return protected


def blocked(relative: str, protected: dict[str, str], keys: list[str] | None = None) -> list[str]:
    keys = sorted(protected) if keys is None else keys
    parents = [relative, *(str(p) for p in Path(relative).parents if str(p) != '.')]
    result = [f'{p}:{protected[p]}' for p in parents if p in protected]
    prefix = relative + '/'
    position = bisect_left(keys, prefix)
    while position < len(keys) and keys[position].startswith(prefix):
        key = keys[position]
        result.append(f'{key}:{protected[key]}')
        position += 1
    return result


def open_inodes() -> set[tuple[int, int]]:
    """Fail closed if process inspection is restricted (host or privileged container)."""
    from backend.storage_descriptor_audit import configured_socket, request_host_inodes
    descriptor_socket = configured_socket()
    if descriptor_socket is not None:
        return request_host_inodes(descriptor_socket)
    result = set()
    for proc in Path('/proc').glob('[0-9]*'):
        try:
            descriptors = list((proc / 'fd').iterdir())
        except FileNotFoundError:
            continue
        except PermissionError as exc:
            raise ValueError('cannot_inspect_open_files') from exc
        for fd in descriptors:
            try:
                s = fd.stat()
                result.add((s.st_dev, s.st_ino))
            except FileNotFoundError:
                continue
    return result


def entries(root: Path, paths: list[str]) -> list[dict]:
    result = []
    for relative in paths:
        path = safe(root, relative)
        targets = [path, *sorted(path.rglob('*'))] if path.is_dir() else [path]
        for item in targets:
            info = identity(item)
            result.append({'path': str(item.relative_to(root)), 'identity': info})
    return result


def candidates(settings: Any, now: datetime, protected: dict[str, str], *, report_after: str = '') -> tuple[list[dict], list[dict]]:
    root = Path(settings.project_root).absolute()
    cutoff = now - timedelta(days=RETENTION_DAYS)
    choices, retained = [], []
    protection_keys = sorted(protected)

    def offer(paths: list[Path], family: str, action: str = 'archive'):
        paths = [p for p in paths if p.exists() and not p.is_symlink()]
        if not paths:
            return
        rels = [str(p.relative_to(root)) for p in paths]
        reasons = [reason for rel in rels for reason in blocked(rel, protected, protection_keys)]
        if reasons:
            retained.append(dict(paths=rels, reason='production_dependency', references=reasons))
            return
        try:
            items = entries(root, rels)
            # Archival/compression maintenance can refresh historical evidence
            # mtime. Capture terminal/index dates and audited family identity
            # determine age; actual mtime is mandatory for mutable logs/tests.
            if family in ('expired_log', 'expired_test_workspace') and any(
                    e['identity']['mtime_ns'] >= int(cutoff.timestamp() * 1e9) for e in items):
                retained.append(dict(paths=rels, reason='actual_mtime_within_14_days'))
                return
        except (OSError, ValueError) as exc:
            retained.append(dict(paths=rels, reason=str(exc)))
            return
        choices.append(dict(id=hashlib.sha256('\n'.join(rels).encode()).hexdigest(),
                            paths=rels, family=family, action=action, entries=items))

    from backend import report_history_retention as retirement
    if (root/'config/project.yaml').exists() and retirement.options(settings).get('quarantine_enabled'):
        gate = retirement.acceptance(settings)
        if gate['ready']:
            from backend.report_history import database
            from core.utils.report_history_store import connection
            with connection(database(settings)) as history_conn:
                paths = [row[0] for row in history_conn.execute('SELECT DISTINCT path FROM views WHERE path>? ORDER BY path',(report_after,))]
            discovery_deadline=time.monotonic()+5
            last_path=report_after
            for rel in paths:
                if time.monotonic()>discovery_deadline:
                    retained.append({'paths':[],'reason':'report_view_discovery_budget','next_after':last_path})
                    break
                last_path=rel
                path = root/rel
                if path.is_file() and not retirement.eligibility(settings,path,now=now,check_gate=False):
                    reasons = [x for x in blocked(rel,protected,protection_keys)
                               if x.rsplit(':',1)[-1] not in retirement.RELEASE_REASONS]
                    if not reasons:
                        items=entries(root,[rel])
                        choices.append(dict(id=hashlib.sha256((retirement.FAMILY+'\n'+rel).encode()).hexdigest(),paths=[rel],
                                            family=retirement.FAMILY,action='archive',entries=items))
        else:
            retained.append({'paths':[], 'reason':'report_retirement_acceptance_pending','references':gate['reasons']})
    runs = root / 'reports/runs'
    for run in sorted(runs.glob('*')):
        if not run.is_dir() or not old_name(run, cutoff):
            continue
        capture = run / 'calculation_inputs'
        if capture.is_dir():
            # Never split a capture from its original execution index and logs.
            indexes = list(capture.glob('calculation_input_index_*.json'))
            if len(indexes) == 1 and read_json(indexes[0]).get('capture_complete') is True:
                offer([capture, run / 'logs/subprocess'], 'workflow_capture')
            else:
                retained.append(dict(paths=[str(capture.relative_to(root))], reason='incomplete_or_unknown_capture'))
        for revision in sorted((run / 'revisions').glob('*')):
            if revision.is_dir():
                offer([revision], 'uncommitted_revision')
    for capture in sorted((root / 'reports/.audit/collection_calculations').glob('*/*')):
        if not capture.is_dir() or not old_name(capture, cutoff):
            continue
        terminal = capture / 'terminal.json'
        if terminal.exists() and read_json(terminal).get('capture_complete') is True:
            offer([capture], 'collection_capture')
    # This historical restore chain has been audited as one indivisible unit.
    offer([root / 'reports/audit/implementation_20260907'], 'historical_implementation_restore_chain')

    # Unique migration backups are intentionally NOT a general age-based family.
    # Only audited legacy recovery units are eligible; all current bindings/recent
    # backup families remain under the existing typed maintenance retention policy.
    backups = root / 'backend/data/backups'
    for path in sorted(backups.glob('report_history_*.db.zst')):
        manifest=path.with_name(path.name+'.manifest.json')
        if old_name(path,cutoff) and manifest.is_file():
            snapshot=read_json(manifest).get('snapshot',{})
            if snapshot.get('purpose')=='report_history':
                offer([path,manifest],'superseded_history_ledger_backup')
    for name in ('bond_funds_pre_screening_cleanup_20260906_043724.db.zst',
                 'bond_funds_pre_task_history_cleanup_20260906_060907.db.zst'):
        path = backups / name
        offer([path, path.with_name(path.name + '.manifest.json')], 'legacy_unique_backup')
    offer([*sorted(backups.glob('rank_*_pre_636_20260908_*.db.zst')),
           *sorted(backups.glob('rank_*_pre_636_20260908_*.db.zst.manifest.json'))], 'paired_rank_636_recovery')
    cold = root / 'backend/data/archives'
    for path in sorted(cold.glob('screening_history_details_*.db.zst')):
        if any(tag in path.name for tag in ('202605', '20260822_20260912')):
            offer([path, path.with_name(path.name + '.manifest.json')], 'legacy_screening_sqlite_recovery')

    output = Path(getattr(settings, 'core_output_dir', root / 'reports/core_output'))
    latest = {}
    for path in sorted(output.glob('*')):
        # Dated generated exports only. Fixed workbooks, catalogs and hidden
        # controls have no age-based eligibility.
        if path.is_file() and re.search(r'20\d{6}_\d{6}', path.name):
            family = re.sub(r'20\d{6}_\d{6}.*', '', path.name)
            latest[family] = path
    consumed = set()
    for path in sorted(output.glob('*')):
        logical = path
        for suffix in ('.zst.receipt.json', '.zst'):
            if path.name.endswith('.csv' + suffix):
                logical = path.with_name(path.name.removesuffix(suffix))
        if logical in consumed:
            continue
        consumed.add(logical)
        if path.is_file() and old_name(logical, cutoff) and path not in latest.values():
            # CSV plus its transparent-storage receipt/representation form one unit.
            peers = ([logical, logical.with_name(logical.name + '.zst'), logical.with_name(logical.name + '.zst.receipt.json')]
                     if logical.suffix == '.csv' else [path])
            if any(p in latest.values() for p in peers):
                continue
            offer(peers, 'old_intermediate_export')
    # Legacy physical cache pairs are retained together offline. Modern small
    # indexes and every workbook they reference stay online.
    cache = runs / '.forward_validation_cache'
    for path in sorted(cache.glob('*.json')):
        data = read_json(path)
        generated = (data.get('summary') or {}).get('generated_at')
        try:
            expired = isinstance(generated, str) and datetime.fromisoformat(generated).astimezone() < cutoff
        except ValueError:
            expired = False
        if 'excel_reference' not in data and path.with_suffix('.xlsx').exists() and expired:
            offer([path, path.with_suffix('.xlsx')], 'legacy_forward_cache_pair')
    # Explicit one-off research families; never age-delete the generic audit
    # tree, active binding audits, or unique recovery controls.
    for family in ('score_dedup_shadow', 'annual_return_gate_shadow', 'holding_no_signal_zero_shadow',
                   'dynamic_threshold_robustness', 'low_vol_risk_metric_distortion',
                   'low_vol_annual_positive_ratio_overlap'):
        directory = root / 'reports/audit' / family
        for path in sorted(directory.glob('*.json')):
            if path.stat().st_mtime < cutoff.timestamp() and old_name(path, cutoff):
                prefix = path.stem
                peers = [p for p in directory.glob(prefix + '*') if p.is_file()]
                if peers and max(p.stat().st_mtime for p in peers) < cutoff.timestamp():
                    offer(peers, 'historical_research_unit')
    # Test workspaces need an explicit successful completion receipt before delete.
    for directory in sorted((root / 'reports/audit/isolated_tests').glob('*')):
        if not directory.is_dir():
            continue
        receipt = directory / 'summary.json'
        if receipt.exists():
            data = read_json(receipt)
            if directory.stat().st_mtime >= cutoff.timestamp():
                continue
            # Keep source recovery/acceptance evidence; only temporary workspaces
            # can eventually be deleted. This first policy archives whole runs.
            offer([directory], 'expired_test_workspace', 'archive')
    for directory in (root / 'logs', root / 'core/logs'):
        for path in sorted(directory.glob('*')):
            if path.is_file() and path.suffix in ('.log', '.gz') and old_name(path, cutoff):
                if os.environ.get('FUND_ANA_STORAGE_LIFECYCLE_PROJECT_ROOT'):
                    retained.append(dict(paths=[str(path.relative_to(root))], reason='host_process_descriptor_check_required'))
                    continue
                offer([path], 'expired_log', 'delete')
    # Prevent nested/double units even with unusual repository layouts.
    accepted, used, descendants = [], set(), set()
    for unit in choices:
        if any(p in descendants or p in used or any(str(parent) in used for parent in Path(p).parents)
               for p in unit['paths']):
            retained.append(dict(paths=unit['paths'], reason='overlapping_unit'))
        else:
            accepted.append(unit)
            used.update(unit['paths'])
            for p in unit['paths']:
                descendants.update(str(parent) for parent in Path(p).parents)
    return accepted, retained


def build_plan(settings: Any, *, now: datetime | None = None, max_seconds: float | None = None,
               families: set[str] | None = None, report_after: str = '') -> dict:
    start = time.monotonic()
    now = now or datetime.now().astimezone()
    root = Path(settings.project_root).absolute()
    protected = protections(settings, now=now)
    choices, retained = candidates(settings, now, protected, report_after=report_after)
    if families is not None:
        choices = [u for u in choices if u['family'] in families]
    opened, hashes, units, planning_complete = open_inodes(), {}, [], True
    for unit in choices:
        if max_seconds is not None and time.monotonic() - start >= max_seconds:
            planning_complete = False
            break
        if any((e['identity']['device'], e['identity']['inode']) in opened for e in unit['entries']):
            retained.append(dict(paths=unit['paths'], reason='open_file_descriptor'))
            continue
        if unit['family'] in ('workflow_capture', 'collection_capture'):
            from backend.calculation_input_capture import validate_calculation_input_index
            indexes = [safe(root, e['path']) for e in unit['entries']
                       if Path(e['path']).name.startswith('calculation_input_index_') and e['path'].endswith('.json')]
            try:
                if len(indexes) != 1:
                    raise ValueError('single_complete_capture_index_required')
                validate_calculation_input_index(indexes[0])
            except (OSError, ValueError, RuntimeError) as exc:
                retained.append(dict(paths=unit['paths'], reason='capture_validation_failed', error=str(exc)))
                continue
        complete = True
        for entry in unit['entries']:
            p = safe(root, entry['path'], allow_leaf_symlink=True)
            info = entry['identity']
            if not same(identity(p), info):
                raise ValueError('identity_changed_during_planning:' + entry['path'])
            if info['kind'] == 'file':
                if max_seconds is not None and time.monotonic() - start >= max_seconds:
                    complete = False
                    break
                key = (info['device'], info['inode'], info['size'], info['mtime_ns'])
                if key not in hashes:
                    hashes[key] = sha(p)
                if not same(identity(p), info):
                    raise ValueError('identity_changed_during_hash:' + entry['path'])
                entry['sha256'] = hashes[key]
        if complete:
            units.append(unit)
        else:
            planning_complete = False
            break
    policy = dict(keep_days=RETENTION_DAYS, archive_root=str(getattr(settings, 'storage_lifecycle_archive_root', DEFAULT_ARCHIVE)),
                  formal_reports='all_online', unknown='retain', same_disk_migration_frees_disk=False)
    return dict(schema_version=SCHEMA, created_at=stamp(), project_root=str(root), policy=policy,
                publication_head_sha256=sha(root / 'reports/.publication_current/publication_state.json')
                if (root / 'reports/.publication_current/publication_state.json').exists() else None,
                protected=protected, retained=retained, units=units,
                planning_complete=planning_complete,
                summary=dict(units=len(units), files=sum(e['identity']['kind'] == 'file' for u in units for e in u['entries']),
                             logical_bytes=sum(e['identity']['size'] for u in units for e in u['entries'] if e['identity']['kind'] == 'file')))


def connect(root: Path) -> sqlite3.Connection:
    path = safe(root, CATALOG)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=1)
    conn.execute('PRAGMA synchronous=FULL')
    conn.execute('CREATE TABLE IF NOT EXISTS units(id TEXT PRIMARY KEY, plan_sha TEXT NOT NULL, state TEXT NOT NULL, payload TEXT NOT NULL, updated_at TEXT NOT NULL, error TEXT)')
    conn.execute('CREATE TABLE IF NOT EXISTS report_retirement_times(unit TEXT PRIMARY KEY, quarantined_at TEXT NOT NULL)')
    conn.execute('CREATE TABLE IF NOT EXISTS unit_paths(path TEXT NOT NULL,unit_id TEXT NOT NULL,PRIMARY KEY(path,unit_id))')
    conn.execute('CREATE TABLE IF NOT EXISTS verified(unit TEXT NOT NULL, path TEXT NOT NULL, identity TEXT NOT NULL, PRIMARY KEY(unit,path))')
    conn.commit()
    return conn


@contextmanager
def locks(settings: Any):
    from backend.storage import WorkflowStore
    root = Path(settings.project_root).absolute()
    store = WorkflowStore(settings.state_file)
    try:
        with store._lock:
            store._acquire_run_lock_locked('storage_lifecycle')
        with ExitStack() as stack:
            paths = ['reports/.publication.lock', 'reports/.audit/storage_lifecycle/.lock',
                     'reports/.calculation_input_pool/.pool.lock', 'reports/.report_artifact_pool/.lock',
                     'reports/runs/.forward_validation_cache/.cache.lock',
                     'backend/data/backups/.backup_creation.lock', 'backend/data/backups/.backup_compression.lock']
            for relative in paths:
                path = safe(root, relative)
                path.parent.mkdir(parents=True, exist_ok=True)
                handle = stack.enter_context(path.open('a+b'))
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield
    finally:
        store.close()


def _write_unit(conn, unit: dict, plan_sha: str, state: str, error: str | None = None):
    conn.execute('INSERT INTO units VALUES(?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET state=excluded.state,updated_at=excluded.updated_at,error=excluded.error',
                 (unit['id'], plan_sha, state, json.dumps(unit, ensure_ascii=False), stamp(), error))
    conn.executemany('INSERT OR IGNORE INTO unit_paths VALUES(?,?)',[(p,unit['id']) for p in unit['paths']])
    if unit.get('family') == 'regeneratable_report_view':
        if state == 'offline':
            conn.execute('INSERT OR REPLACE INTO report_retirement_times VALUES(?,?)',(unit['id'],stamp()))
        elif state == 'online':
            conn.execute('DELETE FROM report_retirement_times WHERE unit=?',(unit['id'],))
    if state in ('offline', 'online', 'deleted', 'retired'):
        # Resume cache belongs to one operation, not to the lifetime of a file.
        # Restore must read SHA again, including when metadata still matches.
        conn.execute('DELETE FROM verified WHERE unit=?', (unit['id'],))
    conn.commit()


def _validate_plan(plan: dict, root: Path):
    if (plan.get('schema_version') != SCHEMA or Path(plan.get('project_root', '')) != root
            or plan.get('policy', {}).get('keep_days') != RETENTION_DAYS):
        raise ValueError('lifecycle_plan_identity_mismatch')
    for unit in plan['units']:
        if unit['action'] not in ('archive', 'delete'):
            raise ValueError('unknown_lifecycle_action')
        for relative in unit['paths']:
            safe(root, relative)
        for e in unit['entries']:
            safe(root, e['path'], allow_leaf_symlink=True)
            if not any(e['path'] == r or e['path'].startswith(r + '/') for r in unit['paths']):
                raise ValueError('entry_outside_unit')
            if e['identity']['kind'] == 'file' and not re.fullmatch('[0-9a-f]{64}', e.get('sha256', '')):
                raise ValueError('plan_hash_required')


def run_plan(settings: Any, plan: dict, *, apply: bool = False, restore: bool = False,
             unit_ids: set[str] | None = None, max_seconds: float = MAX_SECONDS,
             max_files: int = MAX_FILES, max_bytes: int = MAX_BYTES) -> dict:
    root = Path(settings.project_root).absolute()
    _validate_plan(plan, root)
    archive = Path(getattr(settings, 'storage_lifecycle_archive_root', DEFAULT_ARCHIVE)).absolute()
    if str(archive) != plan['policy']['archive_root'] or archive.is_relative_to(root) or root.is_relative_to(archive):
        raise ValueError('archive_root_mismatch_or_nested')
    if any(p.is_symlink() for p in (archive, *archive.parents)):
        raise ValueError('archive_root_symlink')
    digest = hashlib.sha256(json.dumps(plan, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    result = dict(schema_version=SCHEMA, started_at=stamp(), mode='restore' if restore else 'apply' if apply else 'preview',
                  plan_sha256=digest, status='preview', completed=[], deferred=[], errors=[], verified_files=0,
                  verified_bytes=0, migrated_logical_bytes=0, deleted_last_link_allocated_bytes=0)
    if not apply:
        result['candidate_units'] = len(plan['units'])
        return result
    started = time.monotonic()
    selected = [u for u in plan['units'] if unit_ids is None or u['id'] in unit_ids]
    try:
        with locks(settings):
            archive.mkdir(parents=True, exist_ok=True)
            if any(e['identity']['device'] != archive.stat().st_dev for u in selected for e in u['entries']):
                raise ValueError('same_filesystem_rename_required')
            current = protections(settings)
            protection_keys = sorted(current)
            opened = open_inodes()
            with connect(root) as conn:
                for unit in selected:
                    if time.monotonic() - started >= max_seconds:
                        result['deferred'].append({'unit': unit['id'], 'reason': 'time_budget'})
                        break
                    row = conn.execute('SELECT plan_sha,state,payload FROM units WHERE id=?', (unit['id'],)).fetchone()
                    state = row[1] if row else 'planned'
                    if row and row[0] != digest:
                        # A fresh daily plan may find an interrupted, unchanged unit.
                        if json.loads(row[2]) != unit:
                            raise ValueError('unit_already_bound_to_other_plan')
                    if not restore and state in ('offline', 'deleted'):
                        continue
                    if restore and state not in ('offline', 'restoring', 'moving', 'error'):
                        raise ValueError('unit_not_restorable')
                    if restore and unit['action'] == 'delete':
                        raise ValueError('deleted_temporary_unit_not_restorable')
                    if restore and state != 'restoring':
                        conn.execute('DELETE FROM verified WHERE unit=?', (unit['id'],))
                        conn.commit()
                    if not restore:
                        reasons = [x for p in unit['paths'] for x in blocked(p, current, protection_keys)]
                        if unit.get('family') == 'regeneratable_report_view':
                            from backend import report_history_retention as retirement
                            policy_reasons=retirement.eligibility(settings,root/unit['paths'][0])
                            reasons=[x for x in reasons if x.rsplit(':',1)[-1] not in retirement.RELEASE_REASONS]
                            reasons.extend(policy_reasons)
                        if reasons:
                            result['deferred'].append({'unit': unit['id'], 'reason': 'new_production_dependency', 'references': reasons})
                            continue
                    # Moving/restoring are durable intent records. Recovery accepts
                    # each root on exactly one side, never merges directory trees.
                    locations = {}
                    for rel in unit['paths']:
                        src, dst = safe(root, rel), safe(archive, rel)
                        if src.exists() and dst.exists():
                            raise ValueError('both_online_and_archive_exist:' + rel)
                        if not src.exists() and not dst.exists():
                            raise ValueError('both_online_and_archive_missing:' + rel)
                        locations[rel] = src if src.exists() else dst
                    items = []
                    for rel, location in locations.items():
                        items.extend(entries(root if location.is_relative_to(root) else archive, [rel]))
                    if {e['path'] for e in items} != {e['path'] for e in unit['entries']}:
                        raise ValueError('unit_members_changed:' + unit['id'])
                    _write_unit(conn, unit, row[0] if row else digest, 'restoring' if restore else 'verifying')
                    ready = True
                    for entry in unit['entries']:
                        rel, expected = entry['path'], entry['identity']
                        top = next(p for p in unit['paths'] if rel == p or rel.startswith(p + '/'))
                        base = root if locations[top].is_relative_to(root) else archive
                        path = safe(base, rel, allow_leaf_symlink=True)
                        actual = identity(path)
                        if not same(actual, expected):
                            raise ValueError('identity_changed:' + rel)
                        if (actual['device'], actual['inode']) in opened:
                            result['deferred'].append({'unit': unit['id'], 'reason': 'open_file_descriptor'})
                            ready = False
                            break
                        if actual['kind'] != 'file':
                            continue
                        cache_key = json.dumps({**actual, 'ctime_ns': path.lstat().st_ctime_ns}, sort_keys=True)
                        cached = conn.execute('SELECT identity FROM verified WHERE unit=? AND path=?', (unit['id'], rel)).fetchone()
                        if not cached or cached[0] != cache_key:
                            if (result['verified_files'] >= max_files or result['verified_bytes'] + actual['size'] > max_bytes
                                    or time.monotonic() - started >= max_seconds):
                                result['deferred'].append({'unit': unit['id'], 'reason': 'verification_budget'})
                                ready = False
                                break
                            if sha(path) != entry['sha256'] or not same(identity(path), expected):
                                raise ValueError('sha_or_identity_changed:' + rel)
                            result['verified_files'] += 1
                            result['verified_bytes'] += actual['size']
                            conn.execute('INSERT OR REPLACE INTO verified VALUES(?,?,?)', (unit['id'], rel, cache_key))
                            conn.commit()
                    if not ready:
                        break
                    # Recheck open descriptors and identities at the mutation boundary.
                    opened = open_inodes()
                    for entry in unit['entries']:
                        rel = entry['path']
                        top = next(p for p in unit['paths'] if rel == p or rel.startswith(p + '/'))
                        base = root if locations[top].is_relative_to(root) else archive
                        info = identity(safe(base, rel, allow_leaf_symlink=True))
                        if not same(info, entry['identity']) or (info['device'], info['inode']) in opened:
                            raise ValueError('unit_changed_at_commit:' + rel)
                    if unit.get('family') == 'regeneratable_report_view' and not restore:
                        from backend.report_history_retention import record_intent
                        record_intent(settings,unit)
                    _write_unit(conn, unit, row[0] if row else digest, 'restoring' if restore else 'moving')
                    if unit['action'] == 'delete' and not restore:
                        # Only plain files are automatically deleted. Directory test
                        # workspaces remain recoverable until a dedicated verifier exists.
                        if any(e['identity']['kind'] != 'file' for e in unit['entries']):
                            raise ValueError('recursive_delete_not_permitted')
                        for entry in unit['entries']:
                            path = safe(root, entry['path'])
                            s = path.stat()
                            if s.st_nlink == 1:
                                result['deleted_last_link_allocated_bytes'] += s.st_blocks * 512
                            path.unlink()
                            sync_dir(path.parent)
                        terminal = 'deleted'
                    else:
                        for rel in unit['paths']:
                            src, dst = (safe(archive, rel), safe(root, rel)) if restore else (safe(root, rel), safe(archive, rel))
                            if dst.exists() and not src.exists():
                                continue  # crash after a previous rename
                            dst.parent.mkdir(parents=True, exist_ok=True)
                            os.rename(src, dst)
                            sync_dir(src.parent)
                            sync_dir(dst.parent)
                        # Same-filesystem rename keeps every file inode. SHA was
                        # checked above; post-rename identity closes the rename window.
                        base = root if restore else archive
                        for entry in unit['entries']:
                            if not same(identity(safe(base, entry['path'], allow_leaf_symlink=True)), entry['identity']):
                                raise ValueError('post_rename_identity_changed')
                        terminal = 'online' if restore else 'offline'
                        result['migrated_logical_bytes'] += sum(e['identity']['size'] for e in unit['entries'] if e['identity']['kind'] == 'file')
                    _write_unit(conn, unit, row[0] if row else digest, terminal)
                    result['completed'].append({'unit': unit['id'], 'state': terminal, 'paths': unit['paths']})
                result['status'] = 'deferred' if result['deferred'] else 'complete'
    except (BlockingIOError, RuntimeError) as exc:
        from backend.storage import WorkflowLockBusyError
        if isinstance(exc, (BlockingIOError, WorkflowLockBusyError)):
            result.update(status='deferred', skip_reason='workflow_or_storage_lock_busy')
        else:
            raise
    except (OSError, ValueError) as exc:
        result['status'] = 'error'
        result['errors'].append(str(exc))
    result['finished_at'] = stamp()
    return result


def artifact_location(root: Path, path: Path) -> dict:
    """Read catalog only; the web process does not need access to the archive."""
    if path.exists():
        return {'status': 'online'}
    catalog = root / CATALOG
    if not catalog.exists():
        return {'status': 'missing'}
    relative = str(path.relative_to(root))
    aliases = (relative, relative + '.legacy.gz', relative + '.zst')
    try:
        with sqlite3.connect(catalog.as_uri() + '?mode=ro', uri=True, timeout=1) as conn:
            rows=[]
            if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='unit_paths'").fetchone():
                parents=sorted({str(p) for alias in aliases for p in (Path(alias),*Path(alias).parents) if str(p)!='.'})
                marks=','.join('?' for _ in parents)
                rows=conn.execute(f'SELECT DISTINCT u.state,u.payload,u.updated_at FROM units u JOIN unit_paths p ON u.id=p.unit_id WHERE p.path IN ({marks}) ORDER BY u.updated_at DESC',parents).fetchall()
            if not rows:
                rows=conn.execute('SELECT state,payload,updated_at FROM units ORDER BY updated_at DESC')
            for state, payload, updated in rows:
                unit = json.loads(payload)
                if any(alias == p or alias.startswith(p + '/') for p in unit['paths'] for alias in aliases):
                    return {'status': 'original_retired' if state == 'retired' else 'archived_offline' if state == 'offline' else 'corrupt' if state == 'corrupt' else 'storage_unavailable',
                            'unit_id': unit['id'], 'last_verified_at': updated}
    except (OSError, sqlite3.Error, ValueError):
        return {'status': 'storage_catalog_corrupt'}
    return {'status': 'missing'}


def verify_offline(settings: Any, *, unit_ids: set[str] | None = None) -> dict:
    root = Path(settings.project_root).absolute()
    archive = Path(getattr(settings, 'storage_lifecycle_archive_root', DEFAULT_ARCHIVE))
    result = {'verified': [], 'errors': []}
    with locks(settings), connect(root) as conn:
        for uid, payload in conn.execute("SELECT id,payload FROM units WHERE state='offline'").fetchall():
            if unit_ids is not None and uid not in unit_ids:
                continue
            unit = json.loads(payload)
            try:
                for e in unit['entries']:
                    path = safe(archive, e['path'], allow_leaf_symlink=True)
                    if not same(identity(path), e['identity']) or (e['identity']['kind'] == 'file' and sha(path) != e['sha256']):
                        raise ValueError('offline_identity_or_sha_mismatch:' + e['path'])
                result['verified'].append(uid)
            except (OSError, ValueError) as exc:
                conn.execute("UPDATE units SET state='corrupt',error=?,updated_at=? WHERE id=?", (str(exc), stamp(), uid))
                conn.commit()
                result['errors'].append(str(exc))
    return result
