"""Import and expose historical facts without creating publication truth."""
from __future__ import annotations

from datetime import datetime
from io import BytesIO
import hashlib
import json
import os
import re
from pathlib import Path
import sqlite3
import uuid
from typing import Any

import pandas as pd
from openpyxl import load_workbook, Workbook

from core.utils import report_history_store as store


def database(settings: Any) -> Path:
    return Path(getattr(settings, 'report_history_database_path', None)
                or settings.project_root / 'backend/data/report_history.sqlite')


def mode(settings: Any) -> str:
    if not getattr(settings,'project_root',None):
        return 'legacy'
    options = store.project_options(Path(settings.project_root))
    return options[2] if options and options[0] == Path(settings.project_root).resolve() else 'legacy'


def resolve(root: Path, value: str) -> Path:
    return root / store.relative(root, Path(value))


def workbook_payload(path: Path) -> dict:
    workbook = load_workbook(path, read_only=True, data_only=True)
    try:
        return {'contract': 'report-history-workbook-v1', 'sheets': [
            {'name': sheet.title, 'rows': list(sheet.values)} for sheet in workbook]}
    finally:
        workbook.close()


def workbook_frame(path: Path, sheet_name: str, *, header: int) -> pd.DataFrame | None:
    """Use the original Excel parser on ledger cell values, without old bytes."""
    found=store.view(path,'workbook')
    if found is None:return None
    if not path.exists() and not store.available(path,'workbook'):
        raise store.HistoryError('history_original_missing_without_retirement')
    sheet=next((sheet for sheet in found[0]['sheets'] if sheet['name']==sheet_name),None)
    if sheet is None:raise store.HistoryError('history_workbook_sheet_missing:'+sheet_name)
    workbook=Workbook();target=workbook.active;target.title=sheet_name
    for row in sheet['rows']:target.append(row)
    stream=BytesIO();workbook.save(stream);workbook.close();stream.seek(0)
    return pd.read_excel(stream,sheet_name=sheet_name,dtype=str,header=header)


def import_source(settings: Any, path: Path, kinds: set[str], *, verify_existing: bool = False) -> dict:
    root, db = Path(settings.project_root), database(settings)
    before = store.file_identity(path)
    existing = set()
    if db.exists():
        for kind in kinds:
            if store.source_info(root, db, path, kind=kind): existing.add(kind)
    wanted = kinds if verify_existing else kinds - existing
    if not wanted:
        with store.connection(db, write=True) as conn:
            for kind in kinds & {'formal','tracker','watch','core_raw'}:
                info = store.source_info(root, db, path, kind=kind)
                if conn.execute('SELECT 1 FROM member_views WHERE path=? AND sha256=? AND kind=? LIMIT 1',
                                (info['path'],info['sha256'],kind)).fetchone():continue
                store.index_members(conn, info['path'], info['sha256'], kind, info['digest'],
                                    store.get_object(conn, info['digest']))
        return {'path': store.relative(root, path), 'status': 'already_imported'}
    views = parse_source_views(path, wanted)
    if store.file_identity(path) != before: raise store.HistoryError('history_source_changed_during_parse')
    return {'status': 'imported', **store.put_views(root, db, path, views, expected_identity=before)}


def parse_source_views(path: Path, kinds: set[str]) -> dict:
    views = {}
    if 'tracker' in kinds:
        from backend.project_tracker import _legacy_load_tracker_source_frame
        frame, signature, nav_date = _legacy_load_tracker_source_frame(path)
        views['tracker'] = {'frame': store.frame_payload(frame), 'signature': signature, 'nav_date': nav_date}
    if 'formal' in kinds:
        from backend.forward_validation import _legacy_load_formal_frame
        capture = {}
        views['formal'] = store.frame_payload(_legacy_load_formal_frame(path, read_capture=capture))
        views['formal']['observed_sources'] = capture.get('sources', [])
    if 'watch' in kinds:
        from backend.forward_validation import _legacy_load_watch_frame
        views['watch'] = store.frame_payload(_legacy_load_watch_frame(path))
    if 'core_raw' in kinds:
        from core.utils.drawdown_duration import normalize_drawdown_duration_columns
        views['core_raw'] = store.frame_payload(normalize_drawdown_duration_columns(pd.read_excel(path, sheet_name=0)))
    if 'workbook' in kinds: views['workbook'] = workbook_payload(path)
    if 'markdown' in kinds: views['markdown'] = {'text': path.read_text(encoding='utf-8')}
    if 'csv' in kinds:
        views['csv'] = store.frame_payload(pd.read_csv(path, dtype=str, keep_default_na=False))
    return views


def report_sources(settings: Any, report: dict, *, report_path: Path | None = None,
                   inputs_only: bool = False) -> dict[str, set[str]]:
    root = Path(settings.project_root); result = {}
    if inputs_only:
        from backend.forward_validation import _resolve_snapshot_artifact_path
        from backend.publication import _resolve_publish_blockers
        # Validation refreshes and failed runs were never forward snapshots.
        # Keep their JSON identity, without inventing workbook dependencies.
        if (_resolve_publish_blockers(report) or not report.get('generated_at')
                or not (((report.get('database_freshness') or {}).get('effective_nav_snapshot') or {}).get('candidate_date'))):
            return result
        for key,kinds in [('filtered_excel',{'formal'}),('watch_excel',{'watch'})]:
            raw=(report.get('artifacts') or {}).get(key)
            if not raw:continue
            resolved=_resolve_snapshot_artifact_path(report_path,raw)
            if resolved is None:resolved=resolve(root,raw)
            result.setdefault(store.relative(root,resolved),set()).update(kinds)
        return result
    for key, kinds in {'filtered_excel': {'tracker', 'formal', 'workbook'},
                       'watch_excel': {'watch'}, 'project_tracker_excel': {'workbook'},
                       'forward_validation_excel': {'workbook'}}.items():
        value = (report.get('artifacts') or {}).get(key)
        if isinstance(value, str) and value:
            result[store.relative(root, Path(value))] = kinds
    def advertised(value):
        if isinstance(value, dict):
            for item in value.values(): advertised(item)
        elif isinstance(value, list):
            for item in value: advertised(item)
        elif isinstance(value, str) and '\n' not in value:
            kind = {'.xlsx': 'workbook', '.csv': 'csv', '.md': 'markdown'}.get(Path(value).suffix)
            if kind:
                rel = store.relative(root, Path(value))
                if (root/rel).is_file(): result.setdefault(rel,set()).add(kind)
    advertised(report.get('artifacts') or {})
    # Core records have business-date replacement semantics, separate from formal NAV dates.
    def collect(value):
        if isinstance(value, dict):
            for item in value.values(): collect(item)
        elif isinstance(value, list):
            for item in value: collect(item)
        elif isinstance(value, str) and __import__('re').fullmatch(r'(?:股票型_|混合型_)?当日最终优质基金_\d{8}_\d{6}\.xlsx', Path(value).name):
            result.setdefault(store.relative(root, Path(value)), set()).update(('core_raw',))
    collect((report.get('artifacts') or {}).get('core', {}))
    return result


def import_report(settings: Any, path: Path, semantic: str, *, require_complete: bool = False,
                  inputs_only: bool = False) -> dict:
    root, db = Path(settings.project_root), database(settings)
    identity = store.file_identity(path); raw = path.read_bytes(); sha = hashlib.sha256(raw).hexdigest()
    report = json.loads(raw)
    revision = (report.get('publication_control') or {}).get('revision')
    revision = str(revision or ('legacy:' + sha))
    if inputs_only and db.exists():
        with store.connection(db) as conn:
            existing=conn.execute('SELECT path,sha256,semantic,digest FROM reports WHERE run_id=? AND revision=?',
                                  (report['run_id'],revision)).fetchone()
            if existing:
                if existing[:3]!=(store.relative(root,path),sha,semantic):
                    raise store.HistoryError('history_report_identity_conflict')
                saved=store.get_object(conn,existing[3])
                return {'contract':saved['contract'],'run_id':report['run_id'],'revision':revision,
                        'digest':existing[3],'sources':len(saved['sources']),'semantic':semantic}
    sources = report_sources(settings, report, report_path=path, inputs_only=inputs_only)
    source_issues = []
    for rel, kinds in list(sources.items()):
        if not (root / rel).exists():
            source_issues.append({'path': rel, 'kinds': sorted(kinds), 'reason': 'original_missing'})
            if require_complete or kinds != {'core_raw'}:
                raise store.HistoryError('history_required_source_missing:' + rel)
            del sources[rel]
        else:
            import_source(settings, root / rel, kinds)
    bindings = []
    for rel, kinds in sorted(sources.items()):
        for kind in sorted(kinds):
            info = store.source_info(root, db, root / rel, kind=kind)
            if info is None: raise store.HistoryError('history_report_source_unavailable:' + rel)
            bindings.append({**{k: v for k, v in info.items() if k != 'identity'}, 'kind': kind})
    payload = {'contract': 'report-history-publication-v1', 'run_id': report['run_id'],
               'revision': revision, 'semantic': semantic, 'report': report,
               'report_sha256': sha, 'sources': bindings, 'source_issues': source_issues}
    if inputs_only:payload['input_scope']='legacy_forward_inputs_not_formal_publication'
    if store.file_identity(path) != identity: raise store.HistoryError('history_report_changed')
    with store.connection(db, write=True) as conn:
        key = store.put_object(conn, payload)
        current = conn.execute('SELECT digest FROM reports WHERE run_id=? AND revision=?',
                               (report['run_id'], revision)).fetchone()
        if current and current[0] != key: raise store.HistoryError('history_report_identity_conflict')
        conn.execute('INSERT OR IGNORE INTO reports VALUES(?,?,?,?,?,?)',
                     (report['run_id'], revision, store.relative(root, path), sha, semantic, key))
        conn.executemany('INSERT OR IGNORE INTO report_sources VALUES(?,?,?)',
                         [(report['run_id'],revision,rel) for rel in sources])
    return {'contract': payload['contract'], 'run_id': report['run_id'], 'revision': revision,
            'digest': key, 'sources': len(bindings), 'semantic': semantic}


def inventory(settings: Any) -> dict:
    root = Path(settings.project_root); reports = {}; sources = {}; input_only_paths=set()
    state_path = root / 'reports/.publication_current/publication_state.json'
    head = json.loads(state_path.read_text()) if state_path.exists() else {}
    state = head; visited = set()
    while state:
        for entry in state.get('committed_runs', {}).values():
            reports[entry['report_path']] = 'committed'
        previous = state.get('previous_generation_dir')
        if not previous or previous in visited: break
        visited.add(previous)
        previous_path = root / previous / 'publication_state.json'
        if not previous_path.exists(): raise store.HistoryError('history_generation_missing:' + previous)
        state = json.loads(previous_path.read_text())
    registry_path = root / 'reports/publication_registry.json'
    registry = json.loads(registry_path.read_text()) if registry_path.exists() else {}
    for record in registry.get('canonical_runs', []):
        rel = store.relative(root, Path(record['report_path']))
        reports.setdefault(rel, 'legacy_eligible' if record.get('publishable') else 'diagnostic')
    # Forward selection historically sees more legacy snapshots than the
    # formal registry. Preserve those input identities without upgrading them.
    from backend.publication_transaction import visible_report_paths
    for path in visible_report_paths(settings):
        rel=store.relative(root,path)
        if rel not in reports:
            reports[rel]='diagnostic';input_only_paths.add(rel)
    entries = []
    for rel, semantic in sorted(reports.items()):
        path = root / rel
        raw = path.read_bytes(); report = json.loads(raw)
        inputs_only=rel in input_only_paths
        entries.append({'path': rel, 'sha256': hashlib.sha256(raw).hexdigest(), 'semantic': semantic,
                        'identity': store.file_identity(path), 'run_id': report['run_id'],'inputs_only':inputs_only})
        for source, kinds in report_sources(settings, report,report_path=path,inputs_only=inputs_only).items():
            sources.setdefault(source, set()).update(kinds)
    for path in Path(settings.core_output_dir).glob('*当日最终优质基金_*.xlsx'):
        sources.setdefault(store.relative(root, path), set()).update(('core_raw', 'workbook'))
    return {'contract': 'report-history-import-plan-v1', 'generated_at': datetime.now().astimezone().isoformat(),
            'generation': head.get('generation'), 'current_run_id': head.get('current_published_run_id'),
            'reports': entries, 'sources': [{'path': path, 'kinds': sorted(kinds), 'identity': store.file_identity(root/path),
                         'sha256': store.original_hash(root/path)} for path, kinds in sorted(sources.items()) if (root/path).is_file()],
            'blocked_sources': [{'path': path, 'kinds': sorted(kinds), 'reason': 'original_missing'}
                                for path, kinds in sorted(sources.items()) if not (root/path).is_file()]}


def import_plan(settings: Any, plan: dict, *, max_units: int | None = None) -> dict:
    if plan.get('contract') != 'report-history-import-plan-v1': raise store.HistoryError('history_plan_invalid')
    root = Path(settings.project_root); done = []; errors = []
    units = [*({'type': 'source', **x} for x in plan['sources']), *({'type': 'report', **x} for x in plan['reports'])]
    for unit in units:
        if max_units is not None and len(done) >= max_units: break
        try:
            path = root / store.relative(root, Path(unit['path']))
            if unit['type'] == 'source':
                if store.file_identity(path) != unit['identity'] or store.original_hash(path) != unit['sha256']:
                    raise store.HistoryError('history_plan_source_changed')
                result = import_source(settings, path, set(unit['kinds']))
            else:
                if hashlib.sha256(path.read_bytes()).hexdigest() != unit['sha256']:
                    raise store.HistoryError('history_plan_source_changed')
                result = import_report(settings, path, unit['semantic'],inputs_only=unit.get('inputs_only',False))
            done.append({'type': unit['type'], 'path': unit['path'], **result})
        except Exception as exc:
            errors.append({'path': unit['path'], 'error': str(exc), 'error_type': type(exc).__name__})
    return {'status': 'complete' if not errors and not plan.get('blocked_sources') and len(done) == len(units) else 'blocked',
            'completed': len(done), 'units': len(units), 'results': done, 'errors': errors, 'unresolved_sources': plan.get('blocked_sources', [])}


def verify(settings: Any, *, compare_sources: bool = True) -> dict:
    db = database(settings); count = 0; errors = []
    with store.connection(db) as conn:
        check = conn.execute('PRAGMA integrity_check').fetchall()
        if check != [('ok',)]: raise store.HistoryError('history_integrity_check_failed')
        for key, in conn.execute('SELECT digest FROM objects'):
            try: store.get_object(conn, key); count += 1
            except Exception as exc: errors.append({'digest': key, 'error': str(exc)})
        rows = conn.execute('SELECT path,sha256,identity FROM sources').fetchall()
        report_count = conn.execute('SELECT count(*) FROM reports').fetchone()[0]
    if compare_sources:
        for rel, sha, identity in rows:
            path = settings.project_root / rel
            if not path.exists():
                from backend.report_history_retention import original_state
                state = original_state(settings.project_root, path)
                if (state.get('status') in ('archived_offline', 'original_retired')
                        and state.get('replacement_record', {}).get('sha256') == sha):
                    continue
                errors.append({'path': rel, 'error': 'original_missing_without_retirement_verification'}); continue
            # Mutable current aliases may have newer verified versions. Immutable sources may not drift.
            if store.file_identity(path) != json.loads(identity):
                with store.connection(db) as conn:
                    newer = conn.execute('SELECT identity FROM sources WHERE path=?', (rel,)).fetchall()
                if not any(json.loads(row[0]) == store.file_identity(path) for row in newer):
                    errors.append({'path': rel, 'error': 'source_identity_changed'})
                continue
            if store.original_hash(path) != sha: errors.append({'path': rel, 'error': 'source_hash_mismatch'})
    return {'status': 'verified' if not errors else 'blocked', 'objects': count, 'sources': len(rows),
            'reports': report_count, 'errors': errors, 'database_bytes': db.stat().st_size}


def _parity_payload(root: Path, kind: str, payload: dict) -> dict:
    """Compare provenance in project coordinates; never rewrite stored objects."""
    if kind != 'formal' or 'observed_sources' not in payload:
        return payload
    sources = []
    for source in payload['observed_sources']:
        item = dict(source)
        raw = item.get('path')
        if raw is not None:
            if not isinstance(raw, str) or not raw or '..' in Path(raw).parts:
                raise store.HistoryError('history_provenance_path_invalid')
            item['path'] = store.relative(root, Path(raw))
        sources.append(item)
    return {**payload, 'observed_sources': sources}


def semantic_audit(settings: Any, *, only_unverified: bool = False) -> dict:
    """Compare every current original against independently reread legacy values.

    A validation binds the original bytes and exact typed payload, not just counts.
    Unavailable old versions are reported and cannot authorize their retirement.
    """
    db = database(settings); root = settings.project_root
    with store.connection(db) as conn:
        query='SELECT DISTINCT v.path,v.sha256 FROM views v'
        if only_unverified:
            query+=' WHERE NOT EXISTS (SELECT 1 FROM validations p WHERE p.path=v.path AND p.sha256=v.sha256 AND p.kind=v.kind AND p.digest=v.digest)'
        sources = conn.execute(query+' ORDER BY v.path').fetchall()
    results = []; errors = []; cells = 0; parsed_digests = {}
    for rel, sha in sources:
        path = root / rel
        try:
            before = store.file_identity(path)
            if store.original_hash(path) != sha:
                raise store.HistoryError('original_version_not_current')
            with store.connection(db) as conn:
                bindings = conn.execute('SELECT kind,digest FROM views WHERE path=? AND sha256=?', (rel, sha)).fetchall()
            needed={kind for kind,_ in bindings if kind in ('formal','tracker') or (sha,kind) not in parsed_digests}
            actual = parse_source_views(path, needed)
            verified = []
            for kind, key in bindings:
                with store.connection(db) as conn:
                    expected = store.get_object(conn, key)
                observed = store.digest(_parity_payload(root, kind, actual[kind])) if kind in actual else parsed_digests[(sha,kind)]
                if observed != store.digest(_parity_payload(root, kind, expected)):
                    raise store.HistoryError('legacy_ledger_typed_values_differ:' + kind)
                if kind not in ('formal','tracker'):parsed_digests[(sha,kind)]=observed
                frame = expected.get('frame', expected)
                if frame.get('contract') == store.CONTRACT:
                    cells += len(frame['rows']) * len(frame['columns'])
                elif kind == 'workbook':
                    cells += sum(len(row) for sheet in expected['sheets'] for row in sheet['rows'])
                verified.append((rel, sha, kind, key, datetime.now().astimezone().isoformat()))
            if store.file_identity(path) != before or store.original_hash(path) != sha:
                raise store.HistoryError('original_changed_during_parity')
            with store.connection(db, write=True) as conn:
                conn.executemany('INSERT OR IGNORE INTO validations VALUES(?,?,?,?,?)', verified)
            results.append({'path': rel, 'sha256': sha, 'views': dict(bindings)})
        except (ValueError, OSError, sqlite3.Error) as exc:
            errors.append({'path': rel, 'sha256': sha, 'error': str(exc)})
    return {'contract': 'report-history-semantic-parity-v1', 'status': 'verified' if not errors else 'blocked',
            'scope':'previously_unverified_sources' if only_unverified else 'all_sources',
            'completed_at': datetime.now().astimezone().isoformat(), 'sources': len(results),
            'compared_cells': cells, 'results': results, 'errors': errors}


def get_report(settings: Any, run_id: str, revision: str | None = None) -> dict:
    from backend.publication_transaction import load_publication_head
    root = settings.project_root; head = load_publication_head(settings)
    entry = (head.get('committed_runs') or {}).get(run_id)
    with store.connection(database(settings)) as conn:
        if revision:
            row = conn.execute('SELECT digest,path,sha256 FROM reports WHERE run_id=? AND revision=?', (run_id, revision)).fetchone()
        elif entry:
            row = conn.execute('SELECT digest,path,sha256 FROM reports WHERE run_id=? AND path=? AND sha256=?',
                               (run_id, entry['report_path'], entry['report_sha256'])).fetchone()
        else:
            registry = json.loads((root / 'reports/publication_registry.json').read_text())
            selected = next((r for r in registry['canonical_runs'] if r['run_id'] == run_id), None)
            if selected is None: raise FileNotFoundError('history_run_not_registered')
            rel = store.relative(root, Path(selected['report_path']))
            sha = hashlib.sha256((root / rel).read_bytes()).hexdigest()
            row = conn.execute('SELECT digest,path,sha256 FROM reports WHERE run_id=? AND path=? AND sha256=?', (run_id, rel, sha)).fetchone()
        if row is None: raise FileNotFoundError('history_revision_not_available')
        payload = store.get_object(conn, row[0])
        if payload['semantic'] == 'committed':
            # Older revisions require proof in the immutable generation chain, not a DB flag.
            state = head; seen = set(); visible = False
            while state:
                candidate = state.get('committed_runs', {}).get(run_id, {})
                if candidate.get('report_path') == row[1] and candidate.get('report_sha256') == row[2]:
                    visible = True; break
                previous = state.get('previous_generation_dir')
                if not previous or previous in seen: break
                seen.add(previous); state = json.loads((root / previous / 'publication_state.json').read_text())
            if not visible: raise FileNotFoundError('history_revision_not_committed')
        else:
            registry = json.loads((root / 'reports/publication_registry.json').read_text())
            if not any(r['run_id'] == run_id and store.relative(root, Path(r['report_path'])) == row[1]
                       for r in registry['canonical_runs']):
                raise FileNotFoundError('history_legacy_not_registered')
            if hashlib.sha256((root / row[1]).read_bytes()).hexdigest() != row[2]:
                raise store.HistoryError('history_legacy_source_changed')
        return {**payload, 'history_digest': row[0]}


def historical_workbook(settings: Any, payload: dict, name: str) -> dict:
    key = {'formal': 'filtered_excel', 'tracker': 'project_tracker_excel',
           'forward_validation': 'forward_validation_excel'}.get(name)
    if key is None: raise ValueError('history_export_option_invalid')
    path = (payload['report'].get('artifacts') or {}).get(key)
    if not path: raise FileNotFoundError('history_view_not_recorded')
    rel = store.relative(settings.project_root, Path(path))
    source = next((s for s in payload['sources'] if s['path'] == rel and s['kind'] == 'workbook'), None)
    if source is None: raise FileNotFoundError('history_view_not_imported')
    with store.connection(database(settings)) as conn:
        workbook = store.get_object(conn, source['digest'])
    return workbook


def _core_history_source(settings: Any, record: dict) -> Path:
    """Resolve legacy output aliases inside this runtime's protected root."""
    try:
        source=resolve(settings.project_root,str(record.get('source_report_file') or ''))
        if source.is_file():return source
    except store.HistoryError:
        # Historical /app/core/.../output aliases can resolve to an old host
        # mount in a restored or web-only container. Never read across roots.
        pass
    name=str(record.get('source_file_name') or '')
    if not name or Path(name).name!=name or name in ('.','..'):
        raise store.HistoryError('frozen_core_source_name_invalid')
    return resolve(settings.project_root,str(settings.core_output_dir/name))


def freeze_read_inputs(settings: Any, report: dict, directory: Path) -> Path | None:
    """Freeze observed historical views and the core's exact processed inputs."""
    if mode(settings)=='legacy':return None
    from core.bond_fund_screening.utils.accumulation_tracker import AccumulationTracker
    core=(report.get('artifacts') or {}).get('core',{})
    raw=core.get('accumulation_tracker_excel');core_history=[]
    if raw:
        path=resolve(settings.project_root,raw)
        tracker=AccumulationTracker(str(path),verbose=False)
        if not tracker._load_tracker():raise store.HistoryError('frozen_core_tracker_unreadable')
        for record in tracker.processed_results:
            source=_core_history_source(settings,record)
            if store.view(source,'core_raw') is None:import_source(settings,source,{'core_raw'})
            found=store.view(source,'core_raw')
            if found is None or found[1]['sha1']!=record['source_file_hash']:
                raise store.HistoryError('frozen_core_source_hash_mismatch')
            core_history.append({'record':record,'source_digest':found[1]['digest']})
    records=store.captured_reads()
    # Each capture owns local hard links, so it stays readable without the pool
    # or SQLite. Unchanged historical payloads consume blocks only once.
    import gzip
    pool=settings.reports_dir/'.report_history_input_pool'
    local=directory/'history_read_objects'
    pool.mkdir(parents=True,exist_ok=True);local.mkdir(exist_ok=True)
    if pool.is_symlink() or local.is_symlink():raise store.HistoryError('history_input_object_symlink')
    proofs=[];objects={}
    for proof in records:
        key=proof['digest'];pooled=pool/(key+'.json.gz')
        if key not in objects:
            if not pooled.exists():
                # Reads already verified this immutable digest. Rehydrate one
                # object at a time, rather than pinning every historical table
                # throughout the whole report build.
                with store.connection(Path(proof['_database_path'])) as conn:
                    payload=store.get_object(conn,key)
                temporary=pool/('.'+uuid.uuid4().hex)
                try:
                    with temporary.open('xb') as stream:
                        stream.write(gzip.compress(store.canonical(payload),mtime=0))
                        stream.flush();os.fsync(stream.fileno())
                    temporary.chmod(0o444)
                    try:os.link(temporary,pooled)
                    except FileExistsError:pass
                finally:temporary.unlink(missing_ok=True)
                del payload
            _load_frozen_object(pooled,key)
            destination=local/pooled.name
            try:os.link(pooled,destination)
            except FileExistsError:_load_frozen_object(destination,key)
            objects[key]='history_read_objects/'+pooled.name
        proofs.append({k:v for k,v in proof.items() if k not in ('payload','_database_path')})
    for folder in (pool,local):
        descriptor=os.open(folder,os.O_DIRECTORY)
        try:os.fsync(descriptor)
        finally:os.close(descriptor)
    value={'contract':'report-history-read-inputs-v2','run_id':report['run_id'],
           'revision':report['publication_control']['revision'],'records':proofs,
           'objects':objects,'core_history':core_history}
    target=directory/'history_read_inputs.json.gz'
    with target.open('xb') as stream:
        stream.write(gzip.compress(store.canonical(value),mtime=0));stream.flush()
        os.fsync(stream.fileno())
    return target


def _load_frozen_object(path: Path, key: str) -> Any:
    import gzip
    if path.is_symlink():raise store.HistoryError('history_input_object_symlink')
    value=store.decode(json.loads(gzip.decompress(path.read_bytes())))
    if store.digest(value)!=key:raise store.HistoryError('history_input_object_digest_mismatch')
    return value


def load_frozen_read_inputs(path: Path) -> dict:
    import gzip
    value=store.decode(json.loads(gzip.decompress(path.read_bytes())))
    contract=value.get('contract')
    if contract not in ('report-history-read-inputs-v1','report-history-read-inputs-v2'):
        raise store.HistoryError('history_read_inputs_contract_invalid')
    keys=set()
    for proof in value['records']:
        if contract=='report-history-read-inputs-v2':
            key=proof['digest'];relative=value['objects'].get(key)
            if not re.fullmatch('[0-9a-f]{64}',key) or relative!='history_read_objects/'+key+'.json.gz':
                raise store.HistoryError('history_input_object_path_invalid')
            folder=path.parent/'history_read_objects'
            if folder.is_symlink():raise store.HistoryError('history_input_object_symlink')
            proof['payload']=_load_frozen_object(path.parent/relative,key)
        store.verify_embedded(proof);keys.add(proof['digest'])
    if any(item['source_digest'] not in keys for item in value['core_history']):
        raise store.HistoryError('history_core_input_missing')
    return value


def storage_metadata(settings: Any, records: list[dict]) -> None:
    """Enrich a report list without making the current report depend on the ledger."""
    from backend.report_history_retention import original_state
    from urllib.parse import urlencode
    try:
        with store.connection(database(settings)) as conn:
            indexed = {(run, path, sha): key for run, path, sha, key in conn.execute(
                'SELECT run_id,path,sha256,digest FROM reports')}
            for record in records:
                rel=store.relative(settings.project_root,Path(record['report_path']))
                path=settings.project_root/rel
                sha=store.original_hash(path)
                key=indexed.get((record['run_id'],rel,sha))
                if not key:
                    record['history_storage']={'status':'not_imported','complete':False};continue
                payload=store.get_object(conn,key);revision=payload['revision'];views={}
                for name,artifact in [('formal','filtered_excel'),('tracker','project_tracker_excel'),
                                      ('forward_validation','forward_validation_excel')]:
                    raw=payload['report'].get('artifacts',{}).get(artifact)
                    if raw:
                        source_path=store.relative(settings.project_root,raw)
                        if any(s['path']==source_path and s['kind']=='workbook' for s in payload['sources']):
                            views[name]={'original_status':original_state(settings.project_root,settings.project_root/source_path)['status'],
                                         'export_url':f'/api/reports/history/{record["run_id"]}/export?'+urlencode({'revision':revision,'view':name,'format':'xlsx'})}
                record['history_storage']={'status':'available','complete':not payload.get('source_issues'),
                    'revision':revision,'semantic':payload['semantic'],'views':views,
                    'view_url':'/history?'+urlencode({'run_id':record['run_id'],'revision':revision})}
    except (ValueError,OSError,sqlite3.Error) as exc:
        for record in records:
            record['history_storage']={'status':'history_unavailable','complete':False,'error':str(exc)}


def export_report(settings: Any, payload: dict, name: str, fmt: str) -> bytes:
    if fmt not in ('xlsx', 'csv'): raise ValueError('history_export_option_invalid')
    workbook = historical_workbook(settings, payload, name)
    if fmt == 'csv':
        sheet = workbook['sheets'][0]
        import csv
        from io import StringIO
        target = StringIO(newline=''); writer = csv.writer(target)
        writer.writerow(['历史重生成', payload['run_id'], payload['revision'], payload['report'].get('generated_at'),
                         datetime.now().astimezone().isoformat()])
        for row in sheet['rows']: writer.writerow(row)
        return target.getvalue().encode('utf-8-sig')
    result = Workbook(write_only=True)
    for sheet in workbook['sheets']:
        ws = result.create_sheet(sheet['name'])
        for row in sheet['rows']: ws.append(row)
    meta = result.create_sheet('历史生成说明')
    for row in [('原run', payload['run_id']), ('原revision', payload['revision']),
                ('原发布时间', payload['report'].get('generated_at')), ('重新生成时间', datetime.now().astimezone().isoformat()),
                ('说明', '历史内容重生成；非原Excel字节；前瞻结果截至原报告时点'), ('账本摘要', payload['history_digest'])]:
        meta.append(row)
    output = BytesIO(); result.save(output); return output.getvalue()
