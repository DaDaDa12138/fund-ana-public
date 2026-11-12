from datetime import datetime, timedelta
import inspect
import json
import os
from pathlib import Path
from types import SimpleNamespace

from flask import Flask
import pytest

from backend import storage_lifecycle as storage


@pytest.fixture
def settings(tmp_path, monkeypatch):
    root = tmp_path / 'project'
    root.mkdir()
    options = SimpleNamespace(project_root=root, reports_dir=root / 'reports', state_file=root / 'state.json',
                              core_output_dir=root / 'reports/core_output', storage_lifecycle_archive_root=tmp_path / 'offline')
    # These are synthetic storage fixtures, not fabricated fund business data.
    monkeypatch.setattr('backend.calculation_input_capture.validate_calculation_input_index', lambda path: {})
    # Landlock intentionally prevents stat of unrelated process descriptors.
    # Exercise real descriptors only for this synthetic workspace.
    def own_descriptors():
        result = set()
        for fd in Path('/proc/self/fd').iterdir():
            try:
                target = fd.readlink()
                if str(target).startswith(str(root)):
                    info = fd.stat()
                    result.add((info.st_dev, info.st_ino))
            except FileNotFoundError:
                continue
        return result
    monkeypatch.setattr(storage, 'open_inodes', own_descriptors)
    return options


def old(path, content=b'original storage fixture', days=20):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    t = (datetime.now().astimezone() - timedelta(days=days)).timestamp()
    os.utime(path, (t, t))
    for parent in path.parents:
        if parent.name in ('project', 'offline'):
            break
        os.utime(parent, (t, t))
    return path


def capture(settings, run='20260410_010203_sync'):
    directory = settings.reports_dir / 'runs' / run / 'calculation_inputs'
    file = old(directory / 'original.pack')
    old(directory / ('calculation_input_index_' + run + '.json'), b'{"capture_complete":true}')
    old(directory / 'bound_subprocess_index.json', b'{}')
    old(directory.parent / 'logs/subprocess/execution.json', b'{}')
    return file


def test_preview_rename_hardlink_metadata_http_and_restore(settings):
    p = capture(settings)
    alias = old(settings.project_root / 'reports/retained_alias.pack')
    alias.unlink()
    os.link(p, alias)
    before = p.stat()
    plan = storage.build_plan(settings)
    assert len(plan['units']) == 1
    assert storage.run_plan(settings, plan)['status'] == 'preview'
    assert p.exists()
    result = storage.run_plan(settings, plan, apply=True)
    assert result['status'] == 'complete' and not p.exists()
    dest = settings.storage_lifecycle_archive_root / p.relative_to(settings.project_root)
    assert dest.stat().st_ino == alias.stat().st_ino == before.st_ino
    assert dest.stat().st_mtime_ns == before.st_mtime_ns
    assert storage.artifact_location(settings.project_root, p)['status'] == 'archived_offline'
    assert storage.verify_offline(settings)['errors'] == []
    assert storage.run_plan(settings, plan, apply=True)['completed'] == []
    restored = storage.run_plan(settings, plan, apply=True, restore=True)
    assert restored['status'] == 'complete' and p.read_bytes() == alias.read_bytes()
    assert storage.artifact_location(settings.project_root, p)['status'] == 'online'


def test_file_budget_resumes_without_moving_partial_unit(settings):
    p = capture(settings)
    plan = storage.build_plan(settings)
    result = storage.run_plan(settings, plan, apply=True, max_files=1)
    assert result['status'] == 'deferred' and p.exists()
    for _ in range(5):
        result = storage.run_plan(settings, plan, apply=True, max_files=1)
        if result['status'] == 'complete':
            break
    assert result['status'] == 'complete' and not p.exists()


def test_restore_rehashes_unchanged_metadata_and_retires_resume_cache(settings, monkeypatch):
    p = capture(settings)
    plan = storage.build_plan(settings)
    storage.run_plan(settings, plan, apply=True)
    with storage.connect(settings.project_root) as conn:
        assert conn.execute('SELECT COUNT(*) FROM verified').fetchone()[0] == 0
    original, calls = storage.sha, []
    def tracked(path):
        calls.append(path)
        return original(path)
    monkeypatch.setattr(storage, 'sha', tracked)
    assert storage.run_plan(settings, plan, apply=True, restore=True)['status'] == 'complete'
    assert len(calls) >= 4 and p.exists()


def test_formal_manifest_and_legacy_registry_protect(settings):
    p = old(settings.reports_dir / 'runs/20260410_010203_sync/revisions/a/final.xlsx')
    manifest = p.parent / 'validated.json'
    old(manifest, json.dumps({'files': {'final.xlsx': storage.sha(p)}}).encode())
    state = {'committed_runs': {'run': {'report_path': str(p.relative_to(settings.project_root)),
                                      'manifest_path': str(manifest.relative_to(settings.project_root))}}}
    state_path = settings.reports_dir / '.publication_current/publication_state.json'
    state_path.parent.mkdir()
    state_path.write_text(json.dumps(state))
    plan = storage.build_plan(settings)
    assert plan['units'] == []
    assert any('production_dependency' == item['reason'] for item in plan['retained'])


def test_previous_formal_generation_stays_online_without_current_head_reference(settings):
    revision = settings.reports_dir / 'runs/20260410_010203_sync/revisions/previous'
    old(revision / 'publication_state.json', b'{"generation":1}')
    old(revision / 'final.xlsx')
    plan = storage.build_plan(settings)
    assert plan['units'] == []
    assert any(x['reason'] == 'production_dependency' for x in plan['retained'])


def test_recent_parent_cross_date_dependency(settings):
    parent = old(settings.reports_dir / '.audit/collection_calculations/20260410/collect_20260410_010203_abc/terminal.json', b'{"capture_complete":true}')
    old(parent.parent / 'execution.json', b'{}')
    run = datetime.now().strftime('%Y%m%d_%H%M%S_sync')
    report = settings.reports_dir / 'runs' / run / 'bond_analysis_report_test.json'
    report.parent.mkdir(parents=True)
    report.write_text(json.dumps({'collection': {'terminal_path': str(parent.relative_to(settings.project_root))}}))
    plan = storage.build_plan(settings)
    assert plan['units'] == []
    assert storage.blocked(str(parent.parent.relative_to(settings.project_root)), plan['protected'])


@pytest.mark.parametrize('name', ['run_20260410_010203.log', 'database_2026-04-10.log'])
def test_exact_14_day_mtime_boundary_and_open_file(settings, name):
    now = datetime.now().astimezone()
    log = old(settings.project_root / 'logs' / name, days=14)
    boundary = (now - timedelta(days=14)).timestamp()
    boundary_ns = int(boundary * 1e9)
    os.utime(log, ns=(boundary_ns, boundary_ns))
    assert storage.build_plan(settings, now=now)['units'] == []
    os.utime(log, (boundary - 1, boundary - 1))
    with log.open('a'):
        assert storage.build_plan(settings, now=now)['units'] == []
    plan = storage.build_plan(settings, now=now)
    assert plan['units'][0]['action'] == 'delete'
    result = storage.run_plan(settings, plan, apply=True)
    assert result['status'] == 'complete' and not log.exists()
    assert result['deleted_last_link_allocated_bytes'] > 0


def test_changed_or_corrupt_source_refused(settings):
    p = capture(settings)
    plan = storage.build_plan(settings)
    p.write_bytes(b'corrupted')
    result = storage.run_plan(settings, plan, apply=True)
    assert result['status'] == 'error' and p.exists()


def test_offline_corruption_refused_and_catalog_reports_corrupt(settings):
    p = capture(settings)
    plan = storage.build_plan(settings)
    storage.run_plan(settings, plan, apply=True)
    dest = settings.storage_lifecycle_archive_root / p.relative_to(settings.project_root)
    dest.write_bytes(b'corrupted')
    assert storage.verify_offline(settings)['errors']
    assert storage.artifact_location(settings.project_root, p)['status'] == 'corrupt'
    assert storage.run_plan(settings, plan, apply=True, restore=True)['status'] == 'error'


def test_interrupted_multi_root_rename_resumes(settings, monkeypatch):
    p = capture(settings)
    plan = storage.build_plan(settings)
    original = os.rename
    calls = []

    def interrupted(src, dst):
        calls.append(src)
        if len(calls) == 2:
            raise OSError('simulated interruption')
        return original(src, dst)

    monkeypatch.setattr(storage.os, 'rename', interrupted)
    assert storage.run_plan(settings, plan, apply=True)['status'] == 'error'
    monkeypatch.setattr(storage.os, 'rename', original)
    assert storage.run_plan(settings, plan, apply=True)['status'] == 'complete'
    assert not p.exists()
    assert storage.run_plan(settings, plan, apply=True, restore=True)['status'] == 'complete'


def test_lock_busy_is_deferred(settings):
    import fcntl
    p = capture(settings)
    plan = storage.build_plan(settings)
    lock = settings.state_file.with_suffix('.json.lock')
    with lock.open('a+b') as stream:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = storage.run_plan(settings, plan, apply=True)
    assert result['status'] == 'deferred' and not result['completed'] and p.exists()


def test_offline_http_410_without_external_mount(settings):
    from backend.api_routes.reports import register_reports_routes
    from backend.artifact_paths import resolve_project_artifact_path
    p = capture(settings)
    plan = storage.build_plan(settings)
    storage.run_plan(settings, plan, apply=True)
    app = Flask(__name__)
    options = {key: lambda *a, **kw: {} for key in inspect.signature(register_reports_routes).parameters if key != 'app'}
    options['resolve_project_file'] = lambda value: resolve_project_artifact_path(value, project_root=settings.project_root)
    register_reports_routes(app, **options)
    response = app.test_client().get('/api/files/' + str(p.relative_to(settings.project_root)))
    assert response.status_code == 410 and response.json['status'] == 'archived_offline'
    assert 'offline' not in response.json.get('archive_root', '')


def test_unknown_and_active_backup_retained(settings):
    backup = old(settings.project_root / 'backend/data/backups/unique_unknown_20260410.db.zst')
    bound = old(settings.project_root / 'backend/data/backups/rank_hot_pre_636_20260908_011713_517521.db.zst')
    audit = old(settings.reports_dir / 'audit/rank_history_archive/rank_history_archive_apply_runner_20261001.json',
                json.dumps({'status': 'success', 'backup': str(bound.relative_to(settings.project_root))}).encode())
    plan = storage.build_plan(settings)
    assert plan['units'] == [] and backup.exists() and audit.exists()
    assert any(item['reason'] == 'production_dependency' for item in plan['retained'])


def test_offline_snapshot_symlinks_preserved_without_following(settings):
    base = settings.reports_dir / 'audit/implementation_20260907'
    old(base / 'original.bin')
    link = base / 'old_runtime_alias'
    link.symlink_to('/app/missing_historical_runtime')
    plan = storage.build_plan(settings)
    assert len(plan['units']) == 1
    assert storage.run_plan(settings, plan, apply=True)['status'] == 'complete'
    dest = settings.storage_lifecycle_archive_root / link.relative_to(settings.project_root)
    assert dest.is_symlink() and str(dest.readlink()) == '/app/missing_historical_runtime'
    assert storage.verify_offline(settings)['errors'] == []
    assert storage.run_plan(settings, plan, apply=True, restore=True)['status'] == 'complete'


def test_new_formal_reference_blocks_stale_plan(settings):
    p = capture(settings)
    plan = storage.build_plan(settings)
    registry = settings.reports_dir / 'publication_registry.json'
    registry.write_text(json.dumps({'canonical_runs': [{'artifacts': {'input': str(p.relative_to(settings.project_root))}}]}))
    # Explicit recent-run references, rather than historical capture indexes,
    # protect the entire unit even if the plan was made earlier.
    run = settings.reports_dir / 'runs' / datetime.now().strftime('%Y%m%d_%H%M%S_sync')
    run.mkdir()
    (run / 'bond_analysis_report_test.json').write_text(json.dumps({'dependency': str(p.relative_to(settings.project_root))}))
    result = storage.run_plan(settings, plan, apply=True)
    assert result['status'] == 'deferred' and p.exists()


def test_restore_collision_is_refused(settings):
    p = capture(settings)
    plan = storage.build_plan(settings)
    storage.run_plan(settings, plan, apply=True)
    p.parent.mkdir(parents=True)
    p.write_bytes(b'new material')
    result = storage.run_plan(settings, plan, apply=True, restore=True)
    assert result['status'] == 'error' and p.read_bytes() == b'new material'


def test_forward_workbook_reference_keeps_uncommitted_revision_online(settings):
    p = old(settings.reports_dir / 'runs/20260410_010203_sync/revisions/a/forward.xlsx')
    cache = settings.reports_dir / 'runs/.forward_validation_cache/current.json'
    cache.parent.mkdir(parents=True)
    cache.write_text(json.dumps({'excel_reference': {'reports_relative_path': str(p.relative_to(settings.reports_dir))}}))
    plan = storage.build_plan(settings)
    assert plan['units'] == []
    assert any(item['reason'] == 'production_dependency' for item in plan['retained'])


def test_compressed_csv_receipt_and_original_url_remain_one_unit(settings):
    directory = settings.core_output_dir
    logical = directory / 'sample_20260410_010203.csv'
    stored = old(logical.with_name(logical.name + '.zst'))
    receipt = old(logical.with_name(logical.name + '.zst.receipt.json'), b'{}')
    old(directory / 'sample_20260411_010203.csv')
    plan = storage.build_plan(settings)
    unit = next(u for u in plan['units'] if str(stored.relative_to(settings.project_root)) in u['paths'])
    assert str(receipt.relative_to(settings.project_root)) in unit['paths']
    assert storage.run_plan(settings, plan, apply=True)['status'] == 'complete'
    assert storage.artifact_location(settings.project_root, logical)['status'] == 'archived_offline'


def test_legacy_forward_cache_pair_keeps_full_workbook(settings):
    directory = settings.reports_dir / 'runs/.forward_validation_cache'
    book = old(directory / 'oldkey.xlsx')
    old(book.with_suffix('.json'), b'{"summary":{"generated_at":"2026-04-10T01:02:03"}}')
    plan = storage.build_plan(settings)
    assert len(plan['units']) == 1 and len(plan['units'][0]['paths']) == 2
    assert storage.run_plan(settings, plan, apply=True)['status'] == 'complete'
    assert storage.run_plan(settings, plan, apply=True, restore=True)['status'] == 'complete'
    assert book.exists()


def test_automatic_container_maintenance_keeps_logs_for_host_fd_audit(settings, monkeypatch):
    log = old(settings.project_root / 'logs/run_20260410_010203.log')
    monkeypatch.setenv('FUND_ANA_STORAGE_LIFECYCLE_PROJECT_ROOT', str(settings.project_root))
    plan = storage.build_plan(settings)
    assert plan['units'] == [] and log.exists()
    assert plan['retained'][0]['reason'] == 'host_process_descriptor_check_required'


def test_weekly_legacy_ttl_cannot_delete_history_under_lifecycle_policy(settings, monkeypatch):
    from scripts import cleanup_disk_usage as cleanup
    settings.storage_lifecycle_enabled = True
    settings.archived_runs_dir = settings.reports_dir / 'archive/superseded_runs'
    settings.core_root = settings.project_root / 'core'
    historical = old(settings.archived_runs_dir / '20260410_010203_sync/formal.xlsx')
    result = cleanup.run_cleanup(settings, apply=False, archive_keep_count=0, archive_keep_days=0)
    assert result.archive_removed == [] and historical.parent in result.archive_kept
    assert result.to_dict()['archive_retention_mode'] == 'offline_lifecycle_no_historical_deletion'


def test_gc_removes_only_offline_lookup_aliases(settings, monkeypatch):
    from backend.storage_lifecycle_gc import collect_offline_lookup_aliases
    p = capture(settings)
    pool = settings.reports_dir / '.test_source_pool'
    pool.mkdir()
    alias = pool / 'original.tar.gz'
    os.link(p, alias)
    plan = storage.build_plan(settings)
    assert storage.run_plan(settings, plan, apply=True)['status'] == 'complete'
    monkeypatch.setattr('backend.storage_lifecycle_gc.open_inodes', storage.open_inodes)
    preview = collect_offline_lookup_aliases(settings)
    assert preview['candidates'] == [str(alias.relative_to(settings.project_root))] and alias.exists()
    applied = collect_offline_lookup_aliases(settings, apply=True)
    assert applied['removed'] == preview['candidates'] and not alias.exists()
    assert storage.run_plan(settings, plan, apply=True, restore=True)['status'] == 'complete'


def test_gc_keeps_online_local_capture_alias(settings, monkeypatch):
    from backend.storage_lifecycle_gc import collect_offline_lookup_aliases
    p = capture(settings)
    pool = settings.reports_dir / '.test_source_pool'
    pool.mkdir()
    alias = pool / 'original.tar.gz'
    os.link(p, alias)
    online = settings.reports_dir / 'still_used.pack'
    os.link(p, online)
    plan = storage.build_plan(settings)
    storage.run_plan(settings, plan, apply=True)
    monkeypatch.setattr('backend.storage_lifecycle_gc.open_inodes', storage.open_inodes)
    assert collect_offline_lookup_aliases(settings, apply=True)['removed'] == []
    assert alias.exists() and online.exists()


def test_cumulative_tracker_daily_rebuild_inputs_stay_online(settings):
    output = settings.project_root / 'reports/core_output'
    workbook = old(output / '当日最终优质基金_20260410_010203.xlsx')
    sidecar = old(output / '当日最终优质基金_20260410_010203_report_meta.json', b'{}')
    tracker = old(output / '优质基金累积跟踪.xlsx')
    plan = storage.build_plan(settings)
    for path in (workbook, sidecar, tracker):
        assert storage.blocked(str(path.relative_to(settings.project_root)), plan['protected'])
    assert not plan['units']


def test_cumulative_tracker_metadata_protects_named_nonstandard_inputs(settings):
    output = settings.project_root / 'reports/core_output'
    workbook = old(output / 'historical_result_20260410_010203.xlsx')
    sidecar = old(output / 'historical_result_20260410_010203_report_meta.json', b'{}')
    old(output / '优质基金累积跟踪_report_meta.json',
        json.dumps({'processed_result_files': [workbook.name]}).encode())
    plan = storage.build_plan(settings)
    for path in (workbook, sidecar):
        assert storage.blocked(str(path.relative_to(settings.project_root)), plan['protected'])
    assert not plan['units']
