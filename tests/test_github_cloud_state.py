from copy import deepcopy
import hashlib
import os
from pathlib import Path
import sqlite3
import subprocess

import pytest

from backend.github_cloud_state import (
    CloudStateError, Remote, inventory, restore, sha, snapshot, validate_manifest, runtime_snapshot_lock,
)


def store(tmp_path):
    objects = tmp_path / 'objects'; objects.mkdir()
    def put(raw, size):
        compressed = objects / 'new.zst'
        subprocess.run(['zstd', '-q', '-3', '-f', str(raw), '-o', str(compressed)], check=True)
        digest = sha(compressed); target = objects / digest
        compressed.rename(target)
        return dict(sha256=digest, raw_size=size, size=target.stat().st_size, tag='cloud-state-objects-202611')
    def fetch(obj, target): target.write_bytes((objects / obj['sha256']).read_bytes())
    return put, fetch


def fixture(tmp_path):
    root = tmp_path / 'source'; data = root / 'backend/data'; data.mkdir(parents=True)
    reports = root / 'reports'; reports.mkdir()
    with sqlite3.connect(data / 'bond_funds.db') as conn:
        conn.execute('create table facts(code text, nav real, missing real, zero real)')
        conn.execute("insert into facts values('000001',1.234567890123,NULL,0)")
    (reports / 'record.json').write_bytes(b'{"value":null,"zero":0}')
    os.link(reports / 'record.json', reports / 'record-copy.json')
    (reports / 'latest.json').symlink_to('record.json')
    return root


def test_roundtrip_database_null_zero_links_and_reuse(tmp_path):
    root = fixture(tmp_path); put, fetch = store(tmp_path)
    paths = sorted(p for p in root.rglob('*') if p.is_file() or p.is_symlink())
    value = snapshot(root, paths, None, put)
    target = tmp_path / 'restored'; target.mkdir()
    restore(target, value, fetch)
    assert (target / 'reports/latest.json').is_symlink()
    assert (target / 'reports/record-copy.json').stat().st_ino == (target / 'reports/record.json').stat().st_ino
    with sqlite3.connect(target / 'backend/data/bond_funds.db') as conn:
        assert conn.execute('select * from facts').fetchone() == ('000001',1.234567890123,None,0)
    def unexpected(*args): raise AssertionError('unchanged object was uploaded again')
    again = snapshot(root, paths, value, unexpected)
    assert again['objects'] == value['objects']


def test_duplicate_crossing_pack_boundary_keeps_pending_tail(tmp_path,monkeypatch):
    import backend.github_cloud_state as cloud
    monkeypatch.setattr(cloud,'PACK',16)
    root=tmp_path/'boundary';folder=root/'reports';folder.mkdir(parents=True)
    for name,value in [('a',b'x'*10),('b',b'y'*12),('c',b'y'*12),('d',b'z'*7)]:
        (folder/name).write_bytes(value)
    put,fetch=store(tmp_path)
    value=snapshot(root,sorted(folder.iterdir()),None,put)
    target=tmp_path/'boundary-restore';target.mkdir();restore(target,value,fetch)
    assert (target/'reports/b').read_bytes()==(target/'reports/c').read_bytes()==b'y'*12
    assert sorted(x['raw_size'] for x in value['objects'].values())==[13,16]


def test_restore_downloads_are_bounded_and_overlap_without_changing_bytes(tmp_path, monkeypatch):
    import threading
    import backend.github_cloud_state as cloud
    monkeypatch.setattr(cloud, 'PACK', 16)
    root = tmp_path / 'parallel-source'
    folder = root / 'reports'
    folder.mkdir(parents=True)
    for number in range(10): (folder / str(number)).write_bytes(bytes([number]) * 15)
    put, stored_fetch = store(tmp_path)
    value = snapshot(root, sorted(folder.iterdir()), None, put)
    barrier = threading.Barrier(3)
    lock = threading.Lock()
    active = peak = started = 0
    def fetch(obj, target):
        nonlocal active, peak, started
        with lock:
            started += 1
            first_wave = started <= 3
            active += 1
            peak = max(peak, active)
        try:
            if first_wave: barrier.wait(timeout=5)
            stored_fetch(obj, target)
        finally:
            with lock: active -= 1
    target = tmp_path / 'parallel-restore'
    target.mkdir()
    restore(target, value, fetch)
    assert peak == 3 and active == 0 and started == len(value['objects'])
    for path in folder.iterdir():
        assert (target / 'reports' / path.name).read_bytes() == path.read_bytes()


def test_live_wal_rows_survive_closed_native_backup(tmp_path):
    root=fixture(tmp_path);put,fetch=store(tmp_path)
    connection=sqlite3.connect(root/'backend/data/bond_funds.db')
    try:
        connection.execute('PRAGMA journal_mode=WAL')
        connection.execute('PRAGMA wal_autocheckpoint=0')
        connection.execute("insert into facts values('000002',2.345,NULL,0)")
        connection.commit()
        assert (root/'backend/data/bond_funds.db-wal').stat().st_size>0
        value=snapshot(root,[root/'backend/data/bond_funds.db'],None,put)
        target=tmp_path/'wal-restore';target.mkdir();restore(target,value,fetch)
        with sqlite3.connect(target/'backend/data/bond_funds.db') as restored:
            assert restored.execute('select code from facts order by code').fetchall()==[('000001',),('000002',)]
            assert restored.execute('PRAGMA quick_check').fetchone()==('ok',)
    finally:connection.close()


def test_private_account_rows_are_not_uploaded(tmp_path):
    root=fixture(tmp_path)
    with sqlite3.connect(root/'backend/data/bond_funds.db') as conn:
        conn.execute('create table users(username text,password_hash text)')
        conn.execute("insert into users values('synthetic-user','synthetic-hash')")
    def unexpected(*args):raise AssertionError('account data uploaded')
    with pytest.raises(CloudStateError,match='contains_user_accounts'):
        snapshot(root,[root/'backend/data/bond_funds.db'],None,unexpected)


def test_failed_upload_does_not_modify_previous_state(tmp_path):
    root=fixture(tmp_path); put,_=store(tmp_path)
    paths=sorted(p for p in root.rglob('*') if p.is_file() or p.is_symlink())
    previous=snapshot(root,paths,None,put); saved=deepcopy(previous)
    (root/'reports/record.json').write_bytes(b'changed')
    def fail(*args): raise OSError('interrupted upload')
    with pytest.raises(OSError): snapshot(root,paths,previous,fail)
    assert previous==saved


def test_corrupt_object_rejected_before_business_write(tmp_path):
    root=fixture(tmp_path); put,fetch=store(tmp_path)
    value=snapshot(root,sorted(p for p in root.rglob('*') if p.is_file() or p.is_symlink()),None,put)
    def corrupt(obj,path): path.write_bytes(b'corrupt')
    target=tmp_path/'target';target.mkdir()
    with pytest.raises(CloudStateError,match='download_corrupt'):restore(target,value,corrupt)


@pytest.mark.parametrize('path', ['/etc/passwd','../outside','reports/../secret','config/project.yaml',
                                  'backend/data/favorite_fund_queries.json','reports//x','reports/x\\y'])
def test_manifest_rejects_unsafe_or_private_paths(path):
    value=dict(schema='fund-ana-cloud-state-v1',objects={},files=[dict(path=path,kind='symlink',target='x')])
    with pytest.raises(CloudStateError):validate_manifest(value)


def test_manifest_rejects_symlink_parent_even_in_reverse_order():
    value=dict(schema='fund-ana-cloud-state-v1',objects={},files=[
        dict(path='reports/a/child',kind='symlink',target='../target'),
        dict(path='reports/a',kind='symlink',target='target')])
    with pytest.raises(CloudStateError,match='symlink_parent'):validate_manifest(value)


def test_restore_refuses_existing_runtime(tmp_path):
    root=fixture(tmp_path);put,fetch=store(tmp_path)
    value=snapshot(root,sorted(p for p in root.rglob('*') if p.is_file() or p.is_symlink()),None,put)
    with pytest.raises(CloudStateError,match='empty_runtime'):restore(root,value,fetch)


def test_inventory_excludes_favorites_config_backups_and_logs(tmp_path):
    from backend.github_cloud_state import CONFIG_FILES
    root=fixture(tmp_path)
    for folder in ['reports/runs','reports/core_output','backend/data/backups']:(root/folder).mkdir(parents=True)
    (root/'backend/data/favorite_fund_queries.json').write_text('private')
    (root/'backend/data/backups/backup.db').write_text('private')
    for name in CONFIG_FILES:
        path=root/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text('{}')
    (root/'config/project.yaml').write_text('secret')
    (root/'core/config/config.yaml').write_text('secret')
    paths=inventory(root)
    assert root/'backend/data/bond_funds.db' in paths
    assert not any('favorite' in str(p) or 'backups' in str(p) for p in paths)
    assert all(root/name in paths for name in CONFIG_FILES)
    assert root/'config/project.yaml' not in paths and root/'core/config/config.yaml' not in paths
    put,fetch=store(tmp_path);value=snapshot(root,paths,None,put)
    target=tmp_path/'policy-restore';target.mkdir();restore(target,value,fetch)
    assert all((target/name).read_text()=='{}' for name in CONFIG_FILES)


def test_inventory_canonicalizes_directory_alias_children(tmp_path):
    root=fixture(tmp_path)
    for folder in ['reports/runs','reports/core_output']:(root/folder).mkdir()
    revision=root/'reports/revisions/current';revision.mkdir(parents=True)
    (revision/'report.json').write_text('{}')
    (root/'reports/.publication_current').symlink_to('revisions/current')
    (root/'reports/latest.json').unlink()
    (root/'reports/latest.json').symlink_to('.publication_current/report.json')
    paths=inventory(root)
    assert revision/'report.json' in paths
    assert not any('/.publication_current/' in str(p) for p in paths)
    put,fetch=store(tmp_path);value=snapshot(root,paths,None,put)
    target=tmp_path/'alias-restore';target.mkdir();restore(target,value,fetch)
    assert (target/'reports/latest.json').read_text()=='{}'


@pytest.mark.parametrize('corrupt', [False, True])
def test_inventory_keeps_verified_legacy_history_inputs_outside_manifests(tmp_path, corrupt):
    root = fixture(tmp_path)
    for folder in ('reports/runs', 'reports/core_output'):
        (root / folder).mkdir()
    legacy = root / 'reports/runs/20260401_legacy/core_artifacts'
    legacy.mkdir(parents=True)
    workbook = legacy / 'history.xlsx'
    workbook.write_bytes(b'synthetic history source')
    diagnostic = legacy / 'diagnostic.xlsx'
    diagnostic.write_bytes(b'unreferenced diagnostic')
    with sqlite3.connect(root / 'backend/data/report_history.sqlite') as conn:
        conn.execute('CREATE TABLE views(path TEXT,sha256 TEXT,kind TEXT)')
        conn.execute('INSERT INTO views VALUES(?,?,?)',
                     (workbook.relative_to(root).as_posix(), sha(workbook), 'core_raw'))
        conn.execute('INSERT INTO views VALUES(?,?,?)',
                     (diagnostic.relative_to(root).as_posix(), sha(diagnostic), 'workbook'))
    if corrupt:
        workbook.write_bytes(b'corrupt')
        with pytest.raises(CloudStateError, match='history_original_hash_mismatch'):
            inventory(root)
    else:
        paths = inventory(root)
        assert workbook in paths and diagnostic not in paths


def test_hosted_history_readiness_does_not_turn_missing_history_into_empty_success(tmp_path, monkeypatch):
    import json
    from types import SimpleNamespace
    import scripts.validate_github_hosted_history as check
    from core.utils.report_history_store import HistoryError
    report = tmp_path / 'report.json'
    report.write_text(json.dumps({'artifacts': {'filtered_excel': 'reports/current.xlsx'},
                                  'counts': {'final_count': 1}, 'generated_at': '2026-11-01T08:00:00',
                                  'database_freshness': {'effective_nav_snapshot': {'candidate_date': '2026-10-30'}}}))
    head = {'current_published_run_id': 'synthetic', 'generation': 1,
            'committed_runs': {'synthetic': {'report_path': 'report.json'}}}
    monkeypatch.setattr(check, 'load_publication_head', lambda settings: head)
    monkeypatch.setattr(check, 'resolve_committed_path', lambda settings, path: report)
    def missing(*args, **kwargs):
        raise HistoryError('historical_tracker_input_unavailable:workbook_missing')
    monkeypatch.setattr(check, '_load_project_report_history', missing)
    with pytest.raises(HistoryError, match='workbook_missing'):
        check.validate_history(SimpleNamespace(project_root=tmp_path))


def test_cloud_roundtrip_preserves_risk_lineage_decisions(tmp_path):
    import json
    from backend.publication import _resolve_run_lineage_blockers
    root = fixture(tmp_path)
    (root / 'reports/core_output').mkdir()
    run = root / 'reports/runs/20260526_073722_sync'
    run.mkdir(parents=True)
    report = run / 'bond_analysis_report_synthetic.json'
    report.write_text('{}')
    index = run / 'logs/subprocess/subprocess_index.json'
    index.parent.mkdir(parents=True)
    index.write_text(json.dumps({'items': [{'task_name': 'daily_screening_master',
                                          'command': ['python', 'daily_final_screening.py']}]}))
    (index.parent / 'unneeded.log').write_text('diagnostic only')
    before = _resolve_run_lineage_blockers(report)
    assert before[0]['code'] == 'formal_lineage_missing_risk_calculation'
    paths = inventory(root)
    assert index in paths and index.parent / 'unneeded.log' not in paths
    put, fetch = store(tmp_path)
    state = snapshot(root, paths, None, put)
    target = tmp_path / 'lineage-restored'
    target.mkdir()
    restore(target, state, fetch)
    assert _resolve_run_lineage_blockers(target / report.relative_to(root)) == before


def test_hosted_history_readiness_rejects_changed_publication_decisions(tmp_path, monkeypatch):
    import json
    from types import SimpleNamespace
    import scripts.validate_github_hosted_history as check
    (tmp_path / '.cloud-restored.json').write_text(json.dumps(
        {'history_contract': {'schema': 'synthetic', 'records': [{'publishable': False}]}}))
    monkeypatch.setattr(check, 'load_publication_head', lambda settings: {'generation': 1})
    monkeypatch.setattr(check, 'publication_history_contract',
                        lambda settings: {'schema': 'synthetic', 'records': [{'publishable': True}]})
    with pytest.raises(RuntimeError, match='decisions_changed_after_restore'):
        check.validate_history(SimpleNamespace(project_root=tmp_path))


def test_cloud_history_contract_is_independent_of_host_paths_and_record_order(monkeypatch):
    import backend.publication as publication
    from backend.github_cloud_state import publication_history_contract
    records = [{'run_id': 'synthetic-blocked', 'publishable': False,
                'publish_blockers': [{'code': 'formal_lineage_missing_risk_calculation', 'blocking': True}],
                'report_path': '/old/host/reports/record.json'},
               {'run_id': 'synthetic-formal', 'publishable': True, 'publication_committed': True}]
    monkeypatch.setattr(publication, 'build_publication_registry', lambda settings: {'canonical_runs': records})
    before = publication_history_contract(None)
    records.reverse()
    records[1]['report_path'] = '/app/reports/record.json'
    assert publication_history_contract(None) == before
    records[1]['publishable'] = True
    assert publication_history_contract(None) != before


def test_cold_database_preserves_manifest_bytes(tmp_path):
    root=fixture(tmp_path);cold=root/'backend/data/archives/cold.db';cold.parent.mkdir()
    with sqlite3.connect(cold) as conn:
        conn.execute('create table cold(value real)');conn.execute('insert into cold values(1.234)')
    original=sha(cold);put,fetch=store(tmp_path)
    value=snapshot(root,[cold],None,put)
    target=tmp_path/'cold-restore';target.mkdir();restore(target,value,fetch)
    assert sha(target/'backend/data/archives/cold.db')==original


def test_configuration_and_workflow_have_no_host_execution():
    root=Path(__file__).resolve().parents[1]
    text=(root/'.github/workflows/daily-reports.yml').read_text() if (root/'.github').exists() else ''
    # Isolation copies omit .github; runtime wrapper remains verified here.
    assert 'self-hosted' not in text
    assert '/usr/local/libexec/fund-ana-github-job' not in text
    script=(root/'scripts/run_github_hosted_job.py').read_text()
    assert '"docker", "exec"' not in script


def test_hosted_configuration_allows_external_once_but_retires_embedded_web(tmp_path, monkeypatch):
    import shutil
    from types import SimpleNamespace
    import yaml
    from scripts import configure_github_hosted as hosted
    from backend.github_cloud_state import CONFIG_FILES
    from backend.scheduler import ProjectScheduler

    source = Path(__file__).resolve().parents[1]
    for name in ('config/project.example.yaml', 'core/config/config.yaml.template'):
        target = tmp_path / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source / name, target)
    for name in CONFIG_FILES:
        target = tmp_path / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text('{}')
    monkeypatch.setattr(hosted, 'ROOT', tmp_path)
    hosted.main()
    config = yaml.safe_load((tmp_path / 'config/project.yaml').read_text())
    settings = SimpleNamespace(scheduler_enabled=config['scheduler']['enabled'],
                               scheduler_host_mode=config['scheduler']['host_mode'])
    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler._runtime_settings = lambda: settings
    scheduler._host_context = lambda: 'external_scheduler_process'
    scheduler._ensure_runtime_host_allowed()
    scheduler._host_context = lambda: 'embedded_web_scheduler'
    monkeypatch.setenv('FUND_ANA_DISABLE_EMBEDDED_SCHEDULER', '1')
    with pytest.raises(RuntimeError, match='scheduler_host_retired'):
        scheduler._ensure_runtime_host_allowed()
    assert config['scheduler']['storage_lifecycle']['enabled'] is False


def test_remote_commit_refuses_a_concurrent_head_change(tmp_path, monkeypatch):
    remote=Remote.__new__(Remote);remote.token='synthetic-token'
    monkeypatch.setattr(remote,'checkout',lambda directory:'different-head')
    value=dict(schema='fund-ana-cloud-state-v1',created_at='2026-11-08T00:00:00Z',files=[],objects={})
    with pytest.raises(CloudStateError,match='head_changed'):remote.commit(value,'old-head')


def test_remote_read_retries_transient_failure_without_repeating_mutation(monkeypatch):
    import backend.github_cloud_state as cloud
    remote=Remote.__new__(Remote);remote.environment={}
    calls=[]
    def run(args,**kwargs):
        calls.append(args)
        return subprocess.CompletedProcess(args,0 if len(calls)==2 else 1,b'{}',b'HTTP 502')
    monkeypatch.setattr(cloud.subprocess,'run',run)
    monkeypatch.setattr(cloud.time,'sleep',lambda seconds:None)
    assert remote.gh(['api','repos/synthetic/repository'])==b'{}'
    assert len(calls)==2
    calls.clear()
    with pytest.raises(CloudStateError,match='request_failed'):
        remote.gh(['release','upload','synthetic-tag','synthetic-file'])
    assert len(calls)==1


def test_remote_missing_object_is_not_retried(monkeypatch):
    import backend.github_cloud_state as cloud
    remote=Remote.__new__(Remote);remote.environment={};calls=[]
    def run(args,**kwargs):
        calls.append(args);return subprocess.CompletedProcess(args,1,b'{}',b'HTTP 404')
    monkeypatch.setattr(cloud.subprocess,'run',run)
    monkeypatch.setattr(cloud.time,'sleep',lambda seconds:(_ for _ in ()).throw(AssertionError('unexpected retry')))
    assert remote.gh(['api','repos/synthetic/missing'],check=False)==b'{}'
    assert len(calls)==1


def test_new_asset_identity_is_found_without_assuming_append_order(tmp_path,monkeypatch):
    import json
    remote=Remote.__new__(Remote);remote.repository='synthetic/repository'
    release={'id':123,'assets':[{'name':f'old-{i}.zst'} for i in range(100)]}
    monkeypatch.setattr(remote,'release',lambda tag:release)
    uploaded={};calls=[]
    def gh(args):
        calls.append(args)
        if args[:2]==['release','upload']:
            path=Path(args[3]);uploaded.update(name=path.name,size=path.stat().st_size,
                state='uploaded',digest='sha256:'+sha(path))
            return b''
        assert 'page=1' in args[1]
        return json.dumps([uploaded,*release['assets'][:99]]).encode()
    monkeypatch.setattr(remote,'gh',gh)
    raw=tmp_path/'pack.raw';raw.write_bytes(b'synthetic runtime bytes')
    result=remote.put(raw,raw.stat().st_size)
    assert result['sha256']+'.zst'==uploaded['name']
    assert release['assets'][-1]==uploaded


def test_object_download_retries_from_empty_file(tmp_path,monkeypatch):
    import backend.github_cloud_state as cloud
    remote=Remote.__new__(Remote);remote.repository='synthetic/repository';remote.environment={}
    monkeypatch.setattr(remote,'release',lambda tag:{'assets':[{'id':123,'name':'synthetic.zst'}]})
    calls=[]
    def run(args,**kwargs):
        calls.append(args)
        kwargs['stdout'].write(b'partial bytes' if len(calls)==1 else b'complete')
        return subprocess.CompletedProcess(args,1 if len(calls)==1 else 0,None,b'HTTP 502')
    monkeypatch.setattr(cloud.subprocess,'run',run)
    monkeypatch.setattr(cloud.time,'sleep',lambda seconds:None)
    output=tmp_path/'download.zst'
    remote.fetch({'tag':'synthetic','sha256':'synthetic'},output)
    assert len(calls)==2 and output.read_bytes()==b'complete'


def test_snapshot_lock_busy_does_not_change_config_root(tmp_path):
    import fcntl
    import backend.config as config
    root=fixture(tmp_path);(root/'config').mkdir()
    (root/'config/project.yaml').write_text('{}')
    original=config.PROJECT_ROOT
    with (root/'backend/data/workflow_state.json.lock').open('a+b') as held:
        fcntl.flock(held,fcntl.LOCK_EX|fcntl.LOCK_NB)
        with pytest.raises(CloudStateError,match='workflow_busy'):
            with runtime_snapshot_lock(root):raise AssertionError('busy lock entered')
    assert config.PROJECT_ROOT==original


def test_hosted_configuration_preserves_financial_inputs(tmp_path, monkeypatch):
    import yaml
    import scripts.configure_github_hosted as configure
    root=Path(__file__).resolve().parents[1]
    target=tmp_path/'config-test'
    for name in ['config/project.example.yaml','core/config/config.yaml.template']:
        output=target/name;output.parent.mkdir(parents=True,exist_ok=True)
        output.write_bytes((root/name).read_bytes())
    from backend.github_cloud_state import CONFIG_FILES
    for name in CONFIG_FILES:
        output=target/name;output.parent.mkdir(parents=True,exist_ok=True);output.write_text('{}')
    before=yaml.safe_load((target/'config/project.example.yaml').read_text())
    before_core=yaml.safe_load((target/'core/config/config.yaml.template').read_text())
    monkeypatch.setattr(configure,'ROOT',target);configure.main()
    after=yaml.safe_load((target/'config/project.yaml').read_text())
    after_core=yaml.safe_load((target/'core/config/config.yaml').read_text())
    for name in before:
        if name!='scheduler':assert after[name]==before[name]
    before_core.setdefault('web',{}).pop('secret_key',None)
    after_core['web'].pop('secret_key')
    assert after_core==before_core
    assert after['scheduler']['enabled'] is True
    assert after['scheduler']['host_mode'] == 'external_process'
    assert after['scheduler']['weekly_maintenance']['plain_open_tail_follow_up']['enabled'] is True


def test_hosted_configuration_refuses_missing_operational_policy(tmp_path,monkeypatch):
    import scripts.configure_github_hosted as configure
    monkeypatch.setattr(configure,'ROOT',tmp_path)
    with pytest.raises(RuntimeError,match='operational_policy_missing'):configure.main()
