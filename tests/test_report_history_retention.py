"""Isolated artificial retirement boundaries; no production files or cron are touched."""
from datetime import datetime, timedelta
import gzip
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from flask import Flask

from backend import report_history as history
from backend import report_history_retention as retention
from backend import storage_lifecycle as lifecycle
from backend.api_routes.report_history import register_history_routes
from core.utils import report_history_store as store


@pytest.fixture
def settings(tmp_path,monkeypatch):
    root=tmp_path/'project';(root/'config').mkdir(parents=True)
    (root/'config/project.yaml').write_text('report_history:\n  read_mode: ledger\n  quarantine_enabled: true\n  purge_enabled: true\n')
    monkeypatch.setattr(lifecycle,'open_inodes',lambda:set())
    monkeypatch.setattr(retention,'in_container',lambda:False)
    return SimpleNamespace(project_root=root,reports_dir=root/'reports',runs_dir=root/'reports/runs',
        archived_runs_dir=root/'reports/archive/superseded_runs',core_output_dir=root/'reports/core_output',
        state_file=root/'state.json',storage_lifecycle_archive_root=tmp_path/'offline',
        report_history_database_path=root/'backend/data/report_history.sqlite')


def historical(settings,days=20):
    root=settings.project_root;path=root/'reports/runs/20200101_010101_fixture/formal.xlsx';path.parent.mkdir(parents=True)
    pd.DataFrame({'基金代码':['000001'],'数值':[0]}).to_excel(path,index=False)
    t=(datetime.now().astimezone()-timedelta(days=days)).timestamp();os.utime(path,(t,t))
    report={'run_id':'fixture','generated_at':datetime.fromtimestamp(t).isoformat(),
            'counts':{'final_count':1},'artifacts':{'project_tracker_excel':str(path.relative_to(root))}}
    report_path=path.parent/'report.json';report_path.write_text(json.dumps(report))
    result=history.import_report(settings,report_path,'legacy_eligible')
    assert history.semantic_audit(settings)['status']=='verified'
    (root/'reports/publication_registry.json').write_text(json.dumps({'canonical_runs':[{'run_id':'fixture','report_path':str(report_path.relative_to(root))}]}))
    return path,result


def head(settings,current='new'):
    directory=settings.reports_dir/'.publication_current';directory.mkdir()
    (directory/'publication_state.json').write_text(json.dumps({'current_published_run_id':current,'committed_runs':{}}))


def test_no_natural_evidence_means_no_retirement(settings):
    path,_=historical(settings)
    assert not retention.acceptance(settings)['ready']
    assert retention.eligibility(settings,path)
    assert retention.purge(settings,execute=True)['status']=='deferred'
    assert path.exists()


def test_registered_original_cannot_use_ordinary_intermediate_cleanup(settings):
    path, _ = historical(settings, 40)
    protections = lifecycle.protections(settings)
    rel = str(path.relative_to(settings.project_root))
    assert rel in protections
    assert 'report_history_registered_source' in lifecycle.blocked(rel, protections)[0]
    plan = lifecycle.build_plan(settings, families={'intermediate_export'})
    assert not any(rel in item['paths'] for item in plan['units'])


def test_current_core_tracker_is_always_online(settings):
    path = settings.core_output_dir / '优质基金累积跟踪.xlsx'
    path.parent.mkdir(parents=True)
    path.write_bytes(b'current tracker fixture')
    with store.connection(history.database(settings), write=True):
        pass
    rel = str(path.relative_to(settings.project_root))
    protections = lifecycle.protections(settings)
    assert protections[rel] == 'online_tracker'
    assert protections[rel] not in retention.RELEASE_REASONS


def test_container_visibility_does_not_authorize_original_retirement(settings,monkeypatch):
    path,_=historical(settings)
    monkeypatch.setenv('FUND_ANA_STORAGE_LIFECYCLE_PROJECT_ROOT',str(settings.project_root))
    assert retention.eligibility(settings,path,check_gate=False)==['host_process_descriptor_check_required']


def test_web_container_cannot_bypass_descriptor_guard_by_unsetting_environment(settings,monkeypatch):
    path,_=historical(settings)
    monkeypatch.delenv('FUND_ANA_STORAGE_LIFECYCLE_PROJECT_ROOT',raising=False)
    monkeypatch.setattr(retention,'in_container',lambda:True)
    assert retention.eligibility(settings,path,check_gate=False)==['host_process_descriptor_check_required']


def test_busy_natural_maintenance_is_not_completed_acceptance(settings):
    now=datetime.now().astimezone();root=settings.project_root
    control=root/retention.CONTROL;control.mkdir(parents=True)
    parity=root/'parity.json';parity.write_text('{"status":"verified"}')
    (control/'activation.json').write_text(json.dumps({'activated_at':(now-timedelta(days=1)).isoformat(),
        'parity_path':'parity.json','parity_sha256':store.original_hash(parity)}))
    directory=root/'reports/.audit/scheduler_task_runs'/now.strftime('%Y%m%d');directory.mkdir(parents=True)
    value={'task':{'started_at':now.isoformat(),'task_name':'scheduled_storage_lifecycle','status':'skipped'},
           'task_result':{'trigger_source':'cron','status':'skipped','storage_lifecycle':{'status':'deferred'}}}
    (directory/'fixture_storage_lifecycle.json.gz').write_bytes(gzip.compress(json.dumps(value).encode()))
    assert retention.acceptance(settings)['natural_maintenance']==0


@pytest.mark.parametrize('days',[13,14])
def test_recent_publication_protected(settings,monkeypatch,days):
    path,_=historical(settings,days);head(settings)
    monkeypatch.setattr(retention,'backup_covers',lambda *a:True)
    reason=retention.eligibility(settings,path,check_gate=False,
        now=datetime.fromtimestamp(path.stat().st_mtime).astimezone()+timedelta(days=days))
    assert 'publication_within_14_days' in reason


def test_latest_publication_protected_even_when_old(settings,monkeypatch):
    path,_=historical(settings,40);head(settings,'fixture')
    monkeypatch.setattr(retention,'backup_covers',lambda *a:True)
    assert retention.eligibility(settings,path,check_gate=False)==['latest_publication']


def test_backup_required(settings):
    path,_=historical(settings);head(settings)
    assert retention.eligibility(settings,path,check_gate=False)==['verified_backup_not_covering_object']


def test_later_loss_of_sibling_source_blocks_other_originals(settings, monkeypatch):
    path, _ = historical(settings, 40)
    sibling = path.with_name('diagnostic.xlsx')
    sibling.write_bytes(path.read_bytes())
    report = json.loads((path.parent / 'report.json').read_text())
    report['run_id'] = 'second'
    report['artifacts']['diagnostic_excel'] = str(sibling.relative_to(settings.project_root))
    second = path.parent / 'second.json'
    second.write_text(json.dumps(report))
    history.import_report(settings, second, 'diagnostic')
    assert history.semantic_audit(settings)['status'] == 'verified'
    head(settings)
    monkeypatch.setattr(retention, 'backup_covers', lambda *a: True)
    sibling.unlink()
    assert retention.eligibility(settings, path, check_gate=False) == ['owner_original_missing_without_verified_retirement']
    assert path.exists()


def test_new_mtime_protected(settings,monkeypatch):
    path,_=historical(settings);head(settings);os.utime(path,None)
    monkeypatch.setattr(retention,'backup_covers',lambda *a:True)
    assert retention.eligibility(settings,path,check_gate=False)==['actual_mtime_within_14_days']


def test_unknown_missing_is_not_retired(settings):
    path,_=historical(settings);path.unlink()
    assert retention.original_state(settings.project_root,path)['status']!='original_retired'
    assert not store.available(path,'workbook')


def test_api_exports_and_unknown_revision(settings):
    path,result=historical(settings)
    app=Flask(__name__);register_history_routes(app,settings);client=app.test_client()
    response=client.get('/api/reports/history/fixture')
    assert response.status_code==200 and response.json['publication_truth'] is False
    assert response.json['views']['tracker']['original_status']=='online'
    response=client.get('/api/reports/history/fixture/export?view=tracker&format=xlsx')
    assert response.status_code==200 and response.data[:2]==b'PK'
    assert client.get('/api/reports/history/fixture?revision=unknown').status_code==404
    assert client.get('/api/reports/history/fixture/export?view=arbitrary').status_code==400


def test_temporary_storage_timer_starts_on_migration(settings):
    path,_=historical(settings,100)
    unit={'id':'fixture','family':retention.FAMILY,'paths':[str(path.relative_to(settings.project_root))]}
    with lifecycle.connect(settings.project_root) as conn:
        lifecycle._write_unit(conn,unit,'plan','offline')
        at=conn.execute('SELECT quarantined_at FROM report_retirement_times').fetchone()[0]
        assert datetime.now().astimezone()-datetime.fromisoformat(at)<timedelta(seconds=3)
        lifecycle._write_unit(conn,unit,'plan','online')
        assert conn.execute('SELECT count(*) FROM report_retirement_times').fetchone()[0]==0


def test_hard_link_source_same_bytes_new_inode_is_accepted(settings):
    path,_=historical(settings);before=store.view(path,'workbook')[1]['digest']
    raw=path.read_bytes();path.unlink();path.write_bytes(raw)
    assert store.view(path,'workbook')[1]['digest']==before


def test_changed_view_payload_blocks_proof(settings):
    path,_=historical(settings)
    with store.connection(history.database(settings),write=True) as conn:
        conn.execute("UPDATE objects SET payload=x'00' WHERE digest IN (SELECT digest FROM views)")
    with pytest.raises(store.HistoryError):retention.source_proof(settings,path)


def eligible_plan(settings,monkeypatch):
    path,_=historical(settings,40);head(settings)
    monkeypatch.setattr(retention,'acceptance',lambda *a:{'ready':True,'reasons':[]})
    monkeypatch.setattr(retention,'backup_covers',lambda *a:True)
    plan=lifecycle.build_plan(settings,families={retention.FAMILY})
    assert len(plan['units'])==1, json.dumps({'retained':plan['retained'],'protected':plan['protected'],'eligibility':retention.eligibility(settings,path),'options':retention.options(settings)},ensure_ascii=False)
    return path,plan


def age_quarantine(settings,days):
    at=(datetime.now().astimezone()-timedelta(days=days)).isoformat()
    with lifecycle.connect(settings.project_root) as conn:
        conn.execute('UPDATE report_retirement_times SET quarantined_at=?',(at,));conn.commit()


def test_migrate_restore_resets_quarantine_and_keeps_hardlink(settings,monkeypatch):
    path,plan=eligible_plan(settings,monkeypatch)
    link=settings.project_root/'reports/fixture-link.xlsx';os.link(path,link)
    inode=path.stat().st_ino
    assert lifecycle.run_plan(settings,plan,apply=True)['status']=='complete'
    assert retention.original_state(settings.project_root,path)['status']=='archived_offline'
    assert store.available(path,'workbook')
    assert retention.purge(settings,execute=True)['completed']==[]
    assert lifecycle.run_plan(settings,plan,apply=True,restore=True)['status']=='complete'
    assert path.stat().st_ino==inode==link.stat().st_ino
    assert lifecycle.run_plan(settings,plan,apply=True)['status']=='complete'
    with lifecycle.connect(settings.project_root) as conn:
        at=conn.execute('SELECT quarantined_at FROM report_retirement_times').fetchone()[0]
    assert datetime.now().astimezone()-datetime.fromisoformat(at)<timedelta(seconds=3)


def test_only_retired_view_purged_after_30_full_days(settings,monkeypatch):
    path,plan=eligible_plan(settings,monkeypatch)
    lifecycle.run_plan(settings,plan,apply=True)
    age_quarantine(settings,29.999)
    assert retention.purge(settings,execute=True)['completed']==[]
    age_quarantine(settings,30.001)
    result=retention.purge(settings,execute=True)
    assert result['status']=='complete' and len(result['completed'])==1
    assert retention.original_state(settings.project_root,path)['status']=='original_retired'
    assert store.available(path,'workbook')
    assert retention.purge(settings,execute=True)['completed']==[]
    monkeypatch.setattr('backend.publication_transaction.load_publication_head',lambda *a:{})
    assert history.get_report(settings,'fixture')['run_id']=='fixture'
    assert (path.parent/'report.json').exists()


def test_purge_refuses_corrupt_or_open_quarantine(settings,monkeypatch):
    path,plan=eligible_plan(settings,monkeypatch)
    lifecycle.run_plan(settings,plan,apply=True);age_quarantine(settings,31)
    archived=settings.storage_lifecycle_archive_root/path.relative_to(settings.project_root)
    monkeypatch.setattr(lifecycle,'open_inodes',lambda:{(archived.stat().st_dev,archived.stat().st_ino)})
    assert retention.purge(settings,execute=True)['errors'] and archived.exists()
    monkeypatch.setattr(lifecycle,'open_inodes',lambda:set())
    archived.write_bytes(b'corrupted artificial fixture')
    assert retention.purge(settings,execute=True)['errors'] and archived.exists()


def test_reconcile_crash_after_quarantine_unlink(settings,monkeypatch):
    path,plan=eligible_plan(settings,monkeypatch)
    lifecycle.run_plan(settings,plan,apply=True);age_quarantine(settings,31)
    archived=settings.storage_lifecycle_archive_root/path.relative_to(settings.project_root)
    with lifecycle.connect(settings.project_root) as conn:
        conn.execute("UPDATE units SET state='purging'");conn.commit()
    archived.unlink()
    result=retention.purge(settings,execute=True)
    assert result['status']=='complete'
    assert retention.original_state(settings.project_root,path)['status']=='original_retired'


def test_verified_backup_restores_without_source_reports(settings):
    path,_=historical(settings);head(settings)
    backup=retention.create_backup(settings)
    assert backup['independent_restore_verified']
    key=retention.source_proof(settings,path)['digest']
    assert retention.backup_covers(settings,key)
    artifact=Path(backup['artifact_path']);artifact.write_bytes(b'corrupted backup fixture')
    assert not retention.backup_covers(settings,key)


def test_history_database_failure_does_not_return_empty_success(settings):
    path,_=historical(settings)
    app=Flask(__name__);register_history_routes(app,settings);client=app.test_client()
    db=history.database(settings)
    db.unlink()
    assert client.get('/api/reports/history/fixture').status_code==503
    db.write_bytes(b'not sqlite fixture')
    assert client.get('/api/reports/history/fixture').status_code==409


def test_old_restored_catalog_unit_cannot_mask_new_report_retirement(settings,monkeypatch):
    path,plan=eligible_plan(settings,monkeypatch)
    rel=str(path.relative_to(settings.project_root))
    old_unit={'id':hashlib.sha256(rel.encode()).hexdigest(),'family':'intermediate_export','paths':[rel],'entries':[]}
    with lifecycle.connect(settings.project_root) as conn:
        lifecycle._write_unit(conn,old_unit,'older-plan','online')
    assert plan['units'][0]['id']!=old_unit['id']
    assert lifecycle.run_plan(settings,plan,apply=True)['status']=='complete'
    assert retention.original_state(settings.project_root,path)['status']=='archived_offline'


def test_original_url_returns_replacement_and_precise_failure(settings,monkeypatch):
    import inspect
    from backend.api_routes.reports import register_reports_routes
    from backend.artifact_paths import resolve_project_artifact_path
    path,plan=eligible_plan(settings,monkeypatch)
    lifecycle.run_plan(settings,plan,apply=True)
    monkeypatch.setattr('backend.config.load_settings',lambda:settings)
    app=Flask(__name__)
    options={key:lambda *a,**k:{} for key in inspect.signature(register_reports_routes).parameters if key!='app'}
    options['resolve_project_file']=lambda value:resolve_project_artifact_path(value,project_root=settings.project_root)
    register_reports_routes(app,**options);client=app.test_client()
    url='/api/files/'+str(path.relative_to(settings.project_root))
    response=client.get(url)
    assert response.status_code==410 and response.json['alternatives'][0]['run_id']=='fixture'
    history.database(settings).unlink()
    assert client.get(url).status_code==503


def test_online_original_corruption_is_409_without_ledger_dependency(settings):
    import inspect
    from backend.api_routes.reports import register_reports_routes
    from backend.artifact_paths import resolve_project_artifact_path
    path,_=historical(settings)
    (path.parent/'validated.json').write_text(json.dumps({'files':{path.name:store.original_hash(path)}}))
    path.write_bytes(b'corrupt original fixture')
    app=Flask(__name__)
    options={key:lambda *a,**k:{} for key in inspect.signature(register_reports_routes).parameters if key!='app'}
    options['resolve_project_file']=lambda value:resolve_project_artifact_path(value,project_root=settings.project_root)
    register_reports_routes(app,**options)
    assert app.test_client().get('/api/files/'+str(path.relative_to(settings.project_root))).status_code==409
