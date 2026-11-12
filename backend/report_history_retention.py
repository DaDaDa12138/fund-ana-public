"""Narrow retirement policy for ledger-backed report views, never raw evidence."""
from __future__ import annotations

from datetime import datetime, timedelta
import gzip
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import time

from backend import report_history as history
from core.utils import report_history_store as store

FAMILY = 'regeneratable_report_view'
CONTROL = 'reports/.audit/report_history'
RELEASE_REASONS = {'formal_manifest', 'formal_generation_history', 'canonical_history_reference',
                   'cumulative_tracker_rebuild_dependency', 'cumulative_tracker_processed_result',
                   'cumulative_tracker_processed_result_lineage', 'report_history_registered_source'}
_BACKUP_HASHES = {}


def in_container() -> bool:
    return Path('/.dockerenv').is_file()


def options(settings):
    import yaml
    value = yaml.safe_load((settings.project_root/'config/project.yaml').read_text()) or {}
    return value.get('report_history') or {}


def read(path):
    return json.loads(path.read_text())


def acceptance(settings) -> dict:
    root=settings.project_root; reasons=[]; activation=root/CONTROL/'activation.json'
    if not activation.exists(): return {'ready':False,'reasons':['ledger_not_activated'],'natural_daily':0}
    active=read(activation); since=datetime.fromisoformat(active['activated_at']).astimezone()
    parity=root/active['parity_path']
    if (not parity.is_file() or store.original_hash(parity)!=active['parity_sha256']
            or read(parity).get('status')!='verified'):
        reasons.append('parity_evidence_missing_or_changed')
    # Evidence is read from actual cron completion audits, never simulated wakeups.
    daily=[]; maintenance=[]; advanced=[]; daily_dates=set()
    for directory in sorted((root/'reports/.audit/scheduler_task_runs').glob('*')):
        if directory.name < since.strftime('%Y%m%d'): continue
        for path in directory.glob('*.json.gz'):
            if not any(word in path.name for word in ('daily_bond_refresh','storage_lifecycle')):continue
            value=json.loads(gzip.decompress(path.read_bytes())); task=value['task']; result=value['task_result']
            at=datetime.fromisoformat(task['started_at']).astimezone()
            if at<since or result.get('trigger_source')!='cron' or task.get('status') not in ('success','skipped'):continue
            if task['task_name']=='scheduled_daily_bond_refresh':
                if result.get('screening_triggered'):
                    run=result.get('screening_run_id') or result.get('workflow_run_id')
                    head=read(root/'reports/.publication_current/publication_state.json')
                    entry=head.get('committed_runs',{}).get(run)
                    if not entry or not (root/entry['manifest_path']).with_name('history_binding.json').exists():continue
                elif result.get('screening_reason') not in (
                        'effective_nav_snapshot_already_screened', 'effective_nav_date_already_screened',
                        'effective_nav_date_already_screened_pending_raw_nav_completion'):
                    continue
                if at.date() not in daily_dates:
                    daily.append(str(path.relative_to(root)));daily_dates.add(at.date())
                if result.get('screening_reason')=='effective_nav_snapshot_advanced' and result.get('screening_triggered'):
                    advanced.append(str(path.relative_to(root)))
            elif (task['task_name']=='scheduled_storage_lifecycle' and task.get('status')=='success'
                  and (result.get('storage_lifecycle') or {}).get('status')=='complete'):
                maintenance.append(str(path.relative_to(root)))
    if len(daily)<2:reasons.append('two_natural_daily_tasks_pending')
    if not advanced:reasons.append('natural_nav_advance_pending')
    if not maintenance:reasons.append('natural_storage_maintenance_pending')
    validation=root/CONTROL/'workflow_acceptance.json'
    if not validation.is_file():reasons.append('real_workflow_and_same_day_verification_pending')
    else:
        evidence=read(validation)
        if not evidence.get('real_workflow_verified') or not evidence.get('same_day_verified'):
            reasons.append('real_workflow_or_same_day_verification_failed')
        if not evidence.get('evidence'):reasons.append('workflow_evidence_not_bound')
        for item in evidence.get('evidence',[]):
            p=root/store.relative(root,item['path'])
            if not p.is_file() or store.original_hash(p)!=item['sha256']: reasons.append('workflow_evidence_changed')
    return {'ready':not reasons,'reasons':reasons,'natural_daily':len(daily),'natural_advanced':len(advanced),
            'natural_maintenance':len(maintenance),'evidence':daily+advanced+maintenance}


def source_proof(settings, path:Path, sha256:str|None=None) -> dict:
    root=settings.project_root;db=history.database(settings);rel=store.relative(root,path)
    with store.connection(db) as conn:
        rows=conn.execute('SELECT sha256,kind,digest FROM views WHERE path=?',(rel,)).fetchall()
        rows=[r for r in rows if r[1] in ('workbook','csv','markdown') and (sha256 is None or r[0]==sha256)]
        if len({r[0] for r in rows})!=1:raise store.HistoryError('retirement_source_not_unambiguous')
        sha,kind,key=rows[0];store.get_object(conn,key)
        if not conn.execute('SELECT 1 FROM validations WHERE path=? AND sha256=? AND kind=? AND digest=?',
                            (rel,sha,kind,key)).fetchone():
            raise store.HistoryError('retirement_semantic_parity_not_verified')
    return {'path':rel,'sha256':sha,'kind':kind,'digest':key}


def owners(settings,rel:str) -> list[dict]:
    result=[]
    with store.connection(history.database(settings)) as conn:
        for key, in conn.execute('SELECT r.digest FROM reports r JOIN report_sources s ON r.run_id=s.run_id AND r.revision=s.revision WHERE s.path=?',(rel,)):
            payload=store.get_object(conn,key)
            if any(s['path']==rel for s in payload['sources']):result.append(payload)
    return result


def eligibility(settings,path:Path,*,now=None,check_gate=True) -> list[str]:
    if path.suffix not in ('.xlsx','.csv','.md'):return ['not_a_regeneratable_report_extension']
    if history.mode(settings)!='ledger':return ['ledger_reader_not_active']
    # The scheduler's PID namespace cannot prove that host/web processes have
    # closed a file. Containers require the read-only host descriptor service;
    # an unavailable/invalid configured service fails closed at the scan.
    from backend.storage_descriptor_audit import configured_socket
    if (in_container() or os.environ.get('FUND_ANA_STORAGE_LIFECYCLE_PROJECT_ROOT')) and configured_socket() is None:
        return ['host_process_descriptor_check_required']
    if check_gate:
        gate=acceptance(settings)
        if not gate['ready']:return gate['reasons']
    try:
        proof=source_proof(settings,path,store.original_hash(path) if path.exists() else None)
        records=owners(settings,proof['path'])
        if not records:return ['report_owner_not_verified']
        cutoff=(now or datetime.now().astimezone())-timedelta(days=14)
        head=read(settings.project_root/'reports/.publication_current/publication_state.json')
        committed_times = publication_times(settings)
        recovery_keys = {proof['digest']}
        for record in records:
            if record.get('source_issues'):return ['owner_has_unresolved_source_references']
            # Imports prove completeness at that instant. Old writers or an
            # external loss must not silently authorize later retirement.
            for source in record['sources']:
                # The current file is governed by its lifecycle intent; this
                # includes resuming a durable purging intent after unlink.
                if source['path'] == proof['path']:
                    continue
                original = settings.project_root / store.relative(settings.project_root, Path(source['path']))
                if original.exists():
                    continue
                state = original_state(settings.project_root, original)
                replacement = state.get('replacement_record') or {}
                if (state.get('status') not in ('archived_offline', 'original_retired')
                        or replacement.get('sha256') != source['sha256']):
                    return ['owner_original_missing_without_verified_retirement']
            if record['run_id']==head['current_published_run_id']:return ['latest_publication']
            stamp=datetime.fromisoformat(committed_times.get((record['run_id'],record['revision']),
                                         record['report']['generated_at'])).astimezone()
            if stamp>=cutoff:return ['publication_within_14_days']
            recovery_keys.add(store.digest(record))
            recovery_keys.update(s['digest'] for s in record['sources'])
        if path.exists() and datetime.fromtimestamp(path.stat().st_mtime).astimezone()>=cutoff:
            return ['actual_mtime_within_14_days']
        # A matching restored recovery point must contain this exact object.
        if not backup_covers(settings,recovery_keys):return ['verified_backup_not_covering_object']
        return []
    except (OSError,ValueError,sqlite3.Error) as exc:return [str(exc)]


def publication_times(settings) -> dict:
    """Use actual pointer-commit timestamps, never report filename dates."""
    root=settings.project_root;path=root/'reports/.publication_current/publication_state.json'
    times={};seen=set()
    while path.is_file():
        state=read(path)
        if state.get('committed_at'):
            for run,entry in state.get('committed_runs',{}).items():
                key=(run,str(entry.get('revision') or ''))
                stamp=state['committed_at']
                if key not in times or stamp<times[key]:times[key]=stamp
        previous=state.get('previous_generation_dir')
        if not previous or previous in seen:break
        seen.add(previous);path=root/store.relative(root,Path(previous))/'publication_state.json'
        if not path.is_file():raise store.HistoryError('history_generation_missing_for_retention')
    return times


def backup_covers(settings,key) -> bool:
    pointer=settings.project_root/CONTROL/'latest_backup.json'
    if not pointer.exists():return False
    value=read(pointer); artifact=settings.project_root/store.relative(settings.project_root,Path(value['artifact_path']))
    required={key} if isinstance(key,str) else set(key)
    if not (required.issubset(value.get('object_digests',[])) and artifact.is_file()
            and value.get('independent_restore_verified') is True):return False
    before=(*store.file_identity(artifact),artifact.stat().st_ctime_ns)
    if _BACKUP_HASHES.get(str(artifact),{}).get('identity')!=before:
        sha=store.original_hash(artifact)
        if (*store.file_identity(artifact),artifact.stat().st_ctime_ns)!=before:return False
        if len(_BACKUP_HASHES)>16:_BACKUP_HASHES.clear()
        _BACKUP_HASHES[str(artifact)]={'identity':before,'sha256':sha}
    return _BACKUP_HASHES[str(artifact)]['sha256']==value['artifact_sha256']


def create_backup(settings) -> dict:
    from core.utils.sqlite_backup_snapshot import create_verified_sqlite_backup
    db=history.database(settings); stamp=datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    target=settings.project_root/'backend/data/backups'/f'report_history_{stamp}.db'
    value=create_verified_sqlite_backup(db,target,purpose='report_history',backup_label=target.stem,compress=True)
    artifact=Path(value['artifact_path'])
    with tempfile.TemporaryDirectory(prefix='fund-ana-history-restore-') as directory:
        restored=Path(directory)/'history.sqlite'
        with restored.open('xb') as stream:
            subprocess.run(['zstd','-d','-c',str(artifact)],stdout=stream,check=True)
        with store.connection(restored) as conn:
            if conn.execute('PRAGMA integrity_check').fetchall()!=[('ok',)]:raise store.HistoryError('history_backup_integrity')
            keys=[r[0] for r in conn.execute('SELECT digest FROM objects')]
            for key in keys:store.get_object(conn,key)
        value={**{k:str(v) if isinstance(v,Path) else v for k,v in value.items()},
               'artifact_path':str(artifact),'object_digests':keys,'independent_restore_verified':True}
    from backend.storage_lifecycle import atomic_json
    pointer=settings.project_root/CONTROL/'latest_backup.json';atomic_json(pointer,value)
    return value


def record_intent(settings,unit:dict):
    from backend.storage_lifecycle import atomic_json
    if len(unit['entries'])!=1:raise store.HistoryError('report_retirement_single_file_required')
    entry=unit['entries'][0];proof=source_proof(settings,settings.project_root/entry['path'],entry['sha256'])
    records=owners(settings,entry['path']); manifests=[]
    for record in records:
        original=settings.project_root/record.get('report',{}).get('artifacts',{}).get('report_json','')
        manifest=original.parent/'validated.json'
        if manifest.is_file():manifests.append({'path':str(manifest.relative_to(settings.project_root)),
                                               'sha256':store.original_hash(manifest)})
    receipt={'contract':'report-history-retirement-v1','unit_id':unit['id'],'source':proof,
             'manifests':manifests,'planned_at':datetime.now().astimezone().isoformat(),
             'reconstruction':'historical_values_only_not_original_bytes'}
    path=settings.project_root/CONTROL/'retirements'/f'{unit["id"]}.json'
    if path.exists():
        existing=read(path)
        if existing['source']!=proof:raise store.HistoryError('retirement_receipt_conflict')
    else:atomic_json(path,receipt)


def replacement(settings,path:Path,expected:str,manifest:Path) -> bool:
    from backend.storage_lifecycle import artifact_location
    if path.exists():return False
    if manifest.parent.resolve()==(settings.reports_dir/'.publication_current').resolve():return False
    location=artifact_location(settings.project_root,path)
    if location['status'] not in ('archived_offline','original_retired'):return False
    rel=store.relative(settings.project_root,path);unit=location['unit_id']
    receipt=settings.project_root/CONTROL/'retirements'/f'{unit}.json'
    if not receipt.exists():return False
    proof=read(receipt)
    if proof.get('contract')!='report-history-retirement-v1' or proof['source']['sha256']!=expected:return False
    if not any(x['path']==str(manifest.relative_to(settings.project_root)) and
               x['sha256']==store.original_hash(manifest) for x in proof['manifests']):return False
    with store.connection(history.database(settings)) as conn:store.get_object(conn,proof['source']['digest'])
    return True


def _purge(settings,*,execute=False,max_seconds=30,max_files=2000,max_bytes=8*1024**3) -> dict:
    from backend import storage_lifecycle as lifecycle
    result={'status':'preview','completed':[],'errors':[],'released_allocated_bytes':0,'verified_bytes':0,'verified_files':0}
    if not options(settings).get('purge_enabled') or not acceptance(settings)['ready']:
        return {**result,'status':'deferred','reason':'retirement_acceptance_pending'}
    started=time.monotonic();cutoff=datetime.now().astimezone()-timedelta(days=30)
    with lifecycle.locks(settings),lifecycle.connect(settings.project_root) as conn:
        opened=lifecycle.open_inodes()
        for uid,payload,plan_sha,state,quarantined in conn.execute("SELECT u.id,u.payload,u.plan_sha,u.state,t.quarantined_at FROM units u JOIN report_retirement_times t ON t.unit=u.id WHERE u.state IN ('offline','purging')").fetchall():
            unit=json.loads(payload)
            if unit['family']!=FAMILY or datetime.fromisoformat(quarantined)>=cutoff:continue
            if len(result['completed'])>=max_files or time.monotonic()-started>=max_seconds:break
            entry=unit['entries'][0];source=settings.project_root/entry['path']
            if eligibility(settings,source):continue
            archived=Path(settings.storage_lifecycle_archive_root)/entry['path']
            if state=='purging' and not archived.exists() and not source.exists():
                if execute: lifecycle._write_unit(conn,unit,plan_sha,'retired')
                result['completed'].append(uid)
                continue
            info=lifecycle.identity(archived)
            if result['verified_bytes']+info['size']>max_bytes:break
            result['verified_bytes']+=info['size']
            result['verified_files']+=1
            if ((info['device'],info['inode']) in opened or not lifecycle.same(info,entry['identity'])
                    or lifecycle.sha(archived)!=entry['sha256']):
                result['errors'].append({'unit':uid,'error':'quarantined_identity_or_hash_changed'});break
            if execute:
                # Durable delete intent allows reconciliation after unlink but before terminal receipt.
                lifecycle._write_unit(conn,unit,plan_sha,'purging')
                if info['nlink']==1:result['released_allocated_bytes']+=info['allocated']
                archived.unlink();lifecycle.sync_dir(archived.parent)
                lifecycle._write_unit(conn,unit,plan_sha,'retired')
            result['completed'].append(uid)
    result['status']='error' if result['errors'] else ('complete' if execute else 'preview')
    return result


def original_state(root:Path,path:Path) -> dict:
    if path.exists(): return {'status':'online'}
    from backend.storage_lifecycle import artifact_location
    location=artifact_location(root,path)
    rel=store.relative(root,path);uid=location.get('unit_id','unregistered')
    receipt=root/CONTROL/'retirements'/f'{uid}.json'
    if location['status'] in ('archived_offline','original_retired') and receipt.exists():
        value=read(receipt)
        if value.get('contract')!='report-history-retirement-v1' or value.get('source',{}).get('path')!=rel:
            raise store.HistoryError('retirement_path_mismatch')
        options=store.project_options(path)
        if not options:raise store.HistoryError('history_database_unavailable')
        with store.connection(options[1]) as conn:
            source=value['source']
            match=conn.execute('SELECT digest FROM views WHERE path=? AND sha256=? AND kind=?',
                               (rel,source['sha256'],source['kind'])).fetchone()
            if not match or match[0]!=source['digest']:raise store.HistoryError('retirement_binding_mismatch')
            store.get_object(conn,source['digest'])
        return {**location,'replacement_record':value['source']}
    return {'status':'missing' if location['status']=='archived_offline' else location['status']}


def purge(settings,**kwargs):
    from backend.storage import WorkflowLockBusyError
    try:
        return _purge(settings,**kwargs)
    except (BlockingIOError,WorkflowLockBusyError):
        return {'status':'deferred','reason':'workflow_or_storage_lock_busy','completed':[],'errors':[]}
