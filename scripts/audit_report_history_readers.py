#!/usr/bin/env python3
"""Compare real historical calculations using fixed selectors and NAV inputs.

Read-only with respect to report originals. Evidence goes to an explicit new
output directory. No workflow, file cleanup or financial rule changes occur.
"""
from __future__ import annotations

import argparse
import builtins
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT),str(ROOT/'core')]
import pandas as pd
from backend.config import load_settings
from backend import project_tracker as tracker, forward_validation as forward
from backend import report_history as history
from backend.publication import build_publication_registry
from backend.publication_transaction import load_publication_head
from core.utils import report_history_store as store
from bond_fund_screening.utils.accumulation_tracker import AccumulationTracker


@contextmanager
def reading_mode(value):
    previous=os.environ.get('FUND_ANA_REPORT_HISTORY_MODE')
    os.environ['FUND_ANA_REPORT_HISTORY_MODE']=value
    try:yield
    finally:
        if previous is None:os.environ.pop('FUND_ANA_REPORT_HISTORY_MODE',None)
        else:os.environ['FUND_ANA_REPORT_HISTORY_MODE']=previous


def frame_hash(frame):
    return store.digest(store.frame_payload(frame))


def formal_calculation(settings,report,frozen_history):
    return tracker._build_project_final_tracker(settings,
        current_run_id=report['run_id'],current_generated_at=report['generated_at'],
        current_filtered_excel=settings.project_root/report['artifacts']['filtered_excel'],
        current_final_count=report['counts']['final_count'],
        current_nav_snapshot_date=report['database_freshness']['effective_nav_snapshot']['candidate_date'],
        try_reuse_project_final_tracker=lambda *a,**k:None,
        load_project_report_history=lambda *a,**k:frozen_history)


def core_calculation(path,records):
    value=AccumulationTracker(str(path),verbose=False)
    stats=value._rebuild_tracker_from_records(records)
    return value.df_tracker,{'periods':value.total_comparisons,'last_stats':stats}


def forward_calculation(snapshots,nav_inputs,cache):
    results=[]
    code_sha=store.original_hash(Path(forward.__file__))
    cache.mkdir(exist_ok=True)
    for ordinal,(snapshot,nav_input) in enumerate(zip(snapshots,nav_inputs)):
        print('forward snapshot',ordinal+1,len(snapshots),snapshot.run_id,flush=True)
        raw=gzip.decompress(Path(nav_input['path']).read_bytes())
        if hashlib.sha256(raw).hexdigest()!=nav_input['digest']:raise ValueError('frozen_nav_input_changed')
        nav=store.payload_frame(store.decode(json.loads(raw)))
        del raw
        formal=forward._load_formal_frame(snapshot.filtered_excel)
        watch=forward._load_watch_frame(snapshot.watch_excel)
        groups=forward._select_snapshot_groups(formal,watch)
        codes=sorted({code for values in groups.values() for code in values if code})
        nav_map={code:group.copy() for code,group in nav.groupby('fcode',sort=False)} if not nav.empty else {}
        windows=[]
        nav_sha=frame_hash(nav)
        for label,days in forward.FORWARD_VALIDATION_WINDOWS:
            # Reuse only the deterministic NAV calculation on exactly identical
            # frozen inputs. Membership selection and all cohort calculations
            # still run independently for each reader and restored ledger.
            key=store.digest({'code_sha':code_sha,'nav':nav_sha,'codes':codes,
                              'snapshot_date':snapshot.snapshot_date,'days':days})
            cached=cache/(key+'.json.gz')
            if cached.exists():
                payload=store.decode(json.loads(gzip.decompress(cached.read_bytes())))
                if store.digest(payload['frame'])!=payload['digest']:raise ValueError('audit_metrics_cache_corrupt')
                metrics=store.payload_frame(payload['frame'])
            else:
                metrics=forward._build_window_metrics_frame(codes,nav_map,
                    snapshot_date=forward._safe_date(snapshot.snapshot_date),horizon_days=days)
                payload=store.frame_payload(metrics)
                cached.write_bytes(gzip.compress(store.canonical({'frame':payload,'digest':store.digest(payload)}),mtime=0))
            cohorts=[]
            for group,members in groups.items():
                summary,detail=forward._cohort_metrics(group,members,metrics)
                cohorts.append({'group':group,'summary':summary,'detail_digest':store.digest(detail)})
            windows.append({'label':label,'metrics_digest':frame_hash(metrics),'cohorts':cohorts})
        results.append({'run_id':snapshot.run_id,'snapshot_date':snapshot.snapshot_date,
                        'formal_digest':frame_hash(formal),'watch_digest':frame_hash(watch),
                        'groups':groups,'windows':windows})
    return results


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',type=Path,required=True)
    parser.add_argument('--backup',type=Path,help='Independently restore and forbid reading old spreadsheet/CSV bytes')
    parser.add_argument('--resume',action='store_true',help='Reuse exact frozen NAV inputs from this audit directory')
    parser.add_argument('--restore-only',action='store_true',help='Recheck independent restore against completed calculation checkpoints')
    args=parser.parse_args();out=args.output_dir.absolute();out.mkdir(parents=True,exist_ok=args.resume)
    settings=load_settings();head=load_publication_head(settings)
    entry=head['committed_runs'][head['current_published_run_id']]
    report=json.loads((settings.project_root/entry['report_path']).read_bytes())
    registry=build_publication_registry(settings)
    kwargs=dict(current_run_id=report['run_id'],current_generated_at=report['generated_at'],
        current_filtered_excel=settings.project_root/report['artifacts']['filtered_excel'],
        current_final_count=report['counts']['final_count'],
        current_nav_snapshot_date=report['database_freshness']['effective_nav_snapshot']['candidate_date'],
        publication_registry_builder=lambda _:registry)
    paths=sorted(settings.core_output_dir.glob('*累积跟踪.xlsx'))
    if not paths:raise ValueError('core_tracker_not_found')
    core_inputs=[]
    for path in paths:
        loaded=AccumulationTracker(str(path),verbose=False)
        if not loaded._load_tracker() or not loaded.processed_results:raise ValueError('core_records_unavailable')
        core_inputs.append((path,list(loaded.processed_results)))
    current=forward.ForwardValidationSnapshot(run_id=report['run_id'],generated_at=report['generated_at'],
        snapshot_date=kwargs['current_nav_snapshot_date'],filtered_excel=kwargs['current_filtered_excel'],
        watch_excel=settings.project_root/report['artifacts']['watch_excel'],final_count=report['counts']['final_count'],
        stable_final_count=report['counts'].get('stable_bond_final_count',0),
        enhanced_final_count=report['counts'].get('enhanced_bond_final_count',0))
    with reading_mode('legacy'):
        selected=tracker._load_project_report_history(settings,**kwargs)
        snapshots=forward._load_snapshot_records(settings,current)
    frozen=out/'inputs.json.gz'
    nav_directory=out/'nav_inputs';nav_directory.mkdir(exist_ok=True)
    split_index=out/'nav_index.json'
    split=json.loads(split_index.read_bytes()) if args.resume and split_index.exists() else None
    prior=None
    if split:
        if split['source_sha256']!=store.original_hash(frozen):raise ValueError('frozen_input_binding_changed')
        prior=store.decode(split['metadata']);nav_inputs=split['nav_inputs']
    elif args.resume and frozen.exists():
        # Decode typed values one snapshot at a time; cloning the complete
        # 265 MB JSON tree exceeded the host memory limit during real audit.
        prior=json.loads(gzip.decompress(frozen.read_bytes()))
        frames=prior.pop('forward_nav');nav_inputs=[]
        for index,frame in enumerate(frames):
            raw=json.dumps(frame,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
            path=nav_directory/(str(index)+'.json.gz');path.write_bytes(gzip.compress(raw,mtime=0))
            nav_inputs.append({'path':str(path),'digest':hashlib.sha256(raw).hexdigest()})
            frames[index]=None
        del frames,frame,raw
        split_index.write_bytes(store.canonical({'source_sha256':store.original_hash(frozen),
                                                'metadata':prior,'nav_inputs':nav_inputs}))
        prior=store.decode(prior)
    if prior and prior['head']!=entry:raise ValueError('resume_publication_identity_changed')
    if prior:
        core_inputs=[(Path(item['tracker']),item['records']) for item in prior['core_records']]
    if not prior:nav_inputs=[]
    for item in ([] if prior else snapshots):
        with reading_mode('legacy'):
            groups=forward._select_snapshot_groups(forward._load_formal_frame(item.filtered_excel),forward._load_watch_frame(item.watch_excel))
        codes=sorted({code for values in groups.values() for code in values if code})
        stamp=forward._safe_date(item.snapshot_date)
        nav=forward._load_nav_history_for_codes(settings.database_path,codes,
            start_date=(stamp-timedelta(days=forward.START_NAV_MAX_LAG_DAYS)).date().isoformat(),
            end_date=(stamp+timedelta(days=max(d for _,d in forward.FORWARD_VALIDATION_WINDOWS)+forward.END_NAV_MAX_LAG_DAYS)).date().isoformat())
        raw=store.canonical(store.frame_payload(nav));path=nav_directory/(str(len(nav_inputs))+'.json.gz')
        path.write_bytes(gzip.compress(raw,mtime=0))
        nav_inputs.append({'path':str(path),'digest':hashlib.sha256(raw).hexdigest()})
    inputs={'contract':'report-history-reader-inputs-v1','head':entry,'formal_history':[
        {k:str(v) if isinstance(v,Path) else v for k,v in row.items()} for row in selected],
        'core_records':[{ 'tracker':str(path),'records':records} for path,records in core_inputs]}
    if prior:
        if store.digest(inputs)!=store.digest(prior):raise ValueError('resume_frozen_input_identity_changed')
    else:
        frozen.write_bytes(gzip.compress(store.canonical(inputs),mtime=0))
        split_index.write_bytes(store.canonical({'source_sha256':store.original_hash(frozen),
                                                'metadata':inputs,'nav_inputs':nav_inputs}))
    outputs={}
    if args.restore_only:
        if not args.backup:raise ValueError('restore_only_requires_backup')
        for value in ('legacy','ledger'):
            outputs[value]=store.decode(json.loads(gzip.decompress((out/('calculation_'+value+'.json.gz')).read_bytes())))
    for value in (() if args.restore_only else ('legacy','ledger')):
        print('reader audit:',value,flush=True)
        with reading_mode(value):
            selected_now=tracker._load_project_report_history(settings,**kwargs)
            if selected_now!=selected:raise ValueError('formal_history_selector_changed')
            if forward._load_snapshot_records(settings,current)!=snapshots:raise ValueError('forward_selector_changed')
            formal=formal_calculation(settings,report,selected)
            core=[]
            for path,records in core_inputs:
                frame,summary=core_calculation(path,records)
                # Exact same-day replacement/rebuild must be idempotent too.
                again,summary_again=core_calculation(path,records)
                pd.testing.assert_frame_equal(frame,again,check_exact=True)
                if summary!=summary_again:raise ValueError('same_day_rebuild_changed_counts')
                core.append({'path':str(path),'digest':frame_hash(frame),'summary':summary})
            outputs[value]={'formal_tracker':frame_hash(formal[0]),'formal_process':frame_hash(formal[1]),
                'formal_summary':formal[2],'core':core,'forward':forward_calculation(snapshots,nav_inputs,out/'metrics')}
            (out/('calculation_'+value+'.json.gz')).write_bytes(gzip.compress(store.canonical(outputs[value]),mtime=0))
    if store.digest(outputs['legacy'])!=store.digest(outputs['ledger']):
        (out/'differences.json').write_bytes(store.canonical(outputs));raise ValueError('history_calculation_parity_failed')
    backup_verified=False
    if args.backup:
        restore_directory=tempfile.TemporaryDirectory(prefix='fund-ana-history-independent-')
        restored=Path(restore_directory.name)/'restored.sqlite'
        with restored.open('xb') as stream:subprocess.run(['zstd','-d','-c',str(args.backup)],stdout=stream,check=True)
        restored_settings=replace(settings,report_history_database_path=restored)
        with store.connection(restored) as conn:
            if conn.execute('PRAGMA integrity_check').fetchall()!=[('ok',)]:raise ValueError('restored_ledger_corrupt')
            for key, in conn.execute('SELECT digest FROM objects'):store.get_object(conn,key)
        original_options=store.project_options;original_open=Path.open
        original_builtin_open=builtins.open;original_io_open=io.open
        def options(path):
            value=original_options(path)
            return (value[0],restored,'ledger') if value and value[0]==settings.project_root else value
        def require_independent(path):
            if isinstance(path,(str,bytes,os.PathLike)):
                path=Path(os.fsdecode(path)).absolute()
                if path.suffix in ('.xlsx','.csv') and path.is_relative_to(settings.reports_dir):
                    raise ValueError('old_report_bytes_read_during_independent_restore:'+str(path))
        def no_old_reports(path,*a,**k):
            require_independent(path)
            return original_open(path,*a,**k)
        def no_builtin_reports(path,*a,**k):
            require_independent(path);return original_builtin_open(path,*a,**k)
        def no_io_reports(path,*a,**k):
            require_independent(path);return original_io_open(path,*a,**k)
        def original_hash(path,algorithm='sha256'):
            if Path(path).suffix not in ('.xlsx','.csv'):
                with original_open(path,'rb') as stream:
                    return hashlib.file_digest(stream,algorithm).hexdigest()
            info=store.source_info(settings.project_root,restored,path)
            if info is None:raise ValueError('restored_history_source_missing:'+str(path))
            return info[algorithm]
        with reading_mode('ledger'),patch.object(store,'project_options',options),patch.object(store,'original_hash',original_hash),patch.object(Path,'open',no_old_reports),patch.object(builtins,'open',no_builtin_reports),patch.object(io,'open',no_io_reports):
            formal=formal_calculation(restored_settings,report,selected)
            if frame_hash(formal[0])!=outputs['ledger']['formal_tracker']:raise ValueError('restored_formal_changed')
            for (path,records),expected in zip(core_inputs,outputs['ledger']['core']):
                frame,summary=core_calculation(path,records)
                if frame_hash(frame)!=expected['digest'] or summary!=expected['summary']:raise ValueError('restored_core_changed')
            if store.digest(forward_calculation(snapshots,nav_inputs,out/'metrics'))!=store.digest(outputs['ledger']['forward']):
                raise ValueError('restored_forward_changed')
        backup_verified=True
        restore_directory.cleanup()
    result={'contract':'report-history-reader-parity-v1','status':'verified','checked_at':datetime.now().astimezone().isoformat(),
        'run_id':report['run_id'],'revision':entry['revision'],'formal_history_count':len(selected),
        'forward_snapshot_count':len(snapshots),'input_path':str(frozen),'input_sha256':store.original_hash(frozen),
        'nav_index_path':str(split_index),'nav_index_sha256':store.original_hash(split_index),
        'same_day_rebuild_verified':True,'independent_backup_without_old_reports':backup_verified,
        'independent_open_guards':['Path.open','builtins.open','io.open'],
        'results':outputs['ledger']}
    (out/('receipt_guarded.json' if args.restore_only else 'receipt.json')).write_bytes(store.canonical(result));print(json.dumps({k:v for k,v in result.items() if k!='results'},ensure_ascii=False))
    return 0

if __name__=='__main__':raise SystemExit(main())
