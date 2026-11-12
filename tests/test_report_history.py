"""Synthetic serialization/storage fixtures; these are not fund business observations."""
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from core.utils import report_history_store as store
from backend import report_history as history


@pytest.fixture
def settings(tmp_path):
    root = tmp_path / 'project'; (root / 'config').mkdir(parents=True)
    (root / 'config/project.yaml').write_text('report_history:\n  read_mode: ledger\n')
    return SimpleNamespace(project_root=root, reports_dir=root/'reports', runs_dir=root/'reports/runs',
                           archived_runs_dir=root/'reports/archive/superseded_runs',
                           core_output_dir=root/'reports/core_output',
                           report_history_database_path=root/'backend/data/report_history.sqlite')


def sheet(settings):
    path = settings.reports_dir/'runs/fixture/revisions/a/formal.xlsx'
    path.parent.mkdir(parents=True)
    pd.DataFrame({'code': ['000001', '000002'], 'value': [0.0, None], 'state': ['zero', None]}).to_excel(path,index=False)
    return path


def test_nullable_typed_frame_round_trip():
    frame = pd.DataFrame({'code': ['000001', '000002'], 'float': [0.0, float('nan')],
                          'nullable': pd.Series([0, pd.NA], dtype='Int64'),
                          'text': ['', None], 'date': pd.to_datetime(['2020-01-01', None])})
    encoded = store.canonical(store.frame_payload(frame))
    restored = store.payload_frame(store.decode(json.loads(encoded)))
    pd.testing.assert_frame_equal(frame, restored)
    assert restored.loc[0, 'code'] == '000001'
    assert restored.loc[0, 'float'] == 0
    assert restored.loc[1, 'text'] is None


def test_excel_datetime_and_timestamp_keep_distinct_canonical_types():
    from datetime import datetime,date
    values=[datetime(2026,10,30,12,1),date(2026,10,30),pd.Timestamp('2026-10-30T12:01:00')]
    restored=store.decode(json.loads(store.canonical(values)))
    assert [type(v) for v in restored]==[type(v) for v in values]
    assert store.digest(values)==store.digest(restored)


def test_read_never_creates_missing_database(settings):
    path = sheet(settings)
    with pytest.raises(store.HistoryError, match='unavailable'): store.view(path, 'core_raw')
    assert not history.database(settings).exists()


def test_import_idempotence_and_offline_independent_frame(settings):
    path = sheet(settings)
    value = store.frame_payload(pd.read_excel(path, dtype=str))
    store.put_views(settings.project_root, history.database(settings), path, {'fixture': value})
    store.put_views(settings.project_root, history.database(settings), path, {'fixture': value})
    sha = store.original_hash(path); path.unlink()
    found, proof = store.view(path, 'fixture')
    assert store.original_hash(path) == sha
    assert store.payload_frame(found).iloc[0,0] == '000001'
    store.verify_embedded(proof)
    with store.connection(history.database(settings)) as conn:
        assert conn.execute('SELECT count(*) FROM objects').fetchone()[0] == 1
        # Serialization round-trip is not independent legacy/ledger parity.
        assert conn.execute('SELECT count(*) FROM validations').fetchone()[0] == 0


def test_changed_identity_before_write_is_rejected(settings):
    path = sheet(settings); before = store.file_identity(path); path.write_bytes(b'changed fixture')
    with pytest.raises(store.HistoryError, match='changed_before_write'):
        store.put_views(settings.project_root, history.database(settings), path, {}, expected_identity=before)


def test_same_source_different_parsed_values_rejected(settings):
    path = sheet(settings); db=history.database(settings)
    store.put_views(settings.project_root,db,path,{'test': {'value':None}})
    with pytest.raises(store.HistoryError,match='identical_source'):
        store.put_views(settings.project_root,db,path,{'test': {'value':0}})
    assert store.view(path,'test')[0]['value'] is None


def test_corruption_refused(settings):
    path=sheet(settings);db=history.database(settings)
    store.put_views(settings.project_root,db,path,{'test': {'value':None}})
    with store.connection(db,write=True) as conn: conn.execute("UPDATE objects SET payload=x'00'")
    with pytest.raises(store.HistoryError,match='corrupt'):store.view(path,'test')


def test_shadow_mismatch_blocks(settings):
    path=sheet(settings);db=history.database(settings)
    store.put_views(settings.project_root,db,path,{'test':store.frame_payload(pd.DataFrame({'value':[None]}))})
    (settings.project_root/'config/project.yaml').write_text('report_history:\n  read_mode: shadow\n')
    with pytest.raises(store.HistoryError,match='shadow_mismatch'):
        store.read_frame(path,'test',lambda:pd.DataFrame({'value':[0]}))


def test_uncommitted_ledger_row_is_not_visible(settings):
    path=sheet(settings); report_path=path.parent/'report.json'
    report={'run_id':'fixture','publication_control':{'revision':'a'},'artifacts':{}}
    report_path.write_text(json.dumps(report))
    history.import_report(settings,report_path,'committed')
    (settings.reports_dir/'publication_registry.json').write_text('{"canonical_runs": []}')
    with pytest.raises(FileNotFoundError): history.get_report(settings,'fixture','a')


def test_historical_export_preserves_zero_and_missing(settings):
    path=sheet(settings);db=history.database(settings)
    history.import_source(settings,path,{'workbook'})
    info=store.source_info(settings.project_root,db,path,kind='workbook')
    payload={'run_id':'fixture','revision':'a','history_digest':'fixture',
             'report':{'artifacts':{'filtered_excel':store.relative(settings.project_root,path)}},
             'sources':[{**info,'kind':'workbook'}]}
    from io import BytesIO
    from openpyxl import load_workbook
    data=history.export_report(settings,payload,'formal','xlsx')
    workbook=load_workbook(BytesIO(data),data_only=True)
    assert workbook.worksheets[0]['B2'].value==0
    assert workbook.worksheets[0]['B3'].value is None
    assert '历史生成说明' in workbook.sheetnames


def test_path_escape_rejected(settings):
    with pytest.raises(store.HistoryError,match='outside_project'):
        store.relative(settings.project_root,settings.project_root/'../outside')


@pytest.mark.parametrize('changed_proof', [False, True])
def test_parity_normalizes_only_project_provenance_paths(settings, monkeypatch, changed_proof):
    path = sheet(settings)
    local = str(path.with_suffix('.csv'))
    container = '/app/' + store.relative(settings.project_root, Path(local))
    payload = store.frame_payload(pd.DataFrame({'code': ['000001'], 'zero': [0], 'missing': [None]}))
    expected = {**payload, 'observed_sources': [{'path': container, 'sha256': 'a' * 64, 'bytes': 12, 'status': 'read'}]}
    observed = {**payload, 'observed_sources': [{'path': local, 'sha256': ('b' if changed_proof else 'a') * 64,
                                                'bytes': 12, 'status': 'read'}]}
    db = history.database(settings)
    store.put_views(settings.project_root, db, path, {'formal': expected})
    original_digest = store.digest(expected)
    monkeypatch.setattr(history, 'parse_source_views', lambda *_: {'formal': observed})
    result = history.semantic_audit(settings)
    assert result['status'] == ('blocked' if changed_proof else 'verified')
    with store.connection(db) as conn:
        assert conn.execute('SELECT digest FROM views').fetchone()[0] == original_digest
        assert conn.execute('SELECT count(*) FROM validations').fetchone()[0] == (0 if changed_proof else 1)
        assert store.get_object(conn, original_digest) == expected


@pytest.mark.parametrize('path', ['/outside/source.csv', '/app/../outside/source.csv', 'reports/../source.csv'])
def test_parity_provenance_rejects_outside_and_traversal(settings, path):
    with pytest.raises(store.HistoryError):
        history._parity_payload(settings.project_root, 'formal', {'observed_sources': [{'path': path}]})


def test_inventory_covers_forward_legacy_inputs_outside_formal_registry(settings,monkeypatch):
    from backend import publication_transaction
    path=sheet(settings);report_path=path.parent/'old.json'
    report_path.write_text(json.dumps({'run_id':'old','counts':{'final_count':1},'generated_at':'2020-01-01T12:00:00',
        'database_freshness':{'effective_nav_snapshot':{'candidate_date':'2020-01-01'}},
        'artifacts':{'filtered_excel':'reports/moved/formal.xlsx'}}))
    monkeypatch.setattr(publication_transaction,'visible_report_paths',lambda _: [report_path])
    plan=history.inventory(settings)
    assert plan['reports'][0]['inputs_only'] is True
    assert plan['reports'][0]['semantic']=='diagnostic'
    assert plan['sources'][0]['path']==store.relative(settings.project_root,path)
    assert plan['sources'][0]['kinds']==['formal']


def test_observed_reads_freeze_without_mutable_database(settings):
    path=sheet(settings)
    history.import_source(settings,path,{'workbook'})
    @store.capture_reads
    def write():
        store.view(path,'workbook')
        store.view(path,'workbook')
        observed=store.captured_reads()
        assert len(observed)==1 and 'payload' not in observed[0]
        return history.freeze_read_inputs(settings,{'run_id':'fixture','publication_control':{'revision':'a'},'artifacts':{}},path.parent)
    frozen=write()
    path.unlink();history.database(settings).unlink()
    value=history.load_frozen_read_inputs(frozen)
    assert len(value['records'])==1 and value['records'][0]['kind']=='workbook'
    assert value['records'][0]['payload']['sheets'][0]['rows'][1][1]==0
    assert '_database_path' not in value['records'][0]
    assert store.captured_reads()==[]


def test_freeze_rejects_changed_immutable_object(settings):
    path=sheet(settings);history.import_source(settings,path,{'workbook'})
    @store.capture_reads
    def capture():
        store.view(path,'workbook')
        with store.connection(history.database(settings),write=True) as conn:
            conn.execute("UPDATE objects SET payload=x'00'")
        return history.freeze_read_inputs(settings,{'run_id':'fixture','publication_control':{'revision':'a'},'artifacts':{}},path.parent)
    with pytest.raises((ValueError,OSError,EOFError)):
        capture()
    assert not (path.parent/'history_read_inputs.json.gz').exists()


def test_legacy_core_alias_falls_back_inside_root(settings,tmp_path):
    path=settings.core_output_dir/'original.xlsx';path.parent.mkdir(parents=True,exist_ok=True)
    path.write_bytes(b'synthetic source')
    outside=tmp_path/'old-host';outside.mkdir()
    alias=settings.project_root/'core-output-alias';alias.symlink_to(outside,target_is_directory=True)
    record={'source_report_file':str(alias/'original.xlsx'),'source_file_name':'original.xlsx'}
    assert history._core_history_source(settings,record)==path
    assert history._core_history_source(settings,{'source_report_file':str(path),'source_file_name':'original.xlsx'})==path


@pytest.mark.parametrize('name',['../outside.xlsx','/outside.xlsx','..',''])
def test_legacy_core_fallback_cannot_escape_root(settings,name):
    with pytest.raises(store.HistoryError,match='source_name_invalid'):
        history._core_history_source(settings,{'source_report_file':'/old-host/missing.xlsx','source_file_name':name})


@pytest.mark.parametrize('matching',[True,False])
def test_legacy_core_fallback_requires_original_sha1(settings,monkeypatch,matching):
    from core.bond_fund_screening.utils.accumulation_tracker import AccumulationTracker
    path=settings.core_output_dir/'daily.xlsx';path.parent.mkdir(parents=True)
    pd.DataFrame({'基金代码':['000001'],'数值':[0]}).to_excel(path,index=False)
    history.import_source(settings,path,{'core_raw'})
    record={'source_report_file':'/old-host/daily.xlsx','source_file_name':'daily.xlsx',
            'source_file_hash':store.original_hash(path,'sha1') if matching else 'f'*40}
    def load(tracker):
        tracker.processed_results=[record]
        return True
    monkeypatch.setattr(AccumulationTracker,'_load_tracker',load)
    directory=settings.reports_dir/'frozen';directory.mkdir()
    @store.capture_reads
    def freeze():
        return history.freeze_read_inputs(settings,{'run_id':'fixture','publication_control':{'revision':'a'},
            'artifacts':{'core':{'accumulation_tracker_excel':'reports/tracker.xlsx'}}},directory)
    if matching:
        result=history.load_frozen_read_inputs(freeze())
        assert result['core_history'][0]['record']==record
    else:
        with pytest.raises(store.HistoryError,match='source_hash_mismatch'):
            freeze()


def test_frozen_payloads_share_blocks_but_read_independently(settings,tmp_path):
    import shutil
    path=sheet(settings);history.import_source(settings,path,{'workbook'})
    @store.capture_reads
    def capture(directory):
        directory.mkdir()
        store.view(path,'workbook')
        return history.freeze_read_inputs(settings,{'run_id':'fixture','publication_control':{'revision':'a'},'artifacts':{}},directory)
    first=capture(path.parent/'first');second=capture(path.parent/'second')
    a=next((first.parent/'history_read_objects').iterdir())
    b=next((second.parent/'history_read_objects').iterdir())
    assert a.stat().st_ino==b.stat().st_ino
    isolated=tmp_path/'independent';shutil.copytree(second.parent,isolated)
    shutil.rmtree(settings.reports_dir/'.report_history_input_pool')
    history.database(settings).unlink();path.unlink()
    value=history.load_frozen_read_inputs(isolated/second.name)
    assert value['records'][0]['payload']['sheets'][0]['rows'][1][1]==0
    detached=next((isolated/'history_read_objects').iterdir())
    detached.chmod(0o644);detached.write_bytes(b'corrupt fixture')
    with pytest.raises((ValueError,OSError,EOFError)):
        history.load_frozen_read_inputs(isolated/second.name)


def test_inline_v1_frozen_history_stays_readable(tmp_path):
    import gzip
    payload={'example':None,'zero':0}
    record={'contract':store.CONTRACT,'digest':store.digest(payload),'payload':payload}
    path=tmp_path/'old.json.gz'
    path.write_bytes(gzip.compress(store.canonical({'contract':'report-history-read-inputs-v1',
        'records':[record],'core_history':[]})))
    assert history.load_frozen_read_inputs(path)['records'][0]['payload']==payload


def test_tracker_labels_and_processed_rows_read_without_original_excel(settings,monkeypatch):
    import builtins,io
    from backend import project_tracker as tracker
    path=settings.reports_dir/'最终优质基金累积跟踪.xlsx';path.parent.mkdir(parents=True)
    with pd.ExcelWriter(path,engine='openpyxl') as writer:
        pd.DataFrame({'基金代码':['000001'],'项目质量信心':['高信心'],'数值':[0]}).to_excel(writer,sheet_name='完整数据',index=False,startrow=2)
        pd.DataFrame({'项目批次':['fixture'],'业务日期':['2020-01-01']}).to_excel(writer,sheet_name='处理记录',index=False)
    expected=tracker._legacy_read_project_tracker_export_sheet(path,'完整数据')
    process=tracker._legacy_read_project_tracker_process_sheet(path)
    history.import_source(settings,path,{'workbook'})
    original=builtins.open;original_io=io.open
    def deny_path(raw,*args,**kwargs):
        if isinstance(raw,(str,Path)) and Path(raw)==path:raise AssertionError('original_excel_read')
        return original(raw,*args,**kwargs)
    def deny_io(raw,*args,**kwargs):
        if isinstance(raw,(str,Path)) and Path(raw)==path:raise AssertionError('original_excel_read')
        return original_io(raw,*args,**kwargs)
    monkeypatch.setattr(builtins,'open',deny_path);monkeypatch.setattr(io,'open',deny_io)
    pd.testing.assert_frame_equal(expected,tracker._read_project_tracker_export_sheet(path,'完整数据'),check_exact=True)
    pd.testing.assert_frame_equal(process,tracker._read_project_tracker_process_sheet(path),check_exact=True)
