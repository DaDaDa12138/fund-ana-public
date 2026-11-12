"""Real full-refresh logs and task identities, with isolated fault injection."""
from copy import deepcopy
import gzip
import hashlib
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from scripts import audit_data_truthfulness as audit

FIXTURE_PATH = Path(__file__).parent / 'fixtures/full_risk_workflow_truth_20260912.json.gz'


@pytest.fixture
def recorded_run(tmp_path):
    raw = FIXTURE_PATH.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == '643a559eb5530bd3bbe47350d7b07d8f47cd51df15b0fef070f1759ecac7517a'
    frozen = json.loads(gzip.decompress(raw))
    item = deepcopy(frozen['item'])
    for record in frozen['files'].values():
        data = record['text'].encode()
        assert hashlib.sha256(data).hexdigest() == record['sha256']
        path = tmp_path / record['path']
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    index_path = tmp_path / 'reports/runs' / frozen['run_id'] / 'logs/subprocess/subprocess_index.json'
    index = {'run_id': frozen['run_id'], 'items': [item], 'subprocess_count': 1,
             'success_count': 1, 'failed_count': 0, 'failed_task_names': []}
    report = {'run_id': frozen['run_id'], 'final_count': 281,
        'publication_control': {'workflow_options': deepcopy(frozen['workflow_options'])},
        'artifacts': {'core': {'subprocess_log_index_file': str(index_path)}}}
    latest = tmp_path / 'latest.json'
    database = tmp_path / 'tasks.db'
    columns = list(frozen['scheduler_rows'][0])
    with sqlite3.connect(database) as conn:
        conn.execute('''CREATE TABLE task_history (id INTEGER PRIMARY KEY, task_name TEXT,
            status TEXT, started_at TEXT, completed_at TEXT, duration INTEGER,
            records_processed INTEGER, records_failed INTEGER, error_message TEXT, extra_data TEXT)''')
        conn.executemany('INSERT INTO task_history VALUES (?,?,?,?,?,?,?,?,?,?)',
                         [tuple(row[key] for key in columns) for row in frozen['scheduler_rows']])
    settings = SimpleNamespace(project_root=tmp_path, reports_dir=tmp_path / 'reports',
        database_path=database, latest_report_file=latest)

    def save():
        index_path.write_text(json.dumps(index))
        latest.write_text(json.dumps(report))
    save()
    return SimpleNamespace(settings=settings, report=report, item=item, index=index,
        stdout=tmp_path / item['stdout_path_relative'], save=save, frozen=frozen)


def canonical(run):
    return audit._canonical_run_refresh_collection_truth(run.settings, latest_report=run.report,
                                                         run_id=run.report['run_id'])


def test_full_risk_log_uses_actual_complete_totals(recorded_run):
    result, blockers, _ = audit._stdout_result_semantics('calculate-risk', str(recorded_run.stdout))
    assert not blockers
    assert result['business_summary'] == {'target_fund_count': 8050, 'generated_period_count': 12705}
    assert result['completion_stats'][0]['success'] == 5878
    assert result['completion_stats'][0]['skipped'] == 2172


@pytest.mark.parametrize('damage', ['no_stats', 'no_terminal', 'incomplete_count', 'failed',
                                  'duplicate_stats', 'duplicate_terminal', 'mixed_terminal'])
def test_full_risk_log_missing_or_failed_completion_still_blocks(recorded_run, damage):
    text = recorded_run.stdout.read_text()
    if damage == 'no_stats':
        text = text[text.index('✅ 风险指标计算完成:'):]
    elif damage == 'no_terminal':
        text = text[:text.index('✅ 风险指标计算完成:')]
    elif damage == 'incomplete_count':
        text = text.replace('已处理:      8,050', '已处理:      8,049')
    elif damage == 'failed':
        text = text.replace('失败:        0', '失败:        1')
    elif damage == 'duplicate_stats':
        text += text
    elif damage == 'duplicate_terminal':
        text += '\n✅ 风险指标计算完成: 12705个指标\n'
    else:
        text += '\n✅ 净值已更新基金风险指标同步完成: 目标基金8050只, 生成12705个周期\n'
    recorded_run.stdout.write_text(text)
    _, blockers, _ = audit._stdout_result_semantics('calculate-risk', str(recorded_run.stdout))
    assert blockers


def test_explicit_full_risk_rescreen_validates_risk_without_inventing_collection(recorded_run):
    result, blockers, warnings = canonical(recorded_run)
    assert blockers == []
    assert result['collection_expected'] is False
    assert result['required_tasks'] == ['calculate-risk']
    assert result['tasks'][0]['stdout_result_semantics']['business_summary']['generated_period_count'] == 12705
    assert 'current_publication_explicitly_skipped_collection_full_risk_recalculated' in warnings
    upstream = audit._upstream_collection_truth(automation_payload={'scheduled_refresh_task': {
        'current_run_collection_evidence_valid': True, 'publication_collection_subprocess_index': result,
        'current_publication_run_id': recorded_run.report['run_id']}},
        freshness_payload=recorded_run.frozen['freshness'], formal_snapshot_alignment={})
    assert upstream['blockers'] == []
    assert upstream['required_tasks'] == ['calculate-risk']
    assert set(upstream['tasks']) == {'calculate-risk'}


@pytest.mark.parametrize('key,value', [
    ('run_collection', None), ('run_collection', 'false'), ('run_collection', 0), ('run_collection', True),
    ('run_screening', False), ('reuse_existing_outputs', True), ('force_calculate_risk', False),
    ('skip_risk_calculation', True), ('risk_refresh_changed_nav_only', True),
])
def test_ambiguous_or_other_modes_still_require_collection_evidence(recorded_run, key, value):
    recorded_run.report['publication_control']['workflow_options'][key] = value
    result, blockers, _ = canonical(recorded_run)
    assert 'collect-funds' in result['required_tasks']
    assert any('required_tasks_missing' in item for item in blockers)


@pytest.mark.parametrize('damage', ['missing_log', 'missing_force', 'changed_nav', 'target',
    'target_equals', 'target_file_equals', 'abbreviated_target', 'dry_run', 'preview',
    'wrong_script', 'wrong_subcommand', 'meta_command', 'wrong_run'])
def test_claimed_full_refresh_does_not_excuse_missing_or_different_execution(recorded_run, damage):
    if damage == 'missing_log':
        recorded_run.stdout.unlink()
    elif damage == 'missing_force':
        recorded_run.item['command'].remove('--force')
    elif damage == 'wrong_run':
        recorded_run.index['run_id'] = 'unrelated-run'
    elif damage == 'wrong_script':
        recorded_run.item['command'][1] = 'unrelated.py'
    elif damage == 'wrong_subcommand':
        recorded_run.item['command'][2] = 'collect-nav'
    elif damage == 'meta_command':
        recorded_run.item['command'][0] = '/different/python'
    else:
        recorded_run.item['command'].append({'changed_nav': '--changed-nav-only', 'target': '--fcode',
            'target_equals': '--fcode=024519', 'target_file_equals': '--target-fcodes-file=targets.txt',
            'abbreviated_target': '--fco=024519', 'dry_run': '--dry-run', 'preview': '--preview'}[damage])
    if damage != 'meta_command':
        meta_path = recorded_run.settings.project_root / recorded_run.item['meta_path_relative']
        meta = json.loads(meta_path.read_text())
        meta['command'] = recorded_run.item['command']
        meta_path.write_text(json.dumps(meta))
    recorded_run.save()
    _, blockers, _ = canonical(recorded_run)
    assert blockers


def test_latest_completed_degraded_run_is_selected_without_rewriting_old_success(recorded_run):
    result, blockers, _ = audit._find_scheduled_refresh_task_truth(
        recorded_run.settings, run_id=recorded_run.report['run_id'], final_count=281)
    assert blockers == ['scheduled_refresh_task_history_missing_for_run']
    assert result['latest_completed_screening_run']['row']['id'] == 12646
    assert result['latest_completed_screening_run']['row']['status'] == 'degraded'
    assert result['latest_successful_screening_run']['row']['id'] == 12623
    assert result['latest_successful_screening_run']['row']['status'] == 'success'


@pytest.mark.parametrize('degraded_source_valid', [True, False])
def test_newer_degraded_source_must_pass_full_validation(recorded_run, monkeypatch, degraded_source_valid):
    run = recorded_run
    original_find = audit._find_scheduled_refresh_task_truth
    selected = []
    monkeypatch.setattr(audit, '_automation_subprocess_index_truth', lambda *args: ({}, [], []))
    monkeypatch.setattr(audit, '_automation_scheduler_state_truth', lambda *args: ({}, [], []))
    monkeypatch.setattr(audit, '_find_weekly_maintenance_warning_truth', lambda *args: ({}, [], []))

    def find(settings, *, run_id, final_count):
        if run_id == run.report['run_id']:
            return original_find(settings, run_id=run_id, final_count=final_count)
        selected.append(run_id)
        assert run_id == '20260912_134927_sync'
        return ({'matched': True, 'run_id': run_id}, [] if degraded_source_valid else ['source_missing'], [])
    monkeypatch.setattr(audit, '_find_scheduled_refresh_task_truth', find)
    result = audit._automation_script_run_truth(run.settings)
    assert selected == ['20260912_134927_sync']
    if degraded_source_valid:
        assert result['blockers'] == []
    else:
        assert 'latest_scheduler_refresh:source_missing' in result['blockers']
