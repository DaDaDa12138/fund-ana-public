"""Status contracts and negative controls; no synthetic fund values are published."""
from copy import deepcopy
import gzip
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend import scheduler_diagnostics
from backend.weekly_warning_contract import (
    unresolved_stage_detail_errors, weekly_publication_evidence, weekly_publication_interpretation,
)
from scripts import audit_data_truthfulness as audit


@pytest.fixture
def recorded_weekly():
    with gzip.open(Path(__file__).parent / 'fixtures/weekly_failure_diagnostic_20261004.json.gz', 'rt') as stream:
        return json.load(stream)


def test_real_failed_workflow_cannot_be_described_as_completed_publication(recorded_weekly, monkeypatch):
    run, original = recorded_weekly['weekly_run'], recorded_weekly['audit']
    evidence = weekly_publication_evidence(run, original)
    assert evidence['status'] == 'failed'
    assert evidence['workflow_run_id'] == '20261004_043131_sync'
    assert evidence['current_published_run_id'] == '20260930_074105_sync'
    monkeypatch.setattr(scheduler_diagnostics, '_read_json_object', lambda path: deepcopy(original))
    summary = scheduler_diagnostics.build_automation_autonomy_summary(
        {'scheduler_running': True, 'weekly_run': deepcopy(run)},
        {'open_issues': [{'code': 'weekly_maintenance_degraded', 'severity': 'warning'}]},
        manual_operations_enabled=False,
        load_database_overview_fn=lambda snapshot: {},
    )
    detail = summary['warning_issue_details']['weekly_maintenance_degraded']
    assert '工作流失败' in detail['current_interpretation']
    assert '正式发布链已完成' not in detail['current_interpretation']
    assert unresolved_stage_detail_errors(detail) == []
    assert 'collect-period-returns' in detail['latest_resolution_evidence']['collection_observation']['affected_tasks']
    assert run == recorded_weekly['weekly_run']  # Inputs stay historical.


@pytest.mark.parametrize('fault', ['missing', 'different_task', 'stale_workflow', 'missing_finish'])
def test_unbound_weekly_audit_retains_unknown(recorded_weekly, fault):
    original = deepcopy(recorded_weekly['audit'])
    if fault == 'missing':
        original = {}
    elif fault == 'different_task':
        original['task']['started_at'] = '2026-10-03T03:00:00'
    elif fault == 'stale_workflow':
        original['workflow_summary']['started_at'] = '2026-09-30T07:41:05'
    else:
        original['workflow_summary']['finished_at'] = None
    evidence = weekly_publication_evidence(recorded_weekly['weekly_run'], original)
    assert evidence['status'] == 'unknown'
    assert '缺少可绑定证据' in weekly_publication_interpretation(evidence)


@pytest.mark.parametrize('same_run,blocked,expected', [(False, False, 'completed_without_publication'),
                                                        (True, True, 'completed_without_publication'),
                                                        (True, False, 'published')])
def test_successful_workflow_still_needs_this_tasks_publication(recorded_weekly, same_run, blocked, expected):
    original = deepcopy(recorded_weekly['audit'])
    original['workflow_summary']['status'] = 'succeeded'
    if same_run:
        run_id = original['workflow_summary']['run_id']
        original['publication']['current_published_run_id'] = run_id
        original['publication']['current_published_run']['run_id'] = run_id
    original['publication']['current_published_run']['publish_blocked'] = blocked
    assert weekly_publication_evidence(recorded_weekly['weekly_run'], original)['status'] == expected


@pytest.mark.parametrize('status,blocked,expected_error', [
    ('success_published', False, False), ('success_published_with_backlog', False, False),
    ('success_unknown', False, True), ('success_published_with_backlog', True, True),
])
def test_publication_status_enum_preserves_gate_checks(status, blocked, expected_error):
    _, errors = audit._validate_live_publication_status(
        'test', {'publication_status_summary': {'status': status,
                 'latest_successful_is_currently_published': True, 'publish_blocked': blocked}},
        expected_publication={}, require_top_level=False,
    )
    assert any('success_state_mismatch' in e or 'success_publish_blocked' in e for e in errors) == expected_error


def failed_daily():
    evidence = {'status': 'failed', 'screening_triggered': False, 'screening_run_id': None,
                'report_refresh_run_id': None, 'skip_reason': None,
                'started_at': '2026-10-09T07:30:00', 'finished_at': '2026-10-09T07:59:42'}
    return {'daily_screening_run_id': None, 'daily_run': deepcopy(evidence),
            'automation_autonomy': {'daily_screening_run_id': None,
                'automation_lineage': {'daily_screening_run_id': None, 'daily_screening_evidence': deepcopy(evidence)}},
            'source_basis': {'daily_screening_run_id': None, 'daily_screening_evidence': deepcopy(evidence)}}


def test_failed_daily_null_does_not_hide_the_real_failure():
    payload = failed_daily()
    assert audit._daily_screening_absence_explained(payload)
    _, errors = audit._validate_live_scheduler_status(payload, expected_publication={})
    assert 'scheduler_automation_daily_screening_run_id_missing' not in errors
    assert 'scheduler_automation_scheduler_not_running' in errors


def test_failed_status_projection_may_preserve_unknown_completion_flag():
    payload = failed_daily()
    for evidence in (payload['daily_run'], payload['source_basis']['daily_screening_evidence'],
                     payload['automation_autonomy']['automation_lineage']['daily_screening_evidence']):
        evidence['screening_triggered'] = None
    assert audit._daily_screening_absence_explained(payload)


@pytest.mark.parametrize('fault', ['success', 'unfinished', 'conflicting_basis', 'completed_id'])
def test_unexplained_null_and_conflicting_daily_evidence_stay_blocked(fault):
    payload = failed_daily()
    if fault == 'success':
        payload['daily_run']['status'] = 'success'
    elif fault == 'unfinished':
        payload['daily_run']['finished_at'] = None
    elif fault == 'conflicting_basis':
        payload['source_basis']['daily_screening_evidence']['screening_triggered'] = True
    else:
        payload['daily_run']['screening_run_id'] = 'completed-fixture-run'
    assert not audit._daily_screening_absence_explained(payload)


def test_source_comments_and_rejecting_sentinels_are_not_fake_data(tmp_path):
    directory = tmp_path / 'core/utils'
    directory.mkdir(parents=True)
    (directory / 'guard.py').write_text('''"""Never a fabricated live inode."""
def validate(identity, name):
    placeholders = {"-", "None", "none", "NaN", "nan"}
    if not identity or identity in placeholders:
        return False
    elif not name or name in placeholders:
        return False
    return True
''')
    result = audit._source_scan(SimpleNamespace(project_root=tmp_path))
    assert result['suspicious_match_count_seen'] == 0
    assert result['runtime_test_double_match_count_seen'] == 0
    (directory / 'business.py').write_text("fake_business_data = {'annual_return': 0}\n")
    result = audit._source_scan(SimpleNamespace(project_root=tmp_path))
    assert result['suspicious_match_count_seen'] == 1
    assert 'source_truthfulness_suspicious_production_keywords' in result['blockers']


def test_sentinel_used_as_business_value_is_never_allowlisted():
    source = 'placeholders = {"-", "None"}\nfund_values = placeholders\n'
    assert 1 not in audit._source_scan_non_business_lines(source)
