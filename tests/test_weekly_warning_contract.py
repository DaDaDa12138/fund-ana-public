from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'core'))

from backend.scheduler import ProjectScheduler
from backend.weekly_warning_contract import (
    PERSISTED_STAGE_TRUTH_POLICY,
    UNRESOLVED_STAGE_STATUS,
    unresolved_stage_detail_errors,
)
from scripts.audit_data_truthfulness import _validate_live_scheduler_status


def stage_detail():
    return {
        'root_cause_status': UNRESOLVED_STAGE_STATUS, 'root_cause_resolved': False,
        'primary_degraded_stage': 'collect-managers', 'primary_warning_reason': None,
        'primary_cause_is_collection': True, 'truth_policy': PERSISTED_STAGE_TRUTH_POLICY,
        'latest_resolution_evidence': {
            'artifact_path': 'reports/.audit/task.json', 'degraded_stage': 'collect-managers',
            'warning_reason': None, 'primary_cause_is_collection': True,
            'collection_observation': {
                'artifact_path': 'reports/.audit/subprocess_index.json',
                'affected_tasks': ['collect-managers'], 'primary_cause': True,
                'blocker_count': 0, 'warning_count': 1,
            },
        },
    }


def test_unresolved_collection_evidence_does_not_need_archive_fields():
    detail = stage_detail()
    assert unresolved_stage_detail_errors(detail) == []
    assert detail['root_cause_resolved'] is False
    assert 'stale_hot_missing_archive_key_row_count' not in detail['latest_resolution_evidence']


@pytest.mark.parametrize('fault', ['resolved', 'missing_artifact', 'wrong_stage', 'no_collection_evidence', 'wrong_reason', 'wrong_policy'])
def test_missing_or_conflicting_stage_evidence_remains_blocked(fault):
    detail = stage_detail()
    if fault == 'resolved':
        detail['root_cause_resolved'] = True
    elif fault == 'missing_artifact':
        detail['latest_resolution_evidence']['artifact_path'] = None
    elif fault == 'wrong_stage':
        detail['latest_resolution_evidence']['degraded_stage'] = 'collect-nav'
    elif fault == 'no_collection_evidence':
        detail['latest_resolution_evidence']['collection_observation'] = {}
    elif fault == 'wrong_reason':
        detail['primary_warning_reason'] = 'different_reason'
    else:
        detail['truth_policy'] = 'mutate_state'
    assert unresolved_stage_detail_errors(detail)


def test_unresolved_stage_cannot_be_moved_into_resolved_signals():
    detail = stage_detail()
    payload = {'automation_autonomy': {
        'warning_issue_codes': [], 'warning_issue_details': {},
        'resolved_warning_issue_codes': ['weekly_maintenance_degraded'],
        'resolved_warning_issue_details': {'weekly_maintenance_degraded': detail},
    }}
    _, blockers = _validate_live_scheduler_status(payload, expected_publication={})
    assert 'scheduler_weekly_warning_unresolved_stage_in_resolved_signals' in blockers


def test_later_empty_manager_detail_summary_cannot_erase_failed_list_counts():
    logs = [
        '[collect-managers] $ python main.py collect-managers',
        '总任务数: 1,500', '成功: 1,471', '跳过: 0', '失败: 29',
        '成功: 0', '失败: 0', '跳过: 0',
    ]
    result = ProjectScheduler._classify_log_severity(logs)
    assert (result['summary_total'], result['summary_success'], result['summary_failed']) == (1500, 1471, 29)
    assert result['degraded'] is True
    assert result['reason'] == 'manager_collection_failed'


def test_structured_terminal_counts_cover_both_manager_substages():
    logs = ['[collect-managers] $ python main.py collect-managers', '总任务数: 3', '成功: 0', '失败: 0']
    for source, counts in [
        ('fundMNMangerList', {'target_count': 1500, 'success_count': 1471, 'failed_count': 29}),
        ('fundMSNMangerInfo', {'target_count': 3, 'success_count': 2, 'failed_count': 1}),
    ]:
        logs.append('api_source_evidence=' + json.dumps({'source': source, **counts}))
    result = ProjectScheduler._classify_log_severity(logs)
    assert (result['summary_total'], result['summary_success'], result['summary_failed']) == (1503, 1473, 30)
    assert result['reason'] == 'manager_collection_failed'


def test_all_skipped_is_a_normal_no_change_result():
    result = ProjectScheduler._classify_log_severity([
        '[collect-managers] $ python main.py collect-managers',
        '总任务数: 10', '成功: 0', '失败: 0', '跳过: 10',
    ])
    assert result['degraded'] is False
    assert result['reason'] == ''


def test_no_usable_manager_result_always_has_a_reason():
    result = ProjectScheduler._classify_log_severity([
        '[collect-managers] $ python main.py collect-managers',
        '总任务数: 10', '成功: 0', '失败: 0',
    ])
    assert result['degraded'] is True
    assert result['reason'] == 'manager_collection_no_usable_records'
