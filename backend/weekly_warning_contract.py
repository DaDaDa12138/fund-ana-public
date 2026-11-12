"""Validate unresolved scheduler-stage evidence separately from archive recovery."""
from __future__ import annotations

from typing import Any
from datetime import datetime

UNRESOLVED_STAGE_STATUS = 'unresolved_weekly_stage_failure'
PERSISTED_STAGE_TRUTH_POLICY = 'persisted_scheduler_stage_and_read_only_runtime_artifact_evidence_no_state_mutation'


def weekly_publication_evidence(run: dict[str, Any], audit: dict[str, Any]) -> dict[str, Any]:
    """Prove this task's workflow outcome separately from an older valid list."""
    task = audit.get('task') or {}
    workflow = audit.get('workflow_summary') or {}
    publication = audit.get('publication') or {}
    result = {'status': 'unknown', 'workflow_run_id': None, 'workflow_status': None,
              'current_published_run_id': None, 'task_bound': False}
    if not all(isinstance(value, dict) for value in (task, workflow, publication)):
        return result
    if (task.get('task_name') != 'scheduled_weekly_bond_maintenance'
            or task.get('started_at') != run.get('started_at') or not run.get('started_at')):
        return result
    result['task_bound'] = True
    published = publication.get('current_published_run') or {}
    if not isinstance(published, dict):
        return result
    result['current_published_run_id'] = publication.get('current_published_run_id')
    try:
        start, finish = (datetime.fromisoformat(str(task[key])) for key in ('started_at', 'finished_at'))
        workflow_start = datetime.fromisoformat(str(workflow['started_at']))
        workflow_finish = datetime.fromisoformat(str(workflow['finished_at']))
        if not start <= workflow_start <= workflow_finish <= finish:
            return result
    except (KeyError, TypeError, ValueError):
        return result
    run_id = workflow.get('run_id')
    status = workflow.get('status')
    result.update(workflow_run_id=run_id, workflow_status=status)
    if not run_id:
        return result
    if status in {'failed', 'interrupted', 'cancelled'}:
        result['status'] = 'failed'
    elif status in {'success', 'succeeded', 'completed'}:
        result['status'] = 'completed_without_publication'
        if (published.get('run_id') == run_id == publication.get('current_published_run_id')
                and published.get('published') is True and published.get('publishable') is True
                and not published.get('publish_hold') and not published.get('publish_blocked')):
            result['status'] = 'published'
    return result


def weekly_publication_interpretation(evidence: dict[str, Any]) -> str:
    return {
        'published': '本次周维护工作流已完成并正式发布',
        'failed': '本次周维护工作流失败，未完成本次正式发布',
        'completed_without_publication': '本次周维护工作流已完成，未取得本次正式发布证据',
    }.get(evidence.get('status'), '本次周维护工作流及正式发布结果缺少可绑定证据')


def unresolved_stage_detail_errors(detail: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    evidence = detail.get('latest_resolution_evidence')
    evidence = evidence if isinstance(evidence, dict) else {}
    stage = str(detail.get('primary_degraded_stage') or '').strip()
    if detail.get('root_cause_status') != UNRESOLVED_STAGE_STATUS:
        errors.append('root_cause_status_mismatch')
    if detail.get('root_cause_resolved') is not False:
        errors.append('unresolved_stage_marked_resolved')
    if detail.get('truth_policy') != PERSISTED_STAGE_TRUTH_POLICY:
        errors.append('truth_policy_mismatch')
    if not str(evidence.get('artifact_path') or '').strip():
        errors.append('resolution_artifact_missing')
    if not stage:
        errors.append('primary_degraded_stage_missing')
    elif stage != str(evidence.get('degraded_stage') or '').strip():
        errors.append('primary_degraded_stage_mismatch')
    reason = detail.get('primary_warning_reason')
    if reason != evidence.get('warning_reason'):
        errors.append('primary_warning_reason_mismatch')
    primary = detail.get('primary_cause_is_collection')
    if not isinstance(primary, bool) or primary is not evidence.get('primary_cause_is_collection'):
        errors.append('primary_cause_mismatch')
    if stage in {'collect-funds', 'collect-details', 'collect-managers', 'collect-companies', 'collect-period-returns'}:
        observation = evidence.get('collection_observation')
        observation = observation if isinstance(observation, dict) else {}
        if primary is not True or observation.get('primary_cause') is not True:
            errors.append('collection_primary_cause_missing')
        if not str(observation.get('artifact_path') or '').strip():
            errors.append('collection_observation_artifact_missing')
        tasks = observation.get('affected_tasks')
        if not isinstance(tasks, list) or stage not in tasks:
            errors.append('collection_affected_stage_missing')
    # Historical missing warning_reason is audited against task/state/artifact records
    # separately; this validator never invents a reason or declares recovery.
    return errors
