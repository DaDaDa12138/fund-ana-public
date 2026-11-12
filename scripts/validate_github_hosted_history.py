#!/usr/bin/env python3
"""Read the real tracker/forward history contracts before expensive acquisition."""
from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.config import load_settings
from backend.publication_transaction import load_publication_head, resolve_committed_path
from backend.project_tracker import _load_project_report_history
from backend.forward_validation import ForwardValidationSnapshot, _load_snapshot_records
from backend.github_cloud_state import publication_history_contract


def validate_history(settings) -> dict:
    head = load_publication_head(settings)
    if not head:
        raise RuntimeError('cloud_history_formal_head_missing')
    receipt = settings.project_root / '.cloud-restored.json'
    expected = json.loads(receipt.read_text()).get('history_contract') if receipt.exists() else None
    if expected is not None and publication_history_contract(settings) != expected:
        raise RuntimeError('cloud_history_decisions_changed_after_restore')
    run = head['current_published_run_id']
    entry = head['committed_runs'][run]
    report = json.loads(resolve_committed_path(settings, entry['report_path']).read_text())
    artifacts = report['artifacts']
    counts = report['counts']
    nav_date = report['database_freshness']['effective_nav_snapshot']['candidate_date']
    workbook = settings.project_root / artifacts['filtered_excel']
    history = _load_project_report_history(
        settings, current_run_id=run, current_generated_at=report['generated_at'],
        current_filtered_excel=workbook, current_final_count=counts['final_count'],
        current_nav_snapshot_date=nav_date,
    )
    watch = artifacts.get('watch_excel')
    current = ForwardValidationSnapshot(
        run_id=run, generated_at=report['generated_at'], snapshot_date=nav_date,
        filtered_excel=workbook, watch_excel=settings.project_root / watch if watch else None,
        final_count=counts['final_count'], stable_final_count=counts['stable_bond_final_count'],
        enhanced_final_count=counts['enhanced_bond_final_count'],
    )
    snapshots = _load_snapshot_records(settings, current)
    return {'history_readiness': 'passed', 'semantic_baseline': 'matched' if expected is not None else 'legacy_unbound',
            'run_id': run, 'generation': head['generation'],
            'tracker_history_periods': len(history), 'forward_history_snapshots': len(snapshots)}


if __name__ == '__main__':
    print(json.dumps(validate_history(load_settings()), ensure_ascii=False), flush=True)
