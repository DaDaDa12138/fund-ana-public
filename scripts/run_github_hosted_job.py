#!/usr/bin/env python3
"""Run real acquisition and publication inside the ephemeral hosted container."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from backend.config import load_settings
from backend.github_reports import ReportExportError, export_current, parse_scheduler_output, write_json
from backend.publication_transaction import load_publication_head
from backend.github_task_validation import hosted_task_outcome
from backend.shanghai_time import now_shanghai
from backend.github_storage import source_repository, state_repository


def identity(settings) -> dict:
    head = load_publication_head(settings); run = head['current_published_run_id']
    return dict(run_id=run, revision=head['committed_runs'][run]['revision'], generation=head['generation'])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--job', choices=('daily_refresh', 'intraday_refresh', 'weekly_maintenance', 'publish_only'), required=True)
    parser.add_argument('--directory', type=Path, required=True)
    args = parser.parse_args(); args.directory.mkdir(parents=True, exist_ok=True)
    settings = load_settings(); before = identity(settings)
    check = dict(schema='fund-ana-github-check-v1', github_run_id=os.environ['GITHUB_RUN_ID'],
                 attempt=os.environ['GITHUB_RUN_ATTEMPT'], job=args.job, status='failed',
                 executor='github_hosted', checked_at=now_shanghai().isoformat(), timezone='Asia/Shanghai',
                 github_repository=source_repository(), storage_repository=state_repository(),
                 github_event_name=os.environ.get('GITHUB_EVENT_NAME', 'unknown'),
                 github_schedule=os.environ.get('GITHUB_EVENT_SCHEDULE') or None,
                 check_date=now_shanghai().date().isoformat(),
                 collection_requested=args.job != 'publish_only')
    outcome = 'verified_existing_publication'; code = 1
    try:
        if args.job != 'publish_only':
            # No Docker exec, host relay, SSH or local service is involved.
            log = ROOT / 'logs/github-hosted-task.log'; log.parent.mkdir(exist_ok=True)
            with log.open('wb') as output:
                result = subprocess.run([sys.executable, str(ROOT / 'scripts/run_scheduler.py'), '--once', args.job,
                                         '--wait-timeout-seconds', '14400'], stdout=output, stderr=subprocess.STDOUT,
                                        timeout=14500)
            payload = parse_scheduler_output(log.read_text(errors='replace'))
            # Direct run_once returns synchronously; relay mode carries its
            # own terminal flags and must never be inferred from acceptance.
            if payload.get('execution_mode') == 'local_run_once' and payload.get('status') in {'success', 'degraded', 'skipped', 'failed'}:
                payload.setdefault('task_terminal', True)
                payload.setdefault('background_continues', False)
            nested = payload.get('result') if isinstance(payload.get('result'), dict) else {}
            for key in ('status', 'skip_reason', 'warning_reason', 'degraded_stage'):
                check['task_' + key] = payload.get(key) or nested.get(key)
        bundle = export_current(settings, args.directory / 'bundle')
        if args.job != 'publish_only':
            try:
                outcome, proof = hosted_task_outcome(payload, result.returncode, before, identity(settings),
                                                    args.directory / 'bundle', settings.database_path)
                if proof:
                    check['nonblocking_validation'] = proof
                    print('::warning::正式报告已核验；上游缺失或异常收益已核验排除，原任务仍为degraded。', flush=True)
            except ReportExportError as exc:
                outcome = None; check['error_code'] = str(exc)
        check.update(status=outcome or 'failed', publication=bundle['publication'], publication_sync_status='verified_current')
        code = 0 if outcome else 1
    except Exception as exc:
        import traceback
        traceback.print_exc(); check.update(error_type=type(exc).__name__, error_code='hosted_job_failed')
    finally:
        check['finished_at'] = now_shanghai().isoformat()
        write_json(args.directory / 'check.json', check)
        print(json.dumps(check, ensure_ascii=False), flush=True)
    return code


if __name__ == '__main__': raise SystemExit(main())
