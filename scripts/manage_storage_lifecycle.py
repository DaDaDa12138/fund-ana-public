#!/usr/bin/env python3
"""Preview, apply/resume, verify and restore exact offline plans."""
from __future__ import annotations

import argparse
from dataclasses import fields, replace
from datetime import datetime
import json
import os
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'core')]

from backend.config import load_settings
from backend.storage_lifecycle import (
    atomic_json, build_plan, read_json, run_plan, verify_offline,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('plan', 'apply', 'restore', 'verify', 'daily', 'gc', 'purge'), nargs='?', default='plan')
    parser.add_argument('--plan', type=Path)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--unit', action='append', default=[])
    parser.add_argument('--family', action='append', help='Limit preview to a reviewed material family')
    parser.add_argument('--execute', action='store_true', help='Required for mutation; default preview')
    args = parser.parse_args()
    settings = load_settings()
    storage_view = os.environ.get('FUND_ANA_STORAGE_LIFECYCLE_PROJECT_ROOT')
    if storage_view:
        view = Path(storage_view).absolute()
        original = settings.project_root
        # Only lifecycle filesystem operations use the common storage view;
        # Flask's /app artifact root and its traversal protection stay intact.
        mapped = {field.name: view / getattr(settings, field.name).relative_to(original)
                  for field in fields(settings)
                  if isinstance(getattr(settings, field.name), Path)
                  and getattr(settings, field.name).is_relative_to(original)}
        settings = replace(settings, **mapped)
    directory = settings.reports_dir / '.audit/storage_lifecycle'
    directory.mkdir(parents=True, exist_ok=True)
    output = args.output or directory / (args.command + '_' + datetime.now().strftime('%Y%m%d_%H%M%S_%f') + '.json')
    if output.exists():
        parser.error('output already exists')
    if args.command == 'plan':
        result = build_plan(settings, families=set(args.family) if args.family else None)
    elif args.command == 'gc':
        from backend.storage_lifecycle_gc import collect_offline_lookup_aliases
        result = collect_offline_lookup_aliases(settings, apply=args.execute)
    elif args.command == 'purge':
        from backend.report_history_retention import purge
        result=purge(settings,execute=args.execute)
    elif args.command == 'verify':
        result = verify_offline(settings, unit_ids=set(args.unit) or None)
    elif args.command == 'daily':
        started = time.monotonic()
        queue = directory / 'daily_plan.json'
        # Continue a previously verified plan before discovering new candidates.
        if queue.exists():
            plan = read_json(queue)
        else:
            cursor=directory/'report_discovery_cursor.json'
            report_after=read_json(cursor).get('after','') if cursor.exists() else ''
            plan = build_plan(settings, max_seconds=20, report_after=report_after)
            if args.execute:
                atomic_json(queue, plan)
        result = run_plan(settings, plan, apply=args.execute,
                          max_seconds=max(1, 55 - (time.monotonic() - started)))
        if args.execute and result['status'] == 'complete':
            marker=next((item for item in plan.get('retained',[])
                         if item.get('reason')=='report_view_discovery_budget'),{})
            atomic_json(directory/'report_discovery_cursor.json',{'after':marker.get('next_after','')})
            from backend.report_history_retention import purge
            remaining=55-(time.monotonic()-started)
            if remaining>2:
                result['report_view_purge']=purge(settings,execute=True,max_seconds=remaining,
                    max_files=max(0,2000-result['verified_files']),max_bytes=max(0,8*1024**3-result['verified_bytes']))

            remaining = 55 - (time.monotonic() - started)
            if remaining > 5:
                from backend.storage_lifecycle_gc import collect_offline_lookup_aliases
                result['lookup_gc'] = collect_offline_lookup_aliases(
                    settings, apply=True, max_seconds=remaining,
                    max_files=max(0, 2000 - result['verified_files'] - result.get('report_view_purge',{}).get('verified_files',0)),
                    max_bytes=max(0, 8 * 1024**3 - result['verified_bytes'] - result.get('report_view_purge',{}).get('verified_bytes',0)))
            # Preserve the reviewed queue as immutable audit evidence.
            queue.rename(directory / ('completed_plan_' + datetime.now().strftime('%Y%m%d_%H%M%S_%f') + '.json'))
    else:
        if args.plan is None:
            parser.error('--plan is required')
        result = run_plan(settings, read_json(args.plan), apply=args.execute,
                          restore=args.command == 'restore', unit_ids=set(args.unit) or None)
    atomic_json(output, result)
    print(json.dumps({'output': str(output), **{k: v for k, v in result.items() if k not in ('units', 'protected', 'retained')}}, ensure_ascii=False))
    return 1 if result.get('status') == 'error' or result.get('errors') else 0


if __name__ == '__main__':
    raise SystemExit(main())
