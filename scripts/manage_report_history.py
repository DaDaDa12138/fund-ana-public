#!/usr/bin/env python3
"""Preview/import/verify/export the fact ledger. Source files are never moved here."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'core')]
from backend.config import load_settings
from backend import report_history as history
from core.utils.report_history_store import canonical


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('plan', 'import', 'verify', 'parity', 'status', 'export', 'backup', 'acceptance'), nargs='?', default='plan')
    parser.add_argument('--plan', type=Path)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--max-units', type=int)
    parser.add_argument('--only-unverified',action='store_true',help='Parity: audit additions not covered by exact prior source/digest validation')
    parser.add_argument('--run-id')
    parser.add_argument('--revision')
    parser.add_argument('--view', choices=('formal', 'tracker', 'forward_validation'), default='formal')
    parser.add_argument('--format', choices=('xlsx', 'csv'), default='xlsx')
    args = parser.parse_args(); settings = load_settings()
    if args.output and args.output.exists(): parser.error('output already exists')
    if args.command == 'plan': result = history.inventory(settings)
    elif args.command == 'import':
        if not args.plan: parser.error('--plan is required')
        plan = json.loads(args.plan.read_text())
        result = (history.import_plan(settings, plan, max_units=args.max_units) if args.execute else
                  {'status': 'preview', 'reports': len(plan['reports']), 'sources': len(plan['sources'])})
    elif args.command in ('backup','acceptance'):
        from backend.report_history_retention import create_backup,acceptance
        result=acceptance(settings) if args.command=='acceptance' else (create_backup(settings) if args.execute else {'status':'preview'})
    elif args.command == 'parity': result = history.semantic_audit(settings,only_unverified=args.only_unverified)
    elif args.command in ('verify', 'status'):
        result = history.verify(settings, compare_sources=args.command == 'verify')
    else:
        if not args.run_id or not args.output: parser.error('--run-id and --output are required')
        payload = history.get_report(settings, args.run_id, args.revision)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open('xb') as stream:
            stream.write(history.export_report(settings, payload, args.view, args.format))
        result = {'status': 'exported', 'output': str(args.output)}
    if args.output and args.command != 'export':
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open('xb') as stream: stream.write(canonical(result))
    print(json.dumps({k:v for k,v in result.items() if k not in ('results','reports','sources') or not isinstance(v,list)}, ensure_ascii=False))
    return 1 if result.get('errors') or result.get('status') in ('blocked','error') else 0


if __name__ == '__main__':
    raise SystemExit(main())
