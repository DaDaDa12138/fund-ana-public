#!/usr/bin/env python3
"""Bootstrap once, then restore/save operational state from hosted runners."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import shutil
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from backend.github_cloud_state import Remote, inventory, restore, snapshot, runtime_snapshot_lock, publication_history_contract
from backend.github_storage import state_repository


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=('plan', 'restore', 'save', 'status'))
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--repository', default=state_repository())
    parser.add_argument('--receipt', type=Path, required=True)
    args = parser.parse_args(); root = args.root.resolve()
    if args.operation == 'plan':
        paths = inventory(root); seen = set(); total = 0
        for path in paths:
            st = path.lstat(); key = (st.st_dev, st.st_ino)
            if path.is_file() and not path.is_symlink() and key not in seen: total += st.st_size; seen.add(key)
        result = dict(files=len(paths), unique_bytes=total, paths=[str(p.relative_to(root)) for p in paths])
    else:
        remote = Remote(args.repository); previous, head = remote.load()
        if args.operation == 'restore':
            if previous is None: raise RuntimeError('cloud_seed_missing')
            required = sum(x['size'] for x in previous['files'] if x['kind'] == 'file')
            available = shutil.disk_usage(root).free
            if available < required + 5 * 1024**3: raise RuntimeError('cloud_runner_disk_insufficient')
            # Resolve immutable asset catalogs serially before parallel fetches.
            for tag in sorted({obj['tag'] for obj in previous['objects'].values()}):
                remote.release(tag)
            restore(root, previous, remote.fetch)
            result = dict(status='restored', state_commit=head, files=len(previous['files']), bytes=required,
                          history_contract=previous.get('publication_history'))
        elif args.operation == 'save':
            # A hosted save must replace exactly the state restored by this job.
            restored = root / '.cloud-restored.json'
            if restored.exists() and json.loads(restored.read_text())['state_commit'] != head:
                raise RuntimeError('cloud_state_changed_since_restore')
            from backend.publication_transaction import load_publication_head
            with runtime_snapshot_lock(root) as settings:
                current = load_publication_head(settings)
                if not current: raise RuntimeError('cloud_formal_head_missing')
                history_contract = publication_history_contract(settings)
                value = snapshot(root, inventory(root), previous, remote.put)
                value['publication_history'] = history_contract
                value['publication'] = {'generation': current['generation'], 'run_id': current['current_published_run_id']}
                value['source_sha'] = __import__('os').environ.get('GITHUB_SHA')
                commit = remote.commit(value, head)
            result = dict(status='saved', state_commit=commit, files=len(value['files']),
                          objects=len(value['objects']), compressed_bytes=sum(x['size'] for x in value['objects'].values()),
                          publication=value['publication'])
        else:
            result = dict(state_commit=head, files=len((previous or {}).get('files', [])),
                          objects=len((previous or {}).get('objects', {})), publication=(previous or {}).get('publication'))
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    args.receipt.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({k:v for k,v in result.items() if k not in ('paths', 'history_contract')}, ensure_ascii=False), flush=True)
    return 0


if __name__ == '__main__': raise SystemExit(main())
