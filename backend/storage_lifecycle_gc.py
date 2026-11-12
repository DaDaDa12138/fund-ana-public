"""Evict lookup aliases only when verified offline copies survive outside root."""
from __future__ import annotations

from collections import defaultdict
import os
from pathlib import Path
import sqlite3
import time

from backend.storage_lifecycle import connect, identity, locks, open_inodes, safe, sha, sync_dir


def collect_offline_lookup_aliases(settings, *, apply=False, max_seconds=60, max_files=2000, max_bytes=8 * 1024**3):
    root = Path(settings.project_root).absolute()
    archive = Path(settings.storage_lifecycle_archive_root)
    pools = [root / 'reports' / name for name in ('.calculation_input_pool', '.report_artifact_pool', '.test_source_pool')]
    result = {'mode': 'apply' if apply else 'preview', 'removed': [], 'candidates': [], 'status': 'complete',
              'deleted_last_link_allocated_bytes': 0, 'migration_does_not_free_shared_blocks': True}
    started = time.monotonic()
    with locks(settings), connect(root) as conn:
        offline = {}
        for payload, in conn.execute("SELECT payload FROM units WHERE state='offline'"):
            import json
            for e in json.loads(payload)['entries']:
                if e['identity']['kind'] == 'file':
                    offline[(e['identity']['device'], e['identity']['inode'])] = e
        wanted = {}
        for pool in pools:
            if pool.exists():
                for path in pool.iterdir():
                    if path.is_file() and not path.is_symlink():
                        s = path.stat()
                        if (s.st_dev, s.st_ino) in offline:
                            wanted[(s.st_dev, s.st_ino)] = []
        if not wanted:
            return result
        # Locate all ONLINE aliases. st_nlink alone includes offline aliases and
        # cannot distinguish a live local pack from an offline-only lookup link.
        for base, dirs, names in os.walk(root, followlinks=False):
            dirs[:] = [d for d in dirs if d not in ('.venv', '__pycache__', '.pytest_cache') and not (Path(base) / d).is_symlink()]
            for name in names:
                path = Path(base) / name
                s = path.lstat()
                key = (s.st_dev, s.st_ino)
                if key in wanted:
                    wanted[key].append(path)
            if time.monotonic() - started > max_seconds:
                result.update(status='deferred', reason='online_alias_scan_budget')
                return result
        opened, verified_bytes = open_inodes(), 0
        for key, aliases in wanted.items():
            if not aliases or key in opened or any(p.parent not in pools for p in aliases):
                continue
            e = offline[key]
            destination = safe(archive, e['path'])
            if time.monotonic() - started > max_seconds or len(result['candidates']) + len(aliases) > max_files or verified_bytes + e['identity']['size'] > max_bytes:
                result.update(status='deferred', reason='verification_budget')
                break
            if identity(destination)['inode'] != key[1] or sha(destination) != e['sha256']:
                raise ValueError('offline_lookup_copy_not_verified')
            verified_bytes += e['identity']['size']
            for p in aliases:
                relative = str(p.relative_to(root))
                result['candidates'].append(relative)
                if not apply:
                    continue
                actual = p.stat()
                if (actual.st_dev, actual.st_ino) != key or actual.st_nlink <= 1:
                    raise ValueError('lookup_link_identity_changed_or_no_surviving_copy')
                if p.parent == pools[0]:
                    catalog = p.parent / 'catalog.sqlite'
                    with sqlite3.connect(catalog) as index:
                        index.execute('DELETE FROM objects WHERE pack=?', (p.name,))
                        index.execute('DELETE FROM packs WHERE name=?', (p.name,))
                p.unlink()
                sync_dir(p.parent)
                result['removed'].append(relative)
    return result
