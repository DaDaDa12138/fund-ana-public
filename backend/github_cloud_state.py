"""Private, content-addressed runner state; Git contains only its compact index.

Release objects are immutable. A fast-forward cloud-state commit is the only
remote state commit point. Interrupted uploads cannot advance that pointer.
"""
from __future__ import annotations

import base64
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from datetime import datetime
import gzip
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import sqlite3
import stat
import subprocess
import tempfile
import time
from typing import Callable

SCHEMA = 'fund-ana-cloud-state-v1'
BRANCH = 'cloud-state'
CHUNK = 256 * 1024**2
PACK = 64 * 1024**2
DATA_FILES = {'bond_funds.db', 'report_history.sqlite', 'runtime_overrides.yaml',
              'long_term_history_maintenance_state.json', 'long_term_terminal_upstream_exclusions.json',
              'nav_history_maintenance_state.json', 'workflow_state.json', 'scheduler_state.json'}
# Reviewed, credential-free operational policies are state, not source defaults.
CONFIG_FILES = {'core/config/fund_code_alias_review_overrides.json',
                'core/config/fund_exclusion_rules.json', 'config/screening_parquet_bindings.json',
                'config/runtime_overrides.yaml'}


class CloudStateError(RuntimeError):
    pass


def sha(path: Path) -> str:
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def relative(value: str) -> str:
    path = PurePosixPath(value)
    if (path.is_absolute() or '..' in path.parts or not path.parts
            or path.as_posix() != value or '\\' in value):
        raise CloudStateError('cloud_path_invalid')
    if not (value.startswith('reports/') or value.startswith('backend/data/') or value in CONFIG_FILES):
        raise CloudStateError('cloud_path_not_runtime_data')
    if value.startswith('backend/data/') and path.parts[2] not in DATA_FILES | {'archives'}:
        raise CloudStateError('cloud_data_not_allowlisted')
    return value


def link_target(name: str, value: str) -> str:
    if value.startswith('/app/'):
        value = os.path.relpath(value[5:], start=str(PurePosixPath(name).parent))
    if value.startswith('/') or '\\' in value:
        raise CloudStateError('cloud_link_absolute')
    resolved = os.path.normpath(str(PurePosixPath(name).parent / value))
    relative(resolved)
    return value


def inventory(root: Path) -> list[Path]:
    """Dependency-preserving seed, excluding user data, backups and scratch.

    Keep immutable revisions, legacy report sources, core history and the recent
    complete capture units. Never infer publication status from this inventory.
    """
    selected: set[Path] = set()

    def add(path: Path) -> None:
        if path.is_symlink() or path.is_file():
            relative(path.relative_to(root).as_posix())
            selected.add(path)

    def tree(path: Path) -> None:
        if not path.is_dir(): return
        for folder, dirs, names in os.walk(path, followlinks=False):
            for name in names: add(Path(folder) / name)
            for name in list(dirs):
                candidate = Path(folder) / name
                if candidate.is_symlink(): add(candidate); dirs.remove(name)

    for name in DATA_FILES: add(root / 'backend/data' / name)
    for name in CONFIG_FILES: add(root / name)
    tree(root / 'backend/data/archives')
    reports = root / 'reports'
    for path in reports.iterdir():
        if path.is_file() or path.is_symlink():
            if not path.name.endswith(('.tmp', '.lock')): add(path)
    for path in (reports / 'runs').iterdir():
        if not path.is_dir(): continue
        for source in path.iterdir():
            if source.is_file() or source.is_symlink(): add(source)
        # This JSON is publication evidence, despite living under logs. Its
        # absence changes legacy risk-lineage gates and cumulative history.
        add(path / 'logs/subprocess/subprocess_index.json')
        tree(path / 'revisions')
        # Dates are the immutable capture-unit identity, not file mtimes.
        try: age = (datetime.now().date() - datetime.strptime(path.name[:8], '%Y%m%d').date()).days
        except ValueError: age = 0
        if age <= 14: tree(path / 'calculation_inputs')
    for family in ('archive/superseded_runs', '.audit/storage_lifecycle',
                   '.audit/collection_calculations', '.audit/scheduler_task_runs',
                   '.audit/report_history', '.audit/task_history_extra_data',
                   ):
        tree(reports / family)
    # Core processing journals/catalogue and input workbooks are production
    # dependencies. Old diagnostic audit/cache and dedup lookup pools are not.
    for path in (reports / 'core_output').iterdir():
        if path.is_file() or path.is_symlink(): add(path)
    tree(reports / 'core_output/.catalog')
    # Legacy history still reads original workbooks even in ledger mode unless
    # an approved retirement receipt exists. They may live in core_artifacts,
    # outside immutable revision manifests and the core output directory.
    ledger = root / 'backend/data/report_history.sqlite'
    if ledger.is_file():
        with closing(sqlite3.connect(ledger.as_uri() + '?mode=ro', uri=True)) as conn:
            sources = conn.execute("SELECT DISTINCT path,sha256 FROM views WHERE kind IN "
                                   "('formal','tracker','watch','core_raw')").fetchall()
        expected: dict[str, set[str]] = {}
        for name, digest in sources: expected.setdefault(relative(name), set()).add(digest)
        for name, hashes in expected.items():
            path = root / name
            if path.is_file():
                if sha(path) not in hashes:
                    raise CloudStateError('cloud_history_original_hash_mismatch:' + name)
                add(path)
    # Only production lookup namespaces, not arbitrary historical audits which
    # can contain schema dumps, research credentials or unrelated diagnostics.
    for family in ('nav_history_archive', 'rank_history_archive', 'screening_history_archive',
                   'screening_history_archive_rotation', 'screening_columnar_maintenance',
                   'nav_history_maintenance', 'long_term_history_maintenance', 'long_term_history_gap',
                   'risk_coverage_gap_audit', 'data_truthfulness', 'screening_history_cleanup'):
        tree(reports / 'audit' / family)
    # Exclude open lock/WAL/SHM/temp files; SQLite is copied using backup().
    selected = {p for p in selected if not p.name.endswith(('-wal', '-shm', '.lock', '.tmp'))}
    # All aliases must resolve to included data, without introducing host paths.
    pending = list(selected)
    while pending:
        path = pending.pop()
        if not path.is_symlink(): continue
        name = path.relative_to(root).as_posix()
        target = (root / os.path.normpath(str(Path(name).parent / link_target(name, os.readlink(path))))).resolve()
        if not target.is_relative_to(root.resolve()): raise CloudStateError('cloud_alias_outside_project')
        if not target.exists(): raise CloudStateError('cloud_seed_broken_alias:' + name)
        if target.is_file() and target not in selected: add(target); pending.append(target)
        if target.is_dir(): tree(target)
    return sorted(selected)


def publication_history_contract(settings) -> dict:
    """Bind decisions to facts, excluding host paths and mutable display text."""
    from backend.publication import build_publication_registry
    registry = build_publication_registry(settings)
    records = []
    for item in registry['canonical_runs']:
        records.append({
            'run_id': item['run_id'], 'publishable': bool(item.get('publishable')),
            'committed': bool(item.get('publication_committed')),
            'eligible': item.get('formal_publication_eligible'),
            'mode': item.get('publication_mode'),
            'nav_date': (item.get('snapshot') or {}).get('candidate_date'),
            'blockers': sorted({str(row.get('code')) for row in item.get('publish_blockers') or []
                                if row.get('blocking')}),
        })
    return {'schema': 'fund-ana-cloud-history-decisions-v1',
            'records': sorted(records, key=lambda row: row['run_id'])}


def validate_manifest(value: dict) -> None:
    if value.get('schema') != SCHEMA or not isinstance(value.get('files'), list):
        raise CloudStateError('cloud_manifest_invalid')
    objects = value.get('objects', {}); names = set()
    kinds = {x['path']: x['kind'] for x in value['files']}
    for key, item in objects.items():
        if (not re.fullmatch(r'[0-9a-f]{64}', key) or item.get('sha256') != key
                or not re.fullmatch(r'cloud-state-objects-\d{6}(?:-\d+)?', item.get('tag', ''))
                or not 0 < item.get('raw_size', 0) <= CHUNK
                or not 0 < item.get('size', 0) < 2 * 1024**3):
            raise CloudStateError('cloud_object_invalid')
    for item in value['files']:
        name = relative(item['path'])
        if name in names: raise CloudStateError('cloud_duplicate_path')
        if any(kinds.get(parent.as_posix()) == 'symlink'
               for parent in PurePosixPath(name).parents if str(parent) != '.'):
            raise CloudStateError('cloud_symlink_parent')
        if item['kind'] == 'symlink': link_target(name, item['target'])
        elif item['kind'] == 'hardlink':
            if relative(item['target']) not in names or kinds.get(item['target']) != 'file':
                raise CloudStateError('cloud_hardlink_target_invalid')
        elif item['kind'] == 'file':
            if not re.fullmatch(r'[0-9a-f]{64}', item.get('sha256', '')) or item.get('size', -1) < 0:
                raise CloudStateError('cloud_file_invalid')
            end = 0
            for segment in item['segments']:
                obj = objects.get(segment['object'])
                if (obj is None or segment['file_offset'] != end or segment['offset'] < 0
                        or segment['size'] <= 0 or segment['offset'] + segment['size'] > obj['raw_size']):
                    raise CloudStateError('cloud_segment_invalid')
                end += segment['size']
            if end != item['size']: raise CloudStateError('cloud_file_size_invalid')
        else: raise CloudStateError('cloud_kind_invalid')
        names.add(name)


def snapshot(root: Path, paths: list[Path], previous: dict | None,
             put: Callable[[Path, int], dict], *, sqlite_copy: bool = True) -> dict:
    """put returns immutable {sha256, size, raw_size, tag}; no token in state."""
    old = {x['sha256']: x for x in (previous or {}).get('files', []) if x['kind'] == 'file'}
    objects = dict((previous or {}).get('objects', {}))
    files = []; inodes = {}; contents = {}; pending = []; buffer = bytearray()
    with tempfile.TemporaryDirectory(prefix='fund-ana-cloud-pack-') as scratch:
        temporary = Path(scratch)

        def flush() -> None:
            if not buffer: return
            raw = temporary / 'pack.raw'; raw.write_bytes(buffer)
            obj = put(raw, len(buffer)); objects[obj['sha256']] = obj
            for record, offset, length, file_offset in pending:
                record['segments'].append(dict(object=obj['sha256'], offset=offset, size=length, file_offset=file_offset))
            pending.clear(); buffer.clear()

        for number, path in enumerate(paths):
            if number % 1000 == 0: print(json.dumps({'cloud_snapshot_files': number}), flush=True)
            name = relative(path.relative_to(root).as_posix()); before = path.lstat()
            record = dict(path=name, mode=stat.S_IMODE(before.st_mode), mtime_ns=before.st_mtime_ns)
            if path.is_symlink():
                files.append(dict(**record, kind='symlink', target=link_target(name, os.readlink(path))))
                continue
            identity = (before.st_dev, before.st_ino)
            if identity in inodes:
                files.append(dict(**record, kind='hardlink', target=inodes[identity])); continue
            inodes[identity] = name
            source = path
            cold_archive = name.startswith('backend/data/archives/')
            if cold_archive and path.suffix == '.db':
                wal = path.with_name(path.name + '-wal')
                if wal.exists() and wal.stat().st_size:
                    raise CloudStateError('cloud_archive_requires_checkpoint')
            # Cold-store manifests bind exact original bytes. Only live DBs
            # become new native backups; immutable cold files stay byte exact.
            database = sqlite_copy and not cold_archive and path.suffix in ('.db', '.sqlite') and before.st_size > 0
            if database:
                source = temporary / 'database.sqlite'
                with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as src, closing(sqlite3.connect(source)) as dst:
                    if path.name == 'bond_funds.db' and src.execute(
                            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='users'").fetchone():
                        if src.execute('SELECT COUNT(*) FROM users').fetchone()[0]:
                            raise CloudStateError('cloud_database_contains_user_accounts')
                    src.backup(dst)
                    if dst.execute('PRAGMA quick_check').fetchone() != ('ok',):
                        raise CloudStateError('cloud_database_integrity_failed')
            digest = sha(source); size = source.stat().st_size
            record.update(kind='file', sha256=digest, size=size, segments=[])
            files.append(record)
            known = contents.get(digest) or old.get(digest)
            if known is not None:
                # A file can have its first segment populated while the tail
                # is pending. Share the live list until all packs are flushed;
                # copying it here would permanently lose the pending tail.
                if not known['segments'] and size: flush()
                record['segments'] = known['segments']
            else:
                contents[digest] = record
                with source.open('rb') as stream:
                    offset = 0
                    while offset < size:
                        count = min(CHUNK if size > PACK else PACK - len(buffer), size - offset)
                        if count == 0: flush(); continue
                        if size > PACK: flush()
                        data = stream.read(count)
                        if len(data) != count: raise CloudStateError('cloud_source_truncated')
                        pending.append((record, len(buffer), len(data), offset))
                        buffer.extend(data); offset += len(data)
                        if len(buffer) >= PACK: flush()
            if database: source.unlink()
            elif (path.stat().st_size, path.stat().st_mtime_ns) != (before.st_size, before.st_mtime_ns):
                raise CloudStateError('cloud_source_changed')
        flush()
    # Detach aliases (and previous-state lists) only after every segment exists.
    for record in files:
        if record['kind'] == 'file': record['segments'] = [dict(x) for x in record['segments']]
    used = {s['object'] for f in files if f['kind'] == 'file' for s in f['segments']}
    from backend.shanghai_time import now_shanghai
    value = dict(schema=SCHEMA, created_at=now_shanghai().isoformat(), timezone='Asia/Shanghai', files=files,
                 objects={key: objects[key] for key in sorted(used)})
    validate_manifest(value)
    return value


def restore(root: Path, value: dict, fetch: Callable[[dict, Path], None]) -> None:
    validate_manifest(value)
    # A fresh checkout may contain .gitkeep, but never pre-existing runtime data.
    for name in ('backend/data/bond_funds.db', 'reports/.publication_current'):
        if (root / name).exists() or (root / name).is_symlink():
            raise CloudStateError('cloud_restore_requires_empty_runtime')
    needed = {}; by_path = {}
    for item in value['files']:
        name = item['path']; target = root / name
        if any(p.is_symlink() for p in target.parents if p != root.parent):
            raise CloudStateError('cloud_restore_symlink_parent')
        target.parent.mkdir(parents=True, exist_ok=True)
        if item['kind'] != 'file': continue
        with target.open('xb') as stream: stream.truncate(item['size'])
        by_path[name] = item
        for segment in item['segments']: needed.setdefault(segment['object'], []).append((name, segment))
    # Three downloads overlap network latency. Keep only this bounded window
    # on disk; decompression and business-file writes remain serial and verified.
    work = list(needed.items())
    with tempfile.TemporaryDirectory(prefix='fund-ana-cloud-restore-') as scratch, \
            ThreadPoolExecutor(max_workers=3) as pool:
        pending = {}
        def prefetch(index):
            key = work[index][0]
            path = Path(scratch) / (key + '.zst')
            pending[index] = pool.submit(fetch, value['objects'][key], path)
        for index in range(min(3, len(work))): prefetch(index)
        for number, (key, destinations) in enumerate(work):
            pending.pop(number).result()
            if number + 3 < len(work): prefetch(number + 3)
            obj = value['objects'][key]; compressed = Path(scratch) / (key + '.zst'); raw = Path(scratch) / 'object.raw'
            if compressed.stat().st_size != obj['size'] or sha(compressed) != key:
                raise CloudStateError('cloud_download_corrupt')
            # Bound decompression before writing any business file.
            with raw.open('wb') as output:
                process = subprocess.Popen(['zstd', '-q', '-d', '-c', str(compressed)], stdout=subprocess.PIPE)
                assert process.stdout is not None
                remaining = obj['raw_size']
                while remaining:
                    chunk = process.stdout.read(min(1024**2, remaining))
                    if not chunk: break
                    output.write(chunk); remaining -= len(chunk)
                extra = process.stdout.read(1)
                if extra or remaining: process.kill()
                status = process.wait()
                if extra or remaining or status: raise CloudStateError('cloud_decompression_invalid')
            with raw.open('rb') as stream:
                for name, segment in destinations:
                    stream.seek(segment['offset'])
                    with (root / name).open('r+b') as output:
                        output.seek(segment['file_offset']); remaining = segment['size']
                        while remaining:
                            chunk = stream.read(min(1024**2, remaining)); output.write(chunk); remaining -= len(chunk)
            compressed.unlink(); raw.unlink()
            print(json.dumps({'cloud_restored_objects': number + 1, 'total': len(needed)}), flush=True)
    for name, item in by_path.items():
        target = root / name
        if sha(target) != item['sha256']: raise CloudStateError('cloud_restored_file_corrupt')
        os.chmod(target, item['mode'] & 0o777); os.utime(target, ns=(item['mtime_ns'], item['mtime_ns']))
    for item in value['files']:
        target = root / item['path']
        if item['kind'] == 'hardlink': os.link(root / item['target'], target)
        elif item['kind'] == 'symlink': target.symlink_to(link_target(item['path'], item['target']))


def git_environment(token: str) -> dict:
    from backend.shanghai_time import now_shanghai
    env = {k: v for k, v in os.environ.items() if not k.startswith('GIT_') and k not in ('GH_TOKEN', 'GITHUB_TOKEN')}
    auth = base64.b64encode(('x-access-token:' + token).encode()).decode()
    env.update(GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_COUNT='1',
               GIT_CONFIG_KEY_0='http.https://github.com/.extraheader',
               GIT_CONFIG_VALUE_0='AUTHORIZATION: basic ' + auth, GIT_TERMINAL_PROMPT='0',
               TZ='Asia/Shanghai', GIT_AUTHOR_DATE=now_shanghai().isoformat(),
               GIT_COMMITTER_DATE=now_shanghai().isoformat())
    return env


@contextmanager
def runtime_snapshot_lock(root: Path):
    """Same workflow -> publication -> SQLite lock order as production."""
    import fcntl
    import backend.config as config
    from backend.publication_transaction import publication_io_lock
    original = config.PROJECT_ROOT
    try:
        config.PROJECT_ROOT = root
        settings = config.load_settings(root / 'config/project.yaml')
    finally:
        config.PROJECT_ROOT = original
    lock = root / 'backend/data/workflow_state.json.lock'
    with lock.open('a+b') as stream:
        try: fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc: raise CloudStateError('cloud_snapshot_workflow_busy') from exc
        with publication_io_lock(settings): yield settings


class Remote:
    def __init__(self, repository: str):
        if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repository):
            raise CloudStateError('cloud_repository_invalid')
        self.repository = repository
        self.token = os.environ.get('GH_TOKEN') or subprocess.check_output(['gh', 'auth', 'token'], text=True).strip()
        self.environment = dict(os.environ, GH_TOKEN=self.token)
        info = self.gh(['api', 'repos/' + repository])
        if not json.loads(info)['private']: raise CloudStateError('cloud_state_requires_private_repository')
        self.releases: dict = {}

    def gh(self, args: list[str], *, check: bool = True, **kwargs):
        # Only retry idempotent GETs. A failed mutation can already have taken
        # effect remotely and must be reconciled instead of blindly repeated.
        read = args[:1] == ['api'] and not any(
            arg in ('-X', '--method', '-f', '-F', '--field', '--raw-field', '--input')
            or arg.startswith(('--method=', '--field=', '--raw-field=', '--input=')) for arg in args)
        for attempt in range(4 if read else 1):
            try:
                result = subprocess.run(['gh', *args], env=self.environment, capture_output=True,
                                        timeout=900 if args[:2] == ['release', 'upload'] else 120, **kwargs)
            except subprocess.TimeoutExpired:
                if not read or attempt == 3: raise CloudStateError('cloud_github_request_timeout') from None
            else:
                if result.returncode == 0: return result.stdout
                error = result.stderr.decode(errors='replace') if isinstance(result.stderr, bytes) else result.stderr or ''
                retryable = not re.search(r'HTTP (?:400|401|403|404|409|422)\b', error)
                if not read or not retryable or attempt == 3: break
            time.sleep(2 ** attempt)
        if check and result.returncode: raise CloudStateError('cloud_github_request_failed:' + args[0])
        return result.stdout

    def checkout(self, directory: Path) -> str | None:
        from scripts.sync_github_reports import git
        env = git_environment(self.token)
        git(['init', '--initial-branch=' + BRANCH], directory, env)
        git(['remote', 'add', 'origin', 'https://github.com/' + self.repository + '.git'], directory, env)
        found = git(['ls-remote', '--exit-code', '--heads', 'origin', 'refs/heads/' + BRANCH], directory, env, check=False)
        if found.returncode == 2: return None
        if found.returncode: raise CloudStateError('cloud_state_head_unavailable')
        git(['fetch', '--depth=1', 'origin', 'refs/heads/' + BRANCH], directory, env)
        git(['reset', '--hard', 'FETCH_HEAD'], directory, env)
        return git(['rev-parse', 'HEAD'], directory, env).stdout.strip()

    def load(self) -> tuple[dict | None, str | None]:
        with tempfile.TemporaryDirectory() as scratch:
            directory = Path(scratch); head = self.checkout(directory)
            if not head: return None, None
            value = json.loads(gzip.decompress((directory / 'state.json.gz').read_bytes()))
            validate_manifest(value); return value, head

    def commit(self, value: dict, expected: str | None) -> str:
        from scripts.sync_github_reports import git
        validate_manifest(value)
        with tempfile.TemporaryDirectory() as scratch:
            directory = Path(scratch); actual = self.checkout(directory)
            if actual != expected: raise CloudStateError('cloud_state_head_changed')
            raw = json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode()
            (directory / 'state.json.gz').write_bytes(gzip.compress(raw, mtime=0))
            (directory / 'README.md').write_text('# Private operational state\n\nIndex only; immutable objects are private Release assets. No credentials or favorites.\n')
            env = git_environment(self.token)
            git(['config', 'user.name', 'github-actions[bot]'], directory, env)
            git(['config', 'user.email', '41898282+github-actions[bot]@users.noreply.github.com'], directory, env)
            git(['config', 'core.hooksPath', os.devnull], directory, env)
            git(['add', 'state.json.gz', 'README.md'], directory, env)
            git(['commit', '-m', 'state: ' + value['created_at']], directory, env)
            git(['push', 'origin', 'HEAD:refs/heads/' + BRANCH], directory, env)
            return git(['rev-parse', 'HEAD'], directory, env).stdout.strip()

    def release(self, tag: str) -> dict:
        if tag not in self.releases:
            args = ['api', f'repos/{self.repository}/releases/tags/{tag}']
            raw = self.gh(args, check=False)
            try: info = json.loads(raw)
            except ValueError: info = {}
            if 'id' not in info:
                self.gh(['release', 'create', tag, '--repo', self.repository, '--target', 'main',
                         '--prerelease', '--title', 'Private runner state ' + tag, '--notes',
                         'Immutable operational data, not a software release. Do not edit assets.'])
                info = json.loads(self.gh(args))
            # API release detail embeds only a bounded assets list; paginate explicitly.
            info['assets'] = []
            for number in range(1, 12):
                page = json.loads(self.gh(['api', f'repos/{self.repository}/releases/{info["id"]}/assets?per_page=100&page={number}']))
                info['assets'].extend(page)
                if len(page) < 100: break
            self.releases[tag] = info
        return self.releases[tag]

    def put(self, raw: Path, size: int) -> dict:
        compressed = raw.with_suffix('.zst')
        subprocess.run(['zstd', '-q', '-3', '-T2', '-f', str(raw), '-o', str(compressed)], check=True)
        from backend.shanghai_time import now_shanghai
        digest = sha(compressed); tag = 'cloud-state-objects-' + now_shanghai().strftime('%Y%m')
        for suffix in range(100):
            candidate = tag + (f'-{suffix}' if suffix else '')
            release = self.release(candidate)
            existing = next((x for x in release['assets'] if x['name'] == digest + '.zst'), None)
            if existing:
                if existing['size'] != compressed.stat().st_size or existing['state'] != 'uploaded':
                    raise CloudStateError('cloud_existing_object_incomplete')
                tag = candidate; break
            if len(release['assets']) < 900:
                tag = candidate; upload = compressed.with_name(digest + '.zst'); compressed.rename(upload)
                self.gh(['release', 'upload', tag, str(upload), '--repo', self.repository])
                name = upload.name; upload.rename(compressed)
                # GitHub may sort assets by name, not upload time. Search by
                # identity, never assume the new object is on the last page.
                asset = None
                for page_number in range(1, 12):
                    page = json.loads(self.gh(['api', f'repos/{self.repository}/releases/{release["id"]}/assets?per_page=100&page={page_number}']))
                    asset = next((x for x in page if x['name'] == name), None)
                    if asset or len(page) < 100: break
                if (not asset or asset['state'] != 'uploaded' or asset['size'] != compressed.stat().st_size
                        or asset.get('digest') not in (None, 'sha256:' + digest)):
                    raise CloudStateError('cloud_uploaded_object_invalid')
                release['assets'].append(asset)
                break
        else: raise CloudStateError('cloud_release_shards_exhausted')
        result = dict(sha256=digest, raw_size=size, size=compressed.stat().st_size, tag=tag)
        compressed.unlink(); return result

    def fetch(self, obj: dict, output: Path) -> None:
        asset = next((x for x in self.release(obj['tag'])['assets'] if x['name'] == obj['sha256'] + '.zst'), None)
        if not asset: raise CloudStateError('cloud_object_missing')
        # gh streams the authenticated redirect without exposing the token.
        for attempt in range(4):
            # Truncate every failed partial download before retrying. Successful
            # responses still undergo compressed SHA and file SHA validation.
            with output.open('wb') as stream:
                try:
                    result = subprocess.run(['gh', 'api', '-H', 'Accept: application/octet-stream',
                                             f'repos/{self.repository}/releases/assets/{asset["id"]}'],
                                            env=self.environment, stdout=stream, stderr=subprocess.PIPE, timeout=900)
                except subprocess.TimeoutExpired:
                    if attempt == 3: raise CloudStateError('cloud_object_download_timeout') from None
                    retryable = True
                else:
                    if result.returncode == 0: return
                    error = result.stderr.decode(errors='replace')
                    retryable = not re.search(r'HTTP (?:400|401|403|404|409|422)\b', error)
            if not retryable or attempt == 3: raise CloudStateError('cloud_object_download_failed')
            time.sleep(2 ** attempt)
