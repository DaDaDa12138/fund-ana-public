"""Cleanup safety fixtures, without production fund data."""
from pathlib import Path

import pytest

from core.utils import report_history_store as store
from core.bond_fund_screening.tasks import output_retention as retention


def artifacts(tmp_path, mode='ledger'):
    root = tmp_path / 'project'
    (root / 'config').mkdir(parents=True)
    (root / 'config/project.yaml').write_text(f'report_history:\n  read_mode: {mode}\n')
    output = root / 'reports/core_output'
    output.mkdir(parents=True)
    old = output / '当日最终优质基金_20261030_010000.xlsx'
    new = output / '当日最终优质基金_20261030_020000.xlsx'
    old.write_bytes(b'old immutable fixture')
    new.write_bytes(b'new immutable fixture')
    sidecar = retention.resolve_report_meta_path(old)
    sidecar.write_text('{"fixture": true}')
    return root, old, new, sidecar


@pytest.mark.parametrize('registered_sidecar', [False, True])
def test_registered_source_and_sidecar_survive_old_cleanup(tmp_path, registered_sidecar):
    root, old, new, sidecar = artifacts(tmp_path)
    db = root / 'backend/data/report_history.sqlite'
    store.put_views(root, db, sidecar if registered_sidecar else old, {'fixture': {'zero': 0, 'missing': None}})
    result = retention.cleanup_same_day_versions_for_file(new, refresh_manifest=False)
    assert old.exists() and new.exists() and sidecar.exists()
    assert result['kept_file'] == str(new)
    assert result['deleted_count'] == 0
    assert result['retained_files'] == [{'path': str(old), 'reason': 'history_source_requires_lifecycle'}]
    assert retention._delete_file_and_sidecar(old) == []


@pytest.mark.parametrize('failure', ['missing', 'corrupt'])
def test_unavailable_ledger_defers_cleanup(tmp_path, failure):
    root, old, new, sidecar = artifacts(tmp_path)
    if failure == 'corrupt':
        db = root / 'backend/data/report_history.sqlite'
        db.parent.mkdir(parents=True)
        db.write_bytes(b'corrupt database fixture')
    result = retention.cleanup_same_day_versions_for_file(new, refresh_manifest=False)
    assert result['retained_files'][0]['reason'] == 'history_dependency_check_unavailable'
    assert old.exists() and sidecar.exists()


def test_unregistered_old_version_still_uses_original_cleanup(tmp_path):
    root, old, new, sidecar = artifacts(tmp_path)
    with store.connection(root / 'backend/data/report_history.sqlite', write=True):
        pass
    result = retention.cleanup_same_day_versions_for_file(new, refresh_manifest=False)
    assert result['deleted_count'] == 2 and not old.exists() and not sidecar.exists()
    assert new.exists()
