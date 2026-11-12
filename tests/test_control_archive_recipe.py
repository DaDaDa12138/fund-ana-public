"""Archive storage checks use disposable text/binary files, never business data."""
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import tarfile
from types import SimpleNamespace

import pytest

from scripts import control_archive_recipe as storage


@pytest.fixture
def specimen(tmp_path):
    root = tmp_path / "project"
    revision = root / "reports/runs/example/revisions" / ("a" * 32)
    revision.mkdir(parents=True)
    file = revision / ("档案_" + "long_name_" * 15 + ".bin")
    file.write_bytes(bytes(range(256)) * 1024)
    os.chmod(file, 0o640)
    os.utime(file, ns=(1700000000123456789, 1700000000123456789))
    os.link(file, revision / "hardlink.bin")
    (revision / "empty.txt").touch()
    (revision / "alias").symlink_to(file.name)
    state = root / "backend/data/state.json"
    state.parent.mkdir(parents=True)
    state.write_text('{"note":"historical operator state"}\n')
    archive = tmp_path / "closed_control.tar.gz"
    with tarfile.open(archive, "w:gz", format=tarfile.PAX_FORMAT) as tar:
        tar.add(root / "reports", arcname="reports")
        tar.add(root / "backend", arcname="backend")
    os.chmod(archive, 0o640)
    os.utime(archive, ns=(1700000000999999999, 1700000000999999999))
    libraries = list(Path("/usr/lib").glob("*/libz.so.1")) + list(Path("/lib").glob("*/libz.so.1"))
    assert libraries
    return SimpleNamespace(root=root, revision=revision, live_file=file, state=state,
                           archive=archive, package=tmp_path / "package", library=libraries[0].resolve())


def prepare(specimen):
    return storage.prepare_archive(specimen.archive, specimen.package, project_root=specimen.root,
                                   zlib_library=specimen.library)


def members(path):
    with tarfile.open(path, "r:gz") as archive:
        result = []
        for item in archive:
            result.append((item.name, item.type, item.linkname, item.mode, item.uid, item.gid,
                           item.mtime, item.pax_headers,
                           hashlib.file_digest(archive.extractfile(item), "sha256").hexdigest() if item.isfile() else None))
        return result


def edit_recipe(specimen, change):
    path = specimen.package / "recipe.json"
    value = json.loads(path.read_bytes())
    change(value)
    path.write_text(json.dumps(value))


def test_exact_gzip_and_tar_members_survive_original_and_live_file_removal(specimen, tmp_path):
    expected = specimen.archive.read_bytes()
    original_members = members(specimen.archive)
    metadata = specimen.archive.stat()
    result = prepare(specimen)
    assert result["restore"]["status"] == "restored"
    assert result["raw_shared_object_count"] >= 1
    assert result["compressed_object_count"] == 1
    assert specimen.archive.read_bytes() == expected
    specimen.archive.unlink()
    shutil.rmtree(specimen.root)
    restored = tmp_path / "restored.tar.gz"
    result = storage.restore_archive(specimen.package, restored)
    assert result["archive_sha256"] == hashlib.sha256(expected).hexdigest()
    assert restored.read_bytes() == expected
    assert members(restored) == original_members
    actual = restored.stat()
    assert (stat.S_IMODE(actual.st_mode), actual.st_uid, actual.st_gid, actual.st_mtime_ns) == (
        stat.S_IMODE(metadata.st_mode), metadata.st_uid, metadata.st_gid, metadata.st_mtime_ns)


def test_mutable_control_state_is_copied_even_when_it_matches(specimen, tmp_path):
    expected = specimen.archive.read_bytes()
    prepare(specimen)
    specimen.state.write_text("later state")
    target = tmp_path / "restored.gz"
    storage.restore_archive(specimen.package, target)
    assert target.read_bytes() == expected


@pytest.mark.parametrize("which", ["framing", "raw_object", "compressed_object", "library"])
def test_corruption_is_rejected_without_leaving_a_partial_archive(specimen, tmp_path, which):
    prepare(specimen)
    paths = {"framing": specimen.package / "framing.gz", "library": specimen.package / "runtime/libz.so.1",
             "raw_object": next((specimen.package / "objects").glob("*.raw")),
             "compressed_object": next((specimen.package / "objects").glob("*.gz"))}
    path = paths[which]
    data = path.read_bytes()
    path.write_bytes(data[:len(data)//2] + bytes([data[len(data)//2] ^ 1]) + data[len(data)//2 + 1:])
    target = tmp_path / "must_not_remain.gz"
    with pytest.raises((ValueError, gzip.BadGzipFile)):
        storage.restore_archive(specimen.package, target)
    assert not target.exists()
    assert specimen.archive.is_file()


def test_existing_target_and_package_are_not_overwritten(specimen, tmp_path):
    prepare(specimen)
    before = storage.digest(specimen.package / "recipe.json")
    with pytest.raises(storage.ArchiveRecipeError, match="destination_exists"):
        prepare(specimen)
    assert storage.digest(specimen.package / "recipe.json") == before
    target = tmp_path / "existing.gz"
    target.write_bytes(b"keep me")
    with pytest.raises(storage.ArchiveRecipeError, match="target_exists"):
        storage.restore_archive(specimen.package, target)
    assert target.read_bytes() == b"keep me"


def test_expected_archive_identity_is_required_by_consumer(specimen, tmp_path):
    prepare(specimen)
    target = tmp_path / "wrong.gz"
    with pytest.raises(storage.ArchiveRecipeError, match="expected_sha_mismatch"):
        storage.restore_archive(specimen.package, target, expected_sha256="0" * 64)
    assert not target.exists()


def test_symlink_objects_are_rejected(specimen, tmp_path):
    prepare(specimen)
    object = next((specimen.package / "objects").glob("*.raw"))
    object.unlink()
    object.symlink_to(specimen.live_file)
    with pytest.raises(storage.ArchiveRecipeError, match="symlink_path"):
        storage.restore_archive(specimen.package, tmp_path / "wrong.gz")
    assert not (tmp_path / "wrong.gz").exists()


def test_unsupported_original_compressor_preserves_source_and_cleans_staging(specimen):
    raw = gzip.decompress(specimen.archive.read_bytes())
    specimen.archive.write_bytes(gzip.compress(raw, compresslevel=1, mtime=0))
    before = specimen.archive.read_bytes()
    with pytest.raises(storage.ArchiveRecipeError, match="reconstruction_mismatch"):
        prepare(specimen)
    assert specimen.archive.read_bytes() == before
    assert not specimen.package.exists()
    assert not list(specimen.package.parent.glob(".control-recipe-build-*"))


def test_low_space_does_not_start_a_package(specimen, monkeypatch):
    monkeypatch.setattr(storage.shutil, "disk_usage", lambda _: SimpleNamespace(free=0))
    with pytest.raises(storage.ArchiveRecipeError, match="insufficient_space"):
        prepare(specimen)
    assert specimen.archive.is_file() and not specimen.package.exists()


def test_pinned_library_worker_restores_without_inheriting_application_config(specimen, tmp_path, monkeypatch):
    prepare(specimen)
    monkeypatch.setattr(storage.zlib, "ZLIB_RUNTIME_VERSION", "force-worker-for-test")
    monkeypatch.setenv("FUND_ANA_CONTROL_RECIPE_REEXEC", "0")
    target = tmp_path / "from_worker.gz"
    result = storage.restore_archive(specimen.package, target)
    assert result["status"] == "restored"
    assert target.read_bytes() == specimen.archive.read_bytes()


@pytest.mark.parametrize("change,match", [
    (lambda d: d.update(schema_version=True), "format"),
    (lambda d: d["segments"][0].update(literal_offset=2), "framing_order"),
    (lambda d: d.update(archive_mtime_ns=-1), "metadata"),
    (lambda d: d["segments"].__setitem__(0, None), "segment_type"),
])
def test_invalid_recipe_cannot_produce_a_successful_archive(specimen, tmp_path, change, match):
    prepare(specimen)
    edit_recipe(specimen, change)
    target = tmp_path / "invalid.gz"
    with pytest.raises(storage.ArchiveRecipeError, match=match):
        storage.restore_archive(specimen.package, target)
    assert not target.exists()


def test_decompression_budget_and_trailing_streams_are_enforced():
    with pytest.raises(storage.ArchiveRecipeError, match="gzip_size"):
        storage._gunzip(gzip.compress(b"x" * 10000), 10)
    with pytest.raises(storage.ArchiveRecipeError, match="trailing_data"):
        storage._gunzip(gzip.compress(b"one") + gzip.compress(b"two"), 100)


def test_duplicate_manifest_keys_are_rejected(specimen, tmp_path):
    prepare(specimen)
    p = specimen.package / "recipe.json"
    p.write_text(p.read_text().replace('{', '{"format":"duplicate",', 1))
    with pytest.raises(storage.ArchiveRecipeError, match="duplicate_key"):
        storage.restore_archive(specimen.package, tmp_path / "duplicate.gz")


def test_interrupted_delivery_keeps_original_and_removes_owned_temporary_files(specimen, monkeypatch):
    before = specimen.archive.read_bytes()
    def interrupted(*args, **kwargs):
        raise OSError("injected rename interruption")
    monkeypatch.setattr(storage.os, "rename", interrupted)
    with pytest.raises(OSError, match="injected rename"):
        prepare(specimen)
    assert specimen.archive.read_bytes() == before
    assert not specimen.package.exists()
    assert not list(specimen.package.parent.glob(".control-recipe-build-*"))
