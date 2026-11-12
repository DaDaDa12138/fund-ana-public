"""Store a closed gzip/tar archive using original file bytes and literal framing.

The gzip header, tar framing and trailer are retained verbatim. Reconstruction
uses the producer's bundled zlib, then checks the ORIGINAL compressed SHA. Each
package is complete without the application, live files or a shared object pool.
Preparing a package never retires the source archive. The caller owns workflow
locking, purpose/dependency review and any subsequent removal of old aliases.
"""
from __future__ import annotations

import argparse
import base64
import ctypes
from datetime import datetime
import gzip
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import shutil
import stat
import struct
import subprocess
import sys
import tarfile
import tempfile
import zlib

FORMAT = "fund_ana_control_archive_recipe_v1"
MAX_ARCHIVE = 256 * 1024**2
MAX_TAR = 512 * 1024**2
MAX_MEMBER = 64 * 1024**2
MAX_FRAMING = 8 * 1024**2
MAX_RECIPE = 2 * 1024**2
MAX_MEMBERS = 4096
CHUNK = 1024**2
RESERVE = 256 * 1024**2
SHA = re.compile(r"[a-f0-9]{64}\Z")


class ArchiveRecipeError(ValueError):
    pass


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def identity(path: Path) -> tuple:
    s = path.lstat()
    return (s.st_dev, s.st_ino, s.st_mode, s.st_uid, s.st_gid,
            s.st_size, s.st_mtime_ns, s.st_ctime_ns, s.st_nlink)


def safe_path(path: Path) -> Path:
    path = Path(os.path.abspath(path))
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise ArchiveRecipeError("archive_recipe_symlink_path")
    return path


def read_regular(path: Path, maximum: int) -> bytes:
    safe_path(path)
    before = identity(path)
    if not stat.S_ISREG(before[2]) or not 0 <= before[5] <= maximum:
        raise ArchiveRecipeError("archive_recipe_type_or_size")
    with path.open("rb") as stream:
        raw = stream.read(maximum + 1)
    if len(raw) != before[5] or identity(path) != before:
        raise ArchiveRecipeError("archive_recipe_source_changed")
    return raw


def _json_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ArchiveRecipeError("archive_recipe_duplicate_key")
        result[key] = value
    return result


def _decode(value: str, maximum: int) -> bytes:
    if not isinstance(value, str) or len(value) > maximum * 2:
        raise ArchiveRecipeError("archive_recipe_literal_size")
    raw = base64.b64decode(value, validate=True)
    if len(raw) > maximum:
        raise ArchiveRecipeError("archive_recipe_literal_size")
    return raw


def _gunzip(raw: bytes, maximum: int) -> bytes:
    decoder = zlib.decompressobj(31)
    try:
        decoded = decoder.decompress(raw, maximum + 1)
    except zlib.error as error:
        raise ArchiveRecipeError("archive_recipe_gzip_corrupt") from error
    if len(decoded) > maximum or not decoder.eof or decoder.unused_data:
        raise ArchiveRecipeError("archive_recipe_gzip_size_or_trailing_data")
    return decoded


def _gzip_header(path: Path) -> tuple[bytes, bytes]:
    with path.open("rb") as stream:
        header = stream.read(10)
        if len(header) != 10 or header[:3] != b"\x1f\x8b\x08" or header[3] & 0xe0:
            raise ArchiveRecipeError("archive_recipe_gzip_header")
        if header[3] & 4:
            size = stream.read(2)
            header += size + stream.read(struct.unpack("<H", size)[0])
        for flag in (8, 16):
            if header[3] & flag:
                while True:
                    char = stream.read(1)
                    if not char or len(header) > 65536:
                        raise ArchiveRecipeError("archive_recipe_gzip_header")
                    header += char
                    if char == b"\0":
                        break
        if header[3] & 2:
            header += stream.read(2)
        stream.seek(-8, 2)
        return header, stream.read(8)


def _member_path(name: str, project_root: Path) -> Path:
    relative = PurePosixPath(name)
    if relative.is_absolute() or ".." in relative.parts or not relative.parts:
        raise ArchiveRecipeError("archive_recipe_member_path")
    return project_root / Path(relative)


def _write(path: Path, raw: bytes) -> None:
    with path.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def _sync(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _store_object(package: Path, raw: bytes, source: Path, *, may_share: bool) -> dict:
    sha = hashlib.sha256(raw).hexdigest()
    objects = package / "objects"
    for codec in ("raw", "gz"):
        existing = objects / f"{sha}.{codec}"
        if existing.exists():
            encoded = read_regular(existing, MAX_MEMBER + CHUNK)
            previous = encoded if codec == "raw" else _gunzip(encoded, MAX_MEMBER)
            if previous != raw:
                raise ArchiveRecipeError("archive_recipe_object_collision")
            return {"sha256": sha, "size": len(raw), "codec": codec}
    # Only byte-identical regular live files can supply a local hardlink. The
    # object remains present if that live name is later legitimately retired.
    if may_share and source.is_file() and not any(p.is_symlink() for p in (source, *source.parents)):
        before = identity(source)
        if before[5] == len(raw) and read_regular(source, MAX_MEMBER) == raw:
            if identity(source) != before:
                raise ArchiveRecipeError("archive_recipe_live_source_changed")
            target = objects / f"{sha}.raw"
            os.link(source, target, follow_symlinks=False)
            if read_regular(target, MAX_MEMBER) != raw:
                raise ArchiveRecipeError("archive_recipe_link_verification_failed")
            return {"sha256": sha, "size": len(raw), "codec": "raw"}
    encoded = gzip.compress(raw, compresslevel=9, mtime=0)
    _write(objects / f"{sha}.gz", encoded)
    return {"sha256": sha, "size": len(raw), "codec": "gz"}


def prepare_archive(source: Path, package: Path, *, project_root: Path,
                    zlib_library: Path) -> dict:
    """Prepare, completely restore and atomically deliver one local package."""
    source, package = safe_path(source), safe_path(package)
    project_root = safe_path(project_root)
    if package.exists():
        raise ArchiveRecipeError("archive_recipe_destination_exists")
    before = identity(source)
    if not stat.S_ISREG(before[2]) or not 0 < before[5] <= MAX_ARCHIVE:
        raise ArchiveRecipeError("archive_recipe_archive_size")
    if shutil.disk_usage(package.parent).free < MAX_TAR + MAX_ARCHIVE + RESERVE:
        raise ArchiveRecipeError("archive_recipe_insufficient_space")
    original_sha = digest(source)
    header, footer = _gzip_header(source)
    library = zlib_library.resolve(strict=True)
    library_raw = read_regular(library, 4 * CHUNK)
    lib = ctypes.CDLL(str(library))
    lib.zlibVersion.restype = ctypes.c_char_p
    if lib.zlibVersion().decode() != zlib.ZLIB_RUNTIME_VERSION:
        raise ArchiveRecipeError("archive_recipe_producer_zlib_mismatch")
    with tempfile.TemporaryDirectory(prefix=".control-recipe-build-", dir=package.parent) as temporary:
        scratch = Path(temporary)
        staged = scratch / "package"
        staged.mkdir()
        (staged / "objects").mkdir()
        (staged / "runtime").mkdir()
        _write(staged / "runtime/libz.so.1", library_raw)
        _write(staged / "restore_archive.py", Path(__file__).read_bytes())
        plain = scratch / "original.tar"
        tar_bytes = 0
        with gzip.open(source, "rb") as incoming, plain.open("xb") as outgoing:
            while block := incoming.read(CHUNK):
                tar_bytes += len(block)
                if tar_bytes > MAX_TAR:
                    raise ArchiveRecipeError("archive_recipe_tar_budget")
                outgoing.write(block)
        members = []
        with tarfile.open(plain, "r:") as archive:
            for member in archive:
                if len(members) >= MAX_MEMBERS or not (member.isfile() or member.isdir() or member.issym() or member.islnk()):
                    raise ArchiveRecipeError("archive_recipe_member_type_or_count")
                _member_path(member.name, project_root)
                if member.size > MAX_MEMBER or getattr(member, "sparse", None):
                    raise ArchiveRecipeError("archive_recipe_member_size_or_sparse")
                members.append(member)
        framing = bytearray()
        segments = []
        cursor = 0
        with plain.open("rb") as stream:
            for member in (m for m in members if m.isfile() and m.size):
                gap = member.offset_data - cursor
                if gap < 0 or len(framing) + gap > MAX_FRAMING:
                    raise ArchiveRecipeError("archive_recipe_framing_budget")
                literal = stream.read(gap)
                if len(literal) != gap:
                    raise ArchiveRecipeError("archive_recipe_tar_truncated")
                if gap:
                    segments.append({"literal_offset": len(framing), "size": gap})
                    framing.extend(literal)
                raw = stream.read(member.size)
                if len(raw) != member.size:
                    raise ArchiveRecipeError("archive_recipe_tar_truncated")
                parts = PurePosixPath(member.name).parts
                may_share = (len(parts) >= 6 and parts[:2] == ("reports", "runs")
                             and parts[3] == "revisions" and re.fullmatch(r"[a-f0-9]{32}", parts[4]) is not None)
                item = _store_object(staged, raw, _member_path(member.name, project_root), may_share=may_share)
                segments.append({**item, "member": member.name})
                cursor = member.offset_data + member.size
            remaining = stream.read(MAX_FRAMING + 1)
            if len(framing) + len(remaining) > MAX_FRAMING or cursor + len(remaining) != tar_bytes:
                raise ArchiveRecipeError("archive_recipe_framing_budget")
            if remaining:
                segments.append({"literal_offset": len(framing), "size": len(remaining)})
                framing.extend(remaining)
        framing_encoded = gzip.compress(bytes(framing), compresslevel=9, mtime=0)
        _write(staged / "framing.gz", framing_encoded)
        recipe = {"format": FORMAT, "schema_version": 1,
                  "created_at": datetime.now().astimezone().isoformat(),
                  "archive_name": source.name, "archive_sha256": original_sha,
                  "archive_bytes": before[5], "archive_mode": stat.S_IMODE(before[2]),
                  "archive_uid": before[3], "archive_gid": before[4], "archive_mtime_ns": before[6],
                  "gzip_header": base64.b64encode(header).decode(), "gzip_footer": base64.b64encode(footer).decode(),
                  "tar_sha256": digest(plain), "tar_bytes": tar_bytes,
                  "framing_bytes": len(framing), "framing_sha256": hashlib.sha256(framing).hexdigest(),
                  "framing_gzip_sha256": hashlib.sha256(framing_encoded).hexdigest(),
                  "member_count": len(members), "segments": segments,
                  "compression_level": 9, "zlib_version": zlib.ZLIB_RUNTIME_VERSION,
                  "platform": [platform.system(), platform.machine()],
                  "library_sha256": hashlib.sha256(library_raw).hexdigest()}
        _write(staged / "recipe.json", (json.dumps(recipe, ensure_ascii=False, separators=(",", ":")) + "\n").encode())
        restored = scratch / source.name
        verification = restore_archive(staged, restored, expected_sha256=original_sha)
        if digest(restored) != original_sha or identity(source) != before:
            raise ArchiveRecipeError("archive_recipe_source_changed_after_verification")
        for directory in (staged / "objects", staged / "runtime", staged):
            _sync(directory)
        os.rename(staged, package)
        _sync(package.parent)
    return {"status": "prepared", "source_sha256": original_sha, "package": str(package),
            "member_count": len(members), "archive_bytes": before[5], "tar_bytes": tar_bytes,
            "raw_shared_object_count": len(list((package / "objects").glob("*.raw"))),
            "compressed_object_count": len(list((package / "objects").glob("*.gz"))),
            "recipe_sha256": digest(package / "recipe.json"), "restore": verification,
            "original_retired": False}


def _load_package(package: Path) -> tuple[dict, bytes]:
    raw = read_regular(package / "recipe.json", MAX_RECIPE)
    data = json.loads(raw, object_pairs_hook=_json_pairs)
    if (not isinstance(data, dict) or data.get("format") != FORMAT
            or type(data.get("schema_version")) is not int or data["schema_version"] != 1):
        raise ArchiveRecipeError("archive_recipe_format")
    for key, maximum in (("archive_bytes", MAX_ARCHIVE), ("tar_bytes", MAX_TAR), ("framing_bytes", MAX_FRAMING)):
        if type(data.get(key)) is not int or not 0 < data[key] <= maximum:
            raise ArchiveRecipeError("archive_recipe_declared_size")
    segments = data.get("segments")
    if not isinstance(segments, list) or not 0 < len(segments) <= MAX_MEMBERS * 2 + 1:
        raise ArchiveRecipeError("archive_recipe_segments")
    if data.get("compression_level") != 9:
        raise ArchiveRecipeError("archive_recipe_compression_level")
    for key in ("archive_mode", "archive_uid", "archive_gid", "archive_mtime_ns", "member_count"):
        if type(data.get(key)) is not int or data[key] < 0:
            raise ArchiveRecipeError("archive_recipe_metadata")
    if data["archive_mode"] > 0o7777 or not 0 < data["member_count"] <= MAX_MEMBERS:
        raise ArchiveRecipeError("archive_recipe_metadata")
    for key in ("archive_sha256", "tar_sha256", "framing_sha256", "framing_gzip_sha256", "library_sha256"):
        if not isinstance(data.get(key), str) or not SHA.fullmatch(data[key]):
            raise ArchiveRecipeError("archive_recipe_sha_format")
    encoded = read_regular(package / "framing.gz", MAX_FRAMING + CHUNK)
    if hashlib.sha256(encoded).hexdigest() != data["framing_gzip_sha256"]:
        raise ArchiveRecipeError("archive_recipe_framing_corrupt")
    framing = _gunzip(encoded, MAX_FRAMING)
    if len(framing) != data["framing_bytes"] or hashlib.sha256(framing).hexdigest() != data["framing_sha256"]:
        raise ArchiveRecipeError("archive_recipe_framing_corrupt")
    return data, framing


def restore_archive(package: Path, target: Path, *, expected_sha256: str | None = None) -> dict:
    package, target = safe_path(package), safe_path(target)
    data, framing = _load_package(package)
    if expected_sha256 is not None and data["archive_sha256"] != expected_sha256:
        raise ArchiveRecipeError("archive_recipe_expected_sha_mismatch")
    if target.exists():
        raise ArchiveRecipeError("archive_recipe_target_exists")
    if shutil.disk_usage(target.parent).free < data["archive_bytes"] + RESERVE:
        raise ArchiveRecipeError("archive_recipe_insufficient_space")
    if data.get("platform") != [platform.system(), platform.machine()]:
        raise ArchiveRecipeError("archive_recipe_platform_mismatch")
    library = package / "runtime/libz.so.1"
    if hashlib.sha256(read_regular(library, 4 * CHUNK)).hexdigest() != data["library_sha256"]:
        raise ArchiveRecipeError("archive_recipe_library_corrupt")
    if zlib.ZLIB_RUNTIME_VERSION != data["zlib_version"]:
        if os.environ.get("FUND_ANA_CONTROL_RECIPE_REEXEC") == "1":
            raise ArchiveRecipeError("archive_recipe_pinned_zlib_unavailable")
        env = {key: value for key, value in os.environ.items() if key in {"PATH", "LANG", "LC_ALL", "TZ"}}
        env.update(LD_LIBRARY_PATH=str(library.parent), FUND_ANA_CONTROL_RECIPE_REEXEC="1")
        process = subprocess.run([sys.executable, "-I", str(Path(__file__).resolve()), "restore",
                                  "--package", str(package), "--target", str(target),
                                  "--expected-sha256", data["archive_sha256"]],
                                 env=env, capture_output=True, text=True, timeout=180)
        if process.returncode:
            raise ArchiveRecipeError("archive_recipe_worker_failed: " + process.stderr[-2000:])
        return json.loads(process.stdout)
    raw_hash, compressed_hash = hashlib.sha256(), hashlib.sha256()
    compressor = zlib.compressobj(9, zlib.DEFLATED, -zlib.MAX_WBITS)
    raw_size = 0
    created = False
    try:
        with target.open("xb") as output:
            created = True
            def emit(value):
                compressed_hash.update(value)
                output.write(value)
            emit(_decode(data["gzip_header"], 65536))
            literal_cursor = 0
            for item in data["segments"]:
                if not isinstance(item, dict):
                    raise ArchiveRecipeError("archive_recipe_segment_type")
                size = item.get("size")
                if type(size) is not int or not 0 < size <= MAX_MEMBER:
                    raise ArchiveRecipeError("archive_recipe_segment_size")
                if "literal_offset" in item:
                    if item["literal_offset"] != literal_cursor:
                        raise ArchiveRecipeError("archive_recipe_framing_order")
                    block = framing[literal_cursor:literal_cursor + size]
                    literal_cursor += size
                else:
                    sha, codec = item.get("sha256"), item.get("codec")
                    if not isinstance(sha, str) or not SHA.fullmatch(sha) or codec not in {"raw", "gz"}:
                        raise ArchiveRecipeError("archive_recipe_object_reference")
                    encoded = read_regular(package / "objects" / f"{sha}.{codec}", MAX_MEMBER + CHUNK)
                    block = encoded if codec == "raw" else _gunzip(encoded, size)
                    if hashlib.sha256(block).hexdigest() != sha:
                        raise ArchiveRecipeError("archive_recipe_object_corrupt")
                if len(block) != size:
                    raise ArchiveRecipeError("archive_recipe_segment_length")
                raw_size += len(block)
                if raw_size > data["tar_bytes"]:
                    raise ArchiveRecipeError("archive_recipe_tar_length")
                raw_hash.update(block)
                emit(compressor.compress(block))
            emit(compressor.flush())
            emit(_decode(data["gzip_footer"], 8))
            if (literal_cursor != len(framing) or raw_size != data["tar_bytes"]
                    or raw_hash.hexdigest() != data["tar_sha256"]
                    or output.tell() != data["archive_bytes"]
                    or compressed_hash.hexdigest() != data["archive_sha256"]):
                raise ArchiveRecipeError("archive_recipe_reconstruction_mismatch")
            output.flush()
            os.fsync(output.fileno())
        os.chmod(target, data["archive_mode"])
        os.chown(target, data["archive_uid"], data["archive_gid"])
        os.utime(target, ns=(data["archive_mtime_ns"], data["archive_mtime_ns"]))
        _sync(target.parent)
    except BaseException:
        if created:
            target.unlink(missing_ok=True)
        raise
    return {"status": "restored", "archive_sha256": compressed_hash.hexdigest(),
            "archive_bytes": data["archive_bytes"], "tar_sha256": raw_hash.hexdigest(),
            "member_count": data["member_count"], "zlib_version": zlib.ZLIB_RUNTIME_VERSION}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("restore",))
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--target", type=Path, required=True)
    parser.add_argument("--expected-sha256")
    args = parser.parse_args()
    print(json.dumps(restore_archive(args.package, args.target, expected_sha256=args.expected_sha256)))


if __name__ == "__main__":
    main()
