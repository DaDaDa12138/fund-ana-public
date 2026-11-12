"""Lossless pick snapshots; legacy TEXT/NULL and validated compressed BLOBs.

The declared SQLite column remains TEXT. Archive projections decode it back to
the original text, so historical SQLite/Parquet schemas and values stay stable.
"""
from __future__ import annotations

import hashlib
import sqlite3
import struct
import zlib

MAGIC = b"FANAPK01"
HEADER_BYTES = 48
MAX_DECODED_BYTES = 16 * 1024 * 1024
SQL_DECODER = "fund_ana_pick_snapshot_text"


class PickPayloadError(ValueError):
    """A stored snapshot cannot be recovered exactly; never treat it as NULL."""


def decode_snapshot(value: str | bytes | None) -> str | None:
    if value is None or isinstance(value, str):
        return value
    if not isinstance(value, bytes):
        raise PickPayloadError("pick_snapshot_storage_type_invalid")
    if len(value) < HEADER_BYTES or value[:8] != MAGIC:
        raise PickPayloadError("pick_snapshot_header_invalid")
    size = struct.unpack(">Q", value[8:16])[0]
    if size > MAX_DECODED_BYTES:
        raise PickPayloadError("pick_snapshot_decoded_size_limit")
    try:
        decoder = zlib.decompressobj()
        raw = decoder.decompress(value[HEADER_BYTES:], size + 1)
        if (len(raw) != size or not decoder.eof or decoder.unused_data
                or decoder.unconsumed_tail):
            raise PickPayloadError("pick_snapshot_compressed_stream_invalid")
        if hashlib.sha256(raw).digest() != value[16:HEADER_BYTES]:
            raise PickPayloadError("pick_snapshot_sha256_mismatch")
        return raw.decode("utf-8")
    except (zlib.error, UnicodeDecodeError) as error:
        raise PickPayloadError("pick_snapshot_decode_failed") from error


def encode_snapshot(value: str | bytes | None) -> str | bytes | None:
    text = decode_snapshot(value)
    if value is None or isinstance(value, bytes):
        return value
    raw = text.encode("utf-8")
    # Preserve short and exceptionally large legacy text without an artificial
    # size restriction on business history. Only our compressed format is capped.
    if len(raw) <= HEADER_BYTES or len(raw) > MAX_DECODED_BYTES:
        return text
    packed = (MAGIC + struct.pack(">Q", len(raw)) + hashlib.sha256(raw).digest()
              + zlib.compress(raw, level=6))
    return packed if len(packed) < len(raw) else text


def register_sql_decoder(connection: sqlite3.Connection) -> None:
    connection.create_function(SQL_DECODER, 1, decode_snapshot, deterministic=True)
