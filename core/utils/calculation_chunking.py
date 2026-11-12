"""Content-defined byte chunks for exact, immutable calculation input sharing.

Only the storage representation changes. Chunk hashes, ordering and the full
original hash are verified before decoding any scientific value.
"""
from __future__ import annotations

import hashlib
from typing import Iterator

import numpy as np

CHUNK_FORMAT = "content_chunks_v1"
MIN_CHUNK_BYTES = 4 * 1024
TARGET_CHUNK_BYTES = 8 * 1024
MAX_CHUNK_BYTES = 64 * 1024
CHUNK_THRESHOLD_BYTES = 32 * 1024
_SCAN_BYTES = 256 * 1024
_MASK_BITS = 12
_MASK = np.uint64(((1 << _MASK_BITS) - 1) << (64 - _MASK_BITS))
_GEAR = np.array([
    int.from_bytes(hashlib.sha256(b"fund-ana-capture-chunks-v1:" + bytes([value])).digest()[:8], "little")
    for value in range(256)
], dtype=np.uint64)


def _anchor_ends(raw: bytes) -> Iterator[int]:
    # Select high bits of the 64-bit Gear recurrence. A longer byte history
    # avoids the sparse anchors observed with low bits on real NAV JSON.
    # Six prefix passes and 63 overlap bytes make anchors independent of the
    # bounded scan block size; scientific values are never transformed.
    for start in range(0, len(raw), _SCAN_BYTES):
        left = max(0, start - 63)
        right = min(len(raw), start + _SCAN_BYTES)
        hashes = _GEAR[np.frombuffer(raw, dtype=np.uint8, count=right - left, offset=left)]
        for step in (1, 2, 4, 8, 16, 32):
            hashes[step:] = hashes[step:] + (hashes[:-step] << np.uint64(step))
        positions = np.flatnonzero((hashes[start - left:] & _MASK) == 0)
        for position in positions:
            yield start + int(position) + 1


def iter_content_chunks(raw: bytes) -> Iterator[bytes]:
    """Preserve every byte; stable anchors survive insertions and deletions."""
    if not isinstance(raw, bytes):
        raise TypeError("capture_chunks_require_bytes")
    last = 0
    for end in _anchor_ends(raw):
        while end - last > MAX_CHUNK_BYTES:
            yield raw[last:last + MAX_CHUNK_BYTES]
            last += MAX_CHUNK_BYTES
        if end - last >= MIN_CHUNK_BYTES:
            yield raw[last:end]
            last = end
    while len(raw) - last > MAX_CHUNK_BYTES:
        yield raw[last:last + MAX_CHUNK_BYTES]
        last += MAX_CHUNK_BYTES
    if last < len(raw):
        yield raw[last:]
