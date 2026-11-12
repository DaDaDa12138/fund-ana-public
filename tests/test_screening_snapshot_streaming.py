"""Snapshot serialization must retain exact historical signatures and values."""
import hashlib
import json

import pytest

from backend import screening_replay_snapshot as snapshot


@pytest.mark.parametrize('value', [
    {'missing': None, 'zero': 0, 'negative_zero': -0.0, 'code': '000001', 'precise': 1.2345678901234567},
    {'中文': ['基金', '\n\t"\\', 1e-30, 1e100, False, True], 'large': 2 ** 100},
    {'rows': [['000001', None, 0.0, '基金'] for _ in range(10_000)]},
])
@pytest.mark.parametrize('sort_keys', [False, True])
def test_stream_bytes_equal_previous_encoder(value, sort_keys):
    expected = json.dumps(value, ensure_ascii=False, sort_keys=sort_keys,
                          separators=(',', ':'), allow_nan=False).encode('utf-8')
    chunks = list(snapshot._json_chunks(value, sort_keys=sort_keys))
    assert b''.join(chunks) == expected
    if sort_keys:
        assert snapshot._json_sha256(value) == hashlib.sha256(expected).hexdigest()
    if 'rows' in value:
        assert len(chunks) > 1 and max(map(len, chunks)) < 128 * 1024


@pytest.mark.parametrize('invalid', [float('nan'), float('inf'), -float('inf')])
def test_stream_rejects_unencoded_nonfinite_numbers(invalid):
    with pytest.raises(ValueError):
        list(snapshot._json_chunks({'value': invalid}))
