"""Synthetic CSV verifies lossless storage; no fixture is production fund data."""
import gzip
import hashlib
from pathlib import Path

import pandas as pd
import pytest

from backend import audit_support


@pytest.mark.parametrize("shape", ["mixed", "headers_only", "no_columns"])
def test_compressed_csv_preserves_exact_export_bytes_and_values(tmp_path, shape):
    frame = pd.DataFrame({
        "基金代码": ["000001", "000002", "000003"],
        "说明,原文": ['合成数据，"双引号"', "换行\n第二行", ""],
        "真实零与缺失": [0., None, -1.25],
        "观察日期": [pd.Timestamp("2026-01-01"), pd.NaT, pd.Timestamp("2026-01-03")],
    })
    if shape == "headers_only":
        frame = frame.iloc[:0]
    elif shape == "no_columns":
        frame = pd.DataFrame()
    before = frame.copy(deep=True)
    expected = frame.to_csv(index=False).encode("utf-8-sig")
    path = tmp_path / "nested/synthetic.csv.gz"
    metadata = audit_support.write_compressed_audit_csv(path, frame)
    with gzip.open(path, "rb") as stream:
        actual = stream.read()
    assert actual == expected
    assert metadata["uncompressed_bytes"] == len(expected)
    assert metadata["uncompressed_sha256"] == hashlib.sha256(expected).hexdigest()
    assert metadata["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert metadata["rows"] == len(frame) and metadata["columns"] == len(frame.columns)
    assert metadata["bytes"] == path.stat().st_size
    assert metadata["format"] == "csv_gzip" and metadata["encoding"] == "utf-8-sig"
    assert sorted(path.parent.iterdir()) == [path]
    pd.testing.assert_frame_equal(frame, before)
    if shape == "mixed":
        restored = pd.read_csv(path, dtype={"基金代码": str})
        assert restored["基金代码"].tolist() == ["000001", "000002", "000003"]
        assert restored.loc[0, "真实零与缺失"] == 0.
        assert pd.isna(restored.loc[1, "真实零与缺失"])
        assert restored.loc[1, "说明,原文"] == "换行\n第二行"


def test_gzip_bytes_do_not_depend_on_the_output_filename_or_clock(tmp_path):
    frame = pd.DataFrame({"synthetic": ["same content"] * 1000})
    first = audit_support.write_compressed_audit_csv(tmp_path / "first.csv.gz", frame)
    second = audit_support.write_compressed_audit_csv(tmp_path / "second.csv.gz", frame)
    assert first == second
    assert first["bytes"] < first["uncompressed_bytes"]


def test_existing_evidence_is_never_overwritten(tmp_path):
    path = tmp_path / "existing.csv.gz"
    path.write_bytes(b"existing evidence, even if invalid")
    before = path.read_bytes()
    with pytest.raises(FileExistsError):
        audit_support.write_compressed_audit_csv(path, pd.DataFrame({"synthetic": [1]}))
    assert path.read_bytes() == before
    assert sorted(tmp_path.iterdir()) == [path]


def test_partial_serialization_failure_leaves_no_published_or_temporary_csv(tmp_path, monkeypatch):
    def interrupted(self, stream, **kwargs):
        stream.write("synthetic partial export")
        raise OSError("synthetic storage failure")
    monkeypatch.setattr(pd.DataFrame, "to_csv", interrupted)
    path = tmp_path / "failed.csv.gz"
    with pytest.raises(OSError, match="synthetic storage failure"):
        audit_support.write_compressed_audit_csv(path, pd.DataFrame({"synthetic": [1]}))
    assert list(tmp_path.iterdir()) == []


def test_truncated_gzip_is_rejected_before_it_becomes_an_artifact(tmp_path, monkeypatch):
    original = gzip.open
    def truncate_before_read(path, mode):
        path = Path(path)
        path.write_bytes(path.read_bytes()[:-8])
        return original(path, mode)
    monkeypatch.setattr(audit_support.gzip, "open", truncate_before_read)
    with pytest.raises(EOFError):
        audit_support.write_compressed_audit_csv(tmp_path / "truncated.csv.gz",
                                               pd.DataFrame({"synthetic": [1]}))
    assert list(tmp_path.iterdir()) == []


def test_plain_csv_suffix_does_not_hide_compressed_content(tmp_path):
    path = tmp_path / "plain.csv"
    with pytest.raises(ValueError, match="compressed_audit_csv_suffix_required"):
        audit_support.write_compressed_audit_csv(path, pd.DataFrame())
    assert not path.exists()
