"""Typed replay contracts; small synthetic fixtures are not business evidence."""
from __future__ import annotations

from dataclasses import replace
import gzip
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.config import load_settings
from backend import screening_replay_snapshot as snapshot


def test_frame_roundtrip_preserves_precision_nullable_values_and_index():
    frame = pd.DataFrame({
        "code": pd.Series(["000001", "000002", None], dtype="string"),
        "score": [63.02000000000001, 0.0, np.nan],
        "days": pd.Series([0, None, 91], dtype="Int64"),
        "eligible": pd.Series([True, False, None], dtype="boolean"),
        "date": pd.to_datetime(["2026-09-04", None, "2026-09-07"]),
        "ratio": [float("inf"), -float("inf"), 0.0],
    })
    frame.index = pd.Index([19, 3, 100], name="source_row", dtype="int64")
    wire = json.loads(json.dumps(snapshot.freeze_frame(frame), allow_nan=False))
    pd.testing.assert_frame_equal(snapshot.restore_frame(wire), frame, check_exact=True)
    assert wire["rows"][1][1] == 0.0
    assert wire["rows"][2][1] is None
    assert wire["rows"][1][2] is None


def test_empty_frame_keeps_nullable_schema_and_index():
    frame = pd.DataFrame({"code": pd.Series(dtype="string"), "score": pd.Series(dtype="Float64")})
    frame.index = pd.Index([], dtype="int64", name="original_row")
    pd.testing.assert_frame_equal(snapshot.restore_frame(snapshot.freeze_frame(frame)), frame)


def test_embedded_ledger_proof_keeps_its_own_types_paths_and_digest(tmp_path):
    from core.utils import report_history_store as store
    from backend.incremental_review_replay import relative_source_paths

    frame = pd.DataFrame({
        'code': pd.Series(['000001', None], dtype='string'),
        'number': [0.0, float('nan')],
        'nullable': pd.Series([0, None], dtype='Int64'),
        'date': pd.to_datetime(['2026-09-10', None]),
        'infinite': [float('inf'), -float('inf')],
    })
    payload = store.frame_payload(frame)
    source = str(tmp_path / 'reports/source.csv')
    payload['observed_sources'] = [{'path': source, 'status': 'read'}]
    proof = {'contract': store.CONTRACT, 'path': str(tmp_path / 'reports/source.xlsx'),
             'digest': store.digest(payload), 'payload': payload}
    wire = json.loads(json.dumps(relative_source_paths(snapshot._encode(proof), tmp_path), allow_nan=False))
    assert wire['path'] == 'reports/source.xlsx'
    assert wire['payload']['observed_sources'][0]['path'] == source
    store.verify_embedded(wire)
    restored = store.decode(wire)
    pd.testing.assert_frame_equal(store.payload_frame(restored['payload']), frame, check_exact=True)
    wire['payload']['observed_sources'][0]['path'] = 'reports/source.csv'
    with pytest.raises(store.HistoryError, match='history_frozen_record_invalid'):
        store.verify_embedded(wire)


@pytest.mark.parametrize("source_name", ["backend/bond_style_classifier.py", "backend/scale_semantics.py"])
def test_snapshot_binds_style_classifier_source_changes(tmp_path, source_name):
    for name in snapshot.CODE_FILES:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("isolated source fixture", encoding="utf-8")
    before = snapshot.source_code_hashes(tmp_path)
    (tmp_path / source_name).write_text("changed isolated source", encoding="utf-8")
    after = snapshot.source_code_hashes(tmp_path)
    assert before[source_name] != after[source_name]
    assert {key: value for key, value in before.items() if key != source_name} == {
        key: value for key, value in after.items() if key != source_name
    }


def test_duplicate_columns_fail_instead_of_silently_overwriting():
    with pytest.raises(ValueError, match="duplicate_columns"):
        snapshot.freeze_frame(pd.DataFrame([[1, 2]], columns=["score", "score"]))


@pytest.fixture
def written_snapshot(tmp_path, monkeypatch):
    settings = replace(load_settings(), project_root=tmp_path)
    revision = tmp_path / "reports/runs/test/revisions/one"
    revision.mkdir(parents=True)
    frame = pd.DataFrame({"基金代码": ["000001"], "score": [63.02000000000001]})
    filters = {key: frame.copy() for key in (
        "candidate_df", "dynamic_publish_official_df", "final_selection_tracker_features_df",
        "final_selection_contraction_df", "dynamic_official_df",
    )}
    filters.update(style_thresholds={"fixture": 63.02}, global_thresholds={}, style_meta={},
                   effective_nav_candidate_date="2026-09-04",
                   evaluation_times={"decision": "2026-09-07T18:00:01+08:00",
                                     "fixed_post_review": "2026-09-07T18:00:02+08:00",
                                     "dynamic_post_review": "2026-09-07T18:00:03+08:00"})
    summary = {"run_id": "test", "generated_at": "2026-09-07T18:00:00",
               "artifacts": {"report_json": "reports/runs/test/revisions/one/report.json"}}
    monkeypatch.setattr(snapshot, "source_code_hashes", lambda root: {"fixture.py": "fixture"})
    path = snapshot.write_screening_input_snapshot(
        settings, summary, universe=frame, filters=filters, materialized={"official_df": frame},
        report_inputs={"decision_map": {"000001": {"pass": True}}, "soft_warning_map": {}},
    )
    return path


def test_available_inputs_do_not_claim_baseline_reproduction(written_snapshot):
    payload = snapshot.load_screening_input_snapshot(written_snapshot, expected_run_id="test")
    assert payload["input_availability"] == "frozen"
    assert payload["baseline_reproduction_status"] == "not_verified"
    assert len(payload["inputs"]["frames"]) == 7
    assert snapshot.restore_frame(payload["inputs"]["frames"]["formal_after_all_filters"]).iloc[0]["score"] == 63.02000000000001
    assert payload["inputs"]["evaluation_times"]["decision"] == "2026-09-07T18:00:01+08:00"


def test_tampering_with_a_stage_clock_invalidates_the_snapshot(written_snapshot):
    payload = snapshot.load_screening_input_snapshot(written_snapshot)
    payload["inputs"]["evaluation_times"]["decision"] = "2030-01-01T00:00:00+08:00"
    with gzip.open(written_snapshot, "wt", encoding="utf-8") as stream:
        json.dump(payload, stream, allow_nan=False)
    with pytest.raises(ValueError, match="signature_mismatch"):
        snapshot.load_screening_input_snapshot(written_snapshot)


def test_corrupted_input_or_wrong_run_is_rejected(written_snapshot):
    with pytest.raises(ValueError, match="identity_mismatch"):
        snapshot.load_screening_input_snapshot(written_snapshot, expected_run_id="different")
    payload = snapshot.load_screening_input_snapshot(written_snapshot)
    payload["inputs"]["frames"]["formal_after_all_filters"]["rows"][0][1] = 0.0
    with gzip.open(written_snapshot, "wt", encoding="utf-8") as stream:
        json.dump(payload, stream, allow_nan=False)
    with pytest.raises(ValueError, match="signature_mismatch"):
        snapshot.load_screening_input_snapshot(written_snapshot)
