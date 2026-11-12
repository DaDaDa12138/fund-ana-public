"""Isolated mathematical/source fixtures; no production data or network access."""
from __future__ import annotations

from datetime import date
import json
import hashlib
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
from openpyxl import Workbook

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'core'))

from analyzers.risk_analyzer import RiskAnalyzer
from bond_fund_screening.core.nav_window import select_nav_period_window
from bond_fund_screening.core.risk_calculator import RiskCalculator
from bond_fund_screening.core.standard_risk_metrics import calculate_drawdown_stats
from bond_fund_screening.tasks.final_screening_quality_gate_helpers import (
    analyze_candidate_pool_drawdown_duration, build_drawdown_duration_summary,
)
from cli.aux_commands import _resolve_changed_nav_risk_candidates
from database.connection import DatabaseManager
from database.migrations import migrate_add_columns
from database.models import Base, Fund, FundReturns, FundRiskMetrics
from utils import build_nav_snapshot_signature
from core.utils.drawdown_duration import (
    ANNUAL_DRAWDOWN_OBSERVATIONS as FIELD, LEGACY_DRAWDOWN_COLUMNS,
    normalize_drawdown_duration_columns, normalize_drawdown_duration_record,
)
from backend.metric_units import metric_unit_metadata
from backend.publication_checks import _drawdown_source_mismatches
from backend.report_workbook import _add_sheet_with_style


@pytest.mark.parametrize('nav,expected', [
    ([1, 1.1, 1.2], 0), ([1, 1, 1], 0),
    ([1, 0.9, 1, 0.9, 1], 1),
    ([1, 0.9, 0.8, 1, 0.9, 0.8, 0.7], 3),
    ([1, 0.9, 0.9, 0.9], 3), ([1, 2, 1, 2, 1], 1),
    ([], None), ([1], None), ([1, float('nan'), 1], None),
    ([1, float('inf')], None), ([1, 0], None), ([1, -1], None),
])
def test_underwater_spells_count_observations_and_include_unrecovered(nav, expected):
    _, duration = calculate_drawdown_stats(np.asarray(nav))
    assert duration == expected


def test_sparse_calendar_gaps_do_not_invent_observations():
    frame = pd.DataFrame({'nav_date': pd.to_datetime(['2025-09-11', '2025-09-12', '2026-09-11']),
                          'unit_nav': [1, 0.9, 0.8], 'accum_nav': [1, 0.9, 0.8]})
    result = RiskCalculator(risk_free_rate=0.015).calculate_all_metrics(frame)
    assert result['max_drawdown_duration'] == 2


def _candidate_database(path: Path, *, legacy: bool = False) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute('CREATE TABLE funds(fcode TEXT, has_holding_period INTEGER, holding_days INTEGER)')
        conn.execute('CREATE TABLE fund_returns(fcode TEXT, latest_nav_date TEXT)')
        conn.execute('CREATE TABLE fund_nav_history(fcode TEXT, nav_date TEXT)')
        conn.execute('CREATE TABLE fund_risk_metrics(fcode TEXT, period TEXT, end_date TEXT, nav_snapshot_signature TEXT)')
        if not legacy:
            conn.execute('ALTER TABLE fund_risk_metrics ADD COLUMN max_drawdown_duration INTEGER')
            conn.execute('ALTER TABLE fund_risk_metrics ADD COLUMN undefined_metric_reasons_json TEXT')
        conn.execute("INSERT INTO funds VALUES ('990001', 0, 0)")
        conn.execute("INSERT INTO fund_returns VALUES ('990001', '2026-09-11')")
        conn.executemany('INSERT INTO fund_nav_history VALUES (?, ?)', [
            ('990001', value.date().isoformat()) for value in pd.date_range('2025-09-11', '2026-09-11', periods=40)
        ])
        conn.execute('INSERT INTO fund_risk_metrics(fcode,period,end_date,nav_snapshot_signature) VALUES (?,?,?,?)',
                     ('990001', '1y', '2026-09-11', build_nav_snapshot_signature({'latest_nav_date': '2026-09-11'})))


@pytest.mark.parametrize('duration,reasons,expected', [
    (None, None, ['990001']), (None, '{}', ['990001']),
    (0, None, []), (7, None, []),
    (None, '{"max_drawdown_duration":"invalid_observations"}', []),
    (None, '{"sharpe_ratio":"invalid_observations"}', ['990001']),
    (None, '{', ['990001']), (None, '[]', ['990001']),
])
def test_same_nav_missing_duration_is_recalculated_once(tmp_path, duration, reasons, expected):
    path = tmp_path / 'candidates.db'
    _candidate_database(path)
    with sqlite3.connect(path) as conn:
        conn.execute('UPDATE fund_risk_metrics SET max_drawdown_duration=?,undefined_metric_reasons_json=?', (duration, reasons))
    assert _resolve_changed_nav_risk_candidates(path) == expected
    if not expected:
        with sqlite3.connect(path) as conn:
            conn.execute("UPDATE fund_risk_metrics SET nav_snapshot_signature='old'")
        assert _resolve_changed_nav_risk_candidates(path) == ['990001']


def test_unmigrated_risk_schema_is_not_mistaken_for_complete(tmp_path):
    path = tmp_path / 'legacy.db'
    _candidate_database(path, legacy=True)
    assert _resolve_changed_nav_risk_candidates(path) == ['990001']


def test_duration_migration_is_nullable_and_idempotent(tmp_path):
    manager = DatabaseManager(db_path=str(tmp_path / 'migration.db'), wal_mode=False)
    Base.metadata.create_all(manager.engine)
    with sqlite3.connect(manager.db_path) as conn:
        conn.execute('ALTER TABLE fund_risk_metrics DROP COLUMN max_drawdown_duration')
        conn.execute("INSERT INTO fund_risk_metrics(fcode,period,max_drawdown) VALUES('990001','1y',0.012)")
    assert migrate_add_columns(manager.db_path)
    assert migrate_add_columns(manager.db_path)
    with sqlite3.connect(manager.db_path) as conn:
        assert conn.execute('SELECT max_drawdown,max_drawdown_duration FROM fund_risk_metrics').fetchone() == (0.012, None)


def test_annual_window_keeps_one_boundary_and_excludes_older_buffer_and_future():
    frame = pd.DataFrame({'nav_date': pd.to_datetime([
        '2026-09-14', '2025-08-21', '2026-09-11', '2025-09-12', '2025-09-15',
    ]), 'unit_nav': [5, 4, 3, 2, 1]})
    original = frame.copy(deep=True)
    window, meta = select_nav_period_window(frame, period_days=365, window_end_date=date(2026, 9, 13), boundary_lookback_days=30)
    assert window['nav_date'].dt.strftime('%Y-%m-%d').tolist() == ['2025-09-12', '2025-09-15', '2026-09-11']
    assert meta['requested_period_start'] == '2025-09-13'
    assert meta['start_boundary_point_date'] == '2025-09-12'
    assert meta['start_boundary_predecessor_used'] is True
    pd.testing.assert_frame_equal(frame, original)


def test_exact_boundary_is_kept_without_extra_predecessor():
    frame = pd.DataFrame({'nav_date': pd.to_datetime(['2025-08-20', '2025-09-11', '2026-09-11'])})
    window, meta = select_nav_period_window(frame, period_days=365, window_end_date='2026-09-11', boundary_lookback_days=30)
    assert len(window) == 2
    assert meta['start_boundary_point_date'] == '2025-09-11'
    assert meta['start_boundary_predecessor_used'] is False


@pytest.mark.parametrize('invalid', [False, True])
def test_auxiliary_annual_duration_uses_trimmed_window_and_missing_semantics(invalid):
    dates = ['2025-08-20', '2025-09-10', '2025-10-20', '2026-09-11']
    nav = pd.DataFrame({'nav_date': pd.to_datetime(dates), 'unit_nav': [10, 1, 0.9, 1], 'accum_nav': [10, 1, 0.9, 1]})
    if invalid:
        nav.loc[1, 'nav_date'] = pd.NaT
    queried = []
    loader = SimpleNamespace(get_batch_nav_history=lambda codes, days: (queried.append(days) or {'990001': nav}), close=lambda: None)
    result, summary = analyze_candidate_pool_drawdown_duration(
        pd.DataFrame([{'fcode': '990001', 'weighted_score': 1}]), analysis_window_days=365,
        build_drawdown_duration_summary_fn=build_drawdown_duration_summary,
        normalize_fcode_fn=str, data_loader_factory=lambda _: loader, db_path_getter=lambda: 'isolated',
        risk_calculator_factory=lambda: RiskCalculator(risk_free_rate=0.015), print_fn=lambda *args: None,
    )
    assert queried == [395]
    assert summary['duration_unit'] == 'nav_observations'
    if invalid:
        assert pd.isna(result.iloc[0][FIELD]) and summary['missing_count'] == 1
    else:
        assert calculate_drawdown_stats(nav['unit_nav'].to_numpy())[1] == 3
        assert result.iloc[0][FIELD] == 1
        assert summary['high_duration_samples'][0]['max_drawdown_duration_observations'] == 1


@pytest.mark.parametrize('value', [None, 0, 18])
def test_alias_reads_preserve_canonical_missing_and_zero(value):
    old = '1年-最大回撤持续期(天)'
    record = {old: 67, FIELD: value}
    assert normalize_drawdown_duration_record(record) == {FIELD: value}
    frame = normalize_drawdown_duration_columns(pd.DataFrame([record]))
    assert list(frame.columns) == [FIELD]
    assert pd.isna(frame.iloc[0][FIELD]) if value is None else frame.iloc[0][FIELD] == value
    assert record[old] == 67
    assert normalize_drawdown_duration_record({old: 67}) == {FIELD: 67}


@pytest.mark.parametrize('value', [None, 0, 18])
def test_report_uses_current_risk_duration_and_never_legacy_pick(tmp_path, monkeypatch, value):
    from backend import report_builder as builder

    manager = DatabaseManager(db_path=str(tmp_path / 'report.db'), wal_mode=False)
    Base.metadata.create_all(manager.engine)
    with manager.get_session() as session:
        session.add(Fund(fcode='990001', shortname='隔离来源测试债券', fullname='隔离来源测试债券A', ftype='债券型', scale_billion=2.0))
        session.add(FundReturns(fcode='990001', latest_nav_date=date(2026, 9, 11)))
        session.add(FundRiskMetrics(fcode='990001', period='1y', end_date=date(2026, 9, 11),
                                   max_drawdown_duration=value, volatility=0.02, max_drawdown=0.01,
                                   annual_return=3.0, sharpe_ratio=1.0, sortino_ratio=1.0, calmar_ratio=1.0,
                                   var_95=-0.01, var_99=-0.02, positive_ratio=60.0))
    monkeypatch.setattr(builder, 'load_effective_nav_snapshot', lambda _: SimpleNamespace(
        risk_1y_effective_end_date='2026-09-11', risk_1y_latest_end_date='2026-09-11'))
    risk = builder._read_risk_snapshot(Path(manager.db_path))
    assert 'max_drawdown_duration' in risk and 'nav_snapshot_signature' in risk
    frame = builder._build_project_owned_candidate_universe(
        metadata_df=builder._read_metadata(Path(manager.db_path)), manager_metrics_df=pd.DataFrame(),
        returns_df=builder._read_return_snapshot(Path(manager.db_path)), risk_df=risk,
        long_term_feature_df=pd.DataFrame(), accum_nav_df=pd.DataFrame(),
        pick_metrics_df=pd.DataFrame([{'fcode': '990001', 'max_drawdown_duration': 67}]),
        support_df=pd.DataFrame(), decision_map={},
    )
    assert len(frame) == 1
    assert pd.isna(frame.iloc[0][FIELD]) if value is None else frame.iloc[0][FIELD] == value


def test_excel_duration_is_numeric_with_units_and_preserves_missing():
    wb = Workbook()
    _add_sheet_with_style(wb, title='风险', df=pd.DataFrame({FIELD: [0, 18, None]}))
    ws = wb['风险']
    assert ws['A2'].value == 0 and ws['A2'].number_format == '0'
    assert ws['A3'].value == 18 and ws['A3'].number_format == '0'
    assert ws['A4'].value in ('', None)
    assert '不是自然天数' in ws['A1'].comment.text
    metadata = metric_unit_metadata()
    for field in [FIELD, *LEGACY_DRAWDOWN_COLUMNS]:
        assert metadata[field]['raw_unit'] == metadata[field]['display_unit'] == 'nav_observations'
        assert metadata[field]['display_digits'] == 0


def test_legacy_workbook_and_api_readers_do_not_rewrite_history(tmp_path):
    from backend.report_builder import _read_tabular_file
    from backend.project_tracker import _read_project_tracker_export_sheet, _load_tracker_source_frame
    from backend.api_payloads.recommendations import _normalize_sidecar_row

    path = tmp_path / 'legacy.xlsx'
    old = '1年-最大回撤持续期(天)'
    original = pd.DataFrame([{'基金代码': '990001', old: 0, '最新净值日期': '2026-04-30'}])
    with pd.ExcelWriter(path) as writer:
        original.to_excel(writer, sheet_name='Sheet1', index=False)
        original.to_excel(writer, sheet_name='当前在榜', index=False, startrow=2)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    frames = [_read_tabular_file(path), _read_project_tracker_export_sheet(path, '当前在榜'), _load_tracker_source_frame(path)[0]]
    for frame in frames:
        assert old not in frame and frame.iloc[0][FIELD] == '0'
    assert _normalize_sidecar_row({old: 67, FIELD: None})[FIELD] == ''
    assert hashlib.sha256(path.read_bytes()).hexdigest() == digest


@pytest.mark.parametrize('value', [None, 0, 18])
def test_tracker_detail_exposes_duration_without_replacing_canonical_null_or_zero(value):
    from backend.api_payloads.recommendations import build_tracker_current_response
    from backend.app import _normalize_number_text

    row = {'基金代码': '990001', FIELD: value, '1年-最大回撤持续期(天)': 67}
    result = build_tracker_current_response({},
        load_tracker_dataset_fn=lambda _: {
            'tracker_records': [{'基金代码': '990001', '当前是否在榜': '是', '最新1年-最长回撤观测数': 91}],
            'group_records_by_code': {'990001': row},
        },
        normalize_portfolio_groups=lambda x: x,
        normalize_number_text=_normalize_number_text,
        format_tracker_occurrence_text=lambda *_: '',
        apply_since_inception_annualized_return_to_funds_fn=lambda rows: rows,
    )
    assert result['rows'][0][FIELD] == ('' if value is None else str(value))
    assert result['metric_units'][FIELD]['raw_unit'] == 'nav_observations'
    assert result['rows'] == result['funds']
    assert row['1年-最大回撤持续期(天)'] == 67


@pytest.mark.parametrize('row,expected', [
    ({FIELD: None, '最新1年-最长回撤观测数': 67}, None),
    ({FIELD: 0, '最新1年-最长回撤观测数': 67}, 0),
    ({'1年-最大回撤持续期(天)': 18}, 18),
    ({'最新1年-最大回撤持续期(天)': 18}, 18),
])
def test_tracker_compact_legacy_aliases_preserve_explicit_missing(row, expected):
    from backend import app as backend_app

    payload = {'project_tracker_current_preview': [{'基金代码': '990001', **row}]}
    with backend_app.app.test_request_context('/api/tracker/current'):
        response = backend_app._build_tracker_current_compact_response(payload)
    assert response['rows'][0][FIELD] == expected
    assert response['metric_units'][FIELD]['raw_unit'] == 'nav_observations'
    assert payload['project_tracker_current_preview'][0] == {'基金代码': '990001', **row}


@pytest.mark.parametrize('duration', [None, 0, 29])
def test_tracker_compact_reads_omitted_duration_from_same_published_sidecar(tmp_path, monkeypatch, duration):
    from dataclasses import replace
    from backend import app as backend_app

    monkeypatch.setattr(backend_app, 'settings', replace(backend_app.settings, project_root=tmp_path))
    path = tmp_path / 'reports/runs/fixture-run/revisions/fixture-revision/current.json'
    path.parent.mkdir(parents=True)
    detail = {'artifact_type': 'project_tracker_current_detail_sidecar', 'run_id': 'fixture-run',
              'rows': [{'基金代码': '990001', FIELD: duration},
                       {'基金代码': '990002', FIELD: 99}, {'基金代码': '990003', FIELD: 99}]}
    path.write_text(json.dumps(detail, ensure_ascii=False))
    original = path.read_bytes()
    payload = {'run_id': 'fixture-run', 'publication_control': {'revision': 'fixture-revision'},
               'artifacts': {'project_tracker_current_detail_json': str(path.relative_to(tmp_path))},
               'project_tracker_current_preview': [{'基金代码': '990001'},
                   {'基金代码': '990002', FIELD: None}, {'基金代码': '990003', FIELD: 0}]}
    before = json.dumps(payload, sort_keys=True)
    monkeypatch.setattr(backend_app, '_load_tracker_dataset', lambda *_: pytest.fail('compact must not load Excel or full derived metrics'))
    with backend_app.app.test_request_context('/api/tracker/current'):
        response = backend_app._build_tracker_current_compact_response(payload)
    assert [row[FIELD] for row in response['rows']] == [duration, None, 0]
    assert response['metric_units'][FIELD]['raw_unit'] == 'nav_observations'
    assert response['row_count'] == 3 and response['pagination']['returned_count'] == 3
    assert json.dumps(payload, sort_keys=True) == before and path.read_bytes() == original


@pytest.mark.parametrize('failure', ['different_run', 'different_revision', 'duplicate_code', 'missing_file'])
def test_tracker_compact_never_substitutes_unbound_duration(tmp_path, monkeypatch, failure):
    from dataclasses import replace
    from backend import app as backend_app

    monkeypatch.setattr(backend_app, 'settings', replace(backend_app.settings, project_root=tmp_path))
    path = tmp_path / 'reports/runs/fixture-run/revisions/fixture-revision/current.json'
    path.parent.mkdir(parents=True)
    rows = [{'基金代码': '990001', FIELD: 29}]
    if failure == 'duplicate_code':
        rows.append({'基金代码': '990001', FIELD: 99})
    detail = {'artifact_type': 'project_tracker_current_detail_sidecar',
              'run_id': 'another-run' if failure == 'different_run' else 'fixture-run', 'rows': rows}
    if failure != 'missing_file':
        path.write_text(json.dumps(detail, ensure_ascii=False))
    payload = {'run_id': 'fixture-run',
               'publication_control': {'revision': 'another-revision' if failure == 'different_revision' else 'fixture-revision'},
               'artifacts': {'project_tracker_current_detail_json': str(path.relative_to(tmp_path))},
               'project_tracker_current_preview': [{'基金代码': '990001'}]}
    with backend_app.app.test_request_context('/api/tracker/current'):
        response = backend_app._build_tracker_current_compact_response(payload)
    assert response['rows'][0][FIELD] is None


@pytest.mark.parametrize('duration_field', [FIELD, '1年-最大回撤持续期(天)', None])
def test_tracker_workbook_fallback_reads_optional_duration_without_rewriting(tmp_path, duration_field):
    from backend.api_payloads.recommendations import load_tracker_dataset

    row = dict.fromkeys([
        '基金代码', '基金公司', '基金规模(亿)', '近1周(%)', '近1月(%)', '近3月(%)',
        '1年-波动率', '1年-最大回撤(%)', '1年-年化收益(%)', '1年-夏普比率', '最新净值日期', '数据来源',
    ], '')
    row['基金代码'] = '990001'
    if duration_field:
        row[duration_field] = 0
    path = tmp_path / 'filtered.xlsx'
    with pd.ExcelWriter(path) as writer:
        pd.DataFrame([row]).to_excel(writer, sheet_name='纯债稳健榜', index=False)
        pd.DataFrame(columns=row.keys()).to_excel(writer, sheet_name='增强债弹性榜', index=False)
        pd.DataFrame([{'基金代码': '990001'}]).to_excel(writer, sheet_name='推荐理由与风格', index=False)
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    dataset = load_tracker_dataset({'artifacts': {'filtered_excel': path.name}},
        settings=SimpleNamespace(project_root=tmp_path),
        read_excel_cached=lambda file, **kwargs: pd.read_excel(file, dtype=str, keep_default_na=False, **kwargs),
        read_tracker_workbook_sheet=lambda *_args, **_kwargs: pytest.fail('no tracker workbook was supplied'),
    )
    record = dataset['group_records_by_code']['990001']
    assert record.get(FIELD) == ('0' if duration_field else None)
    assert '1年-最大回撤持续期(天)' not in record
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before


@pytest.mark.parametrize('value', [None, 0, 18])
def test_source_check_distinguishes_missing_zero_and_current_observations(value):
    row = {'基金代码': '990001', '最新净值日期': '2026-09-11', FIELD: value}
    risk = {'fcode': '990001', 'period': '1y', 'max_drawdown_duration': value, 'data_points': 250,
            'start_date': '2025-09-11', 'end_date': '2026-09-11',
            'nav_snapshot_signature': build_nav_snapshot_signature({'latest_nav_date': '2026-09-11'}),
            'calculated_at': '2026-09-13T07:00:00'}
    assert _drawdown_source_mismatches({'990001': row}, [risk]) == ([], int(value is None))
    wrong = {**row, FIELD: 67}
    assert _drawdown_source_mismatches({'990001': wrong}, [risk])[0]
    assert _drawdown_source_mismatches({'990001': row}, [{**risk, 'end_date': '2026-04-30'}])[0]


@pytest.mark.parametrize('signature', [None, '', 'f' * 23, 'f' * 25, 'f' * 64, 'g' * 24, 'F' * 24])
def test_source_check_rejects_invalid_snapshot_signature_without_confusing_file_sha(signature):
    row = {'基金代码': '990001', '最新净值日期': '2026-09-11', FIELD: '18.0'}
    risk = {'fcode': '990001', 'period': '1y', 'max_drawdown_duration': '18', 'data_points': '250',
            'start_date': '2025-09-11', 'end_date': '2026-09-11', 'nav_snapshot_signature': signature,
            'calculated_at': '2026-09-13 07:40:13.342630'}
    mismatches, missing = _drawdown_source_mismatches({'990001': row}, [risk])
    assert missing == 0
    assert mismatches == [{'fcode': '990001', 'reason': 'risk_source_binding_missing_or_invalid'}]
    valid = {**risk, 'nav_snapshot_signature': build_nav_snapshot_signature({'latest_nav_date': '2026-09-11'})}
    assert _drawdown_source_mismatches({'990001': row}, [valid]) == ([], 0)
