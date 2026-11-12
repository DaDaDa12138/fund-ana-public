"""Isolated observation/provenance fixtures; no production data or API calls."""
from __future__ import annotations

from copy import deepcopy
from io import BytesIO
import json
from types import SimpleNamespace

import pandas as pd
import pytest

from backend import report_builder
from backend.api_payloads.recommendations import overlay_portfolio_occurrence_metrics, recommendation_export_rows
from backend.recommendations_sidecar import (
    build_recommendations_detail_response,
    overlay_recommendations_tracker_occurrences,
)
from backend.tracker_occurrence import tracker_occurrence_fields, tracker_occurrence_reason
from scripts.audit_data_truthfulness import _compare_tracker_occurrence_rows, _compare_recommendation_reason_briefs


BUILDERS = [report_builder._build_priority_pool_tracker_features,
            report_builder._build_final_selection_tracker_features]


def _history(tmp_path, run_id, date, codes, generated_at):
    path = tmp_path / (run_id + '.xlsx')
    frame = pd.DataFrame([{'基金代码': code, '最新净值日期': date} for code in codes])
    frame.to_excel(path, index=False)
    return {'run_id': run_id, 'generated_at': generated_at, 'business_date': generated_at[:10],
            'excel_path': path, 'nav_snapshot_date': generated_at[:10], 'final_count': len(frame)}


def _features(builder, tmp_path, current):
    return builder(SimpleNamespace(runs_dir=tmp_path), current_run_id='current',
                   current_generated_at='2026-05-21T11:00:00',
                   current_nav_snapshot_date='2026-05-21', current_df=current).set_index('基金代码')


@pytest.mark.parametrize('builder', BUILDERS)
def test_actual_nav_identity_replaces_old_membership_despite_registry_date(builder, tmp_path, monkeypatch):
    first = _history(tmp_path, 'first', '2026-05-18', ['000001', '000002'], '2026-05-19T10:00:00')
    revised = _history(tmp_path, 'revised', '2026-05-18', ['000002'], '2026-05-20T10:00:00')
    same_day = _history(tmp_path, 'same_day', '2026-05-20', ['000003'], '2026-05-21T10:00:00')
    # Deliberately unordered input; generated time controls the latest membership.
    monkeypatch.setattr(report_builder, '_load_project_report_history', lambda *a, **k: [revised, same_day, first])
    current = pd.DataFrame([{'基金代码': code, '最新净值日期': '2026-05-20'} for code in ('000001', '000002')])
    result = _features(builder, tmp_path, current)
    assert set(result.index) == {'000001', '000002'}
    assert set(result['跟踪总期数']) == {2}
    assert result.loc['000001', '出现次数'] == 1
    assert result.loc['000001', '出现频率(%)'] == 50
    assert result.loc['000002', '出现次数'] == 2
    assert result.loc['000002', '出现频率(%)'] == 100


@pytest.mark.parametrize('builder', BUILDERS)
def test_missing_nav_uses_content_signature_instead_of_inventing_run_observations(builder, tmp_path, monkeypatch):
    history = [_history(tmp_path, 'first', '', ['000001'], '2026-05-19T10:00:00'),
               _history(tmp_path, 'repeated', '', ['000001'], '2026-05-20T10:00:00')]
    monkeypatch.setattr(report_builder, '_load_project_report_history', lambda *a, **k: history)
    current = pd.DataFrame([{'基金代码': '000001', '最新净值日期': None}])
    result = _features(builder, tmp_path, current)
    assert result.loc['000001', '跟踪总期数'] == 1
    assert result.loc['000001', '出现次数'] == 1


@pytest.mark.parametrize('builder', BUILDERS)
def test_distinct_nav_dates_remain_distinct_even_when_members_do_not_change(builder, tmp_path, monkeypatch):
    history = [_history(tmp_path, 'first', '2026-05-18', ['000001'], '2026-05-19T10:00:00'),
               _history(tmp_path, 'second', '2026-05-19', ['000001'], '2026-05-20T10:00:00')]
    monkeypatch.setattr(report_builder, '_load_project_report_history', lambda *a, **k: history)
    result = _features(builder, tmp_path, pd.DataFrame([{'基金代码': '000001', '最新净值日期': '2026-05-20'}]))
    assert result.loc['000001', '跟踪总期数'] == result.loc['000001', '出现次数'] == 3


@pytest.mark.parametrize('builder', BUILDERS)
def test_unreadable_history_stops_features_instead_of_fabricating_full_frequency(builder, tmp_path, monkeypatch):
    history = _history(tmp_path, 'first', '2026-05-18', ['000001'], '2026-05-19T10:00:00')
    history['excel_path'].unlink()
    monkeypatch.setattr(report_builder, '_load_project_report_history', lambda *a, **k: [history])
    with pytest.raises(ValueError, match='tracker_observation_source_unavailable'):
        _features(builder, tmp_path, pd.DataFrame([{'基金代码': '000001', '最新净值日期': '2026-05-20'}]))


@pytest.fixture
def publication(tmp_path):
    settings = SimpleNamespace(project_root=tmp_path, runs_dir=tmp_path / 'reports/runs')
    run_id, revision = 'fixture_run', 'a' * 32
    directory = settings.runs_dir / run_id / 'revisions' / revision
    directory.mkdir(parents=True)
    path = directory / 'project_tracker_current_detail.json'
    path.write_text(json.dumps({'run_id': run_id, 'rows': [
        {'基金代码': '000001', '出现次数': '2', '出现频率(%)': '66.67'},
        {'基金代码': '000002', '出现次数': 0, '出现频率(%)': 0},
        {'基金代码': '000003', '出现次数': None, '出现频率(%)': None},
    ]}))
    stale = [{'基金代码': code, '出现次数': 4, '出现频率(%)': '100',
              '入选原因': '同分组同风格相对排名靠前；累计入围 4/4 期；持有期已确认<=90天',
              '投资优先池得分': '76.5', '建议权重': '25%', '1年-夏普比率': None}
             for code in ('000001', '000002', '000003', '000004')]
    report = {'run_id': run_id, 'publication_control': {'commit_protocol': 1, 'revision': revision},
              'artifacts': {'project_tracker_current_detail_json': str(path.relative_to(tmp_path))},
              'project_publish_summary': {'project_tracker_summary': {'total_periods': 3},
                                          'portfolio_suggestions': {'groups': [{'name': 'fixture', 'funds': deepcopy(stale)}]}}}
    response = {'run_id': run_id, 'rows': deepcopy(stale), 'funds': deepcopy(stale),
                'portfolio_group_brief': {'fixture': {'reason_counts': {'累计入围 4/4 期': 4}}},
                'portfolio_suggestions': {'funds': deepcopy(stale), 'groups': [{'name': 'fixture', 'funds': deepcopy(stale)}]}}
    return settings, report, response, path


def _assert_lanes(response):
    expected = [(2, '66.67'), (0, '0'), (None, None), (None, None)]
    for rows in (response['rows'], response['funds'], response['portfolio_suggestions']['funds'],
                 response['portfolio_suggestions']['groups'][0]['funds']):
        assert [(row['出现次数'], row['出现频率(%)']) for row in rows] == expected
        assert rows[1]['出现次数&出现频率(%)'] == '0 次 / 0%'
        assert rows[2]['出现次数&出现频率(%)'] == '- 次 / -'
        assert [row['入选原因'] for row in rows] == [
            f'同分组同风格相对排名靠前；{claim}；持有期已确认<=90天'
            for claim in ('累计入围 2/3 期', '累计入围 0/3 期', '累计入围次数暂无可用数据', '累计入围次数暂无可用数据')
        ]
        assert all(row['投资优先池得分'] == '76.5' and row['建议权重'] == '25%' for row in rows)
        assert all(row['1年-夏普比率'] in (None, '') for row in rows)


def test_new_and_cached_payloads_use_tracker_without_mutating_historical_artifacts(publication):
    settings, report, response, path = publication
    before, original = path.read_bytes(), deepcopy(response)
    corrected = overlay_recommendations_tracker_occurrences(settings, report, response)
    _assert_lanes(corrected)
    _assert_lanes(build_recommendations_detail_response(settings, report))
    assert response == original
    assert before == path.read_bytes()
    assert corrected['tracker_occurrence_basis']['missing_codes'] == ['000003', '000004']


@pytest.mark.parametrize('change', ['wrong_run', 'other_revision', 'missing_file'])
def test_source_from_another_run_or_revision_cannot_supply_occurrences(publication, change):
    settings, report, response, path = publication
    if change == 'wrong_run':
        data = json.loads(path.read_text()); data['run_id'] = 'other'
        path.write_text(json.dumps(data))
    elif change == 'other_revision':
        report['publication_control']['revision'] = 'b' * 32
    else:
        path.unlink()
    result = overlay_recommendations_tracker_occurrences(settings, report, response)
    assert all(row['出现次数'] is None and row['出现频率(%)'] is None for row in result['funds'])
    assert result['tracker_occurrence_basis']['available'] is False


def test_application_cache_hit_is_corrected_before_group_filtering(publication, monkeypatch):
    from backend import app as backend_app
    settings, report, response, path = publication
    before = path.read_bytes()
    monkeypatch.setattr(backend_app, 'settings', settings)
    monkeypatch.setattr(backend_app, '_recommendations_detail_sidecar_enabled', lambda: True)
    monkeypatch.setattr(backend_app, '_build_recommendations_detail_cache_basis', lambda p: {'schema_version': 2})
    monkeypatch.setattr(backend_app, '_probe_recommendations_detail_sidecar_status', lambda *a: {'status': 'hit', 'hit': True})
    monkeypatch.setattr(backend_app, '_load_recommendations_detail_sidecar', lambda *a: deepcopy(response))
    def forbidden(*args, **kwargs):
        raise AssertionError('cache hit must not rebuild or write a committed artifact')
    monkeypatch.setattr(backend_app, 'build_run_local_recommendations_detail_response', forbidden)
    monkeypatch.setattr(backend_app, '_write_recommendations_detail_sidecar', forbidden)
    monkeypatch.setattr(backend_app, '_attach_recommendations_publication_status', lambda p, r: r)
    monkeypatch.setattr(backend_app, '_attach_recommendations_detail_publication_source_contract', lambda r: r)
    result = backend_app._build_recommendations_detail_response(report, 'fixture', audit_read_only=True)
    _assert_lanes(result)
    assert result['payload_scope']['recommendations_detail_diagnostics']['sidecar_hit'] is True
    assert path.read_bytes() == before


@pytest.mark.parametrize('value', [None, '', 'nan', float('inf'), -1, True])
def test_missing_or_invalid_occurrence_never_becomes_zero(value):
    row = tracker_occurrence_fields({'出现次数': value, '出现频率(%)': value})
    assert row['出现次数'] is None and row['出现频率(%)'] is None


def test_excel_and_legacy_overlay_clear_missing_instead_of_reusing_wrong_report_values(publication):
    _, _, response, path = publication
    source = {row['基金代码']: row for row in json.loads(path.read_text())['rows']}
    result = overlay_portfolio_occurrence_metrics(response['portfolio_suggestions'], source,
             normalize_code_text=lambda v: str(v).zfill(6), normalize_number_text=None,
             to_float=None, format_tracker_occurrence_text=None)
    expected = [(2, '66.67'), (0, '0'), (None, None), (None, None)]
    assert [(row['出现次数'], row['出现频率(%)']) for row in result['funds']] == expected


@pytest.mark.parametrize('count,frequency,expected', [(None, None, '- 次 / -'), (0, 0, '0 次 / 0%')])
def test_actual_excel_cells_distinguish_unavailable_history_from_real_zero(count, frequency, expected):
    from openpyxl import load_workbook
    rows = recommendation_export_rows(
        [{'基金代码': '000001', '出现次数': count, '出现频率(%)': frequency}],
        normalize_code_text=lambda value: value,
        normalize_number_text=lambda value, digits: '' if value is None else str(value),
        to_float=None, format_tracker_occurrence_text=None,
        export_columns=['基金代码', '出现次数&出现频率(%)'],
    )
    output = BytesIO()
    rows.to_excel(output, index=False)
    output.seek(0)
    workbook = load_workbook(output, read_only=True, data_only=True)
    assert workbook.active.cell(2, 1).value == '000001'
    assert workbook.active.cell(2, 2).value == expected
    workbook.close()


@pytest.mark.parametrize('count,frequency,total,reason', [
    (90, '100', 89, 'count_outside_unique_observations'),
    (2, '50', 3, 'frequency_denominator_mismatch'),
    (None, None, 3, 'observation_values_unavailable'),
])
def test_truthfulness_audit_detects_count_and_denominator_errors(count, frequency, total, reason):
    source = [{'基金代码': '000001', '出现次数': 2, '出现频率(%)': '66.67'}]
    shown = [{'基金代码': '000001', '出现次数': count, '出现频率(%)': frequency}]
    result = _compare_tracker_occurrence_rows(source, shown, total)
    assert result['aligned'] is False
    assert reason in result['differences'][0]['reasons']


def test_truthfulness_audit_compares_numeric_meaning_and_keeps_unavailable_denominator():
    source = [{'基金代码': '000001', '出现次数': '0', '出现频率(%)': '0.00'}]
    shown = [{'基金代码': '000001', '出现次数': 0, '出现频率(%)': 0}]
    assert _compare_tracker_occurrence_rows(source, shown, 3)['aligned'] is True
    assert _compare_tracker_occurrence_rows(source, shown, None)['aligned'] is None


@pytest.mark.parametrize('total', [None, '', True, -1, 1.5, float('inf'), 1])
def test_reason_never_reuses_old_count_when_denominator_is_missing_or_invalid(total):
    value = tracker_occurrence_reason('累计入围 8/9 期；长期稳定性强', {'出现次数': 2}, total)
    assert value == '累计入围次数暂无可用数据；长期稳定性强'


def test_frequency_reason_preserves_zero_and_refreshes_only_known_generated_claims():
    assert tracker_occurrence_reason(None, {}, None) is None
    assert tracker_occurrence_reason('成立满365天；费率0.2%', {}, None) == '成立满365天；费率0.2%'
    value = tracker_occurrence_reason('累计出现频率 100%；长期稳定性强', {'出现频率(%)': 0}, None)
    assert value == '累计出现频率 0%；长期稳定性强'
    unavailable = tracker_occurrence_reason('累计入围 8/9 期', {}, None)
    assert tracker_occurrence_reason(unavailable, {'出现次数': 2}, 3) == '累计入围 2/3 期'


def test_reason_briefs_and_summary_use_corrected_rows_without_exposing_heavy_rows(publication, monkeypatch):
    from backend import app as backend_app
    settings, report, response, path = publication
    before = deepcopy(report)
    corrected = overlay_recommendations_tracker_occurrences(settings, report, response)
    expected_counts = {'同分组同风格相对排名靠前': 4, '累计入围 2/3 期': 1,
                       '累计入围 0/3 期': 1, '累计入围次数暂无可用数据': 2, '持有期已确认<=90天': 4}
    assert corrected['portfolio_group_brief']['fixture']['reason_counts'] == expected_counts
    monkeypatch.setattr(backend_app, 'settings', settings)
    monkeypatch.setattr(backend_app, '_attach_recommendations_summary_source_contract', lambda p, r: r)
    summary = backend_app._build_recommendations_summary_response(report)
    assert 'rows' not in summary and 'funds' not in summary['portfolio_suggestions']
    assert 'funds' not in summary['portfolio_suggestions']['groups'][0]
    assert summary['portfolio_group_brief']['fixture']['top_reason_tags'] == [
        {'label': '同分组同风格相对排名靠前', 'count': 4}, {'label': '持有期已确认<=90天', 'count': 4},
        {'label': '累计入围次数暂无可用数据', 'count': 2}, {'label': '累计入围 2/3 期', 'count': 1},
    ]
    assert report == before
    assert _compare_recommendation_reason_briefs(corrected['portfolio_suggestions']['groups'],
                                                 summary['portfolio_group_brief'])['aligned'] is True


def test_downloaded_workbook_contains_corrected_observation_reason(publication, monkeypatch):
    from backend import app as backend_app
    from openpyxl import load_workbook
    settings, report, response, path = publication
    original = deepcopy(report)
    source = {row['基金代码']: row for row in json.loads(path.read_text())['rows']}
    monkeypatch.setattr(backend_app, 'settings', settings)
    monkeypatch.setattr(backend_app, '_load_tracker_occurrence_map', lambda p: source)
    monkeypatch.setattr(backend_app, '_apply_since_inception_annualized_return_to_portfolio_summary', lambda p: p)
    buffer, filename = backend_app._build_recommendations_export_workbook(report)
    workbook = load_workbook(buffer, read_only=True, data_only=True)
    checked = 0
    for sheet in workbook:
        values = list(sheet.values)
        headers = values[0]
        if '基金代码' not in headers:
            continue
        rows = [dict(zip(headers, row)) for row in values[1:]]
        assert rows[0]['入选原因'] == '同分组同风格相对排名靠前；累计入围 2/3 期；持有期已确认<=90天'
        assert rows[1]['出现次数&出现频率(%)'] == '0 次 / 0%'
        assert rows[2]['出现次数&出现频率(%)'] == '- 次 / -'
        assert rows[2]['入选原因'] == '同分组同风格相对排名靠前；累计入围次数暂无可用数据；持有期已确认<=90天'
        checked += 1
    workbook.close()
    assert checked == 2 and report == original


def test_favorites_explanation_uses_same_observations_as_its_count(publication, monkeypatch):
    from backend import app as backend_app
    settings, report, response, path = publication
    source = json.loads(path.read_text())['rows']
    filtered_path = settings.project_root / 'selection.xlsx'
    filtered_path.touch()
    report['artifacts']['filtered_excel'] = str(filtered_path)
    original = deepcopy(report)
    frame = pd.DataFrame([{'基金代码': '000001', '基金名称': '隔离测试基金', '正式投资评分': '80',
                           '持有期状态': '已确认<=90天', '正式榜分组': '纯债稳健榜'}])
    monkeypatch.setattr(backend_app, 'settings', settings)
    monkeypatch.setattr(backend_app, '_load_tracker_occurrence_map', lambda p: {r['基金代码']: r for r in source})
    monkeypatch.setattr(backend_app, '_load_tracker_dataset', lambda p: {'tracker_records': source})
    monkeypatch.setattr(backend_app, '_resolve_existing_report_artifact_path', lambda *a, **k: filtered_path)
    monkeypatch.setattr(backend_app, '_read_excel_cached', lambda *a, **k: frame.copy())
    result = backend_app._load_favorite_fund_score_dataset(report)['000001']
    assert result['出现次数'] == 2
    assert '累计入围 2/3 期' in result['推荐解读']
    assert '4/4' not in result['推荐解读']
    assert report == original


@pytest.mark.parametrize('text,error', [
    ('累计入围 3/4 期', 'reason_observation_count_mismatch'),
    ('累计出现频率 100%', 'reason_observation_frequency_mismatch'),
])
def test_truthfulness_audit_detects_stale_reason_even_when_numeric_fields_match(text, error):
    source = [{'基金代码': '000001', '出现次数': 2, '出现频率(%)': '66.67'}]
    result = _compare_tracker_occurrence_rows(source, [{**source[0], '入选原因': text}], 3)
    assert error in result['differences'][0]['reasons']


def test_truthfulness_audit_detects_stale_group_reason_counts():
    groups = [{'name': 'fixture', 'funds': [{'入选原因': '累计入围 2/3 期'}]}]
    result = _compare_recommendation_reason_briefs(groups, {'fixture': {'reason_counts': {'累计入围 3/4 期': 1}}})
    assert result['aligned'] is False and result['checked_groups'] == 1
