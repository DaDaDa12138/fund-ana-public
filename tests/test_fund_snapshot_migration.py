"""Network-disabled migration drills with explicit synthetic fund fixtures."""
from datetime import datetime
import hashlib
import json
from types import SimpleNamespace

import pandas as pd
import pytest
import requests

from backend.fund_snapshot import fetch_fund_snapshots, normalize_snapshot
from backend.valuation_contract import SHANGHAI

NOW = datetime(2026, 10, 10, 15, 0, tzinfo=SHANGHAI)


def settings(tmp_path):
    return SimpleNamespace(reports_dir=tmp_path, tiantian_valuation_primary_url='https://primary.invalid',
                           tiantian_valuation_fallback_url='https://backup.invalid',
                           publication_max_snapshot_age_days=10, autostock_enabled=False)


def row(code='000001', **fields):
    return {'FCODE': code, 'SHORTNAME': 'Synthetic fixture fund', 'PDATE': '2026-10-09',
            'NAV': 1.2, 'SYL_Z': 0, 'SYL_Y': None, 'SYL_3Y': '--', 'SYL_6Y': 'invalid', **fields}


def response(rows, **envelope):
    result = requests.Response()
    result.status_code = 200
    result._content = json.dumps({'success': True, 'errorCode': 0, 'data': rows, **envelope}).encode()
    return result


def test_partial_primary_uses_backup_only_for_missing_codes_and_retains_evidence(tmp_path):
    calls = []
    outcomes = iter([response([row()]), response([row('000002')])])

    def post(endpoint, **kwargs):
        calls.append((endpoint, kwargs['data']['FCODES']))
        return next(outcomes)

    rows, meta = fetch_fund_snapshots(settings(tmp_path), ['000001', '000002'], post=post, now=NOW)
    assert calls == [('https://primary.invalid', '000001,000002'), ('https://backup.invalid', '000002')]
    assert meta['source_complete'] and meta['independent_source'] is False
    assert rows['000001']['lastWeekGrowth'] == 0
    assert rows['000001']['_field_states']['lastWeekGrowth'] == 'real_zero'
    assert rows['000001']['lastMonthGrowth'] is None
    assert rows['000001']['_field_states']['lastMonthGrowth'] == 'upstream_null'
    assert rows['000001']['_field_states']['lastThreeMonthsGrowth'] == 'upstream_null'
    assert rows['000001']['_field_states']['lastSixMonthsGrowth'] == 'parse_failed'
    assert rows['000001']['_field_states']['lastYearGrowth'] == 'field_not_returned'
    assert not {'manager', 'fundScale', 'type', 'holding_days'} & rows['000001'].keys()
    evidence = rows['000002']['_source_evidence']
    from pathlib import Path
    assert hashlib.sha256(Path(evidence['response_artifact']).read_bytes()).hexdigest() == evidence['response_sha256']


@pytest.mark.parametrize('bad', [response([], success=False), response(None), response([row(), row()]),
                                 response([row('000003')]), response([]),
                                 response([row(PDATE='2026-09-01')]), response([row(NAV=0)])])
def test_invalid_empty_stale_and_business_failures_never_become_success(tmp_path, bad):
    rows, meta = fetch_fund_snapshots(settings(tmp_path), ['000001'], post=lambda *a, **k: bad, now=NOW)
    assert rows == {} and not meta['source_complete'] and meta['status'] == 'partial'
    assert meta['missing_codes'] == ['000001']
    assert len(meta['attempts']) == 2


def test_transport_failure_falls_back_with_bounded_attempts(tmp_path):
    calls = []

    def post(endpoint, **kwargs):
        calls.append(endpoint)
        if len(calls) == 1:
            raise requests.ConnectTimeout('synthetic offline fixture')
        return response([row()])

    rows, meta = fetch_fund_snapshots(settings(tmp_path), ['000001'], post=post, now=NOW)
    assert len(calls) == 2 and len(rows) == 1
    assert meta['attempts'][0]['response_state'] == 'failed'
    assert 'response_sha256' not in meta['attempts'][0]


def test_no_50_code_truncation_and_request_limit_is_explicit(tmp_path):
    calls = []

    def post(endpoint, **kwargs):
        codes = kwargs['data']['FCODES'].split(',')
        calls.append(codes)
        return response([row(code) for code in codes])

    codes = [f'{i:06}' for i in range(1, 102)]
    rows, meta = fetch_fund_snapshots(settings(tmp_path), codes, max_codes=100, post=post, now=NOW)
    assert [len(batch) for batch in calls] == [50, 50]
    assert len(rows) == 100 and meta['missing_codes'] == ['000101']
    assert meta['skipped_over_limit_count'] == 1


def test_default_transport_reuses_connections_for_all_batches_and_backup(tmp_path, monkeypatch):
    original_session = requests.Session
    sessions = []
    calls = []
    closed = []

    def factory():
        session = original_session()
        sessions.append(session)
        original_close = session.close

        def post(endpoint, **kwargs):
            codes = kwargs['data']['FCODES'].split(',')
            calls.append((session, endpoint, codes))
            assert kwargs['timeout'] == (5, 12)
            # First primary response lacks one fund; backup must only fill it.
            if len(calls) == 1:
                codes = codes[:-1]
            return response([row(code) for code in codes])

        def close():
            closed.append(session)
            original_close()

        monkeypatch.setattr(session, 'post', post)
        monkeypatch.setattr(session, 'close', close)
        return session

    monkeypatch.setattr(requests, 'Session', factory)
    codes = [f'{i:06}' for i in range(1, 262)]
    rows, meta = fetch_fund_snapshots(settings(tmp_path), codes, now=NOW)
    assert len(sessions) == 1 and closed == sessions
    assert all(call[0] is sessions[0] for call in calls)
    assert [len(call[2]) for call in calls] == [50, 1, 50, 50, 50, 50, 11]
    assert calls[1][1:] == ('https://backup.invalid', ['000050'])
    assert len(rows) == 261 and meta['source_complete']
    assert rows['000050']['lastMonthGrowth'] is None
    assert rows['000050']['lastWeekGrowth'] == 0
    assert rows['000050']['_source_evidence']['endpoint'] == 'https://backup.invalid'
    fetch_fund_snapshots(settings(tmp_path), ['000001'], now=NOW)
    assert len(sessions) == 2 and sessions[0] is not sessions[1]
    assert closed == sessions


@pytest.mark.parametrize('failure', ['validation', 'storage'])
def test_default_transport_closes_session_on_unhandled_errors(tmp_path, monkeypatch, failure):
    from pathlib import Path
    session = requests.Session()
    closed = []
    monkeypatch.setattr(session, 'post', lambda *a, **k: response([row()]))
    monkeypatch.setattr(session, 'close', lambda: closed.append(True))
    monkeypatch.setattr(requests, 'Session', lambda: session)
    codes = ['invalid'] if failure == 'validation' else ['000001']
    if failure == 'storage':
        def fail_write(*args, **kwargs):
            raise OSError('synthetic audit storage failure')
        monkeypatch.setattr(Path, 'write_bytes', fail_write)
    with pytest.raises(ValueError if failure == 'validation' else OSError):
        fetch_fund_snapshots(settings(tmp_path), codes, now=NOW)
    assert closed == [True]


def test_injected_transport_is_not_owned_by_snapshot_fetch(tmp_path, monkeypatch):
    monkeypatch.setattr(requests, 'Session', lambda: pytest.fail('unexpected owned transport'))
    rows, meta = fetch_fund_snapshots(settings(tmp_path), ['000001'],
                                      post=lambda *a, **k: response([row()]), now=NOW)
    assert len(rows) == 1 and meta['source_complete']


def test_disabled_client_never_sends_or_consumes_quota(tmp_path, monkeypatch):
    from core.api.autostock_client import AutoStockClient
    client = AutoStockClient(enabled=False, shared_state_path=str(tmp_path / 'quota.json'))
    monkeypatch.setattr(client, '_send_http_request', lambda *a, **k: pytest.fail('unexpected wire attempt'))
    with pytest.raises(RuntimeError, match='provider_disabled'):
        client._request_with_meta(client.base_url + '/fund')
    assert client.get_call_count() == 0 and not (tmp_path / 'quota.json').exists()


def test_disabled_provider_preserves_report_hard_blocks_and_unknowns(tmp_path, monkeypatch):
    from backend import report_builder
    monkeypatch.setattr(report_builder, '_load_autostock_runtime_for_report', lambda *a: pytest.fail('unexpected client'))
    frame = pd.DataFrame([{'基金代码': '000001', 'AutoStock原始校验状态': 'hard_block',
                           'AutoStock校验备注': 'latest_nav_mismatch'},
                          {'基金代码': '000002', 'AutoStock原始校验状态': 'not_checked'}])
    context = {}
    result = report_builder._supplement_official_autostock_statuses(frame, settings=settings(tmp_path), status_context=context)
    assert result['AutoStock原始校验状态'].tolist() == ['hard_block', 'not_checked']
    assert context['autostock_validation_unavailable']['source_complete'] is False
    assert context['autostock_supplement_records'][0]['outcome'] == 'provider_disabled'


def test_disabled_final_gate_fails_before_client_or_cache(monkeypatch):
    from backend import config
    from bond_fund_screening.tasks import daily_final_screening as screening
    # Explicitly exercise the legacy gate; the runtime default now selects
    # the replacement endpoints, whose unavailable-source gate is separate.
    monkeypatch.setattr(config, 'load_settings',
                        lambda: SimpleNamespace(publication_validation_provider='autostock'))
    monkeypatch.setattr(screening, '_load_autostock_cross_validation_config', lambda: {'enabled': False})
    monkeypatch.setattr(screening, '_load_autostock_validation_cache', lambda *a: pytest.fail('unexpected cache pass'))
    local = pd.DataFrame([dict.fromkeys(screening.AUTOSTOCK_MAINLIST_REQUIRED_LOCAL_COLUMNS)])
    local.loc[0, '基金代码'] = '000001'
    with pytest.raises(RuntimeError, match='independent_source_unavailable'):
        screening._cross_validate_candidate_pool_with_autostock(pd.DataFrame([{'fcode': '000001'}]), df_local_complete=local)


def test_disabled_tail_fill_uses_history_maintenance_without_shadow_writes(tmp_path, monkeypatch):
    from backend import autostock_nav_tail_fill as tail, nav_history_maintenance as history
    calls = []
    monkeypatch.setattr(tail, '_build_autostock_client', lambda *a: pytest.fail('unexpected autostock'))
    monkeypatch.setattr(history, 'run_nav_history_maintenance', lambda s, **k: calls.append(k) or {'status': 'success'})
    config = settings(tmp_path)
    config.scheduler_daily_nav_history_sync_limit = 10
    result = tail.run_autostock_nav_tail_fill(config)
    assert result['source_name'] == 'tiantian_nav_history' and result['independent_source'] is False
    assert calls[0]['limit'] == 10 and 'recent_tail_lag' in calls[0]['allowed_issue_types']


def test_favorites_routes_to_new_snapshot_and_exposes_provider(tmp_path, monkeypatch):
    import backend.app as app
    import backend.fund_snapshot as module
    config = settings(tmp_path)
    monkeypatch.setattr(app, 'settings', config)
    monkeypatch.setattr(app, '_get_autostock_client', lambda: pytest.fail('unexpected autostock'))
    session = requests.Session()
    monkeypatch.setattr(session, 'post', lambda *a, **k: response([row()]))
    monkeypatch.setattr(module.requests, 'Session', lambda: session)
    monkeypatch.setattr(module, 'datetime', SimpleNamespace(now=lambda tz: NOW))
    result = app._fetch_favorite_autostock_snapshot(['000001'])
    assert result['000001']['netWorth'] == 1.2
    assert result.autostock_budget_meta['source'] == 'tiantian_fund_valuation_last'
    assert not result.autostock_budget_meta['independent_source']


def test_runtime_bridge_preserves_disabled_provider():
    from core.utils.project_runtime_config import build_core_runtime_config
    config = build_core_runtime_config(project_payload={'apis': {'autostock': {'enabled': False}}}, compat_defaults={})
    assert config['api']['autostock']['enabled'] is False


def test_disabled_universe_uses_tiantian_and_keeps_autostock_counts_unknown(tmp_path, monkeypatch):
    from backend import bond_universe_sync as universe
    config = settings(tmp_path)
    config.database_path = tmp_path / 'fixture.db'
    context = SimpleNamespace(tiantian_endpoints=object(), autostock_client=object())
    monkeypatch.setattr(universe, '_build_context', lambda s: context)
    monkeypatch.setattr(universe, '_read_local_bond_funds', lambda p: {'000001': {'runtime_excluded': False}})
    monkeypatch.setattr(universe, '_fetch_tiantian_type_codes', lambda *a, **k: ({'000001'}, {}))
    monkeypatch.setattr(universe, '_fetch_tiantian_curated_codes', lambda *a, **k: (set(), {}))
    monkeypatch.setattr(universe, '_apply_external_curation_flags', lambda *a, **k: {})
    monkeypatch.setattr(universe, '_fetch_autostock_bond_directory', lambda *a: pytest.fail('unexpected directory'))
    monkeypatch.setattr(universe, '_build_missing_records_from_search', lambda *a: ([], {}))
    monkeypatch.setattr(universe, '_save_missing_records', lambda *a: {})
    result = universe.run_bond_universe_sync(config)
    assert result['counts_before_sync']['tiantian_31_32_total'] == 1
    assert result['counts_before_sync']['autostock_directory_bond_total'] is None
    assert result['diffs_after_sync']['missing_in_local_vs_autostock_total'] is None
    assert result['coverage_after_sync']['vs_autostock_pct'] is None
    assert result['independent_directory_available'] is False
    assert result['autostock_directory_source_evidence']['source_complete'] is False


def test_partial_replacement_history_is_not_reported_as_completed(tmp_path, monkeypatch):
    from backend import autostock_nav_tail_fill as tail, nav_history_maintenance as history
    monkeypatch.setattr(history, 'run_nav_history_maintenance', lambda *a, **k: {'maintenance_result': {'failed_total': 1}})
    config = settings(tmp_path)
    config.scheduler_daily_nav_history_sync_limit = 10
    with pytest.raises(RuntimeError, match='replacement_partial_failure'):
        tail.run_autostock_nav_tail_fill(config)
