from __future__ import annotations

import json
import multiprocessing
import sys
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from email.utils import format_datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'core'))

from api import autostock_client as module
from api.autostock_client import AutoStockClient, AutoStockQuotaExceeded, AutoStockRateLimiter
from backend.autostock_budget import read_autostock_quota_snapshot as backend_snapshot
from bond_fund_screening.tasks.final_screening_autostock_helpers import read_autostock_quota_snapshot as core_snapshot


class Clock:
    def __init__(self):
        self.now = 1_800_000_000.0

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def response(data=None, *, code=200, status=200, message='操作成功', headers=None):
    return SimpleNamespace(status_code=status, headers=headers or {},
                           json=lambda: {'code': code, 'message': message, 'data': data})


def client_with_responses(tmp_path, monkeypatch, responses, *, limit=100):
    clock = Clock()
    monkeypatch.setattr(module.time, 'sleep', clock.sleep)
    client = AutoStockClient(min_interval=0, max_calls_per_hour=limit,
                            shared_state_path=str(tmp_path / 'quota.json'))
    client.rate_limiter._time_func = clock.time
    client.rate_limiter._sleep_func = clock.sleep
    sent = []
    outcomes = iter(responses)

    def send(method, url, **kwargs):
        sent.append((method, url, kwargs))
        result = next(outcomes)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(client, '_send_http_request', send)
    return client, clock, sent


@pytest.mark.parametrize('first', [response(code=500), response(status=503), requests.Timeout('timeout')])
def test_each_wire_retry_consumes_shared_quota(tmp_path, monkeypatch, first):
    client, _, sent = client_with_responses(tmp_path, monkeypatch, [first, response([{'code': '000001'}])])
    data, meta = client._request_with_meta(client.base_url + '/fund?code=000001')
    assert data == [{'code': '000001'}]
    assert len(sent) == meta['http_attempt_count'] == client.get_call_count() == 2
    assert len(json.loads((tmp_path / 'quota.json').read_text())['recent_call_timestamps']) == 2


def test_retry_stops_before_exceeding_hourly_quota(tmp_path, monkeypatch):
    client, _, sent = client_with_responses(tmp_path, monkeypatch, [response(code=500)], limit=1)
    with pytest.raises(Exception) as error:
        client._request_with_meta(client.base_url + '/fund?code=000001')
    assert len(sent) == 1
    assert error.value.args[1]['http_attempt_count'] == 1
    assert error.value.args[1]['quota_error'] is True


@pytest.mark.parametrize('configured', [None, 0, 100])
def test_unauthenticated_client_has_a_safe_default(tmp_path, configured):
    client = AutoStockClient(max_calls_per_hour=configured, shared_state_path=str(tmp_path / 'quota.json'))
    assert client.max_calls_per_hour == client.rate_limiter.max_calls_per_hour == 100
    for reader in (backend_snapshot, core_snapshot):
        snapshot = reader(shared_state_path=tmp_path / 'quota.json', max_calls_per_hour=configured,
                          min_interval_seconds=0)
        assert snapshot['max_calls_per_hour'] == snapshot['quota_remaining'] == 100


@pytest.mark.parametrize('retry_after', ['90', 'http-date', None])
def test_server_cooldown_is_shared_and_expires_automatically(tmp_path, monkeypatch, retry_after):
    deadline = 1_800_000_090
    header = format_datetime(datetime.fromtimestamp(deadline, timezone.utc), usegmt=True) if retry_after == 'http-date' else retry_after
    client, clock, sent = client_with_responses(tmp_path, monkeypatch, [
        response(status=429, code=429, message='请求频次限制', headers={'Retry-After': header} if header else {}),
        response([{'code': '000001'}]),
    ])
    with pytest.raises(Exception) as error:
        client._request_with_meta(client.base_url + '/fund?code=000001')
    delay = 3600 if retry_after is None else 90
    assert error.value.args[1]['retry_after'] == delay
    other = AutoStockRateLimiter(min_interval=0, max_calls_per_hour=100,
                                 shared_state_path=str(tmp_path / 'quota.json'), time_func=clock.time)
    with pytest.raises(AutoStockQuotaExceeded):
        other.wait_if_needed()
    snapshots = [reader(shared_state_path=tmp_path / 'quota.json', max_calls_per_hour=100,
                        min_interval_seconds=0, now=clock.now) for reader in (backend_snapshot, core_snapshot)]
    assert snapshots[0] == snapshots[1]
    assert snapshots[0]['quota_remaining'] == 0
    assert snapshots[0]['rate_limit_wait_seconds'] == delay
    assert snapshots[0]['recent_call_count'] == 1
    clock.sleep(delay)
    client._request_with_meta(client.base_url + '/fund?code=000001')
    assert len(sent) == 2


def _reserve_in_process(path):
    limiter = AutoStockRateLimiter(min_interval=0, max_calls_per_hour=7, shared_state_path=path)
    count = 0
    for _ in range(4):
        try:
            limiter.wait_if_needed()
            count += 1
        except AutoStockQuotaExceeded:
            break
    return count


def test_four_processes_share_one_hourly_budget(tmp_path):
    path = str(tmp_path / 'quota.json')
    with ProcessPoolExecutor(max_workers=4, mp_context=multiprocessing.get_context('fork')) as pool:
        counts = list(pool.map(_reserve_in_process, [path] * 4))
    assert sum(counts) == 7
    assert len(json.loads(Path(path).read_text())['recent_call_timestamps']) == 7


@pytest.mark.parametrize('payload', [
    '', ' ', '{"schema_version":99,"recent_call_timestamps":[]}',
    '{"last_call_time":', '[]', '{}', '{"recent_call_timestamps":["bad"]}',
    '{"recent_call_timestamps":[NaN]}', '{"recent_call_timestamps":[],"blocked_until":Infinity}',
    '{"recent_call_timestamps":[],"last_call_time":-1}',
])
def test_corrupt_quota_state_never_resets_to_free_calls(tmp_path, monkeypatch, payload):
    client, _, sent = client_with_responses(tmp_path, monkeypatch, [])
    (tmp_path / 'quota.json').write_text(payload)
    with pytest.raises(Exception) as error:
        client._request_with_meta(client.base_url + '/fund?code=000001')
    assert not sent
    assert error.value.args[1]['quota_reason'] == 'quota_state_unavailable'
    assert (tmp_path / 'quota.json').read_text() == payload
    for reader in (backend_snapshot, core_snapshot):
        snapshot = reader(shared_state_path=tmp_path / 'quota.json', max_calls_per_hour=100,
                          min_interval_seconds=0)
        assert snapshot['quota_remaining'] == 0
        assert snapshot['state_read_error']


def test_batch_deduplicates_inputs_and_honors_fifty_code_boundary(tmp_path, monkeypatch):
    codes = [str(i).zfill(6) for i in range(75)]
    client, _, sent = client_with_responses(tmp_path, monkeypatch, [
        response([{'code': code, 'netWorth': 1} for code in codes[:50]]),
        response([{'code': code, 'netWorth': 1} for code in codes[50:]]),
    ])
    rows, meta = client.get_fund_batch_with_meta(codes + [' 000001 ', '1'], batch_size=100)
    assert len(rows) == 75 and len(sent) == 2
    assert meta['deduplicated_code_count'] == 2
    assert meta['effective_batch_size'] == 50
    assert meta['http_attempt_count'] == 2
    assert meta['source_evidence']['source_complete'] is True


@pytest.mark.parametrize('flag', [{'requestFail': 1}, {'resolveFail': '1'}, {'requestFail': 'bad'}])
def test_business_failure_rows_are_missing_and_never_success(tmp_path, monkeypatch, flag):
    client, _, _ = client_with_responses(tmp_path, monkeypatch, [response([
        {'code': '000001', 'netWorth': 0, 'dayGrowth': '0', 'requestFail': 0, 'resolveFail': 0},
        {'code': '000002', 'netWorth': 1.1, **flag},
        {'code': '999999', 'netWorth': 1.2},
    ])])
    rows, meta = client.get_fund_batch_with_meta(['000001', '000002'])
    assert rows == [{'code': '000001', 'netWorth': 0, 'dayGrowth': '0', 'requestFail': 0, 'resolveFail': 0}]
    assert meta['missing_codes_preview'] == ['000002']
    assert meta['rejected_record_count'] == 2
    assert meta['source_evidence']['source_complete'] is False


def test_conflicting_duplicate_response_does_not_choose_a_business_value(tmp_path, monkeypatch):
    client, _, _ = client_with_responses(tmp_path, monkeypatch, [response([
        {'code': '000001', 'netWorth': 1.1}, {'code': '000001', 'netWorth': 1.2},
    ])])
    rows, meta = client.get_fund_batch_with_meta(['000001'])
    assert rows == []
    assert meta['missing_count'] == 1
    assert meta['rejected_records'][0]['reason'] == 'conflicting_duplicate'


def test_detail_batch_rejects_a_malformed_object_payload(tmp_path, monkeypatch):
    client, _, _ = client_with_responses(tmp_path, monkeypatch, [response({'requestFail': 1})])
    assert client.get_fund_detail_batch(['000001']) == []
    assert client.get_last_fund_detail_batch_meta()['source_complete'] is False


def test_position_detects_observed_success_envelope_with_request_failure(tmp_path, monkeypatch):
    client, _, _ = client_with_responses(tmp_path, monkeypatch, [response({'requestFail': 1, 'resolveFail': 0})])
    with pytest.raises(Exception) as error:
        client.get_fund_position('000015')
    assert error.value.args[1]['failure_kind'] == 'upstream_business_data_failed'
    assert error.value.args[1]['quota_error'] is False


def test_single_detail_never_returns_another_fund(monkeypatch):
    client = AutoStockClient.__new__(AutoStockClient)
    monkeypatch.setattr(client, 'get_fund_detail_batch', lambda *a, **k: [{'code': '000002'}])
    with pytest.warns(DeprecationWarning):
        assert client.get_fund_detail('000001') == {}


def test_rank_retries_at_only_one_layer(tmp_path, monkeypatch):
    client, _, sent = client_with_responses(tmp_path, monkeypatch, [response(code=500)] * 3)
    assert client.get_all_bond_funds_from_rank(page_size=50) == []
    assert len(sent) == client.get_call_count() == 3
    assert client.get_last_rank_fetch_meta()['source_complete'] is False


def test_rank_repeated_page_stops_without_faking_completeness(tmp_path, monkeypatch):
    page = {'rank': [{'code': '000001'}], 'totalRecord': 100, 'requestFail': 0}
    client, _, sent = client_with_responses(tmp_path, monkeypatch, [response(page), response(page)])
    assert client.get_all_bond_funds_from_rank(page_size=50) == [{'code': '000001'}]
    assert len(sent) == 2
    assert client.get_last_rank_fetch_meta()['stop_reason'] == 'repeated_page'
    assert client.get_last_rank_fetch_meta()['source_complete'] is False


@pytest.mark.parametrize('start,end', [('2026-09-01&code=000002', None), ('20260901', None), ('2026-09-04', '2026-08-01')])
def test_invalid_history_range_never_reaches_upstream(tmp_path, monkeypatch, start, end):
    client, _, sent = client_with_responses(tmp_path, monkeypatch, [])
    with pytest.raises(ValueError):
        client.get_fund_detail_batch(['000001'], start_date=start, end_date=end)
    assert sent == []


def test_machine_contract_matches_actual_manager_and_batch_parameters():
    from api.endpoints import FundAPIEndpoints
    from backend.upstream_api_contract import upstream_api_source_contract

    calls = []
    endpoints = FundAPIEndpoints(SimpleNamespace(get=lambda path, params: calls.append((path, params))))
    endpoints.get_manager_info('30185328')
    contract = upstream_api_source_contract(settings=SimpleNamespace(
        tiantian_base_url='http://proxy.test/tt', autostock_base_url='http://proxy.test/custom-prefix',
    ))
    tt = {x['path']: x for x in contract['source_groups']['tiantian']['endpoints']}
    assert set(tt[calls[0][0]]['required_params']) == set(calls[0][1]) == {'FCODE'}
    auto = {x['path']: x for x in contract['source_groups']['autostock']['endpoints']}
    assert set(auto['/fund/detail/list']['required_params']) == {'code'}
    assert auto['/fund/rank']['method'] == 'POST'
    assert '/fund/hot' in auto and not auto['/fund/hot']['used_by']


def test_tiantian_rank_never_silently_requests_unsupported_pages():
    from api.endpoints import FundAPIEndpoints

    calls = []
    endpoints = FundAPIEndpoints(SimpleNamespace(get=lambda path, params: calls.append((path, params))))
    with pytest.raises(ValueError):
        endpoints.get_bond_fund_rank(page_size=100)
    assert not calls
    endpoints.get_bond_fund_rank(page_size=30, only_buyable=True)
    assert calls[0][1]['BUY'] == 'true'


def test_numeric_growth_keeps_zero_and_preserves_undefined():
    from api.autostock_endpoints import DataConverter

    assert DataConverter.parse_growth_rate(1.25) == DataConverter.parse_growth_rate('1.25%') == 1.25
    assert DataConverter.parse_growth_rate(0) == 0
    for value in (None, '--', 'NaN', float('inf')):
        assert DataConverter.parse_growth_rate(value) is None
    assert DataConverter.convert_fund_type('unknown') is None
    assert DataConverter.convert_to_local_fund_type('unknown') is None


def test_tiantian_request_exception_without_response_keeps_original_failure():
    from api.retry_handler import RetryHandler

    error = requests.RequestException('transport unavailable')
    assert RetryHandler().should_retry(error, 1) == (False, 0)


def test_nav_fallback_bounds_history_without_discarding_current_fields():
    from backend.autostock_nav_tail_fill import _run_fund_detail_retry_stage

    calls = []
    def detail(codes, **kwargs):
        calls.append(kwargs)
        return [{'code': codes[0], 'netWorth': 1.2, 'netWorthDate': '2026-09-04'}]
    client = SimpleNamespace(get_fund_detail_batch=detail, get_last_fund_detail_batch_meta=lambda: {})
    rows, meta = _run_fund_detail_retry_stage(client, ['000001'], batch_size=10)
    assert rows[0]['netWorthDate'] == '2026-09-04'
    assert calls[0]['start_date'] == meta['history_start_date']
    assert calls[0]['end_date'] == meta['history_end_date']
    assert (datetime.fromisoformat(calls[0]['end_date']) - datetime.fromisoformat(calls[0]['start_date'])).days == 31
