from __future__ import annotations

import hashlib

import pytest

from backend.fund_status_api import (
    FundStatusPayloadError,
    fetch_status_snapshot,
    parse_status_payload,
)


def _body(*rows: list[str], record: int | None = None) -> bytes:
    suffix = f',record:"{record}"' if record is not None else ",record:\"0\""
    import json

    return ("var reData={datas:" + json.dumps(list(rows), ensure_ascii=False) + suffix + "};").encode()


def test_parse_status_payload_keeps_blank_next_open_date_and_field_order() -> None:
    payload = _body(
        ["000001", "测试基金", "债券型-纯债", "1.0000", "10-16", "开放申购", "开放赎回", "", "10.0", "100", "1", "1", "0.1%"],
        record=1,
    )
    parsed = parse_status_payload(payload)
    assert parsed["declared_records"] == 1
    assert parsed["rows"] == [
        {
            "fcode": "000001",
            "shortname": "测试基金",
            "ftype": "债券型-纯债",
            "nav": "1.0000",
            "nav_date_mmdd": "10-16",
            "purchase_status": "开放申购",
            "redemption_status": "开放赎回",
            "next_open_date": "",
            "purchase_min": "10.0",
            "daily_limit": "100",
            "discount": "1",
            "is_buyable_flag": "1",
            "sales_fee": "0.1%",
        }
    ]


@pytest.mark.parametrize(
    "payload",
    [
        b"{}",
        b"var reData={datas:[[\"000001\"]],record:\"1\"};",
        b"var reData={datas:[[\"BAD\",\"x\",\"x\",\"1\",\"x\",\"x\",\"x\",\"\",\"\",\"\",\"\",\"\",\"\"]],record:\"1\"};",
    ],
)
def test_changed_or_ambiguous_payload_fails_closed(payload: bytes) -> None:
    with pytest.raises(FundStatusPayloadError):
        parse_status_payload(payload)


def test_duplicate_code_and_declared_count_fail_closed() -> None:
    row = ["000001", "测试基金", "债券型-纯债", "1.0000", "10-16", "开放申购", "开放赎回", "", "10.0", "100", "1", "1", "0.1%"]
    with pytest.raises(FundStatusPayloadError, match="status_duplicate_identity"):
        parse_status_payload(_body(row, row, record=2))
    with pytest.raises(FundStatusPayloadError, match="status_record_count_mismatch"):
        parse_status_payload(_body(row, record=2))


class _Response:
    status_code = 200

    def __init__(self, body: bytes):
        self.content = body

    def raise_for_status(self) -> None:
        return None


def test_fetch_is_read_only_and_never_infers_holding_days() -> None:
    body = _body(
        ["000001", "测试基金", "债券型-纯债", "1.0000", "10-16", "开放申购", "开放赎回", "", "10.0", "100", "1", "1", "0.1%"],
        record=1,
    )

    def fake_get(*args, **kwargs):
        assert kwargs["params"]["page"] == "1,50000"
        return _Response(body)

    result = fetch_status_snapshot(codes=["000001"], get=fake_get)
    assert result["status"] == "ok"
    assert result["source_complete"] is True
    assert result["holding_days_written"] == 0
    assert result["holding_period_inference"] == "forbidden"
    assert result["response_sha256"] == hashlib.sha256(body).hexdigest()


def test_requested_missing_code_is_partial() -> None:
    body = _body(
        ["000001", "测试基金", "债券型-纯债", "1.0000", "10-16", "开放申购", "开放赎回", "", "10.0", "100", "1", "1", "0.1%"],
        record=1,
    )
    result = fetch_status_snapshot(codes=["000001", "000002"], get=lambda *a, **k: _Response(body))
    assert result["status"] == "partial"
    assert result["missing_codes"] == ["000002"]
    assert result["source_complete"] is False
