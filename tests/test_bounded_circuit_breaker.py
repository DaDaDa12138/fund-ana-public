"""Regression coverage for the production API client's bounded circuit wait."""
from pathlib import Path

import pytest

from api import client as client_module
from api import rate_limiter as limiter_module
from api.client import APIClient
from api.rate_limiter import RateLimiter
from utils.project_runtime_config import load_runtime_config


def _limiter(**overrides):
    return RateLimiter({
        "qps": 120,
        "max_workers": 1,
        "min_interval": 0,
        "random_delay": [0, 0],
        "failure_threshold": 1,
        "timeout_duration": 300,
        "circuit_breaker_max_wait": 0,
        **overrides,
    })


def test_open_circuit_fails_immediately_and_keeps_concurrency_available(monkeypatch):
    limiter = _limiter()
    limiter.record_failure()
    failure_time = limiter.circuit_breaker.last_failure_time
    monkeypatch.setattr(
        limiter.circuit_breaker._recovery_event,
        "wait",
        lambda **_: pytest.fail("open circuit must not wait through upstream cooldown"),
    )
    for _ in range(100):
        with pytest.raises(RuntimeError, match="circuit breaker open"):
            limiter.acquire()
    assert limiter.circuit_breaker.is_open
    assert limiter.circuit_breaker.last_failure_time == failure_time
    assert limiter.circuit_breaker.failure_count == 1
    # Rejected work did not take a worker slot.
    assert limiter.semaphore.acquire(blocking=False)
    limiter.semaphore.release()


def test_normal_closed_circuit_can_acquire_and_release():
    limiter = _limiter()
    with limiter.acquire() as acquired:
        acquired.record_success(0.001)
    assert limiter.semaphore.acquire(blocking=False)
    limiter.semaphore.release()


def test_client_merges_max_wait_from_real_compat_config(monkeypatch):
    monkeypatch.setattr(client_module, "HAS_ENHANCED_LOGGER", False)
    config = load_runtime_config(Path(__file__).resolve().parents[1] / "config/project.yaml")
    circuit = config["api"]["tiantian"]["circuit_breaker"]
    assert circuit["enabled"] is True
    assert circuit["timeout_duration"] == 300
    client = APIClient(config)
    assert client.rate_limiter.circuit_breaker_max_wait == 0
    for _ in range(circuit["failure_threshold"]):
        client.rate_limiter.record_failure()
    assert client.retry_handler.max_attempts == 2
    monkeypatch.setattr(
        limiter_module.time, "sleep",
        lambda *_: pytest.fail("local circuit rejection must not sleep or retry"),
    )
    monkeypatch.setattr(
        client.rate_limiter.circuit_breaker._recovery_event, "wait",
        lambda **_: pytest.fail("client must not wait for the upstream cooldown"),
    )
    called = []
    monkeypatch.setattr(client, "_make_request", lambda *args, **kwargs: called.append(1))
    result = client.get("/never-called")
    assert not result
    assert result["error_type"] == "RuntimeError"
    assert "circuit breaker open" in result["error"]
    assert called == []
    stats = client.get_stats()
    assert stats["total_requests"] == stats["failed_requests"] == 1
    assert stats["attempt_total_requests"] == stats["successful_requests"] == 0


def test_zero_wait_keeps_cooldown_and_half_open_recovery(monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(limiter_module.time, "time", lambda: clock[0])
    limiter = _limiter()
    limiter.record_failure()
    clock[0] += 299
    with pytest.raises(RuntimeError, match="circuit breaker open"):
        limiter.acquire()
    clock[0] += 1
    with limiter.acquire() as acquired:
        acquired.record_success(0.001)
    assert limiter.circuit_breaker._half_open
    for _ in range(limiter.circuit_breaker.HALF_OPEN_SUCCESS_THRESHOLD - 1):
        with limiter.acquire() as acquired:
            acquired.record_success(0.001)
    assert not limiter.circuit_breaker._half_open
    assert not limiter.circuit_breaker.is_open
    assert limiter.circuit_breaker.failure_count == 0
