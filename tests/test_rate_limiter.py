"""Offline tests for the token-bucket rate limiter and RestClient backoff.

These tests use a fake clock so no real time passes: ``time_fn`` reads a
mutable ``now`` and the async ``sleep_fn`` advances it (recording total slept).
Network is stubbed with a minimal async context-manager response object.

``asyncio_mode = "auto"`` (pyproject) means async test functions need no
decorator.
"""

from __future__ import annotations

from typing import Any

import pytest

from src.config import Settings
from src.rest_client import BinanceBanError, RestClient, TokenBucket


class FakeClock:
    """Mutable virtual clock; ``sleep`` advances ``now`` and tallies the wait."""

    def __init__(self) -> None:
        self.now = 0.0
        self.slept = 0.0

    def time(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        assert seconds >= 0.0
        self.now += seconds
        self.slept += seconds


# --------------------------------------------------------------------------- #
# TokenBucket
# --------------------------------------------------------------------------- #


async def test_tokens_deplete_on_acquire() -> None:
    clock = FakeClock()
    bucket = TokenBucket(60, throttle_ratio=1.0, time_fn=clock.time, sleep_fn=clock.sleep)

    await bucket.acquire(10)
    assert bucket.tokens == pytest.approx(50.0)
    await bucket.acquire(20)
    assert bucket.tokens == pytest.approx(30.0)
    assert clock.slept == 0.0  # plenty of headroom, no waiting


async def test_acquire_blocks_until_refilled() -> None:
    # capacity 60 -> refill rate 1 token/sec.
    clock = FakeClock()
    bucket = TokenBucket(60, throttle_ratio=1.0, time_fn=clock.time, sleep_fn=clock.sleep)

    await bucket.acquire(60)  # drains the bucket
    assert bucket.tokens == pytest.approx(0.0)

    # Needs 30 more tokens; at 1 token/sec that is ~30s of sleeping.
    await bucket.acquire(30)
    assert clock.slept == pytest.approx(30.0)
    assert bucket.tokens == pytest.approx(0.0)


async def test_sync_from_header_lowers_tokens() -> None:
    clock = FakeClock()
    bucket = TokenBucket(100, throttle_ratio=1.0, time_fn=clock.time, sleep_fn=clock.sleep)

    bucket.sync_from_header(70)
    assert bucket.tokens == pytest.approx(30.0)

    # Clamping: out-of-range values are bounded to [0, capacity].
    bucket.sync_from_header(250)
    assert bucket.tokens == pytest.approx(0.0)
    bucket.sync_from_header(-5)
    assert bucket.tokens == pytest.approx(100.0)


async def test_throttle_adds_extra_delay() -> None:
    # Unthrottled reference: throttle_ratio 1.0 never triggers extra delay.
    clock_plain = FakeClock()
    plain = TokenBucket(100, throttle_ratio=1.0, time_fn=clock_plain.time, sleep_fn=clock_plain.sleep)
    await plain.acquire(90)  # used fraction 0.90, no throttle delay
    assert clock_plain.slept == 0.0

    # Throttled: same acquire crosses the 0.8 threshold and incurs extra delay.
    clock_thr = FakeClock()
    thr = TokenBucket(100, throttle_ratio=0.8, time_fn=clock_thr.time, sleep_fn=clock_thr.sleep)
    await thr.acquire(90)  # used fraction 0.90 > 0.80
    assert clock_thr.slept > 0.0
    assert clock_thr.slept > clock_plain.slept


# --------------------------------------------------------------------------- #
# RestClient retry / ban handling
# --------------------------------------------------------------------------- #


class StubResponse:
    """Minimal async context-manager standing in for an aiohttp response."""

    def __init__(self, status: int, headers: dict[str, str], payload: Any) -> None:
        self.status = status
        self.headers = headers
        self._payload = payload

    async def __aenter__(self) -> "StubResponse":
        return self

    async def __aexit__(self, *exc: Any) -> bool:
        return False

    async def json(self) -> Any:
        return self._payload

    async def text(self) -> str:
        return str(self._payload)


class StubSession:
    """Returns queued responses in order; records requests for assertions."""

    def __init__(self, responses: list[StubResponse]) -> None:
        self._responses = responses
        self.calls: list[tuple[str, str]] = []

    def request(self, method: str, url: str, params: Any = None) -> StubResponse:
        self.calls.append((method, url))
        return self._responses.pop(0)

    async def close(self) -> None:  # pragma: no cover - not exercised here
        pass


def _settings() -> Settings:
    # warmup_klines must be >= window_size (validator); defaults satisfy this.
    return Settings(
        _env_file=None,
        rest_weight_limit=2400,
        rest_weight_throttle_ratio=0.8,
        rest_base_url="https://fapi.binance.com",
    )


async def test_request_retries_on_429() -> None:
    clock = FakeClock()
    responses = [
        StubResponse(429, {"Retry-After": "2", "X-MBX-USED-WEIGHT-1M": "10"}, {}),
        StubResponse(200, {"X-MBX-USED-WEIGHT-1M": "11"}, {"ok": True}),
    ]
    session = StubSession(responses)
    client = RestClient(_settings(), session=session, sleep_fn=clock.sleep)  # type: ignore[arg-type]

    result = await client._request("GET", "/fapi/v1/ping", None, weight=1)

    assert result == {"ok": True}
    assert len(session.calls) == 2  # retried exactly once
    assert clock.slept == pytest.approx(2.0)  # slept ~Retry-After


async def test_request_raises_on_418_without_retry() -> None:
    clock = FakeClock()
    responses = [StubResponse(418, {"Retry-After": "30"}, "banned")]
    session = StubSession(responses)
    client = RestClient(_settings(), session=session, sleep_fn=clock.sleep)  # type: ignore[arg-type]

    with pytest.raises(BinanceBanError):
        await client._request("GET", "/fapi/v1/ping", None, weight=1)

    assert len(session.calls) == 1  # did NOT retry
    assert clock.slept == 0.0  # did NOT sleep on the Retry-After


async def test_request_gives_up_after_max_429() -> None:
    clock = FakeClock()
    # Always 429: the request must give up rather than loop forever.
    responses = [StubResponse(429, {"Retry-After": "0"}, {}) for _ in range(20)]
    session = StubSession(responses)
    client = RestClient(_settings(), session=session, sleep_fn=clock.sleep)  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="persistent HTTP 429"):
        await client._request("GET", "/fapi/v1/ping", None, weight=1)

    # 1 initial attempt + _MAX_429_RETRIES retries, then it raises.
    assert len(session.calls) == 6
    assert clock.slept > 0.0  # a zero Retry-After is floored to a real backoff


async def test_backfill_drops_forming_last_candle() -> None:
    clock = FakeClock()
    # 4 klines oldest->newest; the LAST is the still-forming candle.
    klines = [
        [i * 60_000, "1", "1", "1", str(100 + i), "5", i * 60_000 + 59_999, "0", 0, "0", "0", "0"]
        for i in range(4)
    ]
    session = StubSession([StubResponse(200, {"X-MBX-USED-WEIGHT-1M": "1"}, klines)])
    client = RestClient(_settings(), session=session, sleep_fn=clock.sleep)  # type: ignore[arg-type]

    candles = await client.backfill("BTCUSDT")

    assert len(candles) == 3  # the forming last candle is dropped
    assert all(c.is_closed for c in candles)
    assert candles[-1].close == 102.0  # i=2, not the i=3 forming candle
