"""Offline tests for the REST polling collector."""

from __future__ import annotations

import pytest

from src.config import Settings
from src.store import Candle, SymbolStore
from src.rest_poller import RestPollingCollector, _interval_seconds


def _settings(**overrides) -> Settings:
    return Settings(_env_file=None, **overrides)


def _candle(open_time: int, close: float) -> Candle:
    return Candle(open_time, close, close, close, close, 1.0, True)


class StubRest:
    """Minimal RestClient stand-in exposing recent_klines."""

    def __init__(self, klines: dict[str, list[Candle]]) -> None:
        self.klines = klines
        self.calls = 0

    async def recent_klines(self, symbol: str, limit: int = 3) -> list[Candle]:
        self.calls += 1
        return list(self.klines.get(symbol, []))


def test_interval_seconds() -> None:
    assert _interval_seconds("1m") == 60
    assert _interval_seconds("5m") == 300
    assert _interval_seconds("1h") == 3600
    with pytest.raises(ValueError):
        _interval_seconds("10x")


async def test_poll_once_feeds_store_and_fires() -> None:
    store = SymbolStore(5)
    fired: list[str] = []
    rest = StubRest({"BTCUSDT": [_candle(60_000, 100.0), _candle(120_000, 101.0)]})
    poller = RestPollingCollector(_settings(), store, ["BTCUSDT"], lambda s: fired.append(s), rest)

    await poller.poll_once()
    assert [c.close for c in store.window("BTCUSDT")] == [100.0, 101.0]
    assert fired == ["BTCUSDT", "BTCUSDT"]  # two genuinely-new closed candles


async def test_poll_once_dedups_repeated_candles() -> None:
    store = SymbolStore(5)
    fired: list[str] = []
    rest = StubRest({"BTCUSDT": [_candle(60_000, 100.0)]})
    poller = RestPollingCollector(_settings(), store, ["BTCUSDT"], lambda s: fired.append(s), rest)

    await poller.poll_once()
    await poller.poll_once()  # same candle again -> deduped by open_time
    assert fired == ["BTCUSDT"]
    assert len(store.window("BTCUSDT")) == 1

    rest.klines["BTCUSDT"].append(_candle(120_000, 101.0))  # a new candle closes
    await poller.poll_once()
    assert fired == ["BTCUSDT", "BTCUSDT"]
    assert [c.close for c in store.window("BTCUSDT")] == [100.0, 101.0]


async def test_poll_once_supports_async_callback() -> None:
    store = SymbolStore(5)
    fired: list[str] = []

    async def cb(symbol: str) -> None:
        fired.append(symbol)

    rest = StubRest({"ETHUSDT": [_candle(60_000, 50.0)]})
    poller = RestPollingCollector(_settings(), store, ["ETHUSDT"], cb, rest)
    await poller.poll_once()
    assert fired == ["ETHUSDT"]
