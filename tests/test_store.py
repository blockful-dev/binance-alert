"""Unit tests for :mod:`src.store` — the rolling-window state store."""

from __future__ import annotations

from src.store import Candle, SymbolStore


def _candle(open_time: int, close: float, *, is_closed: bool = True, volume: float = 1.0) -> Candle:
    return Candle(
        open_time=open_time,
        open=close,
        high=close,
        low=close,
        close=close,
        volume=volume,
        is_closed=is_closed,
    )


def test_window_is_bounded_and_ordered() -> None:
    store = SymbolStore(window_size=3)
    for i in range(5):
        store.update("BTCUSDT", _candle(i * 60, float(i)))

    window = store.window("BTCUSDT")
    assert [c.close for c in window] == [2.0, 3.0, 4.0]  # oldest -> newest, last 3
    assert store.is_ready("BTCUSDT") is True
    assert store.last_close("BTCUSDT") == 4.0


def test_update_returns_true_only_on_close() -> None:
    store = SymbolStore(window_size=5)

    # In-progress candle: not appended, tracked as current, returns False.
    assert store.update("ETHUSDT", _candle(0, 10.0, is_closed=False)) is False
    assert store.window("ETHUSDT") == []
    assert store.current("ETHUSDT") is not None
    assert store.current("ETHUSDT").close == 10.0

    # Closing the candle: appended to the window, current cleared, returns True.
    assert store.update("ETHUSDT", _candle(0, 11.0, is_closed=True)) is True
    assert [c.close for c in store.window("ETHUSDT")] == [11.0]
    assert store.current("ETHUSDT") is None


def test_backfill_keeps_only_closed_tail() -> None:
    store = SymbolStore(window_size=2)
    candles = [
        _candle(0, 1.0, is_closed=True),
        _candle(60, 2.0, is_closed=True),
        _candle(120, 3.0, is_closed=True),
        _candle(180, 4.0, is_closed=False),  # in-progress -> dropped
    ]
    store.backfill("XRPUSDT", candles)

    # Only closed candles, bounded to the last `window_size`.
    assert [c.close for c in store.window("XRPUSDT")] == [2.0, 3.0]
    assert store.current("XRPUSDT") is None


def test_is_ready_and_unknown_symbol() -> None:
    store = SymbolStore(window_size=2)
    store.update("BTCUSDT", _candle(0, 1.0))
    assert store.is_ready("BTCUSDT") is False  # only 1 of 2
    store.update("BTCUSDT", _candle(60, 2.0))
    assert store.is_ready("BTCUSDT") is True

    assert store.is_ready("NOPE") is False
    assert store.window("NOPE") == []
    assert store.last_close("NOPE") is None


def test_update_dedups_closed_by_open_time() -> None:
    store = SymbolStore(window_size=5)
    assert store.update("BTCUSDT", _candle(60, 1.0)) is True
    # Same open_time again (e.g. WS and REST both deliver it) -> ignored.
    assert store.update("BTCUSDT", _candle(60, 9.0)) is False
    # An older closed candle -> ignored.
    assert store.update("BTCUSDT", _candle(30, 9.0)) is False
    # A newer closed candle -> appended.
    assert store.update("BTCUSDT", _candle(120, 2.0)) is True
    assert [c.close for c in store.window("BTCUSDT")] == [1.0, 2.0]


def test_ensure_registers_empty_window() -> None:
    store = SymbolStore(window_size=4)
    store.ensure("DOGEUSDT")
    assert "DOGEUSDT" in store.symbols()
    assert store.window("DOGEUSDT") == []
