"""In-memory rolling-window state store.

Holds, per symbol, a bounded deque of *closed* candles plus the single
*in-progress* (not-yet-closed) candle. Only closed candles enter the window;
the in-progress candle is tracked separately and never used for detection.

This module defines the :class:`Candle` data contract shared across the REST
backfill, the WebSocket collector, and the detector.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class Candle:
    """A single OHLCV candle.

    ``open_time`` is the kline open time in epoch milliseconds (Binance native).
    ``is_closed`` mirrors the kline ``x`` flag: True once the interval has ended.
    """

    open_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    is_closed: bool


class SymbolStore:
    """Per-symbol rolling windows of closed candles.

    The store is the single source of truth for candle history. It is written
    by the REST warm-up (:meth:`backfill`) and the live WS feed
    (:meth:`update`), and read by the detector (:meth:`window`).

    Not internally locked: drive it from a single asyncio event loop.
    """

    def __init__(self, window_size: int) -> None:
        if window_size < 1:
            raise ValueError("window_size must be >= 1")
        self._window_size = window_size
        self._closed: dict[str, deque[Candle]] = {}
        self._current: dict[str, Candle] = {}

    @property
    def window_size(self) -> int:
        return self._window_size

    def symbols(self) -> list[str]:
        """All symbols known to the store (those that have been backfilled/seen)."""
        return list(self._closed.keys())

    def ensure(self, symbol: str) -> None:
        """Register ``symbol`` with an empty window if not already present."""
        if symbol not in self._closed:
            self._closed[symbol] = deque(maxlen=self._window_size)

    def backfill(self, symbol: str, candles: list[Candle]) -> None:
        """Replace ``symbol``'s window with the tail of ``candles``.

        Only closed candles are kept. Used on start-up and on WS reconnect to
        (re)seed history. Candles are assumed ordered oldest -> newest.
        """
        dq: deque[Candle] = deque(
            (c for c in candles if c.is_closed),
            maxlen=self._window_size,
        )
        self._closed[symbol] = dq
        self._current.pop(symbol, None)

    def update(self, symbol: str, candle: Candle) -> bool:
        """Apply a live kline update for ``symbol``.

        If ``candle.is_closed`` it is appended to the rolling window (and the
        in-progress slot cleared) and the method returns ``True`` — signalling
        the detector should run. Otherwise the in-progress slot is updated and
        the method returns ``False``.
        """
        self.ensure(symbol)
        if candle.is_closed:
            dq = self._closed[symbol]
            if dq and candle.open_time <= dq[-1].open_time:
                # Already have this (or an older) closed candle — idempotent so
                # overlapping sources (WS + REST fallback) can't double-append.
                return False
            dq.append(candle)
            self._current.pop(symbol, None)
            return True
        self._current[symbol] = candle
        return False

    def window(self, symbol: str) -> list[Candle]:
        """Return the closed-candle window (oldest -> newest) for ``symbol``."""
        dq = self._closed.get(symbol)
        return list(dq) if dq else []

    def current(self, symbol: str) -> Candle | None:
        """Return the in-progress (unclosed) candle for ``symbol``, if any."""
        return self._current.get(symbol)

    def is_ready(self, symbol: str) -> bool:
        """True once ``symbol``'s window is full (enough history to evaluate)."""
        dq = self._closed.get(symbol)
        return bool(dq) and len(dq) >= self._window_size

    def last_close(self, symbol: str) -> float | None:
        """Most recent closed price for ``symbol``, or ``None`` if no history."""
        dq = self._closed.get(symbol)
        return dq[-1].close if dq else None
