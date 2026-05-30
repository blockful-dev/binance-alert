"""REST polling collector — a fallback data source when WebSocket push is
unavailable (e.g. the host can complete the WS handshake but receives no market
data).

Once per candle interval it fetches each symbol's most recent closed kline via
the rate-limited :class:`~src.rest_client.RestClient` and feeds it into the
shared :class:`SymbolStore`. The store deduplicates by ``open_time``, so only a
genuinely new closed candle fires ``on_candle_close`` — making this safe to run
alongside (or hand off to/from) the WebSocket collector.

For ~500 symbols on a 1m interval this is ~500 weight/min, comfortably under the
2400/min IP budget the token bucket enforces.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from typing import Awaitable, Callable

from src.config import Settings
from src.rest_client import BinanceBanError, RestClient
from src.store import SymbolStore

logger = logging.getLogger(__name__)

_POLL_CONCURRENCY = 20  # max concurrent klines requests (the token bucket caps weight)
_POLL_LIMIT = 3  # klines per request -> 2 closed candles (covers a missed cycle)
_POLL_OFFSET_SECONDS = 2.0  # poll this long after the interval boundary (let the candle close)

OnCandleClose = Callable[[str], Awaitable[None]] | Callable[[str], None]

_UNIT_SECONDS = {"m": 60, "h": 3600, "d": 86400}


def _interval_seconds(interval: str) -> int:
    """Convert a Binance interval like ``1m``/``5m``/``1h`` to seconds."""
    try:
        value, unit = int(interval[:-1]), interval[-1]
        return value * _UNIT_SECONDS[unit]
    except (ValueError, KeyError, IndexError) as exc:
        raise ValueError(f"unsupported interval: {interval!r}") from exc


class RestPollingCollector:
    """Poll recent klines per symbol and feed closed candles into the store."""

    def __init__(
        self,
        settings: Settings,
        store: SymbolStore,
        symbols: list[str],
        on_candle_close: OnCandleClose,
        rest_client: RestClient,
        *,
        time_fn: Callable[[], float] = time.time,
        sleep_fn: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._settings = settings
        self._store = store
        self._symbols = list(symbols)
        self._on_candle_close = on_candle_close
        self._rest = rest_client
        self._time_fn = time_fn
        self._sleep_fn = sleep_fn
        self._stop = asyncio.Event()

    async def run(self) -> None:
        """Poll every interval (aligned to the candle boundary) until stopped."""
        self._stop.clear()
        interval_s = _interval_seconds(self._settings.interval)
        logger.info("rest polling started symbols=%d interval=%ds", len(self._symbols), interval_s)
        await self._poll_once_safe()  # immediate catch-up (deduped against warm-up)
        while not self._stop.is_set():
            now = self._time_fn()
            next_boundary = (int(now) // interval_s + 1) * interval_s
            delay = max(0.0, next_boundary + _POLL_OFFSET_SECONDS - now)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
                break  # stop fired while waiting
            except asyncio.TimeoutError:
                pass
            await self._poll_once_safe()
        logger.info("rest polling stopped")

    async def stop(self) -> None:
        """Signal the poll loop to exit."""
        self._stop.set()

    async def _poll_once_safe(self) -> None:
        try:
            await self.poll_once()
        except BinanceBanError:
            logger.critical("REST polling halted: 418 ban")
            self._stop.set()

    async def poll_once(self) -> None:
        """Fetch recent closed candles for every symbol and feed the store."""
        sem = asyncio.Semaphore(_POLL_CONCURRENCY)

        async def one(symbol: str) -> None:
            async with sem:
                try:
                    candles = await self._rest.recent_klines(symbol, _POLL_LIMIT)
                except BinanceBanError:
                    raise
                except Exception as exc:  # noqa: BLE001 — per-symbol best effort
                    logger.debug("poll failed symbol=%s err=%s", symbol, exc)
                    return
                for candle in candles:
                    if self._store.update(symbol, candle):
                        await self._fire(symbol)

        await asyncio.gather(*(one(s) for s in self._symbols))

    async def _fire(self, symbol: str) -> None:
        """Invoke ``on_candle_close`` (sync or async), swallowing callback errors."""
        try:
            result = self._on_candle_close(symbol)
            if inspect.isawaitable(result):
                await result
        except Exception as exc:  # noqa: BLE001 — a callback error must not stop polling
            logger.warning("on_candle_close failed symbol=%s err=%s", symbol, exc)
