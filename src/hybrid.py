"""Hybrid collector: WebSocket first, automatic REST-polling fallback.

Runs the :class:`~src.ws_client.WSCollector` and watches its frame counter. If
no frames arrive for ``settings.ws_stale_seconds`` (e.g. the host can open the
WS handshake but Binance never pushes data), it transparently activates the
:class:`~src.rest_poller.RestPollingCollector`. When WS frames resume it stops
polling again. Both feed the same store and callback; the store's ``open_time``
dedup keeps a brief overlap idempotent.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable

from src.config import Settings
from src.rest_client import RestClient
from src.rest_poller import RestPollingCollector
from src.store import SymbolStore
from src.ws_client import WSCollector

logger = logging.getLogger(__name__)

_CHECK_INTERVAL = 5.0  # how often the monitor re-evaluates WS health

OnCandleClose = Callable[[str], Awaitable[None]] | Callable[[str], None]


class HybridCollector:
    """Coordinate a WS collector and a REST poller with automatic failover."""

    def __init__(
        self,
        settings: Settings,
        store: SymbolStore,
        symbols: list[str],
        on_candle_close: OnCandleClose,
        rest_client: RestClient,
    ) -> None:
        self._settings = settings
        self._ws = WSCollector(settings, store, symbols, on_candle_close, backfill_fn=rest_client.backfill)
        self._poller = RestPollingCollector(settings, store, symbols, on_candle_close, rest_client)
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self._poll_task: asyncio.Task[None] | None = None
        self._polling = False
        # Health-tracking state (also used by the unit-tested _evaluate()).
        self._last_count = 0
        self._last_progress = 0.0

    async def run(self) -> None:
        """Run the WS collector plus the health monitor until stopped."""
        self._stop.clear()
        self._tasks = [
            asyncio.create_task(self._ws.run(), name="hybrid-ws"),
            asyncio.create_task(self._monitor(), name="hybrid-monitor"),
        ]
        try:
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            pass

    async def stop(self) -> None:
        """Stop the monitor, the WS collector, and any active REST polling."""
        self._stop.set()
        await self._deactivate_polling()
        await self._ws.stop()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []

    @property
    def polling(self) -> bool:
        """True while the REST fallback is active."""
        return self._polling

    def _evaluate(self, now: float, count: int) -> str:
        """Pure decision step: returns ``activate`` / ``deactivate`` / ``none``.

        ``now`` is a monotonic timestamp, ``count`` the WS frame counter.
        """
        if count > self._last_count:
            self._last_count = count
            self._last_progress = now
        stale = (now - self._last_progress) >= self._settings.ws_stale_seconds
        if stale and not self._polling:
            return "activate"
        if not stale and self._polling:
            return "deactivate"
        return "none"

    async def _monitor(self) -> None:
        loop = asyncio.get_running_loop()
        self._last_count = self._ws.frames_received
        self._last_progress = loop.time()
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=_CHECK_INTERVAL)
                break
            except asyncio.TimeoutError:
                pass
            action = self._evaluate(loop.time(), self._ws.frames_received)
            if action == "activate":
                logger.warning(
                    "WS feed stale (>= %.0fs no frames) — activating REST polling fallback",
                    self._settings.ws_stale_seconds,
                )
                await self._activate_polling()
            elif action == "deactivate":
                logger.info("WS feed recovered — stopping REST polling fallback")
                await self._deactivate_polling()

    async def _activate_polling(self) -> None:
        if self._polling:
            return
        self._poll_task = asyncio.create_task(self._poller.run(), name="hybrid-poller")
        self._polling = True

    async def _deactivate_polling(self) -> None:
        if not self._polling:
            return
        await self._poller.stop()
        if self._poll_task is not None:
            self._poll_task.cancel()
            await asyncio.gather(self._poll_task, return_exceptions=True)
            self._poll_task = None
        self._polling = False
