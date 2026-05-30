"""WebSocket market-data collector with sharding and reconnect.

Connects to Binance Futures combined streams, one ``@kline_<interval>`` stream
per symbol, sharded across multiple connections (each capped at
``settings.max_streams_per_connection``). The first shard also subscribes to
``!markPrice@arr`` (per the documented architecture); that payload is not yet
consumed by the detector and is ignored on receipt.

Each shard runs an independent reconnect loop with exponential backoff. The
warm-up in ``main`` seeds history before the first connect, so a shard only
re-backfills its symbols on a *reconnect* (never on the first connect). A
periodic monitor logs a heartbeat and flags stale (silent) shards.

Kline updates flow into the shared :class:`SymbolStore` on the read loop. When
a candle closes, the symbol is handed to a separate dispatch worker via a queue
so that a slow alert path (e.g. a hung Telegram POST) can never stall the
socket read loop and starve other symbols on the shard.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
from typing import Awaitable, Callable

import websockets
from websockets.exceptions import ConnectionClosed

from src.config import Settings
from src.store import Candle, SymbolStore

logger = logging.getLogger(__name__)

_MAX_BACKOFF = 60.0
_STABLE_AFTER = 30.0  # seconds connected before backoff is considered reset-worthy
_MAX_CONSECUTIVE_FAILURES = 5  # escalate to ERROR after this many connects that never succeed
_HEARTBEAT_INTERVAL = 60.0  # seconds between heartbeat logs
_STALE_FEED_SECONDS = 120.0  # a shard silent longer than this is flagged stale
_CLOSE_QUEUE_MAX = 10_000  # bound the candle-close dispatch queue

OnCandleClose = Callable[[str], Awaitable[None]] | Callable[[str], None]
BackfillFn = Callable[[str], Awaitable[list[Candle]]]


class WSCollector:
    """Collect klines over sharded combined WS streams, dispatch closes off-loop."""

    def __init__(
        self,
        settings: Settings,
        store: SymbolStore,
        symbols: list[str],
        on_candle_close: OnCandleClose,
        backfill_fn: BackfillFn | None = None,
    ) -> None:
        self._settings = settings
        self._store = store
        self._symbols = list(symbols)
        self._on_candle_close = on_candle_close
        self._backfill_fn = backfill_fn

        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self._close_queue: asyncio.Queue[str] = asyncio.Queue(maxsize=_CLOSE_QUEUE_MAX)

        # Liveness instrumentation.
        self._last_frame_at: dict[int, float] = {}
        self._frame_count = 0

        # Shard symbols into groups of at most max_streams_per_connection.
        cap = max(1, settings.max_streams_per_connection)
        self._shards: list[list[str]] = [
            self._symbols[i : i + cap] for i in range(0, len(self._symbols), cap)
        ] or [[]]

    # --- Public API ---------------------------------------------------------

    @property
    def frames_received(self) -> int:
        """Total frames received across all shards (monotonic; for liveness checks)."""
        return self._frame_count

    async def run(self) -> None:
        """Launch shard tasks, a dispatch worker, and a monitor; await them all."""
        self._stop.clear()
        self._tasks = [
            asyncio.create_task(self._run_shard(shard, idx, is_first=(idx == 0)), name=f"ws-shard-{idx}")
            for idx, shard in enumerate(self._shards)
        ]
        self._tasks.append(asyncio.create_task(self._dispatch_worker(), name="ws-dispatch"))
        self._tasks.append(asyncio.create_task(self._monitor(), name="ws-monitor"))
        try:
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            pass

    async def stop(self) -> None:
        """Signal graceful shutdown and cancel all tasks."""
        self._stop.set()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []

    # --- Internals ----------------------------------------------------------

    def _build_url(self, shard_symbols: list[str], is_first: bool) -> str:
        """Build a combined-stream URL for the shard's kline streams."""
        interval = self._settings.interval
        streams = [f"{s.lower()}@kline_{interval}" for s in shard_symbols]
        if is_first:
            streams.append("!markPrice@arr")
        path = "/".join(streams)
        return f"{self._settings.ws_base_url}/stream?streams={path}"

    async def _backfill_shard(self, shard_symbols: list[str]) -> None:
        """Re-seed the window of every symbol owned by this shard."""
        if self._backfill_fn is None:
            return
        for symbol in shard_symbols:
            try:
                candles = await self._backfill_fn(symbol)
                self._store.backfill(symbol, candles)
            except Exception as exc:  # noqa: BLE001 — backfill is best-effort
                logger.warning("backfill failed symbol=%s err=%s", symbol, exc)

    async def _run_shard(self, shard_symbols: list[str], shard_idx: int, is_first: bool) -> None:
        """Reconnect loop for a single shard with exponential backoff."""
        url = self._build_url(shard_symbols, is_first)
        backoff = 1.0
        loop = asyncio.get_running_loop()
        first_connect = True
        consecutive_failures = 0

        while not self._stop.is_set():
            connected = False
            connected_at: float | None = None
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    connected = True
                    consecutive_failures = 0
                    self._last_frame_at[shard_idx] = loop.time()
                    logger.info(
                        "ws connected shard=%d streams=%d first=%s",
                        shard_idx,
                        len(shard_symbols) + int(is_first),
                        is_first,
                    )
                    # main() already warmed up before the first connect, so only
                    # re-backfill on a RECONNECT (avoids a duplicate full backfill).
                    if not first_connect:
                        await self._backfill_shard(shard_symbols)
                    first_connect = False
                    connected_at = loop.time()
                    await self._read_loop(ws, shard_idx)
            except asyncio.CancelledError:
                raise
            except (ConnectionClosed, OSError) as exc:
                logger.warning("ws disconnected shard=%d err=%s", shard_idx, exc)
            except Exception as exc:  # noqa: BLE001 — keep the loop alive
                logger.warning("ws error shard=%d err=%s", shard_idx, exc)

            if self._stop.is_set():
                break

            if not connected:
                # Never established a connection this round — likely a bad URL,
                # DNS failure, or a persistent handshake rejection.
                consecutive_failures += 1
                if consecutive_failures == _MAX_CONSECUTIVE_FAILURES:
                    logger.error(
                        "ws shard=%d cannot connect (%d consecutive failures) — check ws_base_url / network",
                        shard_idx,
                        consecutive_failures,
                    )
            elif connected_at is not None and (loop.time() - connected_at) >= _STABLE_AFTER:
                # The connection we just had stayed up long enough — reset backoff.
                backoff = 1.0

            # Sleep before reconnecting, but wake immediately if stop fires.
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=backoff)
                break  # stop fired during backoff
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * 2, _MAX_BACKOFF)

        logger.info("ws shard stopped shard=%d", shard_idx)

    async def _read_loop(self, ws, shard_idx: int) -> None:
        """Read and dispatch messages until the socket closes or stop fires."""
        loop = asyncio.get_running_loop()
        while not self._stop.is_set():
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
            except asyncio.TimeoutError:
                # Periodic wake-up so we notice a stop request promptly.
                continue
            self._last_frame_at[shard_idx] = loop.time()
            self._frame_count += 1
            await self._handle_message(raw)

    async def _handle_message(self, raw: str | bytes) -> None:
        """Parse one combined-stream frame and dispatch by event type."""
        try:
            msg = json.loads(raw)
            data = msg["data"]
        except (ValueError, TypeError, KeyError) as exc:
            logger.debug("skip malformed frame err=%s", exc)
            return

        # !markPrice@arr arrives as a list; subscribed per the architecture but
        # not consumed by the detector — ignore it.
        if isinstance(data, list):
            return

        if isinstance(data, dict) and data.get("e") == "kline":
            self._handle_kline(data)
            return

        # Unknown payload shape — ignore quietly.
        logger.debug("skip unknown frame stream=%s", msg.get("stream"))

    def _handle_kline(self, data: dict) -> None:
        """Apply a kline update; enqueue the symbol when its candle closes."""
        try:
            k = data["k"]
            symbol = k["s"]
            candle = Candle(
                open_time=int(k["t"]),
                open=float(k["o"]),
                high=float(k["h"]),
                low=float(k["l"]),
                close=float(k["c"]),
                volume=float(k["v"]),
                is_closed=bool(k["x"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            logger.debug("skip malformed kline err=%s", exc)
            return

        if self._store.update(symbol, candle):
            self._enqueue_close(symbol)

    def _enqueue_close(self, symbol: str) -> None:
        """Hand a closed-candle symbol to the dispatch worker (non-blocking)."""
        try:
            self._close_queue.put_nowait(symbol)
        except asyncio.QueueFull:
            logger.warning("close-dispatch queue full; dropping candle-close symbol=%s", symbol)

    async def _dispatch_worker(self) -> None:
        """Drain the close queue and run the alert callback off the read loop."""
        while not self._stop.is_set():
            try:
                symbol = await asyncio.wait_for(self._close_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                await self._process_close(symbol)
            finally:
                self._close_queue.task_done()

    async def _process_close(self, symbol: str) -> None:
        """Invoke ``on_candle_close``, supporting sync and async callbacks."""
        try:
            result = self._on_candle_close(symbol)
            if inspect.isawaitable(result):
                await result
        except Exception as exc:  # noqa: BLE001 — a callback error must not kill the worker
            logger.warning("on_candle_close failed symbol=%s err=%s", symbol, exc)

    async def _monitor(self) -> None:
        """Periodically log a heartbeat and flag stale (silent) shards."""
        loop = asyncio.get_running_loop()
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=_HEARTBEAT_INTERVAL)
                break  # stop fired
            except asyncio.TimeoutError:
                pass
            now = loop.time()
            stale = [idx for idx, ts in self._last_frame_at.items() if now - ts > _STALE_FEED_SECONDS]
            logger.info(
                "ws heartbeat shards=%d frames=%d queued=%d stale=%s",
                len(self._shards),
                self._frame_count,
                self._close_queue.qsize(),
                stale or "none",
            )
            for idx in stale:
                logger.warning("ws shard=%d stale: no frames for %.0fs", idx, now - self._last_frame_at[idx])
