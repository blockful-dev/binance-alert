"""Application entry point: warm-up -> collect -> detect -> notify.

Pipeline:
1. REST warm-up — fetch the tradable USDT-perpetual universe (liquidity
   filtered) and backfill each symbol's rolling window with historical klines.
2. WebSocket collection — subscribe to every ``<symbol>@kline_<interval>`` plus
   ``!markPrice@arr`` over sharded combined streams.
3. Detection — on each candle close, run :class:`~src.detector.TrendDetector`
   over the symbol's window.
4. Notification — emit cooldown-gated alerts to console (always) and Telegram
   (when configured).

Shuts down cleanly on SIGINT/SIGTERM and halts immediately on an IP ban (418).
"""

from __future__ import annotations

import asyncio
import logging
import signal

from src.config import Settings, get_settings
from src.detector import TrendDetector
from src.hybrid import HybridCollector
from src.notifier import Notifier
from src.rest_client import BinanceBanError, RestClient
from src.rest_poller import RestPollingCollector
from src.store import SymbolStore
from src.ws_client import WSCollector

logger = logging.getLogger(__name__)

_WARMUP_CONCURRENCY = 20  # max concurrent backfill requests (the bucket caps weight)


def setup_logging(level: str) -> None:
    """Configure structured key-value logging on the root logger."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s level=%(levelname)s logger=%(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )


async def _warmup(rest: RestClient, store: SymbolStore, symbols: list[str]) -> None:
    """Backfill every symbol's window via the rate-limited REST client.

    A non-ban error for a single symbol is logged and skipped (the WS feed will
    fill it in); a :class:`BinanceBanError` aborts the whole warm-up.
    """
    sem = asyncio.Semaphore(_WARMUP_CONCURRENCY)

    async def one(symbol: str) -> None:
        async with sem:
            try:
                candles = await rest.backfill(symbol)
                store.backfill(symbol, candles)
            except BinanceBanError:
                raise
            except Exception as exc:  # noqa: BLE001 — warm-up is best-effort per symbol
                logger.warning("warmup backfill failed symbol=%s err=%s", symbol, exc)

    await asyncio.gather(*(one(s) for s in symbols))
    ready = sum(1 for s in symbols if store.is_ready(s))
    logger.info("warmup complete symbols=%d ready=%d", len(symbols), ready)


def _build_collector(settings, store, symbols, on_close, rest):
    """Pick the data-source collector for ``settings.data_source``.

    All three expose the same ``run()`` / ``stop()`` interface.
    """
    if settings.data_source == "ws":
        return WSCollector(settings, store, symbols, on_close, backfill_fn=rest.backfill)
    if settings.data_source == "rest":
        return RestPollingCollector(settings, store, symbols, on_close, rest)
    return HybridCollector(settings, store, symbols, on_close, rest)


def _make_on_close(store: SymbolStore, detector: TrendDetector, notifier: Notifier):
    """Build the candle-close callback wiring detection to notification."""

    async def on_close(symbol: str) -> None:
        if not store.is_ready(symbol):
            return
        signal_ = detector.evaluate(symbol, store.window(symbol))
        if signal_ is not None:
            await notifier.notify(signal_)

    return on_close


async def main(settings: Settings | None = None) -> None:
    """Run the full pipeline until stopped."""
    settings = settings or get_settings()
    setup_logging(settings.log_level)
    logger.info(
        "starting interval=%s window=%d source=%s telegram=%s",
        settings.interval,
        settings.window_size,
        settings.data_source,
        settings.telegram_enabled,
    )

    rest = RestClient(settings)
    notifier = Notifier(settings)
    store = SymbolStore(settings.window_size)
    detector = TrendDetector(settings)
    collector: WSCollector | None = None

    try:
        symbols = await rest.get_trading_symbols()
        if not symbols:
            logger.warning("no symbols passed the universe filter; nothing to do")
            return
        await _warmup(rest, store, symbols)

        collector = _build_collector(
            settings, store, symbols, _make_on_close(store, detector, notifier), rest
        )

        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop_event.set)
            except NotImplementedError:  # pragma: no cover - non-Unix
                pass

        run_task = asyncio.create_task(collector.run(), name="ws-run")
        stop_task = asyncio.create_task(stop_event.wait(), name="stop-wait")
        await asyncio.wait({run_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
        if stop_event.is_set():
            logger.info("shutdown signal received")
        stop_task.cancel()
    except BinanceBanError:
        logger.critical("halting: Binance returned 418 (IP ban) — not retrying")
    finally:
        if collector is not None:
            await collector.stop()
        await rest.close()
        await notifier.close()
        logger.info("stopped")


def run() -> None:
    """Synchronous console-script entry point (``binance-trend-bot``)."""
    try:
        asyncio.run(main())
    except KeyboardInterrupt:  # pragma: no cover - interactive Ctrl-C
        pass


if __name__ == "__main__":
    run()
