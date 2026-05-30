"""Binance USDT-M Futures REST client with a weight-based rate limiter.

The :class:`TokenBucket` models Binance's per-IP ``REQUEST_WEIGHT`` budget: a
fixed ``capacity`` of weight units replenishes continuously over a 60s window.
Every request acquires its documented weight *before* being sent, and the
bucket is reconciled against the server's ``X-MBX-USED-WEIGHT-1M`` response
header so our local view stays aligned with Binance's authoritative count.

:class:`RestClient` exposes the two calls the bot needs at start-up: the
trading-symbol universe (filtered by contract type + 24h liquidity) and the
historical-kline backfill that seeds :class:`~src.store.SymbolStore`.

On a 418 (IP auto-ban) we raise :class:`BinanceBanError` immediately and do not
retry, to avoid extending the ban.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Awaitable, Callable

import aiohttp

from src.config import Settings
from src.store import Candle

logger = logging.getLogger(__name__)

_REFILL_WINDOW_SECONDS = 60.0
_MAX_429_RETRIES = 5  # give up after this many consecutive 429s on one request
_RETRY_AFTER_FLOOR = 0.5  # minimum backoff so a missing/zero Retry-After can't tight-loop


class BinanceBanError(Exception):
    """Raised on HTTP 418 — the IP has been auto-banned by Binance."""


class TokenBucket:
    """Continuous-refill weight limiter for the REQUEST_WEIGHT budget.

    The full ``capacity`` replenishes every 60 seconds, i.e. at
    ``capacity / 60`` tokens per second. :meth:`acquire` blocks (via the
    injected ``sleep_fn``) until enough tokens are available; once usage passes
    ``throttle_ratio`` it adds a small pre-emptive delay to slow the caller.
    """

    def __init__(
        self,
        capacity: int,
        *,
        throttle_ratio: float = 0.8,
        time_fn: Callable[[], float] = time.monotonic,
        sleep_fn: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        self._capacity = float(capacity)
        self._throttle_ratio = throttle_ratio
        self._time_fn = time_fn
        self._sleep_fn = sleep_fn
        self._refill_rate = self._capacity / _REFILL_WINDOW_SECONDS
        self._tokens = self._capacity
        self._updated = time_fn()

    def _refill(self) -> None:
        """Add tokens accrued since the last update, capped at capacity."""
        now = self._time_fn()
        elapsed = now - self._updated
        if elapsed > 0:
            self._tokens = min(self._capacity, self._tokens + elapsed * self._refill_rate)
            self._updated = now

    async def acquire(self, weight: int) -> None:
        """Block until ``weight`` tokens are available, then consume them.

        When the post-consumption used fraction would exceed ``throttle_ratio``
        an extra delay is added to pre-emptively slow the request rate.
        """
        if weight <= 0:
            return
        if weight > self._capacity:
            # Unsatisfiable forever — surface the misconfiguration instead of
            # spinning (e.g. rest_weight_limit set below a request's weight).
            raise ValueError(
                f"request weight {weight} exceeds bucket capacity {self._capacity:.0f}"
            )
        self._refill()
        # Wait until we have enough tokens (refilling as time passes).
        while self._tokens < weight:
            deficit = weight - self._tokens
            wait = deficit / self._refill_rate
            await self._sleep_fn(wait)
            self._refill()
        self._tokens -= weight

        used_fraction = 1.0 - self._tokens / self._capacity
        if used_fraction >= self._throttle_ratio:
            # Pre-emptive throttle: how far past the threshold we are, expressed
            # as the time needed to refill back down to the threshold.
            over_tokens = (used_fraction - self._throttle_ratio) * self._capacity
            extra = over_tokens / self._refill_rate
            await self._sleep_fn(extra)
            self._refill()

    def sync_from_header(self, used_weight: int) -> None:
        """Reconcile local tokens to ``capacity - used_weight`` (clamped)."""
        self._refill()
        tokens = self._capacity - float(used_weight)
        self._tokens = max(0.0, min(self._capacity, tokens))

    @property
    def tokens(self) -> float:
        """Currently available tokens (after refilling to *now*)."""
        self._refill()
        return self._tokens


class RestClient:
    """Thin async wrapper over the Binance Futures REST endpoints we use."""

    def __init__(
        self,
        settings: Settings,
        session: aiohttp.ClientSession | None = None,
        *,
        sleep_fn: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._settings = settings
        self._session = session
        self._owns_session = session is None
        self._sleep_fn = sleep_fn
        self._bucket = TokenBucket(
            settings.rest_weight_limit,
            throttle_ratio=settings.rest_weight_throttle_ratio,
            sleep_fn=sleep_fn,
        )

    @property
    def bucket(self) -> TokenBucket:
        return self._bucket

    def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None,
        weight: int,
    ) -> Any:
        """Send a rate-limited request, retrying once on 429, raising on 418."""
        session = self._ensure_session()
        url = self._settings.rest_base_url + path
        # Pay the weight once up front; a 429 retry must not re-charge it (the
        # response header reconciles our bucket to the server's true usage).
        await self._bucket.acquire(weight)
        attempts_429 = 0
        while True:
            async with session.request(method, url, params=params) as resp:
                used = resp.headers.get("X-MBX-USED-WEIGHT-1M")
                if used is not None:
                    try:
                        self._bucket.sync_from_header(int(used))
                    except ValueError:
                        logger.warning("bad X-MBX-USED-WEIGHT-1M header value=%r", used)

                status = resp.status
                if status == 429:
                    attempts_429 += 1
                    if attempts_429 > _MAX_429_RETRIES:
                        raise RuntimeError(
                            f"persistent HTTP 429 from {path} after {_MAX_429_RETRIES} retries"
                        )
                    retry_after = max(
                        _RETRY_AFTER_FLOOR, _parse_retry_after(resp.headers.get("Retry-After"))
                    )
                    logger.warning(
                        "rate limited path=%s status=429 retry_after=%.1f attempt=%d",
                        path,
                        retry_after,
                        attempts_429,
                    )
                    await self._sleep_fn(retry_after)
                    continue
                if status == 418:
                    logger.critical("IP banned path=%s status=418", path)
                    raise BinanceBanError(f"HTTP 418 from {path}")
                if 200 <= status < 300:
                    return await resp.json()

                body = (await resp.text())[:200]
                raise RuntimeError(f"HTTP {status} from {path}: {body}")

    async def get_trading_symbols(self) -> list[str]:
        """Return TRADING USDT-perpetual symbols above the 24h volume floor.

        Intersects the exchange-info universe (status/contractType/quoteAsset)
        with the 24h ticker universe (quoteVolume >= ``min_quote_volume_24h``),
        returning the sorted intersection.
        """
        info = await self._request("GET", "/fapi/v1/exchangeInfo", None, weight=1)
        tradable: set[str] = set()
        for s in info.get("symbols", []):
            if (
                s.get("status") == "TRADING"
                and s.get("contractType") == "PERPETUAL"
                and s.get("quoteAsset") == "USDT"
            ):
                tradable.add(s["symbol"])

        tickers = await self._request("GET", "/fapi/v1/ticker/24hr", None, weight=40)
        liquid: set[str] = set()
        floor = self._settings.min_quote_volume_24h
        for t in tickers:
            try:
                qv = float(t.get("quoteVolume", 0.0))
            except (TypeError, ValueError):
                continue
            if qv >= floor:
                liquid.add(t["symbol"])

        symbols = sorted(tradable & liquid)
        logger.info(
            "symbol universe tradable=%d liquid=%d selected=%d",
            len(tradable),
            len(liquid),
            len(symbols),
        )
        return symbols

    async def _fetch_klines(self, symbol: str, limit: int) -> list[Candle]:
        """Fetch ``limit`` klines for ``symbol`` and return only CLOSED candles.

        With no time range the LAST kline is the currently-forming candle (the
        WS @kline stream keeps updating it); it is dropped so callers only ever
        see closed candles, oldest -> newest.
        """
        weight = 1 if limit <= 100 else 2 if limit <= 500 else 5
        params = {"symbol": symbol, "interval": self._settings.interval, "limit": limit}
        raw = await self._request("GET", "/fapi/v1/klines", params, weight=weight)
        return [
            Candle(
                open_time=int(a[0]),
                open=float(a[1]),
                high=float(a[2]),
                low=float(a[3]),
                close=float(a[4]),
                volume=float(a[5]),
                is_closed=True,
            )
            for a in raw[:-1]
        ]

    async def backfill(self, symbol: str) -> list[Candle]:
        """Fetch ``warmup_klines`` historical closed candles for ``symbol``.

        Ready for :meth:`~src.store.SymbolStore.backfill`.
        """
        return await self._fetch_klines(symbol, self._settings.warmup_klines)

    async def recent_klines(self, symbol: str, limit: int = 3) -> list[Candle]:
        """Fetch the most recent closed candles for ``symbol`` (REST polling).

        ``limit`` is small (default 3 -> 2 closed candles, enough to cover a
        missed cycle); the rolling store deduplicates by ``open_time``.
        """
        return await self._fetch_klines(symbol, limit)

    async def close(self) -> None:
        """Close the underlying session if this client created it."""
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None


def _parse_retry_after(value: str | None, *, default: float = 1.0) -> float:
    """Parse a ``Retry-After`` header (seconds) into a float, with a default."""
    if value is None:
        return default
    try:
        return max(0.0, float(value))
    except ValueError:
        return default
