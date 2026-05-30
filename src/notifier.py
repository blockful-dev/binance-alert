"""Alert delivery with per-symbol+direction cooldown.

The :class:`Notifier` turns a detector :class:`~src.detector.Signal` into a
human-readable alert. Every signal is logged to the console; when Telegram is
configured it is additionally pushed via the Bot API. Repeat alerts for the
same ``(symbol, direction)`` within ``cooldown_minutes`` are suppressed.

Network and timing are kept injectable so the unit tests run without real I/O
or real waits. The Telegram bot token is never logged.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Callable

import aiohttp

from src.config import Settings
from src.detector import Signal

logger = logging.getLogger(__name__)

_TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"
_TELEGRAM_TIMEOUT = 5.0  # seconds; a hung Telegram must not stall alerting
_DIRECTION_MARKER = {"up": "\U0001F7E2▲", "down": "\U0001F534▼"}


class Notifier:
    """Emit alerts for trend signals, with per-symbol+direction cooldown."""

    def __init__(
        self,
        settings: Settings,
        session: aiohttp.ClientSession | None = None,
        *,
        time_fn: Callable[[], float] = time.monotonic,
    ) -> None:
        self._settings = settings
        self._session = session
        self._owns_session = session is None
        self._time_fn = time_fn
        # (symbol, direction) -> monotonic time of last emitted alert.
        self._last_sent: dict[tuple[str, str], float] = {}

    async def notify(self, signal: Signal) -> bool:
        """Emit an alert for ``signal`` unless it is within cooldown.

        Returns ``True`` when an alert was emitted, ``False`` when suppressed by
        the per-``(symbol, direction)`` cooldown. Never raises on send errors.
        """
        key = (signal.symbol, signal.direction)
        now = self._time_fn()
        last = self._last_sent.get(key)
        cooldown = self._settings.cooldown_minutes * 60.0
        if last is not None and (now - last) < cooldown:
            logger.debug(
                "alert suppressed (cooldown) symbol=%s direction=%s",
                signal.symbol,
                signal.direction,
            )
            return False

        self._last_sent[key] = now
        logger.info(
            "signal symbol=%s direction=%s slope=%.6f r_squared=%.3f "
            "roc=%.4f price=%s timestamp=%d",
            signal.symbol,
            signal.direction,
            signal.slope,
            signal.r_squared,
            signal.roc,
            signal.price,
            signal.timestamp,
        )

        if self._settings.telegram_enabled:
            await self._send_telegram(self._format(signal))
        return True

    async def _send_telegram(self, text: str) -> None:
        """POST ``text`` to Telegram; log and swallow any failure."""
        session = self._ensure_session()
        url = _TELEGRAM_API.format(token=self._settings.telegram_bot_token)
        payload = {
            "chat_id": self._settings.telegram_chat_id,
            "text": text,
            "parse_mode": "HTML",
        }
        try:
            timeout = aiohttp.ClientTimeout(total=_TELEGRAM_TIMEOUT)
            async with session.post(url, json=payload, timeout=timeout) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning(
                        "telegram send failed status=%d body=%s", resp.status, body[:200]
                    )
        except Exception as exc:  # noqa: BLE001 - never let alerting crash the app.
            logger.warning("telegram send error: %s", exc)

    def _ensure_session(self) -> aiohttp.ClientSession:
        """Lazily create the owned session on first Telegram send."""
        if self._session is None:
            self._session = aiohttp.ClientSession()
            self._owns_session = True
        return self._session

    def _format(self, signal: Signal) -> str:
        """Render a tidy HTML message for ``signal``."""
        marker = _DIRECTION_MARKER.get(signal.direction, signal.direction)
        when = datetime.fromtimestamp(signal.timestamp / 1000, tz=timezone.utc)
        when_str = when.strftime("%Y-%m-%d %H:%M:%S UTC")
        return (
            f"{marker} <b>{signal.symbol}</b> {signal.direction.upper()}\n"
            f"price: <code>{signal.price:g}</code>\n"
            f"roc: <code>{signal.roc * 100:+.2f}%</code>\n"
            f"slope: <code>{signal.slope:+.6f}</code>  "
            f"R²: <code>{signal.r_squared:.3f}</code>\n"
            f"<i>{when_str}</i>"
        )

    async def close(self) -> None:
        """Close the session only if this notifier created it."""
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None
