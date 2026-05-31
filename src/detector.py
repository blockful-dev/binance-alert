"""Trend detection engine.

Runs on each *closed* candle. A symbol is flagged as trending only when EVERY
enabled condition agrees on the same direction (logical AND) — this suppresses
the noisy single-indicator false positives that are common on minute candles:

1. Linear regression of ``log(close)``: slope sign -> direction, R² -> how
   cleanly the points sit on the trend line.
2. EMA alignment: ``close > EMA(fast) > EMA(slow)`` for an uptrend (reversed
   for a downtrend).
3. Cumulative rate-of-change over the window vs. a floor.
4. Net window move expressed in ATR (volatility) units vs. a floor — a
   volatility-normalized magnitude check, so the same threshold adapts to each
   symbol's natural range instead of a fixed percentage that means very
   different things on BTC vs. a low-cap alt.
5. Last-candle volume vs. the window-average volume.

Each condition is independently toggle-able via the matching ``use_*`` setting.
``slope``, ``r_squared``, ``roc`` and ``atr_move`` are always computed and
reported on the emitted :class:`Signal`; the toggles only control whether a
threshold is *enforced*.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np

from src.config import Settings
from src.store import Candle

logger = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class Signal:
    """A detected sustained trend, emitted when a candle closes."""

    symbol: str
    direction: str  # "up" | "down"
    slope: float  # regression slope of log(close); its sign sets ``direction``
    r_squared: float  # goodness-of-fit of the log-price trend line, in [0, 1]
    roc: float  # cumulative rate of change over the window (a fraction)
    atr_move: float  # net window move in ATR units (signed); volatility-normalized magnitude
    price: float  # last close
    timestamp: int  # last candle open_time, epoch milliseconds


def _ema(values: np.ndarray, period: int) -> float:
    """Return the final EMA of ``values`` (alpha = 2/(period+1), seeded at v[0])."""
    alpha = 2.0 / (period + 1.0)
    ema = float(values[0])
    for v in values[1:]:
        ema = alpha * float(v) + (1.0 - alpha) * ema
    return ema


def _atr(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray) -> float:
    """Mean True Range over the window, in price units.

    True Range for candle *i* is ``max(high-low, |high-prev_close|,
    |low-prev_close|)``; the first candle has no in-window predecessor so it
    falls back to ``high-low``. Returned as a simple mean (not Wilder-smoothed),
    which is sufficient for a single window-wide volatility estimate.
    """
    n = len(closes)
    if n == 0:
        return 0.0
    tr = np.empty(n, dtype=float)
    tr[0] = highs[0] - lows[0]
    if n > 1:
        prev_close = closes[:-1]
        hl = highs[1:] - lows[1:]
        hc = np.abs(highs[1:] - prev_close)
        lc = np.abs(lows[1:] - prev_close)
        tr[1:] = np.maximum(hl, np.maximum(hc, lc))
    return float(tr.mean())


class TrendDetector:
    """Evaluate a symbol's rolling window for a clean, sustained trend."""

    def __init__(self, settings: Settings) -> None:
        self._s = settings

    def evaluate(self, symbol: str, candles: list[Candle]) -> Signal | None:
        """Return a :class:`Signal` if ``candles`` form a clean trend, else None.

        Operates on the most recent ``window_size`` candles. Returns ``None`` if
        there is insufficient history or any enabled condition fails (or the
        conditions disagree on direction).
        """
        s = self._s
        if len(candles) < s.window_size:
            return None
        window = candles[-s.window_size :]
        closes = np.array([c.close for c in window], dtype=float)
        if np.any(closes <= 0.0):
            # log() is undefined for non-positive prices; treat as no signal.
            return None
        volumes = np.array([c.volume for c in window], dtype=float)

        # --- 1. Log-price linear regression: slope (direction) + R² ---
        x = np.arange(len(closes), dtype=float)
        log_close = np.log(closes)
        slope_arr, intercept = np.polyfit(x, log_close, 1)
        slope = float(slope_arr)
        if slope == 0.0:
            return None  # perfectly flat -> no direction.
        direction = "up" if slope > 0.0 else "down"

        fitted = slope * x + intercept
        ss_res = float(np.sum((log_close - fitted) ** 2))
        ss_tot = float(np.sum((log_close - log_close.mean()) ** 2))
        r_squared = 0.0 if ss_tot == 0.0 else 1.0 - ss_res / ss_tot

        if s.use_r_squared and r_squared < s.r_squared_min:
            return None

        # --- 2. EMA alignment ---
        if s.use_ema:
            ema_fast = _ema(closes, s.ema_fast)
            ema_slow = _ema(closes, s.ema_slow)
            last = float(closes[-1])
            aligned = (
                last > ema_fast > ema_slow
                if direction == "up"
                else last < ema_fast < ema_slow
            )
            if not aligned:
                return None

        # --- 3. Cumulative ROC over the window ---
        roc = float(closes[-1] / closes[0] - 1.0)
        if s.use_roc:
            if abs(roc) < s.roc_min:
                return None
            if (roc > 0.0) != (direction == "up"):
                return None  # ROC sign disagrees with the slope direction.

        # --- 4. ATR-normalized magnitude: net move measured in volatility units ---
        highs = np.array([c.high for c in window], dtype=float)
        lows = np.array([c.low for c in window], dtype=float)
        atr = _atr(highs, lows, closes)
        net_move = float(closes[-1] - closes[0])
        atr_move = net_move / atr if atr > 0.0 else 0.0
        if s.use_atr:
            if atr <= 0.0 or abs(atr_move) < s.atr_mult:
                return None  # move too small relative to the symbol's own range.
            if (atr_move > 0.0) != (direction == "up"):
                return None  # net move disagrees with the slope direction.

        # --- 5. Volume confirmation ---
        if s.use_volume:
            mean_vol = float(volumes.mean())
            if mean_vol <= 0.0 or volumes[-1] < s.volume_mult * mean_vol:
                return None

        return Signal(
            symbol=symbol,
            direction=direction,
            slope=slope,
            r_squared=r_squared,
            roc=roc,
            atr_move=atr_move,
            price=float(closes[-1]),
            timestamp=window[-1].open_time,
        )
