"""Unit tests for :mod:`src.detector`. Pure, deterministic, no network."""

from __future__ import annotations

import math

import numpy as np

from src.config import Settings
from src.detector import Signal, TrendDetector
from src.store import Candle

WINDOW = 30


def make_settings(**overrides) -> Settings:
    """Build a Settings with explicit, env-independent defaults for tests."""
    base = dict(
        interval="1m",
        window_size=WINDOW,
        r_squared_min=0.75,
        ema_fast=9,
        ema_slow=21,
        roc_min=0.008,
        volume_mult=1.2,
        use_r_squared=True,
        use_ema=True,
        use_roc=True,
        use_volume=True,
        min_quote_volume_24h=10_000_000,
        cooldown_minutes=15,
        telegram_bot_token=None,
        telegram_chat_id=None,
        rest_base_url="https://fapi.binance.com",
        ws_base_url="wss://fstream.binance.com",
        rest_weight_limit=2400,
        rest_weight_throttle_ratio=0.8,
        warmup_klines=50,
        max_streams_per_connection=200,
        log_level="INFO",
    )
    base.update(overrides)
    return Settings(_env_file=None, **base)


def make_candles(closes, volumes=None) -> list[Candle]:
    """Build a list of closed candles from ``closes`` (and optional ``volumes``).

    ``open_time`` is a deterministic per-index millisecond stamp. OHLC are
    derived simply from the close (only ``close``/``volume`` matter to the
    detector).
    """
    if volumes is None:
        volumes = [100.0] * len(closes)
    candles: list[Candle] = []
    for i, (c, v) in enumerate(zip(closes, volumes)):
        candles.append(
            Candle(
                open_time=1_000 + i * 60_000,
                open=float(c),
                high=float(c),
                low=float(c),
                close=float(c),
                volume=float(v),
                is_closed=True,
            )
        )
    return candles


def _vol_with_spike(n: int, base: float = 100.0, spike: float = 1000.0) -> list[float]:
    """Flat baseline volume with a spike on the last candle."""
    vols = [base] * n
    vols[-1] = spike
    return vols


def test_clean_uptrend_fires_up() -> None:
    closes = [100.0 * math.exp(0.01 * i) for i in range(WINDOW)]
    candles = make_candles(closes, volumes=_vol_with_spike(WINDOW))
    det = TrendDetector(make_settings())

    sig = det.evaluate("BTCUSDT", candles)

    assert isinstance(sig, Signal)
    assert sig.direction == "up"
    assert sig.r_squared > 0.99
    assert sig.roc > 0
    assert sig.symbol == "BTCUSDT"
    assert sig.timestamp == candles[-1].open_time
    assert sig.price == candles[-1].close


def test_clean_downtrend_fires_down() -> None:
    closes = [100.0 * math.exp(-0.01 * i) for i in range(WINDOW)]
    candles = make_candles(closes, volumes=_vol_with_spike(WINDOW))
    det = TrendDetector(make_settings())

    sig = det.evaluate("ETHUSDT", candles)

    assert isinstance(sig, Signal)
    assert sig.direction == "down"
    assert sig.roc < 0
    assert sig.r_squared > 0.99


def test_sideways_noise_returns_none() -> None:
    rng = np.random.default_rng(42)
    closes = (100.0 + rng.normal(0.0, 0.05, WINDOW)).tolist()
    candles = make_candles(closes, volumes=_vol_with_spike(WINDOW))
    det = TrendDetector(make_settings())

    assert det.evaluate("XRPUSDT", candles) is None


def test_too_few_candles_returns_none() -> None:
    closes = [100.0 * math.exp(0.01 * i) for i in range(WINDOW - 1)]
    candles = make_candles(closes, volumes=_vol_with_spike(WINDOW - 1))
    det = TrendDetector(make_settings())

    assert det.evaluate("BTCUSDT", candles) is None


def test_volume_toggle_interaction() -> None:
    # Clean uptrend, but the last candle has LOW (baseline) volume — no spike.
    closes = [100.0 * math.exp(0.01 * i) for i in range(WINDOW)]
    flat_vol = [100.0] * WINDOW
    candles = make_candles(closes, volumes=flat_vol)

    # use_volume=True -> volume condition fails -> None.
    det_vol = TrendDetector(make_settings(use_volume=True))
    assert det_vol.evaluate("BTCUSDT", candles) is None

    # use_volume=False -> volume condition skipped -> Signal fires.
    det_no_vol = TrendDetector(make_settings(use_volume=False))
    sig = det_no_vol.evaluate("BTCUSDT", candles)
    assert isinstance(sig, Signal)
    assert sig.direction == "up"


def test_r_squared_gate_rejects_choppy_rise() -> None:
    # Net-rising but choppy series so R^2 falls below the threshold.
    base = [100.0 * math.exp(0.004 * i) for i in range(WINDOW)]
    # Alternating zig-zag wobble large enough to wreck the linear R^2 on logs,
    # while keeping a positive overall slope and ROC.
    wobble = [4.0 if i % 2 == 0 else -4.0 for i in range(WINDOW)]
    closes = [b + w for b, w in zip(base, wobble)]
    candles = make_candles(closes, volumes=_vol_with_spike(WINDOW))

    det = TrendDetector(make_settings())
    # Confirm the premise: R^2 really is below the gate.
    log_close = np.log(np.array(closes))
    x = np.arange(WINDOW, dtype=float)
    slope, intercept = np.polyfit(x, log_close, 1)
    fitted = slope * x + intercept
    ss_res = float(np.sum((log_close - fitted) ** 2))
    ss_tot = float(np.sum((log_close - log_close.mean()) ** 2))
    r2 = 1.0 - ss_res / ss_tot
    assert r2 < 0.75

    assert det.evaluate("BTCUSDT", candles) is None
