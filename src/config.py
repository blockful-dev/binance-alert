"""Application configuration.

All tunables live here as a single :class:`Settings` model. Values may be
overridden via environment variables or a local ``.env`` file (see
``.env.example``). Field names map to upper-case env vars
(e.g. ``r_squared_min`` <- ``R_SQUARED_MIN``).

No secrets are required for market-data collection; ``telegram_*`` are the only
optional credentials and are never logged.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Strongly-typed, env-overridable configuration."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- Candle / data ---
    interval: str = Field(default="1m", description="Kline interval, e.g. 1m/3m/5m.")
    window_size: int = Field(default=30, ge=5, description="Rolling window length (closed candles).")

    # --- Detection thresholds ---
    r_squared_min: float = Field(default=0.75, ge=0.0, le=1.0, description="Min R^2 for a 'clean' trend.")
    ema_fast: int = Field(default=9, ge=1, description="Fast EMA period.")
    ema_slow: int = Field(default=21, ge=2, description="Slow EMA period.")
    roc_min: float = Field(default=0.008, ge=0.0, description="Min absolute cumulative ROC over window (fraction).")
    atr_mult: float = Field(
        default=3.0, ge=0.0, description="Min net move in ATR (volatility) units; adapts magnitude to each symbol's range."
    )
    volume_mult: float = Field(default=1.2, ge=0.0, description="Min last-candle volume / window-avg volume.")

    # --- Detection condition toggles ---
    use_r_squared: bool = True
    use_ema: bool = True
    use_roc: bool = True
    use_atr: bool = True
    use_volume: bool = True

    # --- Universe liquidity filter ---
    min_quote_volume_24h: float = Field(
        default=500_000, ge=0.0, description="Drop symbols below this 24h quote volume (USDT)."
    )

    # --- Alerting ---
    cooldown_minutes: int = Field(default=15, ge=0, description="Per-symbol+direction alert cooldown.")
    telegram_bot_token: str | None = Field(default=None, repr=False)
    telegram_chat_id: str | None = Field(default=None, repr=False)

    # --- Connectivity / limits ---
    rest_base_url: str = "https://fapi.binance.com"
    ws_base_url: str = "wss://fstream.binance.com"
    rest_weight_limit: int = Field(default=2400, ge=1, description="IP REQUEST_WEIGHT budget per minute.")
    rest_weight_throttle_ratio: float = Field(
        default=0.8, gt=0.0, le=1.0, description="Pre-emptively throttle once usage exceeds this fraction."
    )
    warmup_klines: int = Field(default=50, ge=1, description="Number of historical klines to backfill per symbol.")
    max_streams_per_connection: int = Field(
        default=200, ge=1, le=1024, description="Streams per WS connection (sharding)."
    )

    # --- Data source / fallback ---
    data_source: Literal["ws", "rest", "hybrid"] = Field(
        default="hybrid",
        description="ws: WebSocket only · rest: REST polling only · hybrid: WS with automatic REST fallback.",
    )
    ws_stale_seconds: float = Field(
        default=30.0, gt=0.0, description="Hybrid: activate REST polling after this many seconds with no WS frames."
    )

    # --- Logging ---
    log_level: str = "INFO"

    @model_validator(mode="after")
    def _validate(self) -> "Settings":
        if self.ema_fast >= self.ema_slow:
            raise ValueError("ema_fast must be < ema_slow")
        if self.warmup_klines <= self.window_size:
            # The most recent REST kline is the still-forming candle and is
            # dropped on backfill, so we need strictly more than a full window
            # to leave a complete window of closed candles.
            raise ValueError("warmup_klines must be > window_size")
        if self.telegram_bot_token and not self.telegram_chat_id:
            raise ValueError("telegram_chat_id is required when telegram_bot_token is set")
        return self

    @property
    def telegram_enabled(self) -> bool:
        """True only when both token and chat id are present."""
        return bool(self.telegram_bot_token and self.telegram_chat_id)


@lru_cache
def get_settings() -> Settings:
    """Return a process-wide cached :class:`Settings` instance."""
    return Settings()
