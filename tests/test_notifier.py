"""Offline sanity tests for :mod:`src.notifier`.

Telegram is disabled (default settings) so no network is touched. Timing is
driven by an injected mutable ``now`` so cooldown logic is tested instantly.
"""

from __future__ import annotations

from dataclasses import dataclass

from src.config import Settings
from src.notifier import Notifier


@dataclass(frozen=True)
class _FakeSignal:
    """Duck-typed stand-in for ``src.detector.Signal`` (read-only attrs)."""

    symbol: str
    direction: str
    slope: float
    r_squared: float
    roc: float
    price: float
    timestamp: int


def _signal(direction: str = "up") -> _FakeSignal:
    return _FakeSignal(
        symbol="BTCUSDT",
        direction=direction,
        slope=0.0123,
        r_squared=0.91,
        roc=0.012,
        price=65000.0,
        timestamp=1_700_000_000_000,
    )


def _settings(cooldown_minutes: int = 15) -> Settings:
    # No telegram token -> telegram disabled -> console-only path.
    # _env_file=None keeps the test hermetic against a developer's local .env.
    return Settings(_env_file=None, cooldown_minutes=cooldown_minutes)


async def test_cooldown_suppresses_then_allows_after_window() -> None:
    clock = {"now": 1000.0}
    notifier = Notifier(_settings(cooldown_minutes=15), time_fn=lambda: clock["now"])

    # First alert emitted.
    assert await notifier.notify(_signal("up")) is True
    # Immediate identical-direction repeat is suppressed.
    assert await notifier.notify(_signal("up")) is False

    # Advance just past the 15-minute cooldown (900s).
    clock["now"] += 15 * 60 + 1
    assert await notifier.notify(_signal("up")) is True

    await notifier.close()


async def test_opposite_direction_not_suppressed() -> None:
    clock = {"now": 0.0}
    notifier = Notifier(_settings(cooldown_minutes=15), time_fn=lambda: clock["now"])

    assert await notifier.notify(_signal("up")) is True
    # Same symbol, opposite direction -> independent cooldown key.
    assert await notifier.notify(_signal("down")) is True
    # ...but the "down" direction is now itself on cooldown.
    assert await notifier.notify(_signal("down")) is False

    await notifier.close()
