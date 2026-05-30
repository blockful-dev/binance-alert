"""Offline tests for the hybrid collector's failover decision logic."""

from __future__ import annotations

from src.config import Settings
from src.hybrid import HybridCollector
from src.store import SymbolStore


class FakeRest:
    async def backfill(self, symbol: str):
        return []

    async def recent_klines(self, symbol: str, limit: int = 3):
        return []


def _hybrid(stale_seconds: float = 30.0) -> HybridCollector:
    s = Settings(_env_file=None, ws_stale_seconds=stale_seconds)
    return HybridCollector(s, SymbolStore(5), ["BTCUSDT"], lambda x: None, FakeRest())


async def test_evaluate_activates_when_ws_stale_and_recovers() -> None:
    h = _hybrid(stale_seconds=30.0)
    h._last_count = 0
    h._last_progress = 0.0

    # 10s in, still 0 frames — below the 30s threshold → no action.
    assert h._evaluate(10.0, 0) == "none"
    assert h._evaluate(40.0, 0) == "activate"  # 40s with no frames → fall back to REST

    h._polling = True  # (the monitor would have set this)
    assert h._evaluate(41.0, 5) == "deactivate"  # frames resumed → recover to WS

    h._polling = False
    assert h._evaluate(45.0, 9) == "none"  # frames flowing, not polling → steady state


async def test_evaluate_no_flap_while_frames_flow() -> None:
    h = _hybrid(stale_seconds=10.0)
    h._last_count = 0
    h._last_progress = 0.0
    # Frames increase every check — never goes stale.
    assert h._evaluate(5.0, 3) == "none"
    assert h._evaluate(10.0, 7) == "none"
    assert h._evaluate(20.0, 11) == "none"
    assert h.polling is False
