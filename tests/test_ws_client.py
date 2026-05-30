"""Offline unit tests for :mod:`src.ws_client`.

No network: frames are fed to the parse/dispatch path directly and the dispatch
worker is driven by hand. ``asyncio_mode = "auto"`` (pyproject) means async test
functions need no decorator.
"""

from __future__ import annotations

import asyncio
import json

from src.config import Settings
from src.store import SymbolStore
from src.ws_client import WSCollector


def _settings(**overrides) -> Settings:
    return Settings(_env_file=None, **overrides)


def _noop(_symbol: str) -> None:
    return None


def _kline_frame(symbol: str, close: float, is_closed: bool, *, open_time: int = 0, vol: float = 1.0) -> str:
    return json.dumps(
        {
            "stream": f"{symbol.lower()}@kline_1m",
            "data": {
                "e": "kline",
                "k": {
                    "t": open_time,
                    "s": symbol,
                    "o": str(close),
                    "h": str(close),
                    "l": str(close),
                    "c": str(close),
                    "v": str(vol),
                    "x": is_closed,
                },
            },
        }
    )


# --- sharding & URL building ------------------------------------------------


async def test_sharding_splits_at_cap() -> None:
    coll = WSCollector(
        _settings(max_streams_per_connection=2),
        SymbolStore(3),
        ["AUSDT", "BUSDT", "CUSDT", "DUSDT", "EUSDT"],
        _noop,
    )
    assert [len(s) for s in coll._shards] == [2, 2, 1]


async def test_empty_symbols_yields_single_shard() -> None:
    coll = WSCollector(_settings(), SymbolStore(3), [], _noop)
    assert coll._shards == [[]]


async def test_build_url_first_shard_has_markprice() -> None:
    s = _settings(interval="1m")
    coll = WSCollector(s, SymbolStore(3), ["BTCUSDT", "ETHUSDT"], _noop)

    first = coll._build_url(["BTCUSDT", "ETHUSDT"], is_first=True)
    assert first.startswith(f"{s.ws_base_url}/stream?streams=")
    assert "btcusdt@kline_1m/ethusdt@kline_1m" in first
    assert first.endswith("!markPrice@arr")

    non_first = coll._build_url(["BTCUSDT"], is_first=False)
    assert "markPrice" not in non_first


# --- message parsing & store/queue dispatch ---------------------------------


async def test_closed_kline_updates_store_and_enqueues() -> None:
    store = SymbolStore(2)
    coll = WSCollector(_settings(), store, ["BTCUSDT"], _noop)

    # In-progress candle: nothing enters the window, nothing enqueued.
    await coll._handle_message(_kline_frame("BTCUSDT", 100.0, is_closed=False))
    assert store.window("BTCUSDT") == []
    assert coll._close_queue.qsize() == 0

    # Closed candle: window updated and the symbol enqueued for dispatch.
    await coll._handle_message(_kline_frame("BTCUSDT", 101.0, is_closed=True, open_time=60_000))
    assert [c.close for c in store.window("BTCUSDT")] == [101.0]
    assert coll._close_queue.get_nowait() == "BTCUSDT"


async def test_markprice_payload_is_ignored() -> None:
    coll = WSCollector(_settings(), SymbolStore(2), ["BTCUSDT"], _noop)
    await coll._handle_message(json.dumps({"stream": "!markPrice@arr", "data": [{"s": "BTCUSDT", "p": "100"}]}))
    assert coll._close_queue.qsize() == 0  # no crash, nothing enqueued


async def test_malformed_frames_are_skipped() -> None:
    coll = WSCollector(_settings(), SymbolStore(2), ["BTCUSDT"], _noop)
    await coll._handle_message("not json")
    await coll._handle_message(json.dumps({"no": "data"}))
    await coll._handle_message(json.dumps({"stream": "x", "data": {"e": "kline"}}))  # missing "k"
    assert coll._close_queue.qsize() == 0


# --- callback dispatch ------------------------------------------------------


async def test_process_close_supports_sync_and_async_callbacks() -> None:
    sync_hits: list[str] = []
    coll_sync = WSCollector(_settings(), SymbolStore(2), ["BTCUSDT"], lambda s: sync_hits.append(s))
    await coll_sync._process_close("BTCUSDT")
    assert sync_hits == ["BTCUSDT"]

    async_hits: list[str] = []

    async def acb(symbol: str) -> None:
        async_hits.append(symbol)

    coll_async = WSCollector(_settings(), SymbolStore(2), ["ETHUSDT"], acb)
    await coll_async._process_close("ETHUSDT")
    assert async_hits == ["ETHUSDT"]


async def test_process_close_swallows_callback_errors() -> None:
    def boom(_symbol: str) -> None:
        raise RuntimeError("callback failed")

    coll = WSCollector(_settings(), SymbolStore(2), ["BTCUSDT"], boom)
    await coll._process_close("BTCUSDT")  # must not raise


async def test_dispatch_worker_drains_queue_and_fires_callback() -> None:
    fired: list[str] = []

    async def cb(symbol: str) -> None:
        fired.append(symbol)

    coll = WSCollector(_settings(), SymbolStore(2), ["BTCUSDT"], cb)
    coll._enqueue_close("BTCUSDT")

    worker = asyncio.create_task(coll._dispatch_worker())
    for _ in range(100):
        if fired:
            break
        await asyncio.sleep(0.01)
    coll._stop.set()
    await asyncio.wait_for(worker, timeout=2.0)

    assert fired == ["BTCUSDT"]
