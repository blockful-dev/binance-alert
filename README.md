# Binance Trend Bot

Real-time **trend screener** for Binance USDT-M perpetual futures. It monitors
every liquid perpetual over WebSocket and flags symbols that are **sustained
trending** — moving cleanly in one direction — emitting an alert to the console
and (optionally) Telegram.

> ⚠️ This is a **monitoring/screener tool only**. It does **not** place orders or
> manage positions. A detected trend is not a guarantee of profit — trend
> indicators lag, so by the time a signal fires the move may be well underway.
> All trading decisions and outcomes are your own responsibility. Comply with
> Binance's API terms and rate-limit policy.

## How it works

```
Binance USDT-M Futures (perpetuals)
        │  WebSocket push
        ▼
WSCollector ──  <symbol>@kline_<interval>   (+ !markPrice@arr, subscribed but unused)
   │            sharded across connections · auto-reconnect · heartbeat / stale-feed monitor
   │  closed candle → dispatch queue
   ▼
SymbolStore ── per-symbol rolling window of CLOSED candles (deque)
   ▲                          │  worker drains the queue; on each candle close:
   │ REST warm-up (once)      ▼
RestClient ──────────────►  TrendDetector ── slope+R² / EMA / ROC / ATR / volume  (AND)
 (exchangeInfo, ticker/24hr,    │
  klines; token-bucket)         ▼
                            Notifier ── 15m cooldown → console + Telegram (5s timeout)
```

- **All live prices arrive via WebSocket push** (no REST polling) — low latency,
  minimal rate-limit usage.
- REST is used **only at start-up**: to select symbols and backfill history.
  Every REST call passes through a **token-bucket rate limiter** that respects
  the IP `REQUEST_WEIGHT` budget, syncs to the `X-MBX-USED-WEIGHT-1M` header,
  backs off on `429` (bounded retries), and **stops immediately on `418`** (IP
  ban) without retrying.
- Detection runs **only when a candle closes** (`kline` `x` flag). In-progress
  candles are tracked separately and never used for a decision. (The most recent
  REST kline — the still-forming candle — is dropped on backfill.)
- Alert delivery is **decoupled from the socket read loop** via a dispatch queue,
  so a slow/hung Telegram can never stall the feed for other symbols.

## Symbol universe (liquidity filter)

At start-up the bot picks the symbols to watch by intersecting two REST calls:

1. **`GET /fapi/v1/exchangeInfo`** → keep symbols with `status == TRADING`,
   `contractType == PERPETUAL`, and `quoteAsset == USDT`.
2. **`GET /fapi/v1/ticker/24hr`** → keep symbols whose **`quoteVolume`** (24h
   turnover in USDT) `≥ MIN_QUOTE_VOLUME_24H` (default **500,000**).

The intersection is the subscribed universe. It is a **start-up snapshot** — to
pick up newly-listed perpetuals (or drop delisted ones), restart the process.
The selected count is logged at start-up: `symbol universe tradable=… liquid=… selected=…`.

## Data source modes (`DATA_SOURCE`)

Live candle data can come from the WebSocket push, from REST polling, or both:

| Mode | Behaviour |
|---|---|
| `ws` | WebSocket push only (lowest latency; the original design). |
| `rest` | REST polling only — fetch each symbol's latest closed kline once per interval. Use when your network can't receive Binance's futures WebSocket data. |
| `hybrid` *(default)* | WebSocket first; if **no WS frames arrive for `WS_STALE_SECONDS`**, automatically fall back to REST polling, and switch back when WS frames resume. |

Both sources feed the same store and detection pipeline; the store deduplicates
by candle `open_time`, so a brief WS↔REST overlap can't double-fire.

> **Why hybrid is the default.** Some networks complete the WS *handshake* but
> receive **no market-data frames** (Binance accepts the connection but streams
> nothing to that IP). Hybrid detects this (`frames=0`) and transparently keeps
> the screener working over REST. On a 1m interval, polling ~500 symbols is
> ~500 weight/min — well under the 2400/min IP budget the token bucket enforces.
> When this happens you'll see: `WS feed stale … activating REST polling fallback`.

## What makes an alert fire

All of the following gates must pass, in order:

1. **Symbol is in the universe** (liquidity filter above).
2. **A candle closes** (`x = true`) — detection never runs on a forming candle.
3. **Window is full** — `WINDOW_SIZE` (default 30) closed candles are available.
4. **All enabled detection conditions agree on the same direction** (logical AND):

   | Condition | Default | Meaning |
   |---|---|---|
   | Slope (direction) | — | sign of the `log(close)` linear-regression slope → up/down |
   | **R²** (`USE_R_SQUARED`) | `≥ 0.75` | the trend is *clean* (points hug the trend line) |
   | **EMA alignment** (`USE_EMA`) | `9 / 21` | up: `close > EMA9 > EMA21`; down: reversed |
   | **Cumulative ROC** (`USE_ROC`) | `≥ 0.8%` | window start→end move, sign matches direction |
   | **ATR move** (`USE_ATR`) | `≥ 3.0×` | net window move in ATR (volatility) units — adapts the magnitude check to each symbol's own range |
   | **Volume** (`USE_VOLUME`) | `≥ 1.2×` | the just-closed candle's volume ≥ window-average × 1.2 |

5. **Not within cooldown** — no alert for the same `(symbol, direction)` in the
   last `COOLDOWN_MINUTES` (default 15). Opposite direction is an independent key.

Then: always logged to the console, and pushed to Telegram if configured.

> The AND of five conditions is intentionally strict to suppress minute-bar
> noise — alerts are relatively rare. The **volume gate on the triggering
> candle** is usually the most selective. ROC checks a *fixed* percentage; the
> **ATR move** checks the same move in units of the symbol's own volatility, so
> a fixed threshold means the same thing on BTC and a low-cap alt. See *Tuning*
> below to loosen them.

---

## Requirements

- [**uv**](https://docs.astral.sh/uv/) (project/dependency manager). Install:
  `curl -LsSf https://astral.sh/uv/install.sh | sh`
- Python 3.12 is pinned via `.python-version`; uv fetches it automatically if missing.
- No API keys — the bot consumes **public** market data only. (Telegram is the
  only optional credential.)

## Quick start

```bash
# 1. Install deps into a managed .venv (creates it + uses uv.lock)
uv sync

# 2. (optional) configure — defaults work out of the box
cp .env.example .env                 # then edit if you want

# 3. Run
uv run binance-trend-bot
```

That's it — `uv run` activates the project's environment automatically (no
manual `source .venv/bin/activate` needed). With an empty/absent `.env` it runs
on defaults (console alerts, 500k USDT liquidity floor, 1m candles).

## Running

### Foreground

```bash
uv run binance-trend-bot
# or, equivalently:
uv run python -m src.main
```

Stop with **`Ctrl-C`** (SIGINT) or send **SIGTERM** — WebSocket connections and
HTTP sessions are closed cleanly on shutdown.

### Background (long-running)

```bash
# simplest: nohup + log file
nohup uv run binance-trend-bot > bot.log 2>&1 &
tail -f bot.log

# stop it
kill -TERM <pid>
```

For 24/7 use, prefer a supervisor (systemd / pm2 / docker). Minimal systemd unit
(after `uv sync`, the console script lives in the project `.venv`):

```ini
# /etc/systemd/system/binance-trend-bot.service
[Service]
WorkingDirectory=/path/to/binance-alert
ExecStart=/path/to/binance-alert/.venv/bin/binance-trend-bot
EnvironmentFile=/path/to/binance-alert/.env
Restart=always
RestartSec=5
[Install]
WantedBy=multi-user.target
```

> Restart periodically (e.g. daily) so newly-listed perpetuals enter the
> universe — the symbol list is snapshotted once at start-up.

### Enabling Telegram alerts (optional)

1. Create a bot via **@BotFather** → copy the **bot token**.
2. Get your **chat id** (e.g. message your bot, then open
   `https://api.telegram.org/bot<TOKEN>/getUpdates` and read `chat.id`).
3. Put both in `.env`:

   ```bash
   TELEGRAM_BOT_TOKEN=123456:ABC-your-token
   TELEGRAM_CHAT_ID=123456789
   ```

With both set, alerts go to console **and** Telegram; with either missing, it's
console-only. The token is never logged.

### Verifying it's working

On start-up you should see (structured key=value logs):

```
... starting interval=1m window=30 source=hybrid telegram=False
... symbol universe tradable=… liquid=… selected=…
... warmup complete symbols=… ready=…
... ws connected shard=0 streams=… first=True
```

With `DATA_SOURCE=rest` (or after a hybrid fallback) you'll instead see
`rest polling started symbols=… interval=60s`. WebSocket mode also logs a
liveness heartbeat every ~60s:

```
... ws heartbeat shards=… frames=… queued=0 stale=none
```

A `stale=[…]` / `ws shard=N stale` warning means a shard stopped receiving
frames. When a trend is detected you'll see a `signal symbol=… direction=…`
line (and a Telegram message if configured).

## Configuration reference

Override any value via environment variable or `.env` (`UPPER_SNAKE_CASE` of the
field name). Every value has a default.

| Key | Default | Description |
|---|---|---|
| `INTERVAL` | `1m` | Kline interval (`1m`/`3m`/`5m`…). |
| `WINDOW_SIZE` | `30` | Rolling-window length (closed candles). |
| `R_SQUARED_MIN` | `0.75` | Min R² for a "clean" trend. |
| `EMA_FAST` / `EMA_SLOW` | `9` / `21` | EMA periods (`fast < slow`). |
| `ROC_MIN` | `0.008` | Min cumulative ROC over the window (fraction, 0.8%). |
| `ATR_MULT` | `3.0` | Min net move in ATR (volatility) units — volatility-normalized magnitude, adapts to each symbol's range. |
| `VOLUME_MULT` | `1.2` | Min last-candle-volume / window-avg-volume. |
| `USE_R_SQUARED` / `USE_EMA` / `USE_ROC` / `USE_ATR` / `USE_VOLUME` | `true` | Per-condition toggles. |
| `MIN_QUOTE_VOLUME_24H` | `500000` | Liquidity filter — drop symbols below this 24h quote volume (USDT). |
| `COOLDOWN_MINUTES` | `15` | Per-symbol+direction alert cooldown. |
| `TELEGRAM_BOT_TOKEN` | _(empty)_ | Set with `TELEGRAM_CHAT_ID` to enable Telegram; else console only. |
| `TELEGRAM_CHAT_ID` | _(empty)_ | Target chat for Telegram alerts. |
| `REST_WEIGHT_LIMIT` | `2400` | IP `REQUEST_WEIGHT` budget per minute. |
| `REST_WEIGHT_THROTTLE_RATIO` | `0.8` | Pre-emptively throttle past this used fraction. |
| `WARMUP_KLINES` | `50` | Historical klines backfilled per symbol (**must be `> WINDOW_SIZE`** — the forming last candle is dropped). |
| `MAX_STREAMS_PER_CONNECTION` | `200` | WS streams per connection (sharding). |
| `DATA_SOURCE` | `hybrid` | `ws` / `rest` / `hybrid` (see *Data source modes*). |
| `WS_STALE_SECONDS` | `30` | Hybrid: activate REST polling after this long with no WS frames. |
| `LOG_LEVEL` | `INFO` | Logging level. |

The `REQUEST_WEIGHT` budget is **per IP** — running multiple instances on one IP
splits the budget.

### Tuning alert frequency

- **More alerts** (looser): lower `R_SQUARED_MIN` (e.g. `0.6`), lower `ROC_MIN`
  (e.g. `0.004`), lower `ATR_MULT` (e.g. `2.0`) or `USE_ATR=false`, lower
  `VOLUME_MULT` (e.g. `1.0`) or `USE_VOLUME=false`, smaller `WINDOW_SIZE`, lower
  `MIN_QUOTE_VOLUME_24H`.
- **Fewer / higher-quality alerts** (stricter): raise the same thresholds, or
  raise `COOLDOWN_MINUTES`.
- Disable any single condition with its `USE_*=false` toggle.

## Test

```bash
uv run pytest
```

37 tests, fully offline: network and timing are mocked/injected, so the
token-bucket backoff, detector logic, store windowing/dedup, WS frame
parsing/dispatch, REST polling, hybrid failover decision, and notifier cooldown
run instantly and deterministically.

## Managing dependencies (uv)

```bash
uv sync                  # install exactly what uv.lock pins (runtime + dev)
uv add <package>         # add a runtime dependency
uv add --dev <package>   # add a dev/test dependency
uv lock --upgrade        # refresh the lockfile to newest allowed versions
```

`uv.lock` and `.python-version` are committed for reproducible installs; the
managed `.venv/` is git-ignored.

## Project layout

```
src/
  config.py       # pydantic settings (env-overridable)
  rest_client.py  # REST + token-bucket rate limiter (429/418 handling, klines backfill)
  ws_client.py    # WebSocket collector (sharding, reconnect, dispatch queue, heartbeat)
  rest_poller.py  # REST polling collector (fallback data source)
  hybrid.py       # WS-with-automatic-REST-fallback coordinator
  store.py        # per-symbol rolling windows (closed candles only, open_time dedup)
  detector.py     # trend indicators + AND decision
  notifier.py     # console/Telegram alerts + cooldown
  main.py         # asyncio entry point (warm-up → collect → detect → notify)
tests/
  test_detector.py    test_rate_limiter.py   test_store.py
  test_notifier.py    test_ws_client.py      test_rest_poller.py   test_hybrid.py
```
