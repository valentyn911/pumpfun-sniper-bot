# Project Context — PumpFun Sniper Bot

Read this file at the start of every session to get full project context without re-reading all source files.

---

## Purpose

Automated Solana token sniper for **pump.fun** (and letsbonk.fun, partially).

For every new token launch the bot:
1. Detects the token in real-time (WebSocket listener)
2. Fetches dev wallet info, GMGN token history, and on-chain BC dev-buy amount — in parallel
3. Applies configurable filters (min dev buy, ATH of dev's last 5 tokens, migration count, TX count, lifetime, entry MC)
4. Sends a Telegram alert (✅ passes all filters OR ❌ rejected with reason — suppressed when at capacity)
5. Optionally auto-buys tokens that pass all filters (live or test/simulation mode)
6. Monitors open positions via `accountSubscribe` WebSocket and auto-sells on TP / SL / trailing stop

Controlled via a local web dashboard (`dashboard.html` served by `bot_server.py` on `localhost:8765`).

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.11+ |
| Blockchain | Solana mainnet |
| Runtime | `uv` (virtualenv + deps) |
| RPC / WebSocket | Helius (`mainnet.helius-rpc.com`) |
| Fast tx submission | Helius Staked Transactions (`staked.helius-rpc.com`) |
| Token history | GMGN OpenAPI (primary) + gmgn-cli subprocess (fallback) |
| Telegram | `aiohttp` direct HTTP (no library) |
| Dashboard server | Python `http.server.HTTPServer` on port 8765 |
| Async event loop | `asyncio` + `uvloop` (optional, auto-detected) |

---

## Directory Structure

```
pumpfun-bonkfun-bot/
├── src/
│   ├── scanner_runner.py          # Main bot loop — listener, filters, buy dispatch
│   ├── scanner_position_monitor.py # Position monitor — WebSocket accountSubscribe, TP/SL/trail
│   ├── monitoring/
│   │   └── dev_checker.py         # Dev wallet info: balance, age, pump.fun history
│   ├── notifications/
│   │   └── telegram_reporter.py   # Send Telegram messages via Bot API
│   ├── trading/
│   │   └── platform_aware.py      # PlatformAwareBuyer + PlatformAwareSeller
│   ├── core/                      # SolanaClient, Wallet, PriorityFeeManager, pubkeys
│   ├── platforms/                 # pump.fun + letsbonk address derivation + IX builders
│   └── interfaces/                # TokenInfo, Platform enum, abstract interfaces
├── bot_server.py                  # Dashboard HTTP server + bot process control
├── dashboard.html                 # Single-page control dashboard (served at localhost:8765)
├── bot_config.json                # Live runtime config (gitignored) — presets, stats, flags
├── bots/
│   └── bot-scanner-telegram.yaml  # Bot startup config (platform, listener, env vars)
├── idl/
│   └── pump_fun_idl.json          # Pump.fun Anchor IDL (incomplete — see CLAUDE.md gotchas)
├── logs/                          # Per-session log files (gitignored)
├── .env                           # Private keys, RPC endpoints, API keys (gitignored)
└── CONTEXT.md                     # This file
```

---

## Key Files — What Each Does

### `src/scanner_runner.py` (1826 lines)
Central bot coroutine. Entry point: `run_scanner(config_path)`.

**Key globals:**
- `_position_entry_lock: asyncio.Lock` — atomic gate for claiming position slots
- `_sol_price_usd: float` — refreshed every 30 s from Binance
- `processed_mints: dict[str, float]` — deduplication (mint → first-seen monotonic time, cleaned every 60 s)
- `_BOT_CONFIG_PATH` — path to `bot_config.json`

**Config helpers:**
- `_read_bot_config() -> dict | None` — synchronous JSON read (called frequently, non-blocking)
- `_update_bot_config(updates: dict)` — async merge write under `_config_lock`; handles nested `"stats"` dict specially (merges instead of overwrites)

**Main flow per token:**
1. `on_new_token(token_info)` — deduplication check, increments `tokens_found_today`, dispatches `_check_and_notify` as `create_task`
2. `_check_and_notify(token_info, count, rpc, tg)`:
   - **STEP 1**: Read `open_positions` / `max_concurrent_positions` → set `_any_position_open` and `_at_capacity`. If `_at_capacity` → silent return immediately (no Telegram, no data fetch)
   - Fetches dev wallet info + GMGN history concurrently (4 s timeout) + BC dev-buy RPC in background
   - Applies filters 1–6 (mayhem skip, data quality, ATH, migrations, TX count, lifetime, dev buy). Each rejection sends Telegram ONLY when `not _any_position_open`
   - On all-pass: sends ✅ alert
   - **Live buy path**: if `auto_trading=True`, creates fresh `PlatformAwareBuyer` per buy (reads preset live), takes `_position_entry_lock`, re-checks capacity, increments `open_positions`, executes buy, launches `monitor_position` task with `position_close_fn=None`
   - **Test mode path**: if `test_mode=True`, takes same `_position_entry_lock`, increments `open_positions`, simulates buy, launches `monitor_position_test` task with `_on_test_close` as `position_close_fn`

**Position slot lifecycle (live):**
- `open_positions` incremented inside `_position_entry_lock` (atomic)
- `monitor_position` decrements in its `finally` block unconditionally
- Failure paths before monitor launch decrement immediately: `await _update_bot_config({"open_positions": max(0, open_pos)})`
- `open_pos` variable = value BEFORE increment (used for rollback: `max(0, open_pos)` = previous value)

**Position slot lifecycle (test):**
- Same: `open_positions` incremented inside lock
- `_on_test_close()` creates async task that decrements
- `monitor_position_test` calls `position_close_fn()` in its `finally`
- Failure paths before monitor launch decrement immediately

### `src/scanner_position_monitor.py` (954 lines)

**Live monitor** (`monitor_position`):
- `accountSubscribe` WebSocket loop — fires on every on-chain buy/sell of the token
- Decodes raw BC account bytes → current price (no extra RPC)
- Checks SL, TP, trailing stops on every price tick
- Sells via `PlatformAwareSeller`, retries until confirmed, then marks level as fired
- `finally` block: decrements `open_positions`, calls `position_close_fn` if set
- Crashes notify Telegram with "open_positions may need manual reset" warning

**Test monitor** (`monitor_position_test`):
- Same WebSocket loop, same price decode
- Simulates sells (no on-chain execution), sends Telegram notifications
- Uses `preset_snapshot` from entry time (never re-reads config)
- On full close: calls `_update_test_stat(key, pnl_delta)` to write `test_wins`/`test_losses`/`test_total_pnl_sol`
- `finally` block: calls `position_close_fn()` — does NOT touch `open_positions` directly

**Stat writers:**
- `_update_test_stat(key, pnl_delta)` — reads file inside `_spm_config_lock`, increments `test_wins`/`test_losses`, accumulates `test_total_pnl_sol`
- `_update_live_stat(key, pnl_delta)` — same pattern for `real_wins`/`real_losses`/`real_total_pnl_sol`. Called from `_sell()` when `remaining <= _DUST_TOKENS` (full position close only)

**Note on locks:** `_spm_config_lock` (in this file) and `_config_lock` (in scanner_runner) are separate locks protecting the same `bot_config.json`. Stat writes are infrequent, so real-world collision risk is low. A formal fix would require sharing a lock.

### `bot_server.py` (409 lines)

HTTP server for the dashboard. Endpoints:

| Method | Path | Description |
|---|---|---|
| GET | `/` | Serve `dashboard.html` |
| GET | `/api/status` | `{running, pubkey, stats, open_positions, max_concurrent_positions}` |
| GET | `/api/config` | Full `bot_config.json` contents |
| GET | `/api/balance` | SOL balance via RPC `getBalance` |
| GET | `/api/wallet` | `{pubkey}` |
| POST | `/api/start` | Kill existing scanner, reset stats+counter, start fresh |
| POST | `/api/stop` | Kill scanner, unconditionally reset stats+counter |
| POST | `/api/reset-positions` | Set `open_positions=0` only (stats untouched) |
| POST | `/api/config` | Save full config JSON |
| POST | `/api/save-key` | Write `SOLANA_PRIVATE_KEY` to `.env` |

**Important**: `/api/stop` resets stats AND `open_positions` unconditionally (not guarded by whether the bot was actually running). This is intentional — fixes stuck counter when bot crashes.

### `bot_config.json` (runtime, gitignored)

```json
{
  "active_preset": 1,
  "max_concurrent_positions": 1,
  "open_positions": 0,
  "auto_trading": false,
  "test_mode": false,
  "mode": "infinite",
  "max_trades": 10,
  "stats": {
    "tokens_found_today": 0,
    "tokens_passed_filters": 0,
    "buys_executed": 0,
    "test_buys_executed": 0,
    "test_wins": 0,
    "test_losses": 0,
    "test_total_pnl_sol": 0.0,
    "real_wins": 0,
    "real_losses": 0,
    "real_total_pnl_sol": 0.0
  },
  "presets": {
    "1": { "name": "Preset 1", "buy_amount_sol": 0.01, ... },
    "2": { ... },
    "3": { ... }
  }
}
```

### `dashboard.html` (1015 lines)

Single-page app served at `localhost:8765`. Features:
- **Bot Control**: Start/Stop buttons, Auto Trading toggle, 🧪 Test Mode toggle (amber), Mode selector (Infinite/N trades), Max Concurrent Positions
- **Test Mode Banner**: Yellow warning banner when `test_mode=true`
- **Wallet**: Private key input (save to `.env`), public address display, SOL balance with refresh
- **Presets 1-3**: Tabs with Entry fields (buy amount, fees, slippage, max retries), Filters (dev buy, ATH, migrations, TX count, lifetime, entry MC range), Trailing Stops, TP/SL rows
- **Stats**: Tokens Found, Passed Filters, Buys Executed, Open Positions (with [Reset] button), 🧪 Test Mode Stats section (hidden until `test_buys_executed > 0`), 💰 Live Trading Stats section (hidden until `buys_executed > 0`)
- Stats poll every 3 s via `/api/status`
- Config changes debounce-save via `/api/config` (600 ms)

### `src/monitoring/dev_checker.py`

Fetches dev wallet info within 1.5 s timeout:
- `getBalance` (lamports → SOL)
- `getSignaturesForAddress(limit=1000)` → wallet age (oldest sig blockTime) + tx count
- Batch `getTransaction` for last 10 sigs → parse `CreateEvent` binary (base64 `Program data:` log) → recent token mints

Returns `DevWalletInfo` with `timed_out=True` on timeout (scanner_runner rejects those tokens).

### `src/notifications/telegram_reporter.py`

Sends HTML-formatted messages to Telegram via `sendMessage` API. 10 s timeout per request. Returns `True`/`False`. No retries (fire-and-forget).

### `bots/bot-scanner-telegram.yaml`

```yaml
name: bot-scanner-telegram
platform: pump_fun
listener_type: logs
wss_endpoint: ${SOLANA_NODE_WSS_ENDPOINT}
rpc_endpoint: ${SOLANA_NODE_RPC_ENDPOINT}
env_file: .env
telegram:
  bot_token: ${TELEGRAM_BOT_TOKEN}
  chat_id: ${TELEGRAM_CHAT_ID}
```

---

## Multi-Position Mode

The bot supports up to `max_concurrent_positions` simultaneous open positions (default 1).

**Counter:** `open_positions` in `bot_config.json` (integer). Replaces the old `_position_active` boolean flag (removed).

**Rules:**
1. `open_pos >= max_concurrent` → complete silence (no Telegram, no data fetch, immediate return)
2. `open_pos > 0` (any position open, below capacity) → suppress rejection alerts; send ✅ alerts and buy alerts normally
3. `open_pos == 0` → all alerts sent

**Race protection:** `_position_entry_lock` (asyncio.Lock) is held while reading config + incrementing `open_positions`. Both live and test gates use this same lock — they cannot interleave.

**Stuck counter:** If bot crashes mid-trade, `open_positions` may stay > 0. Fixes:
- Press Stop → Start (unconditionally resets to 0)
- Click [Reset] button in dashboard (POST `/api/reset-positions`)

---

## Test Mode (Paper Trading)

When `test_mode=True` in config:
- After a token passes all filters, a simulated buy is created at current BC price × 1.15 (15% slippage simulation)
- Simulated positions participate in `open_positions` counter — same capacity rules as live mode
- `monitor_position_test` watches the BC address via `accountSubscribe`
- On TP/SL/trail trigger: sends Telegram notification with simulated PnL (no on-chain tx)
- On full close: writes `test_wins`/`test_losses`/`test_total_pnl_sol` to config
- Stats shown in dashboard under 🧪 Test Mode Stats section

Test mode is independent of `auto_trading`. Both can be on simultaneously (test simulates, live buys). They share the `open_positions` slot counter.

---

## Filters (Preset-level, re-read live per token)

| Filter | Field | Default | Notes |
|---|---|---|---|
| Mayhem mode skip | (auto) | always | Silent skip, no Telegram |
| Dev buy amount | `min_dev_buy_sol` | 0.1 | Skip if `< min`. Can disable with `dev_buy_check_enabled=false` |
| ATH of last 5 tokens | `min_ath_last5` | 0 | 0 = disabled. `ath_require_all`: all 5 must pass vs any 1 |
| Migrations in last 5 | `min_migrations_last5` | 0 | Token migrated = ATH ≥ $35,000 |
| TX count range | `min_tx_count` / `max_tx_count` | 0 | 0 = disabled. `tx_count_require_all`: all 5 vs any 1 |
| Token lifetime | `min_lifetime_minutes` | 0 | Minutes between create and last trade. `lifetime_require_all` |
| Entry MC range | `min_entry_mc_usd` / `max_entry_mc_usd` | 0 | Checked via live BC read inside position lock gate |

Rejection alerts are suppressed when any position is open (`open_pos > 0`).

Data quality guards (dev timeout, GMGN fail, empty history, dev data missing) always reject with alert — unless `_any_position_open`.

---

## Exit Strategies (Position Monitor)

All configured per-preset, re-read live from config on every price tick (live mode only; test mode uses snapshot):

**Take Profits:** Up to 8 levels. Each: `price_pct` (% above entry to trigger) + `position_pct` (% of original tokens to sell). `position_pct` is % of the original buy amount, not current remaining.

**Stop Losses:** Up to 3 levels. Each: `price_pct` (% below entry to trigger) + `position_pct`.

**Trailing Stops:** Up to 3. Each: `activation_pct` (% above entry to arm) + `trail_size_pct` (% drop from peak to fire) + `position_pct`. Each trailing stop tracks its own peak independently.

Moonbag = remaining tokens after all TP/trail fires (whatever % wasn't covered by exits).

All sell tasks retry indefinitely (0.5 s between attempts) until on-chain confirmation. Fired flags set only after confirmed sell to prevent double-firing.

---

## Data Flow Timing

Per-token latency breakdown:
```
Token detected (WS)
  ↓ ~0 ms
STEP 1: capacity check (sync read)
  ↓ ~0 ms
Parallel Phase 1:
  - Dev wallet check (Helius getSignaturesForAddress + batch getTransaction) ~800-1200 ms
  - GMGN HTTP API call ~200-400 ms
  - BC dev-buy RPC (fires immediately, runs in background) ~300-500 ms
  ↓ ~1300 ms (max of above)
Filters 1-6 applied (sync, ~0 ms)
  ↓
Phase 2: await BC dev-buy result (usually already done, ~0 ms extra)
  ↓
Telegram alert sent + buy execution (async, non-blocking)
```

Total: ~1.3-1.5 s from token detection to buy dispatch (dominated by GMGN call).

---

## Environment Variables (`.env`)

```
SOLANA_PRIVATE_KEY=<base58>
SOLANA_NODE_RPC_ENDPOINT=https://mainnet.helius-rpc.com/?api-key=...
SOLANA_NODE_WSS_ENDPOINT=wss://mainnet.helius-rpc.com/?api-key=...
HELIUS_STAKED_URL=https://staked.helius-rpc.com/?api-key=...
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
GMGN_API_KEY=...              # Optional; enables direct HTTP path (faster than CLI)
```

---

## Running the System

```bash
# Start dashboard server (keep running in a terminal)
uv run bot_server.py
# Dashboard at http://localhost:8765/

# Start bot manually (bot_server.py does this when you press Start)
uv run src/scanner_runner.py bots/bot-scanner-telegram.yaml

# Check if bot is running
pgrep -f scanner_runner.py
```

---

## Known Design Notes / Gotchas

1. **`open_pos` rollback value**: When the lock gate increments `open_positions` from N to N+1, `open_pos = N` is saved. All failure paths use `await _update_bot_config({"open_positions": max(0, open_pos)})` which writes N (the pre-increment value). This is correct — it rolls back the increment.

2. **`_update_bot_config` stats merge**: The function does `cfg.setdefault("stats", {}).update(v)` for stats updates. This means stat values passed in are WRITTEN DIRECTLY, not incremented from current file. Callers compute the new value themselves before calling. This means two concurrent callers with the same stale read can lose an increment. For display counters (`tokens_found_today` etc.) this is acceptable; for position counters it's safe because the lock gate serializes access.

3. **`_spm_config_lock` vs `_config_lock`**: Different lock objects protecting the same file. Low collision risk in practice (stat writes are rare). Not fixed to avoid cross-module lock sharing.

4. **Dev buy RPC retry logic**: `_fetch_bc_dev_buy` retries when `vsol == INITIAL` (30 SOL) because the BC account may not yet reflect the dev buy. Up to 5 attempts × 250 ms = 1.25 s max. Since Phase 1 takes ~1.3 s, this runs concurrently and adds zero latency in the common case.

5. **Entry MC check inside lock gate**: `_check_entry_mc` is awaited AFTER `open_positions` was already incremented. If MC check fails, the counter is rolled back immediately with `await _update_bot_config({"open_positions": max(0, open_pos)})`.

6. **Test mode slippage simulation**: Entry price = `cur_price_test * 1.15` (15% worse than current BC price). This models realistic fill price for a new token.

7. **`bot_mode` and `max_trades_count` read at startup only**: Mode and max-trades limit are read once when `run_scanner` starts. Changes while running take effect on next Start.

8. **`monitor_position` crash notification**: If the live position monitor crashes, it sends a Telegram alert warning the user that `open_positions` may need manual reset via the [Reset] button. The `finally` block still runs the decrement.

9. **pump.fun BC account size**: 83 bytes. Fields at byte 8: `virtual_token_reserves (u64)`, byte 16: `virtual_sol_reserves (u64)`. Price = `(vsol / vtoken) * 1e6 / 1e9`. Trailing fields: `is_mayhem_mode (bool)` + `is_cashback_coin (bool)` at bytes 81-82.

10. **Cashback sell account count**: Non-cashback sell = 16 accounts; cashback sell = 17 accounts (inserts `user_volume_accumulator` PDA before `bonding-curve-v2`). Detected from BC byte 82 or `is_cashback_coin` field set during buy.

11. **Blacklisted mints**: `BLACKLISTED_MINTS` frozenset in scanner_runner — silent skip, no Telegram. Currently contains one known garbage token.

---

## Recent Session Changes (2026-05-23 / 2026-05-24)

### Multi-position mode (Counter-based)
- Removed `_position_active` boolean flag entirely from scanner_runner
- Replaced with `open_positions` counter comparisons at start of `_check_and_notify`
- `_position_entry_lock` retained for atomic increment
- Both live and test paths participate in the counter (test positions count toward capacity)

### Stats System
- Added: `test_wins`, `test_losses`, `test_total_pnl_sol`, `real_wins`, `real_losses`, `real_total_pnl_sol`
- `_update_test_stat` extended with `pnl_delta` parameter
- `_update_live_stat` added to `scanner_position_monitor.py` (called on full position close)
- Dashboard shows 🧪 Test Mode Stats and 💰 Live Trading Stats sections (hidden until `> 0` buys)
- WR% and color-coded PnL displayed

### Bug fixes
- `/api/stop` unconditional reset (removed `if ok:` guard that caused stuck counter when bot crashed)
- `/api/reset-positions` endpoint added + [Reset] button in dashboard
- `statMaxPos.textContent` line removed (was destroying the span and causing TypeError that silently broke all downstream stats updates on every poll)
- `_update_live_stat` unused `update_config` parameter removed

### Dashboard
- Test Mode toggle restored (amber, between Auto Trading and Mode selector)
- Yellow test mode banner added
- Open Positions card has [Reset] button (calls `/api/reset-positions`)
