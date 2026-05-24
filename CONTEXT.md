# Project Context — PumpFun Sniper Bot

Read this file at the start of every session. It is the single source of truth for the project state. A new agent reading only this file should be able to continue development without any questions.

Last updated: 2026-05-24

---

## 1. Project Purpose

Automated Solana token sniper for **pump.fun** (letsbonk.fun support partially implemented but not actively used).

Full pipeline per new token:
1. Detect token launch in real-time (Helius WSS `logsSubscribe`)
2. Fetch dev wallet info, GMGN token history, on-chain dev-buy amount — in parallel
3. Apply configurable filters (dev buy, ATH, migrations, TX count, lifetime, entry MC)
4. Send Telegram alert (✅ pass or ❌ reject with reason — rejection suppressed when any position open)
5. Optionally execute a real buy (`auto_trading=True`) and/or a simulated paper trade (`test_mode=True`)
6. Monitor open positions via `accountSubscribe` WebSocket; auto-exit on TP / SL / trailing stop

Controlled through a local web dashboard at `http://localhost:8765/`.

---

## 2. Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.11+ |
| Blockchain | Solana mainnet |
| Runtime | `uv` (virtualenv + package management) |
| RPC | Helius (`mainnet.helius-rpc.com`) |
| Fast tx submission | Helius Staked (`staked.helius-rpc.com`) |
| Token history | GMGN OpenAPI (primary, needs `GMGN_API_KEY`) + gmgn-cli NPX (fallback) |
| Telegram | `aiohttp` direct HTTP — no library |
| Dashboard server | Python `http.server.HTTPServer` on port 8765 |
| Async | `asyncio` + `uvloop` (auto-detected, optional) |
| Transaction building | `solders` (keypair, pubkey, instructions) |
| Serialization | Anchor IDL + manual struct parsing |

---

## 3. File & Folder Structure

```
pumpfun-bonkfun-bot/
├── src/
│   ├── scanner_runner.py              # ★ Main bot — listener, filters, buy dispatch
│   ├── scanner_position_monitor.py   # ★ Position monitor — TP/SL/trailing, WebSocket
│   ├── monitoring/
│   │   ├── dev_checker.py            # Dev wallet: balance, age, pump.fun history
│   │   ├── listener_factory.py       # Creates listener by type (logs/blocks/geyser)
│   │   ├── universal_logs_listener.py # logsSubscribe WebSocket listener
│   │   ├── block_listener.py         # blockSubscribe listener (Chainstack/Helius only)
│   │   └── base_listener.py          # Abstract BaseTokenListener
│   ├── notifications/
│   │   └── telegram_reporter.py      # send_message(), send_startup_message()
│   ├── trading/
│   │   └── platform_aware.py         # PlatformAwareBuyer, PlatformAwareSeller
│   ├── core/
│   │   ├── client.py                 # SolanaClient (RPC + tx building)
│   │   ├── wallet.py                 # Wallet (keypair from base58)
│   │   ├── priority_fee/manager.py   # PriorityFeeManager (fixed or dynamic)
│   │   └── pubkeys.py                # Constants: LAMPORTS_PER_SOL, TOKEN_DECIMALS
│   ├── platforms/
│   │   ├── pumpfun/
│   │   │   ├── address_provider.py   # BC v2, bonding-curve PDA derivation
│   │   │   ├── instruction_builder.py # build_buy_instruction, build_sell_instruction
│   │   │   ├── curve_manager.py      # get_pool_state (decodes 83-byte BC account)
│   │   │   └── event_parser.py       # Parse CreateEvent from logsSubscribe
│   │   └── letsbonk/                 # Parallel structure for letsbonk
│   └── interfaces/
│       └── core.py                   # TokenInfo, Platform enum, abstract interfaces
├── bot_server.py                     # ★ Dashboard HTTP server + process control
├── dashboard.html                    # ★ Single-page dashboard (served at :8765)
├── bot_config.json                   # [gitignored] Runtime config — presets, stats, flags
├── bots/
│   └── bot-scanner-telegram.yaml     # Bot startup config (platform, listener, env vars)
├── idl/
│   └── pump_fun_idl.json             # Anchor IDL (incomplete — see §16)
├── logs/                             # [gitignored] Per-session log files
├── .env                              # [gitignored] Private keys, RPC, API keys
├── CONTEXT.md                        # This file
└── CLAUDE.md                         # Project instructions for Claude agents
```

---

## 4. Full Pipeline — Token Detection to Position Close

### Step 1 — Token Detection

`bots/bot-scanner-telegram.yaml` sets `listener_type: logs`. `ListenerFactory` creates a `UniversalLogsListener`.

`UniversalLogsListener.listen_for_tokens()` opens a WebSocket to `SOLANA_NODE_WSS_ENDPOINT` and sends:
```json
{"method": "logsSubscribe", "params": [{"mentions": [PUMP_FUN_PROGRAM_ID]}, {"commitment": "processed"}]}
```
Every log entry mentioning the pump.fun program ID is received. The pump.fun `event_parser.py` parses `CreateEvent` from the base64-encoded `Program data:` log line → populates a `TokenInfo` object with `mint`, `name`, `symbol`, `creator`, `bonding_curve`, `virtual_sol_reserves`, `is_mayhem_mode`, `is_cashback_coin`.

### Step 2 — Dispatch

`on_new_token(token_info)` is called by the listener for each parsed token.

```python
# Deduplication (Step 3)
if mint_str in processed_mints:
    return  # logsSubscribe fires multiple times per tx
processed_mints[mint_str] = time.monotonic()

token_count += 1
current_count = token_count

# Increment tokens_found_today counter (async write)
asyncio.create_task(_update_bot_config({"stats": {"tokens_found_today": current_count}}))

# Dispatch processing as non-blocking task
asyncio.create_task(_check_and_notify(token_info, current_count, rpc_endpoint, telegram))
```

### Step 3 — Deduplication

`processed_mints: dict[str, float]` — mint address → first-seen monotonic timestamp. `_cleanup_processed_mints()` removes entries older than 1800 s (30 min) every 60 s. Prevents duplicate alerts when `logsSubscribe` fires multiple log events per single transaction.

### Step 4 — Read Config + Capacity Check

```python
_cap_cfg = _read_bot_config() or {}
_open_pos_cap = int(_cap_cfg.get("open_positions", 0))
_max_concurrent_cap = int(_cap_cfg.get("max_concurrent_positions", 1))
_any_position_open = _open_pos_cap > 0
_at_capacity = _open_pos_cap >= _max_concurrent_cap
if _at_capacity:
    return  # complete silence — no Telegram, no data fetch
```

`_any_position_open = True` means rejection alerts are suppressed for the rest of this token's processing. The actual buy gate re-reads config inside a lock (Step 16).

### Step 5 — Filter 1: Mayhem Mode

If `token_info.is_mayhem_mode` → silent skip (no Telegram, no data fetch). Mayhem coins use a different fee recipient; the bot is not configured for them.

### Step 6 — Parallel Phase 1 (4 s timeout)

Four tasks fire simultaneously at token detection time:

```
task_dev   = check_dev_wallet(creator_str, rpc)          # Helius: balance + sigs + history
task_gmgn  = _get_gmgn_dev_tokens(creator_str, mint)     # GMGN API or gmgn-cli
task_bc_buy = _fetch_bc_dev_buy(bc_str, rpc)             # reads vsol from BC account
task_mint  = _check_mint_freeze(mint_str, rpc)            # informational only
```

All four run concurrently. GMGN typically takes ~1.3 s. BC calls run in the background during that wait. `asyncio.wait_for(..., timeout=4.0)` wraps the dev + GMGN gather.

**`check_dev_wallet`** (`dev_checker.py`, 1.5 s internal timeout):
- `getBalance` → `sol_balance`
- `getSignaturesForAddress(limit=1000)` → `wallet_age_str` (from oldest blockTime), `total_launches` count
- Batch `getTransaction` for 10 most recent sigs → parse `CreateEvent` binary → `recent_tokens` list

**`_get_gmgn_dev_tokens`**:
- Primary: direct HTTP to `openapi.gmgn.ai/v1/user/created_tokens` (needs `GMGN_API_KEY`)
- Fallback: `npx gmgn-cli portfolio created-tokens --chain sol --wallet {addr} --raw`
- Returns: last 5 tokens sorted by `create_timestamp` desc (excluding current mint), each with `ath_market_cap`

**`_fetch_bc_dev_buy`** (5 retries × 250 ms backoff):
- `getAccountInfo` on the BC address → read `virtual_sol_reserves` at offset 16 (u64 little-endian)
- `dev_buy_sol = (vsol - 30_000_000_000) / 1e9`
- Retries while `vsol == INITIAL` (30 SOL) — the BC account may lag behind the dev buy tx

**Stream 4 — BC Signatures** (optional, after Phase 1, only when TX count or lifetime filter is configured):
```python
if _need_sigs and histories and rpc:
    sig_data = await asyncio.wait_for(
        _fetch_token_signatures_batch(histories, rpc), timeout=1.5
    )
```
Fetches `(tx_count, last_blocktime)` for each of the dev's last 5 tokens' BC v2 addresses in parallel.

### Step 7 — Data Quality Guards

After Phase 1 completes, check:
- `dev.timed_out` → reject with "data fetch timeout"
- `gmgn_failed` → reject with "GMGN data unavailable"
- `not histories` → reject with "no token history for this dev"
- `dev.sol_balance is None and dev.wallet_age_str is None and dev.total_launches is None` → reject with "dev data unavailable"

All rejections send a Telegram `❌` message **only if `not _any_position_open`**.

### Step 8 — Filter 3: ATH of Last 5 Tokens

```python
if min_ath > 0 and histories:
    if ath_require_all:
        # Every token must have ATH >= min_ath
    else:
        # At least one token must have ATH >= min_ath
```

`TokenHistory.migrated = ath_market_cap >= 35_000` (used for Filter 4).

### Step 9 — Filter 4: Migrations in Last 5

```python
mig_count = sum(1 for h in histories if h.migrated is True)
if mig_count < min_migrations_last5:  # reject
```

### Step 10 — Filter 5: TX Count Range

Applies to dev's last 5 tokens' BC signature counts (from Stream 4). `tx_count_require_all`:
- `True` → every token must be within `[min_tx_count, max_tx_count]`
- `False` → at least one token must be in range

`0` = disabled for min or max. Rugs typically 5–50 txs; real tokens 100+.

### Step 11 — Filter 6: Token Lifetime

`lifetime_minutes = (last_blocktime - create_timestamp) / 60`. Dev's last 5 tokens must meet `min_lifetime_minutes`. `lifetime_require_all` = all vs any. `None` data → treated as passing (don't reject on missing).

### Step 12 — Phase 2: Collect Dev Buy

After filters (which take ~1.3 s), await the BC buy task with 0.5 s remaining timeout:
```python
bc_result = await asyncio.wait_for(task_bc_buy, timeout=0.5)
dev_buy_sol = bc_result  # float or None
```
Usually already done — zero added latency in common case.

### Step 13 — Filter 2: Dev Buy Amount

```python
if filt.get("dev_buy_check_enabled", True):
    if dev_buy_sol is not None and dev_buy_sol < min_dev_buy_sol:
        reject  # "Dev buy X SOL (min Y SOL)"
```

`dev_buy_check_enabled=False` → dev buy shown in alert but filter skipped (enables faster entry without waiting for BC read).

### Step 14 — All Filters Passed

```python
asyncio.create_task(_update_bot_config({"stats": {"tokens_passed_filters": N+1}}))
full_message = "✅ ПРОШЁЛ ВСЕ ФИЛЬТРЫ\n" + format_token_alert(...)
```

**`format_token_alert`** builds the full Telegram alert with:
- Token name, symbol, mint (copyable `<code>`), GMGN link
- Dev address, wallet age, SOL balance, total launches (from GMGN total field if available, else from Helius signatures)
- Dev buy amount at launch
- Last 5 tokens: name, migrated ✅/❌, ATH MC (💚 highlighted if ≥ $20,000)

`gmgn.ai/sol/token/{mint}` — all pump.fun token links use GMGN.

### Step 15 — Atomic Entry Gate

```python
async with _position_entry_lock:
    _gate_open = int(_read_bot_config().get("open_positions", 0))
    _gate_max = int(_read_bot_config().get("max_concurrent_positions", 1))
    if _gate_open >= _gate_max:
        return  # race: another coroutine beat us to the slot
    if _stop_scanning:
        return
    await _update_bot_config({"open_positions": _gate_open + 1})
    open_pos = _gate_open  # save pre-increment value for rollback
```

`_position_entry_lock` is a module-level `asyncio.Lock()`. Both live and test paths use **the same lock** — they cannot interleave. After claiming the slot, `open_pos` holds the pre-increment value used in all failure-path rollbacks: `await _update_bot_config({"open_positions": max(0, open_pos)})`.

### Step 16a — Test Mode Path

Runs when `test_mode=True` in config. Shares `_position_entry_lock` and `open_positions` counter with live mode.

```python
cur_price_test = await _get_current_token_price_sol(bc_str, rpc)
# _get_current_token_price_sol: 5x retry, 250ms backoff
# Retries when MC estimate <= $500 (stale pre-dev-buy BC state)
# Returns price in SOL per whole token

simulated_entry_price = cur_price_test * 1.15  # +15% slippage simulation
simulated_entry_mc = simulated_entry_price * 1_000_000_000 * _sol_price_usd

# MC range filter applies at slippage-adjusted price
if min_entry_mc > 0 and simulated_entry_mc < min_entry_mc: rollback + return
if max_entry_mc > 0 and simulated_entry_mc > max_entry_mc: rollback + return

sim_pos = SimulatedPosition(
    entry_price_sol=simulated_entry_price,
    entry_sol=preset.buy_amount_sol,
    preset_snapshot=dict(preset),  # snapshot at entry time
    ...
)

await _update_bot_config({"stats": {"test_buys_executed": N+1}})
successful_buys_this_session += 1  # shared with live, counts toward N-trades limit

asyncio.create_task(monitor_position_test(
    sim_pos=sim_pos,
    position_close_fn=_on_test_close,  # decrements open_positions when closed
))
```

`_on_test_close()` schedules an async task that decrements `open_positions`:
```python
def _on_test_close() -> None:
    async def _decrement() -> None:
        _c = _read_bot_config() or {}
        await _update_bot_config(
            {"open_positions": max(0, int(_c.get("open_positions", 1)) - 1)}
        )
    asyncio.get_running_loop().create_task(_decrement())
```

### Step 16b — Live Mode Path

Runs when `auto_trading=True` AND `buyer` (PlatformAwareBuyer) is initialized.

```python
# Create fresh buyer per trade (reads current preset live)
fresh_buyer = PlatformAwareBuyer(
    amount=preset.buy_amount_sol,
    slippage=preset.buy_slippage / 100,
    max_retries=preset.max_retries,
    jito_tip_lamports=preset.jito_tip_sol * 1e9,
    extreme_fast_mode=True,
)

# MC range filter (live path uses actual RPC check via _check_entry_mc)
mc_passes, mc_skip_msg = await _check_entry_mc(filt, bc_str, rpc, label)
if not mc_passes:
    await _update_bot_config({"open_positions": max(0, open_pos)})
    buy_block = mc_skip_msg  # shown in Telegram alert

# Execute buy
buy_result = await fresh_buyer.execute(token_info)
# PlatformAwareBuyer.execute():
#   - Gets BC pool state (skipped in extreme_fast_mode, uses event data)
#   - Refreshes mayhem/cashback flags from chain (even in fast mode, 4 retries)
#   - Builds buy instruction (18 accounts post 2026-04-28)
#   - Sends transaction with Jito bundle tip
#   - Confirms transaction
#   - Parses actual tokens received and SOL spent from tx (5 retries)

if buy_result.success:
    await _update_bot_config({"stats": {"buys_executed": N+1}})
    asyncio.create_task(monitor_position(
        token_amount=buy_result.amount,
        entry_price=buy_result.price,
        position_close_fn=None,  # monitor_position decrements open_positions in its own finally
    ))
else:
    await _update_bot_config({"open_positions": max(0, open_pos)})  # rollback
```

---

## 5. Position Monitor — Live (`monitor_position`)

**File:** `src/scanner_position_monitor.py`

```
accountSubscribe WebSocket (commitment=processed)
  ↓ every on-chain buy/sell of the token
Raw account bytes → _price_from_raw() → current price in SOL/token
  ↓
_on_price(current_price):
  1. SL check (ascending price_pct → least loss fires first)
  2. TP check (ascending price_pct → smallest gain first)
  3. Trailing stop check (each independent)
  → Each trigger → asyncio.create_task(_sell_with_retry(...))
```

**Key invariants:**
- `token_amount` is set once at entry from `buy_result.amount`, never changes
- All sell amounts = `token_amount * (level.position_pct / 100)` — percent of **original** position, not current remaining
- `remaining` tracks how much is left; `_DUST_TOKENS = 1.0` = fully exited
- `sells_in_progress: set[str]` prevents re-firing a level while its sell is in-flight
- TP/SL indices added to `tp_fired`/`sl_fired` sets **only after on-chain confirmation**
- `trail_states[i]["fired"]` set to `True` only after confirmation
- Settings re-read from `bot_config.json` on **every price tick** (live changes take effect immediately)

**`_sell_with_retry`**: retries indefinitely with 0.5 s delay until `result.success`. Fires Telegram on failure. Never gives up.

**`_sell`**: Creates `PlatformAwareSeller`, executes on-chain sell, computes PnL, sends Telegram notification. On full close (`remaining <= _DUST_TOKENS`): appends "🔒 ПОЗИЦИЯ ЗАКРЫТА" block and calls `_update_live_stat(key, pnl_delta)`.

**`finally` block** (always runs, even on crash):
```python
finally:
    cur_cfg = read_config() or {}
    await update_config({"open_positions": max(0, cur_cfg.get("open_positions", 1) - 1)})
    if position_close_fn is not None:
        position_close_fn()
```
`position_close_fn=None` for live positions (already handled here). On crash: sends Telegram alert warning user to check wallet and possibly press [Reset].

---

## 6. Position Monitor — Test Mode (`monitor_position_test`)

**Same WebSocket** as live. Same price decode. Same SL → TP → Trailing logic.

**Key differences from live:**
- `original_amount` set once at entry (= `simulated_token_amount`), never changes
- All sell amounts = `original_amount * (position_pct / 100)` — percent of original
- `preset_snapshot` used for all decisions — **never re-reads config** (ensures test results are deterministic to entry conditions)
- Sells are instant (no blockchain tx, no retry)
- `trail_states` built from `preset_snapshot.trailing_stops` at entry

**`_simulate_sell_task`**: computes gross PnL, net PnL (after `priority_fee_sol`), sends Telegram. On full close (`remaining <= _DUST_TOKENS`): sends "🏁 ПОЗИЦИЯ ЗАКРЫТА (СИМУЛЯЦИЯ)" block and calls `asyncio.create_task(_update_test_stat(key, pnl_delta=final_net))`.

**`finally` block**:
```python
finally:
    if position_close_fn is not None:
        position_close_fn()  # → _on_test_close() → decrements open_positions
```
Does **not** touch `open_positions` directly — that's `_on_test_close`'s job.

---

## 7. Key Design Decisions

**`extreme_fast_mode=True` always on:**
In normal mode, `PlatformAwareBuyer` does an RPC `get_pool_state` call before building the buy instruction to get current price and flags. `extreme_fast_mode` skips this — instead it:
1. Uses the `extreme_fast_token_amount` pre-calculated estimate for slippage calc
2. Still refreshes `is_mayhem_mode` / `is_cashback_coin` / `creator` from chain with 4 retries (these determine fee recipient and account count — wrong values = ConstraintSeeds 0x7d6 or NotAuthorized 0x1770 error)
Slippage is set high (30%) to cover price estimation error. Net: ~200 ms faster per buy.

**`logsSubscribe` as primary listener:**
Fires within 100–300 ms of block finalization at `processed` commitment. PumpPortal fires at 200–500 ms but adds external dependency. Geyser is fastest (~50 ms) but requires premium endpoint. `logsSubscribe` is the best free/standard tier option.

**`position_pct` always from `token_amount` (original), not `remaining`:**
If a user sets TP1=50%, TP2=50%, they sell 50% at TP1 and 50% at TP2 — total 100% of original. This is intuitive: "sell X% of what I bought." The alternative (% of remaining) produces unexpected sells at later levels.

**`asyncio.Lock` for atomic entry:**
Without the lock, two coroutines could both read `open_positions=0`, both see capacity available, and both increment — resulting in `open_positions=2` with only 1 allowed. The lock ensures read-increment-write is atomic. Since it's an `asyncio.Lock`, it only yields to other coroutines at explicit `await` points.

**Counter not boolean:**
Old code used `_position_active: bool` — this only supported 1 concurrent position. Counter supports `max_concurrent_positions` (currently tested up to 5).

**Non-blocking sells:**
`_on_price()` dispatches sells as `asyncio.create_task()` and returns immediately. This ensures the next price notification is processed without waiting for the sell to confirm. Multiple levels can fire in the same price tick.

**TP/SL fired only after confirmed success:**
If a sell fails (network error, slippage), the level is not marked fired. `_sell_with_retry` retries until success. This guarantees no level is silently lost.

**MC price with 5x retry and $500 threshold:**
`CreateEvent` fires when the BC account is initialized — this is **before** the dev buy. Reading BC immediately would return `vsol = 30 SOL` (initial state). The retry loop waits for `MC estimate > $500`, which confirms the dev buy has been reflected in the BC account. $500 is safely below the minimum legitimate initial MC (~$1,400 at $50 SOL).

**Dev buy with 5x retry:**
Same reason — BC `virtual_sol_reserves` may show INITIAL state immediately after CreateEvent.

**Rejection alert suppression:**
When any position is open (`open_pos > 0`), the Telegram channel is used exclusively for position updates. Rejection alerts (which fire for most tokens) would overwhelm the channel. ✅ alerts still send (rare, high-signal).

---

## 8. Test Mode — Complete Description

**Activation:** Toggle in dashboard (🧪 Test Mode, amber color). Saves `test_mode: true` to `bot_config.json`. Yellow banner appears at top of dashboard: "⚠️ TEST MODE ACTIVE — No real transactions will be executed".

**Entry:** After a token passes all filters, bot reads current BC price via `_get_current_token_price_sol` (same 5x retry, $500 threshold), then:
- `simulated_entry_price = current_price * 1.15` (models 15% buy slippage)
- `simulated_entry_mc = simulated_entry_price * 1_000_000_000 * _sol_price_usd`
- MC range filter (`min_entry_mc_usd` / `max_entry_mc_usd`) applied at slippage-adjusted MC
- `SimulatedPosition` created with `preset_snapshot` (copy of current preset at entry time)

**Monitoring:** `monitor_position_test` connects same `accountSubscribe` WebSocket. Real live prices. Same TP/SL/trailing stop logic as live. Uses `preset_snapshot` (not re-read) so config changes after entry don't affect the test.

**Exits:** Instant simulation — no blockchain tx. PnL computed as `sol_received - entry_sol - fees`. `priority_fee_sol` deducted from PnL per exit (jito + gas not simulated, only priority fee).

**Telegram alerts:** Identical format to live alerts, clearly labeled `🧪 TEST MODE`.

**Stats written on full close:**
- `test_wins` or `test_losses` incremented
- `test_total_pnl_sol` accumulated

**N-trades limit:** Test buys increment `successful_buys_this_session` — same counter as live buys. When limit reached, `_stop_scanning = True` and all new tokens are ignored.

**Capacity:** Test positions occupy `open_positions` slots same as live positions. Max concurrent applies to combined live+test.

**Can run simultaneously with live mode:** If both `auto_trading=True` and `test_mode=True`, the same token triggers both paths. Both compete for `open_positions` slots via the same lock.

---

## 9. `bot_config.json` — Full Structure

```json
{
  "active_preset": 2,
  "max_concurrent_positions": 5,
  "open_positions": 0,
  "auto_trading": false,
  "test_mode": true,
  "mode": "infinite",
  "max_trades": 5,
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
    "1": {
      "name": "Preset 1",
      "buy_amount_sol": 0.2,
      "priority_fee_sol": 0.00022,
      "jito_tip_sol": 0.0022,
      "gas_fee_sol": 0.00005,
      "buy_slippage": 30,
      "sell_slippage": 30,
      "max_retries": 2,
      "take_profits": [
        {"price_pct": 10, "position_pct": 25},
        {"price_pct": 25, "position_pct": 25},
        {"price_pct": 50, "position_pct": 25},
        {"price_pct": 100, "position_pct": 25}
      ],
      "stop_losses": [
        {"price_pct": 30, "position_pct": 100}
      ],
      "trailing_stops": [
        {"enabled": true, "activation_pct": 5, "trail_size_pct": 20, "position_pct": 50},
        {"enabled": true, "activation_pct": 10, "trail_size_pct": 20, "position_pct": 50}
      ],
      "filters": {
        "min_dev_buy_sol": 0.9,
        "dev_buy_check_enabled": false,
        "min_ath_last5": 5000,
        "ath_require_all": true,
        "min_migrations_last5": 0,
        "min_tx_count": 50,
        "max_tx_count": 0,
        "tx_count_require_all": true,
        "min_lifetime_minutes": 3,
        "lifetime_require_all": true,
        "min_entry_mc_usd": 0,
        "max_entry_mc_usd": 0
      }
    },
    "2": { "...": "same structure" },
    "3": { "...": "same structure" }
  }
}
```

**Important field notes:**
- `open_positions` — current live counter. Reset to 0 on Stop or [Reset]. Incremented/decremented atomically by scanner.
- `mode` — `"infinite"` (run forever) or `"n"` (stop after `max_trades` successful buys)
- `test_mode` — bool; scanner reads this on every token that passes filters
- `trailing_stops` — array format (up to 3 per preset). Old config may have `trailing_stop` (single object, legacy — still supported via fallback in live monitor)
- All stat fields reset to 0 on every Start and Stop press

---

## 10. Dashboard — All Controls

Served at `http://localhost:8765/` by `bot_server.py`. Polls `/api/status` every 3 s. Config changes debounce-save via POST `/api/config` after 600 ms.

### Bot Control Card
| Control | Saves to | Notes |
|---|---|---|
| ▶ Start Bot | — | POST `/api/start` → kills existing, resets stats+counter, launches scanner |
| ■ Stop Bot | — | POST `/api/stop` → kills scanner, resets ALL stats and `open_positions` to 0 |
| Auto Trading | `auto_trading` | Blue toggle. Controls whether real buys execute |
| 🧪 Test Mode | `test_mode` | Amber toggle. Enables paper trading. Shows yellow banner when ON |
| Mode: Infinite | `mode = "infinite"` | Run forever |
| Mode: N trades | `mode = "n"` | Stop after N successful buys (shared live+test counter) |
| Max Trades input | `max_trades` | N-trades limit value |
| Max Concurrent Positions | `max_concurrent_positions` | Max simultaneous open positions (live + test combined) |

### Wallet Card
| Control | Notes |
|---|---|
| Private Key (password) | POST `/api/save-key` → writes `SOLANA_PRIVATE_KEY` to `.env` |
| Show/Hide | Toggles input type password/text |
| Public Address | Derived from private key, displayed read-only |
| Balance | SOL balance via RPC; refreshed every 30 s or [↻ Refresh] |

### Presets 1 / 2 / 3 (tabs)
Each preset has two columns:

**Left — Entry:**
- Buy Amount (SOL) → `buy_amount_sol`
- Priority Fee (SOL) → `priority_fee_sol`
- Jito Tip (SOL) → `jito_tip_sol`
- Gas Fee (SOL) → `gas_fee_sol`
- Buy Slippage (%) → `buy_slippage`
- Sell Slippage (%) → `sell_slippage`
- Max Retries → `max_retries`

**Right — Entry Filters:**
- Min Dev Buy (SOL) + toggle Dev Buy Check → `min_dev_buy_sol`, `dev_buy_check_enabled`
- Min ATH of last 5 ($) → `min_ath_last5`
- ATH mode toggle → `ath_require_all` (Every token / Any 1 of 5)
- Min Migrations in last 5 → `min_migrations_last5`
- TX Count Min–Max → `min_tx_count`, `max_tx_count`
- TX count mode toggle → `tx_count_require_all`
- Min Token Lifetime (minutes) → `min_lifetime_minutes`
- Lifetime mode toggle → `lifetime_require_all`
- Entry MC Range Min–Max ($) → `min_entry_mc_usd`, `max_entry_mc_usd`
- Trailing Stops (up to 3): Activation %, Trail %, Position %; toggle enabled → `trailing_stops[]`

**Below columns — TP / SL:**
- Take Profit (up to 8): Price % | Position % → `take_profits[]`
- Stop Loss (up to 3): Price % | Position % → `stop_losses[]`
- TP Summary: shows total % sold and moonbag %

**Activate button:** Sets `active_preset` to this preset number.

### Stats Card
| Stat | Source |
|---|---|
| Tokens Found | `stats.tokens_found_today` |
| Passed Filters | `stats.tokens_passed_filters` |
| Buys Executed | `stats.buys_executed` |
| Open Positions X/Y | `open_positions` / `max_concurrent_positions` |
| [Reset] button | POST `/api/reset-positions` → sets `open_positions=0` only (stats untouched) |

**🧪 Test Mode Stats** (hidden until `test_buys_executed > 0`):
- Simulated: `test_buys_executed`
- Wins: `test_wins` (green)
- Losses: `test_losses` (red)
- WR: `test_wins / (test_wins + test_losses) * 100`%
- PnL: `test_total_pnl_sol` in SOL (color-coded) + % of deployed capital

**💰 Live Trading Stats** (hidden until `buys_executed > 0`):
- Trades: `buys_executed`
- Wins: `real_wins`, Losses: `real_losses`
- WR%, PnL: `real_total_pnl_sol` in SOL + %

PnL % = `pnl_sol / (buys * buy_amount_sol) * 100` using active preset's `buy_amount_sol`.

---

## 11. Telegram Alert Formats

All token links: `https://gmgn.ai/sol/token/{MINT}`
All tx links: `https://solscan.io/tx/{SIG}`
All prices shown as MC in USD, not raw token price.

### Scanner Started
```
✅ Сканер запущен!

👀 Слежу за новыми токенами на pump.fun
📩 Как только появится новый токен — сразу пришлю сюда
```

### Token Passed All Filters (✅)
```
✅ ПРОШЁЛ ВСЕ ФИЛЬТРЫ
⚡ ПРОВЕРКА #42
🔥 PEPE (PEPE)
📋 <MINT>
🔗 https://gmgn.ai/sol/token/<MINT>

👤 ДЕВ: <DEV_ADDR>
📅 Возраст кошелька: 14 дн.
💰 Баланс: 2.341 SOL
🚀 Всего запусков: 7
💵 Дев купил при запуске: 0.500 SOL
📦 Последние токены дева:
  1. TOKEN1 — ✅ Мигрировал | ATH: $45,231 💚
  2. TOKEN2 — ❌ Нет | ATH: $2,100
  3. TOKEN3 — ❌ Нет | ATH: $890
  4. TOKEN4 — ✅ Мигрировал | ATH: $38,000 💚
  5. TOKEN5 — ❌ Нет | ATH: $1,200

🕐 14:23:01.445
```

### Token Rejected (❌) — only sent when no position open
```
❌ $PEPE — PEPE
📋 <MINT>
🔗 https://gmgn.ai/sol/token/<MINT>
🚫 Not passed: Dev buy 0.050 SOL (min 0.100 SOL)
```

Possible reasons: `data fetch timeout`, `GMGN data unavailable`, `GMGN: no token history for this dev`, `dev data unavailable`, `ATH $X best of 5 (min $Y)`, `Migrations N/5 (min M)`, `TX count N (range min–max) for TOKENNAME`, `Token lifetimes [x.x, ...] min (min Y min), none passed`, `Dev buy X SOL (min Y SOL)`

### 🧪 Test Mode Buy Alert
```
🧪 TEST MODE — СИМУЛИРОВАННАЯ ПОКУПКА
🔥 PEPE ($PEPE)
📍 MC входа (с учётом слиппеджа 15%): $4,830
💰 Вложено (симуляция): 0.100000 SOL → 12345.6789 токенов
💸 Комиссии (не выплачены): priority 0.00100 + jito 0.00300 + gas 0.00001 = 0.00401 SOL
💼 Итого (симуляция): 0.104010 SOL
📈 Стратегия выхода:
  TP1: +10% → 50% позиции
  TP2: +20% → 50% позиции
  SL1: -23% → 100% позиции
  Trail1: act +5% | trail -20% | 100%
```

### Real Buy Alert (✅ КУПЛЕНО) — appended to the ✅ pass alert
```
✅ КУПЛЕНО
💰 Вложено: 0.099834 SOL ($14.73) → 12345.6789 токенов
📊 MC входа: $4,205
💸 Комиссии: priority 0.00100 + jito 0.00300 + gas 0.00005 = 0.00405 SOL ($0.598)
💼 Итого: 0.103884 SOL ($15.33)
📈 Стратегия выхода:
  TP1: +10% → 50% позиции
  SL1: -30% → 100% позиции
🔗 Solscan TX
```

### 🧪 Test Mode Exit Alert
```
🧪 💚 TEST MODE — ТЕЙК-ПРОФИТ   (or 🔴 СТОП-ЛОСС / 🟡 ТРЕЙЛИНГ СТОП)

🔥 PEPE (PEPE)
📋 <MINT>
🔗 GMGN

📊 Симулированный выход
├ Триггер: TP1 (+10%) | MC: $5,313
├ MC выхода: $5,313
├ Продано токенов: 6172 (50.0% позиции)
└ SOL получено (симуляция): 0.050012 SOL

⏱ Время в позиции: 2m 15s

📈 Симулированный PnL
├ Брутто PnL: +0.000012 SOL (+5.0%)
├ Комиссии (симуляция): 0.00100 SOL
└ Чистый PnL: -0.000988 SOL (-0.9%)

📦 Остаток (симуляция): 6172 токенов (50.0%)
```

When fully closed, replaces the remainder line with:
```
🏁 ПОЗИЦИЯ ЗАКРЫТА (СИМУЛЯЦИЯ)
├ Вложено: 0.100000 SOL
├ Получено: 0.115000 SOL
├ Комиссии итого: 0.002000 SOL
└ Итоговый PnL: +0.013000 SOL (+13.0%)
```

### Real Exit Alert (💚/🔴/🔶)
```
💚 ПРОДАЖА ТЕЙК-ПРОФИТ | PEPE[AbCd1234]   (or 🔴 СТОП-ЛОСС / 🔶 ТРЕЙЛИНГ-СТОП)
📌 TP1 (+10%) | MC: $5,313
📊 MC выхода: $5,313
🪙 Продано: 6172.3456 токенов (50.0%) → 0.050012 SOL
⏱ Время: 2m 15s
📈 Брутто PnL: +0.000012 SOL (+0.1%) / $0.00
💸 Комиссии: 0.00100 + 0.00300 = 0.00400 SOL
🏆 Чистый PnL: -0.003988 SOL (-4.0%)
📊 Остаток: 6172.3456 токенов (50.0%)
🔗 GMGN | TX
```

When fully closed (`remaining <= 1.0`), replaces остаток line with:
```
🔒 ПОЗИЦИЯ ЗАКРЫТА
  Вложено: 0.099834 SOL | Получено: 0.115000 SOL
  Комиссии итого: 0.008000 SOL
  Итоговый PnL: +0.007166 SOL (+7.2%)
```

### Sell Failed
```
❌ SELL FAILED | PEPE[AbCd1234]
📌 SL1 (-30%) | MC: $2,950
⚠️ Transaction failed to confirm: <SIG>
```

### N-Trades Session Complete
```
✅ Лимит торгов достигнут!
🎯 Выполнено 5/5 покупок.
🔴 Новые токены больше не обрабатываются.
```
Test mode variant:
```
✅ Сессия завершена.
🎯 Выполнено 5/5 сделок.
🔴 Бот приостановлен. Нажми Stop → Start для новой сессии.
```

### Scanner Crash
```
🚨 КРАШ СКАНЕРА
<error text>
📊 Открытых позиций: 2
🔄 Покупок в сессии: 3
```

### Live Position Monitor Crash
```
🚨 Live position monitor crashed
💊 PEPE[AbCd1234]
📋 <MINT>
⚠️ Check wallet manually. open_positions may need manual reset.
RuntimeError: websockets timeout
```

---

## 12. Current Status (as of 2026-05-24)

| Setting | Value |
|---|---|
| `auto_trading` | `false` (test mode only, no real buys) |
| `test_mode` | `true` |
| `active_preset` | 2 |
| `max_concurrent_positions` | 5 |
| Listener | `logsSubscribe` via Helius WSS |
| `extreme_fast_mode` | always `True` (hardcoded) |
| Bot start method | Dashboard at `localhost:8765` |

**Preset 2 (active):**
- Buy: 0.1 SOL, priority 0.001, jito 0.003
- Slippage: 30% buy / 25% sell
- Filters: `dev_buy_check_enabled=false`, `min_tx_count=50` (all 5 tokens), `min_lifetime_minutes=5` (all 5), MC range $2,000–$6,000
- TP: +10% → 50%, +20% → 50%
- SL: -23% → 100%
- Trail: activate +5%, trail -20%, 100% position

**Recent stats (this session):** 942 tokens found, 14 passed filters, 8 test buys, 4 wins / 2 losses, PnL +0.0147 SOL.

---

## 13. Open Items / Next Steps

**Not yet implemented:**
- **TP + Trailing Stop pairing**: Discussed — each TP paired with its own Trailing Stop that activates when TP fires (mutual exclusion, trailing takes over after TP hit). Not implemented; currently TP and Trail are independent.
- **SL validation in dashboard**: TP total validates against 100% (shows error if >100%). SL has no validation.
- **Geyser listener**: Code structure ready (`listener_factory.py`), endpoint not configured. Would give ~50 ms detection vs ~200 ms for logsSubscribe.
- **Market cap-based selling**: Road map idea — exit when token MC drops below entry MC (independent of price %).
- **mode/max_trades counter**: `successful_buys_this_session` resets only on bot restart (Stop + Start). There's no way to reset it via dashboard without restarting.

**Known architectural notes:**
- `_spm_config_lock` (in `scanner_position_monitor.py`) and `_config_lock` (in `scanner_runner.py`) are separate lock objects protecting the same `bot_config.json` file. Low real-world collision risk (stat writes are rare). Not fixed to avoid cross-module lock sharing.
- Stat counters (`test_buys_executed`, `tokens_found_today`, etc.) use a pre-read value that could be stale under concurrent token processing. Impact: counter may undercount by 1 if two tokens pass simultaneously. Display-only; not safety-critical.

---

## 14. Known Issues (Resolved This Session)

| Bug | Status | Fix |
|---|---|---|
| `_position_active` boolean didn't support multi-position | ✅ Fixed | Replaced with `open_positions` counter + `_position_entry_lock` |
| `/api/stop` only reset stats when bot was running (`if ok:` guard) → stuck counter | ✅ Fixed | Removed guard; reset is unconditional |
| `statMaxPos.textContent` after `textContent` overwrote parent → TypeError → all stats updates below it skipped silently | ✅ Fixed | Removed redundant line |
| `_update_live_stat` accepted unused `update_config` param | ✅ Fixed | Parameter removed from signature and call site |
| Dev buy set to 0 when event `vsr == INITIAL` (pre-dev-buy) | ✅ Fixed | Only set from event when `vsr > INITIAL`; otherwise leaves `None` so RPC fallback runs |
| BC price reads stale pre-dev-buy state → entry MC calculated at $337 instead of real $4K+ | ✅ Fixed | `_get_current_token_price_sol`: 5x retry with $500 MC threshold |
| GMGN CLI returned token count incorrectly | ✅ Fixed | Use `total` field from API response; fallback to `len(tokens)+` |

---

## 15. Running the Bot

```bash
# Start dashboard server (keep running)
uv run bot_server.py
# Dashboard → http://localhost:8765/

# Start bot manually (same as pressing Start in dashboard)
uv run src/scanner_runner.py bots/bot-scanner-telegram.yaml

# Check if running
pgrep -f scanner_runner.py

# View live logs
tail -f logs/bot-scanner-telegram_*.log

# Push to GitHub
cd /Users/valentyn/Documents/Bot\ Projects/pumpfun-bonkfun-bot
git add -A && git commit -m "message" && git push origin main
```

---

## 16. pump.fun Protocol Notes

Critical gotchas that are NOT documented in the IDL:

**BC v2 account (83 bytes):**
```
[0:8]    discriminator
[8:16]   virtual_token_reserves  u64 (little-endian)
[16:24]  virtual_sol_reserves    u64 (little-endian)
[24:32]  real_token_reserves     u64
[32:40]  real_sol_reserves       u64
[40:48]  token_total_supply      u64
[48:56]  complete                bool (padded)
[56:64]  ...
[81]     is_mayhem_mode          bool
[82]     is_cashback_coin        bool
```
Price formula: `price = (vsol / vtoken) * 10^6 / 10^9` (SOL per whole token, 6 decimals).

**BC `buy` instruction (post 2026-04-28 upgrade) — 18 accounts:**
The IDL says 17; reality is 18. Trailing account is one of 8 `BREAKING_FEE_RECIPIENTS` (mutable). Position: after `bonding-curve-v2`. Always cross-check against a recent successful on-chain tx.

**BC `sell` instruction — 16 or 17 accounts:**
- Non-cashback: 16 accounts
- Cashback: 17 accounts — inserts `user_volume_accumulator` PDA (`["user_volume_accumulator", user]`) BEFORE `bonding-curve-v2`
Detect cashback via BC account byte 82 (`is_cashback_coin`) or from CreateEvent payload.

**`bonding-curve-v2` PDA seed:** `["bonding-curve-v2", mint]` under the pump program (not `bonding-curve` — that's the old format).

**`pool-v2` (PumpSwap) PDA seed:** `["pool-v2", base_mint]` under the pump-amm program.

**`create_v2` instruction args:**
`name (str), symbol (str), uri (str), creator (pubkey), is_mayhem_mode (bool), is_cashback_enabled (OptionBool)`. `OptionBool` = 1 byte (not 2; it's a struct wrapping a bool).

**Creator vault:** BC.creator may be delegated to a PFEE-owned PDA after the initial creator buy (post 2026-04-28). Always re-read `creator` from current BC state before selling (done in `PlatformAwareSeller` and `PlatformAwareBuyer` extreme_fast_mode refresh).

**`extreme_fast_mode` caveat:** Skips the initial `get_pool_state` call. Still refreshes `is_mayhem_mode`, `is_cashback_coin`, and `creator` from chain with 4 retries (150 ms between). Without this refresh: wrong fee recipient (NotAuthorized 0x1770) or wrong account count (ConstraintSeeds 0x7d6).

**IDL file (`idl/pump_fun_idl.json`):** Incomplete. Does not list `bonding-curve-v2` or `pool-v2`. Do not trust it as the authoritative account list.
