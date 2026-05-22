# Project Context — PumpFun Sniper Bot

Read this file at the start of every session to get full project context without re-reading all source files.

---

## Purpose

Automated Solana token sniper for **pump.fun** and **letsbonk.fun**.

For every new token launch the bot:
1. Detects the token in real-time (WebSocket listener)
2. Fetches dev wallet info, GMGN token history, and on-chain BC dev-buy amount — in parallel
3. Applies configurable filters (min dev buy, ATH of dev's last 5 tokens, migration count)
4. Sends a full Telegram alert (passes or rejects with reason)
5. Optionally auto-buys tokens that pass all filters
6. Monitors open positions via `accountSubscribe` WebSocket and auto-sells on TP / SL / trailing stop

Controlled through a local web dashboard (`dashboard.html` served by `bot_server.py` on `localhost:8765`).

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.11+ |
| Blockchain | Solana mainnet |
| RPC / WebSocket | Helius (`mainnet.helius-rpc.com`) |
| Fast tx submission | Helius Staked Transactions (`staked.helius-rpc.com`) |
| Token detection | PumpPortal WebSocket (default), blockSubscribe, logsSubscribe, Geyser (gRPC) |
| Position monitoring | Solana `accountSubscribe` WebSocket (event-driven, no polling) |
| On-chain decoding | Direct struct.unpack on raw account bytes (no IDL dependency for price) |
| Dev history | `gmgn-cli` NPX subprocess → JSON |
| Notifications | Telegram Bot API (HTML parse mode) |
| Dashboard | Pure HTML/JS + Python stdlib `HTTPServer` |
| Config | `bot_config.json` (runtime state) + `bots/*.yaml` (scanner config) + `.env` (secrets) |
| Package manager | `uv` |
| Linter / formatter | Ruff |

---

## File & Folder Structure

```
pumpfun-bonkfun-bot/
│
├── bot_server.py                  # Dashboard HTTP server (port 8765)
├── dashboard.html                 # Web UI for config, presets, stats, wallet
├── bot_config.json                # Runtime config: presets, stats, open_positions (gitignored)
├── push.sh                        # One-command git push shortcut
├── CONTEXT.md                     # This file
├── CLAUDE.md                      # Claude Code instructions (project conventions)
│
├── bots/                          # Scanner/sniper YAML configs
│   ├── bot-scanner-telegram.yaml  # Main scanner config (used by bot_server.py)
│   ├── bot-sniper-1-geyser.yaml
│   ├── bot-sniper-2-logs.yaml
│   ├── bot-sniper-3-blocks.yaml
│   └── bot-sniper-4-pp.yaml
│
├── src/
│   ├── scanner_runner.py          # Main scanner loop (token detection → filter → buy)
│   ├── scanner_position_monitor.py # Position monitor (accountSubscribe TP/SL/trailing)
│   ├── bot_runner.py              # Original sniper bot entry point (pre-dashboard)
│   ├── config_loader.py           # YAML config loader with env-var interpolation
│   │
│   ├── core/
│   │   ├── client.py              # SolanaClient: RPC calls, tx send, account fetch
│   │   ├── wallet.py              # Keypair loading from base58 private key
│   │   ├── pubkeys.py             # Program IDs, constants (LAMPORTS_PER_SOL, etc.)
│   │   ├── rpc_rate_limiter.py    # Token-bucket RPC rate limiter
│   │   └── priority_fee/
│   │       ├── manager.py         # PriorityFeeManager: fixed or dynamic fee
│   │       ├── dynamic_fee.py     # getRecentPrioritizationFees-based dynamic fee
│   │       └── fixed_fee.py       # Fixed microlamport fee
│   │
│   ├── interfaces/
│   │   └── core.py                # Abstract base types: Platform enum, TokenInfo dataclass,
│   │                              #   BuyResult, SellResult, BaseBuyer/Seller interfaces
│   │
│   ├── platforms/
│   │   ├── pumpfun/
│   │   │   ├── address_provider.py   # Derive bonding-curve-v2 PDA, fee recipient
│   │   │   ├── curve_manager.py      # Decode BC account, compute buy/sell amounts
│   │   │   ├── event_parser.py       # Parse CreateEvent from logsSubscribe / blockSubscribe
│   │   │   ├── instruction_builder.py # Build buy/sell Solana instructions (18/16/17 accounts)
│   │   │   └── pumpportal_processor.py # Parse PumpPortal WS messages → TokenInfo
│   │   └── letsbonk/
│   │       ├── address_provider.py
│   │       ├── curve_manager.py      # Decode pool-state via IDL
│   │       ├── event_parser.py
│   │       ├── instruction_builder.py
│   │       └── pumpportal_processor.py
│   │
│   ├── monitoring/
│   │   ├── base_listener.py           # Abstract BaseListener
│   │   ├── listener_factory.py        # ListenerFactory.create_listener(type, ...)
│   │   ├── universal_logs_listener.py # logsSubscribe-based listener
│   │   ├── universal_block_listener.py # blockSubscribe-based listener
│   │   ├── universal_geyser_listener.py # Geyser gRPC listener
│   │   ├── universal_pumpportal_listener.py # PumpPortal WebSocket listener
│   │   ├── dev_checker.py             # Fetch dev wallet age, SOL balance, launch count
│   │   └── onchain_checker.py         # On-chain helpers (mint/freeze authority check)
│   │
│   ├── trading/
│   │   ├── base.py                # BaseBuyer / BaseSeller
│   │   ├── platform_aware.py      # PlatformAwareBuyer / PlatformAwareSeller
│   │   │                          #   (dispatches to pumpfun or letsbonk impl)
│   │   ├── universal_trader.py    # High-level trader with retry logic
│   │   └── position.py            # Position state tracking
│   │
│   ├── notifications/
│   │   └── telegram_reporter.py   # TelegramReporter: send_message, send_startup_message
│   │
│   ├── cleanup/
│   │   ├── manager.py             # CleanupManager: close empty token accounts
│   │   └── modes.py               # Cleanup modes enum
│   │
│   ├── utils/
│   │   ├── logger.py              # get_logger(), setup_file_logging()
│   │   ├── idl_manager.py         # Load / cache IDL JSON files
│   │   └── idl_parser.py          # Parse Anchor IDL → instruction layout
│   │
│   └── geyser/                    # Generated protobuf stubs for Geyser gRPC
│
├── idl/                           # Solana program IDL files
│   ├── pump_fun_idl.json          # pump.fun IDL (incomplete — see CLAUDE.md)
│   ├── pump_swap_idl.json         # PumpSwap (post-migration AMM)
│   ├── pump_fees.json
│   ├── raydium_amm_idl.json
│   └── raydium_launchlab_idl.json
│
├── learning-examples/             # Standalone research / test scripts (not production)
│   ├── manual_buy.py / manual_sell.py
│   ├── bonding-curve-progress/
│   ├── listen-new-tokens/
│   ├── listen-migrations/
│   ├── pumpswap/
│   ├── letsbonk-buy-sell/
│   └── ...
│
└── .env                           # Secrets (gitignored)
    # Keys: SOLANA_NODE_RPC_ENDPOINT, SOLANA_NODE_WSS_ENDPOINT,
    #       SOLANA_PRIVATE_KEY, HELIUS_STAKED_URL,
    #       TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
    #       GMGN_API_KEY, GEYSER_ENDPOINT, GEYSER_API_TOKEN,
    #       BUY_AMOUNT_SOL, BUY_SLIPPAGE, PRIORITY_FEE_SOL, AUTO_BUY_ENABLED
```

---

## Key Files — What Each Does

### `src/scanner_runner.py`
The main entry point for the scanning bot. Run via `bot_server.py` or directly with `uv run src/scanner_runner.py bots/bot-scanner-telegram.yaml`.

Flow per new token:
1. **Deduplication** — `processed_mints` dict prevents double-processing the same mint (logsSubscribe fires multiple log entries per tx)
2. **Parallel fetch (4 s timeout)**:
   - Stream 1: mint/freeze authority check + BC dev-buy SOL from raw account bytes
   - Stream 2+3: dev wallet stats (`check_dev_wallet`) + GMGN last-5-tokens history (`gmgn-cli`)
3. **Data quality guards** — if any source timed out, GMGN failed, or dev has no history → reject with reason
4. **Filter 2**: dev buy >= `min_dev_buy_sol`
5. **Filter 3**: ATH of dev's last 5 tokens vs `min_ath_last5` (any-1 or all-5 mode)
6. **Filter 4**: migrations count vs `min_migrations_last5`
7. **Telegram alert** — full message with checkmark or X + reason
8. **Auto-buy** — if `auto_trading=true` and position limit not reached, buys via `PlatformAwareBuyer`
9. **Position monitor** — after successful buy, spawns `monitor_position()` as asyncio task

Reads live settings from `bot_config.json` on every token so changes take effect without restart.

### `src/scanner_position_monitor.py`
Monitors an open position after a buy. Uses Solana `accountSubscribe` WebSocket — every on-chain buy/sell of the token triggers a notification, giving ~50–200 ms latency vs 1 s polling.

On each account notification:
- Decodes raw bonding-curve bytes → current price (no extra RPC call)
- Re-reads `bot_config.json` for live TP/SL settings
- Checks stop-loss levels (ascending order, one action per notification)
- Checks take-profit levels (ascending order, one action per notification)
- Checks trailing stop (activate → track peak → fire when drop from peak exceeds threshold)
- Executes sell via `PlatformAwareSeller`, sends Telegram notification
- Decrements `open_positions` in `bot_config.json` when position closes

`position_pct` in each level = % of **remaining** tokens (not original), so you can chain partial exits.

### `dashboard.html`
Single-page web UI. Fetches config from `bot_server.py` REST API on load. Features:
- **Bot Control**: Start / Stop bot process, Auto Trading toggle, Infinite / N-trades mode, max concurrent positions
- **Wallet**: Enter private key (saved to `.env`), show public address, SOL balance
- **Presets** (3 tabs): Buy amount, priority fee, gas fee, slippage, max retries, entry filters (min dev buy, ATH threshold, migration count), take-profit rows (up to 8), stop-loss rows (up to 3), trailing stop
- **Stats**: Tokens found today, passed filters, buys executed, open positions

All changes save to `bot_config.json` via `POST /api/config`. Preset activation updates `active_preset`.

### `bot_server.py`
Minimal Python stdlib HTTP server on `localhost:8765`. No dependencies beyond the project itself.

| Endpoint | Method | Description |
|---|---|---|
| `/` | GET | Serve `dashboard.html` |
| `/api/status` | GET | Bot running state + stats + open positions |
| `/api/config` | GET | Full `bot_config.json` |
| `/api/config` | POST | Overwrite `bot_config.json` |
| `/api/balance` | GET | SOL balance via RPC |
| `/api/start` | POST | Kill existing + start `scanner_runner.py` subprocess |
| `/api/stop` | POST | `pkill -f scanner_runner.py` |
| `/api/save-key` | POST | Write `SOLANA_PRIVATE_KEY` to `.env` |

Start with: `python bot_server.py`

### `bot_config.json`
Runtime state file read/written by both `bot_server.py` and `scanner_runner.py`. **Gitignored** (contains runtime state, reset on each bot start).

Key fields:
```json
{
  "active_preset": 1,
  "max_concurrent_positions": 1,
  "open_positions": 0,
  "auto_trading": false,
  "mode": "n",
  "max_trades": 10,
  "stats": { "tokens_found_today": 0, "tokens_passed_filters": 0, "buys_executed": 0 },
  "presets": {
    "1": {
      "buy_amount_sol": 0.01, "priority_fee_sol": 0.001, "gas_fee_sol": 0.00005,
      "buy_slippage": 30, "sell_slippage": 30, "max_retries": 2,
      "take_profits": [{ "price_pct": 20, "position_pct": 95 }],
      "stop_losses":  [{ "price_pct": 30, "position_pct": 95 }],
      "trailing_stops": [{ "enabled": true, "activation_pct": 10, "trail_size_pct": 20, "position_pct": 95 }],
      "filters": { "min_dev_buy_sol": 0.1, "min_ath_last5": 3000, "ath_require_all": true, "min_migrations_last5": 0, "min_entry_mc_usd": 0, "max_entry_mc_usd": 0 }
    }
  }
}
```

---

## What Already Works

| Feature | Status |
|---|---|
| Token detection (PumpPortal WS) | Working, default listener |
| Token detection (logsSubscribe) | Working |
| Token detection (blockSubscribe) | Working (requires paid RPC tier) |
| Token detection (Geyser gRPC) | Code ready, needs Geyser endpoint |
| Deduplication | `processed_mints` dict, 30-min window |
| Mayhem-mode filter | Silent skip (no Telegram) |
| Dev wallet check (age, balance, launches) | Via on-chain RPC |
| GMGN dev history (last 5 tokens + ATH) | Via `gmgn-cli` subprocess |
| On-chain BC dev-buy detection | Raw struct.unpack from account bytes |
| Filter: min dev buy SOL | Done |
| Filter: min ATH (any-1 or all-5 mode) | Done |
| Filter: migrations count | Done |
| Telegram alerts (pass + reject with reason) | HTML parse mode |
| Auto-buy (PlatformAwareBuyer) | Enabled via `auto_trading` flag |
| Position monitor (accountSubscribe WS) | Event-driven, ~50-200 ms latency |
| Take-profit (multi-level, partial) | Done |
| Stop-loss (multi-level, partial) | Done |
| Trailing stop (activation → peak track → fire) | Done |
| Sell Telegram notifications | With PnL% |
| Dashboard (presets, stats, wallet) | Done |
| Multi-platform: pump.fun + letsbonk.fun | Done |
| Helius Staked TX (fast submission) | Via `HELIUS_STAKED_URL` |
| Cashback-coin sell path (17 accounts) | See CLAUDE.md protocol notes |
| Cleanup empty token accounts | `cleanup/manager.py` |
| MC Range Entry Filter (min/max_entry_mc_usd) | Done — checked at buy moment via BC reserves + background SOL/USD price |

---

## Current Status (as of last session — 2026-05-22)

- **auto_trading = false** — bot is in scan-only / alert mode, no real buys
- **Active preset = 1** with `ath_require_all: true` and `min_ath_last5: 3000` — strict filter
- **Stats snapshot**: 90 tokens found, 7 passed filters, 0 buys executed
- **Geyser not configured** — `GEYSER_ENDPOINT` and `GEYSER_API_TOKEN` are empty in `.env`

---

## Known Issues / Notes

1. **`gas_fee_sol` is unused** — present in `bot_config.json` and dashboard UI but `scanner_runner.py` never reads it. Transaction fees are controlled by `priority_fee_sol` only.

2. **Stale-mint cleanup comment wrong** — `scanner_runner.py` line ~583 says "5 minutes" but cutoff is `1800` s (30 min). Comment-only bug, behavior is fine.

3. **No auth on dashboard server** — `bot_server.py` has no authentication. Anyone with localhost access can start/stop the bot or overwrite the private key via `/api/save-key`. Keep the server local-only.

4. **`ath_require_all: true` is very strict** — requires every one of the dev's last 5 tokens to have ATH >= threshold. This is why the pass rate is low (~8%). Consider `ath_require_all: false` or lowering `min_ath_last5` for more signals.

5. **`gmgn-cli` dependency** — GMGN fetch uses `npx gmgn-cli` subprocess. If Node.js / npm cache is cold, first call can be slow. Requires `~/.config/gmgn/.env` with GMGN API key, not the project `.env`.

6. **pump.fun IDL is incomplete** — see `CLAUDE.md` for full protocol notes. BC buy is 18 accounts, sell is 16/17 (cashback path). Do not rely on the IDL alone for account lists.

---

## Running the Bot

```bash
# Start dashboard (then open http://localhost:8765)
python bot_server.py

# Or run scanner directly
uv run src/scanner_runner.py bots/bot-scanner-telegram.yaml

# Push changes to GitHub
./push.sh
```

---

## Important pump.fun Protocol Notes (summary)

Full details in `CLAUDE.md`. Key gotchas:
- BC v2 PDA seed: `["bonding-curve-v2", mint]` under pump program
- BC `buy` = 18 accounts (post 2026-04-28), last account is one of 8 fee recipients
- BC `sell` = 16 accounts (non-cashback) or 17 (cashback — inserts `user_volume_accumulator` before BC)
- Cashback detection: byte 82 of BC account data, or `is_cashback_coin` on `TokenInfo`
- BondingCurve account = 83 bytes; trailing bytes: `is_mayhem_mode`, `is_cashback_coin`
- `extreme_fast_mode` skips RPC fetch — event parser must populate `is_mayhem_mode` and `is_cashback_coin` from `CreateEvent`

---

## Session Update — 2026-05-22

### Completed This Session

- `accountSubscribe` WebSocket price monitoring (50–200 ms latency, zero polling)
- All filter checks run in parallel via `asyncio.gather` (single flat gather, no nested wrappers)
- Mayhem tokens: silent skip, no Telegram message
- GMGN failure: token rejected, not passed through
- Deduplication: 30-min window via `processed_mints` dict
- Two Telegram message formats: short (failed + reason) and full (passed all filters)
- ATH filter toggle: "any 1 of 5" vs "all 5" (`ath_require_all`)
- Two-phase parallel fetch: GMGN + Helius start at **mint time** (not after dev buy arrives); dev buy check runs last after other filters pass (`task_bc_buy` as background `asyncio.Task`)
- `dev_buy_check_enabled` toggle in dashboard Entry Filters — when OFF: dev buy shown in alert but not enforced as filter
- TX count range filter (Filter 5): min/max tx count for dev's last 5 tokens, `tx_count_require_all` toggle
- Token lifetime filter (Filter 6): min lifetime in minutes for dev's last 5 tokens, `lifetime_require_all` toggle
- Both new filters use Helius `getSignaturesForAddress` (5 parallel calls, ~150 ms, chained after GMGN)
- Trailing stop reworked: trail% now measured from **activation price** (not entry); supports up to 3 independent trailing stops in `trailing_stops` array; each tracks its own `peak` starting from activation
- Dashboard: trailing stops replaced with multi-row list (+ Add button, up to 3); per-row hint shows calculated initial floor % from entry
- `bot_config.json`: `trailing_stop` single object → `trailing_stops` array; backward compat kept in monitor
- GitHub repo created: https://github.com/valentyn911/pumpfun-sniper-bot

### Open Tasks (next session)

1. **Geyser listener** — fully implemented code-wise; needs non-empty `GEYSER_ENDPOINT` and `GEYSER_API_TOKEN` in `.env`, then switch `listener_type: geyser` in bot YAML for ~100 ms detection vs ~300 ms PumpPortal

2. **Sniper wallet analysis** — write `wallet_analysis.py` using Helius RPC for wallet `3ixtZXbbJjNEq89GbeFu8vJMGDMNSePofNEf5kecVhsA`:
   - Always buys 1.27 SOL at MC ~$2.85–3.05K (pump.fun launch)
   - Gets 35.8M or 38M tokens
   - Exit: 2–6 sells, largest portion first at lowest TP
   - Losses: always 2×50% sells, SL ~-8% on second half
   - Winners: progressive ladder, trailing stop fires last ~13% on pullback
   - Script should parse Helius transaction history and reconstruct entry/exit logic

3. **`HELIUS_GATEKEEPER_URL`** — added to `.env.example`; obtain from Helius dashboard and populate `.env` for gated RPC access

### Architecture Notes (current state)

**Token processing flow (as implemented):**
```
Token minted → dev address known
│
├── task_bc_buy  (asyncio.Task, fires immediately)
├── task_mint    (asyncio.Task, fires immediately)
│
└── Phase 1 await (4 s timeout):
    ├── check_dev_wallet()        ~300 ms
    └── _get_gmgn_dev_tokens()   ~1.3 s  ← bottleneck
        └── Stream 4: getSignaturesForAddress ×5  ~150 ms (after GMGN)
│
Data guards → Filter 3 (ATH) → Filter 4 (Mig) → Filter 5 (TX) → Filter 6 (Lifetime)
│
└── Phase 2: await task_bc_buy (0.5 s cap, usually instant)
    └── Filter 2 (dev buy) — last, only if dev_buy_check_enabled=true
        └── ✅ ALL PASSED → Telegram + optional auto-buy
```

**Trailing stop formula (corrected):**
- Activation price = `entry × (1 + activation_pct/100)`
- At activation: `state["peak"] = current_price`
- Fire when: `current_price ≤ state["peak"] × (1 − trail_size_pct/100)`
- Initial floor: `activation_price × (1 − trail_size_pct/100)` = `entry × (1+act/100) × (1−trail/100)`

---

## Session Update — 2026-05-23

### Completed This Session

- **Jito MEV tip**: `jito_tip_sol` field in all presets; tip instruction injected as first instruction in every tx (buy + sell); random tip account selected per-tx from 8 official Jito tip accounts; wired through `core/client.py` → `PlatformAwareBuyer/Seller` → `scanner_position_monitor._sell()` → dashboard Jito Tip field
- **Dev buy display fix**: Was reading `virtual_sol_reserves` from BC account ~500 ms after event, inflated by other snipers' buys. Fix: use `solAmount` from PumpPortal event directly (new `dev_buy_sol` field on `TokenInfo`). Old `_fetch_bc_dev_buy()` kept as fallback for non-PumpPortal listeners.
- **Market Cap Range entry filter**: `min_entry_mc_usd` / `max_entry_mc_usd` in filters (0 = disabled). Checked immediately before buy — reads BC reserves → price SOL/token × 1B supply × SOL/USD. SOL/USD served by a background `_sol_price_updater()` task (Binance, every 30 s) — zero latency on buy path. Dashboard inputs added to Entry Filters section.

### Real Latency Numbers (from logs — 6,923 tokens, PumpPortal listener)

| Stage | Avg | Min | Max |
|---|---|---|---|
| mint→Phase1 complete (GMGN + dev wallet) | 763 ms | 614 ms | 3024 ms |
| Stream4 (getSignaturesForAddress ×N) | 151 ms | 78 ms | 1506 ms |
| mint→all filters done (Phase1+sigs) | 890 ms | 615 ms | 3025 ms |
| filters→send (buy tx) | logged — no buys yet (auto_trading=false) |
| send→confirm | logged — no buys yet (auto_trading=false) |

GMGN is the bottleneck (~763 ms avg). `[TIMING]` log lines now emitted for all 4 stages so buy-path latency will appear in logs once auto_trading is enabled.

### `bot_config.json` breaking change

`trailing_stop` single object → `trailing_stops` array. Backward compat kept in `scanner_position_monitor.py` (reads both forms).

### Current Status

- **auto_trading = false** — scan + alert only, no real buys
- **All timing instrumented**: `[TIMING] mint→filters`, `filters→send`, `send→confirm`, `mint→confirm` logged on every buy
