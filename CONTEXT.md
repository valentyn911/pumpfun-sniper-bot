# Project Context — PumpFun Sniper Bot
*Read this file at the start of EVERY session. It contains 
the full project state. Do not ask the user to re-explain anything 
that is documented here.*

---

## 1. PROJECT PURPOSE

Automated Solana token sniper for pump.fun and letsbonk.fun.

For every new token launch the bot:
1. Detects the token in real-time via WebSocket listener
2. Fetches dev wallet info, GMGN token history, on-chain BC 
   dev-buy amount — in parallel
3. Applies configurable filters (each filter has an ON/OFF toggle)
4. Sends a full Telegram alert (passes or rejects with reason)
5. Optionally auto-buys tokens that pass all filters
6. Monitors open positions via accountSubscribe WebSocket
7. Auto-sells on TP / SL / trailing stop

Controlled through a local web dashboard (dashboard.html served 
by bot_server.py on localhost:8765).

---

## 2. TECH STACK

| Layer | Technology |
|---|---|
| Language | Python 3.11+ |
| Blockchain | Solana mainnet |
| RPC / WebSocket | Helius mainnet |
| Fast tx submission | Helius Staked Transactions |
| Token detection | logsSubscribe (default) / PumpPortal / blockSubscribe / Geyser |
| Position monitoring | Solana accountSubscribe WebSocket (processed commitment) |
| On-chain decoding | Direct struct.unpack on raw BC account bytes |
| Dev history | GMGN OpenAPI HTTP + gmgn-cli NPX fallback |
| Notifications | Telegram Bot API (HTML parse mode) |
| Dashboard | Pure HTML/JS + Python stdlib HTTPServer |
| Config | bot_config.json + bots/*.yaml + .env |
| Package manager | uv |
| File locking | filelock (AsyncFileLock + FileLock) |

---

## 3. FILE STRUCTURE

```
pumpfun-sniper-bot/
├── bot_server.py              # Dashboard HTTP server (port 8765)
├── dashboard.html             # Web UI — config, presets, stats
├── bot_config.json            # Runtime config (gitignored)
├── CONTEXT.md                 # This file — full project context
├── CLAUDE.md                  # Claude Code instructions
│
├── bots/
│   └── bot-scanner-telegram.yaml  # Active scanner config
│
└── src/
    ├── scanner_runner.py          # Main bot process
    ├── scanner_position_monitor.py # Position monitor (TP/SL/Trail)
    ├── core/
    │   ├── client.py              # SolanaClient — RPC + tx send
    │   ├── wallet.py              # Keypair management
    │   ├── pubkeys.py             # Program IDs and constants
    │   └── priority_fee/
    │       ├── manager.py         # PriorityFeeManager
    │       ├── helius_fee.py      # HeliusFeeEstimator
    │       ├── dynamic_fee.py     # Dynamic fee strategy
    │       └── fixed_fee.py       # Fixed fee strategy
    ├── platforms/pumpfun/         # pump.fun specific logic
    ├── trading/
    │   ├── platform_aware.py      # PlatformAwareBuyer/Seller
    │   └── position.py            # Position state
    ├── monitoring/
    │   ├── dev_checker.py         # Dev wallet analysis
    │   └── universal_*_listener.py # Token detection listeners
    └── notifications/
        └── telegram_reporter.py   # Telegram message builder
```

---

## 4. bot_config.json STRUCTURE

```json
{
  "active_preset": 1,
  "max_concurrent_positions": 1,
  "open_positions": 0,
  "auto_trading": false,
  "test_mode": true,
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
    "1": {
      "name": "Preset 1",
      "buy_amount_sol": 0.1,
      "max_priority_fee_sol": 0.001,
      "jito_tip_sol": 0.003,
      "gas_fee_sol": 0.00005,
      "buy_slippage": 30,
      "sell_slippage": 30,
      "max_retries": 2,
      "fee_mode": "auto",
      "take_profits": [
        {"price_pct": 15, "position_pct": 33},
        {"price_pct": 30, "position_pct": 33},
        {"price_pct": 50, "position_pct": 33}
      ],
      "stop_losses": [
        {"price_pct": 15, "position_pct": 100}
      ],
      "trailing_stops": [
        {"enabled": true, "activation_pct": 0,
         "trail_size_pct": 15, "position_pct": 33},
        {"enabled": true, "activation_pct": 15,
         "trail_size_pct": 10, "position_pct": 33},
        {"enabled": true, "activation_pct": 30,
         "trail_size_pct": 25, "position_pct": 33}
      ],
      "filters": {
        "min_dev_buy_sol": 0.1,
        "dev_buy_check_enabled": false,
        "min_ath_last5": 6000,
        "ath_require_all": true,
        "ath_enabled": true,
        "min_migrations_last5": 1,
        "migrations_enabled": false,
        "new_wallet_enabled": true,
        "min_tx_count": 60,
        "max_tx_count": 0,
        "tx_count_require_all": true,
        "tx_count_enabled": true,
        "min_lifetime_minutes": 5,
        "lifetime_require_all": true,
        "lifetime_enabled": true,
        "min_entry_mc_usd": 2000,
        "max_entry_mc_usd": 6000,
        "entry_mc_enabled": false
      }
    }
  }
}
```

---

## 5. PIPELINE — FROM MINT TO SELL

### Detection (logsSubscribe):
Helius WSS → logsSubscribe on PUMP_FUN_PROGRAM_ID →
CreateEvent parsed → on_new_token() called
Latency: ~100–300ms from mint to Python

### Filter pipeline in _check_and_notify():
1. Deduplication check (processed_mints dict, 30-min window)
2. Capacity check (open_positions < max_concurrent_positions)
3. Blacklist check
4. Mayhem mode filter (silent skip)
5. FETCH DECISIONS (critical — read carefully):
   - skip_gmgn = True when ath_enabled=False AND migrations_enabled=False
     (Lifetime and TX Count do NOT affect this decision)
   - _need_sigs = True when tx_count_enabled=True OR lifetime_enabled=True
   - skip_dev_buy = True when dev_buy_check_enabled=False
   - skip_mc = True when entry_mc_enabled=False
6. Parallel Phase 1 (4s timeout):
   - GMGN fetch (if not skip_gmgn): dev's last 5 tokens, ATH, migrations
   - Dev wallet check: age, balance, total_launches via Helius RPC
   - BC dev buy fetch (if not skip_dev_buy): raw accountInfo bytes
7. Stream 4 (if _need_sigs): getSignaturesForAddress batch for 5 tokens
8. Data quality guards
9. New Wallet check: if is_new_wallet AND new_wallet_enabled=False → silent skip
10. Filter 3: ATH (if ath_enabled)
11. Filter 4: Migrations (if migrations_enabled)
12. Filter 5: TX Count (if tx_count_enabled)
13. Filter 6: Lifetime (if lifetime_enabled)
14. Filter 2: Dev Buy (if dev_buy_check_enabled)
15. Telegram alert sent
16. Buy execution (if auto_trading=True)
17. monitor_position() spawned as asyncio task

### PIPELINE LOG LINE (added recently):
Every token logs: [PIPELINE] skip_gmgn=X | need_sigs=X | dev_buy_check=X

### Position monitoring:
- accountSubscribe on bonding_curve_address
- Commitment: processed (fastest possible)
- Price formula: vsol_reserves / vtoken_reserves / 1000
- Backup poll: every 0.5s (recently changed from 2.0s)
- Max inactivity timeout: 300s (5 minutes)
- On timeout: uses last_known_price (NOT entry_price — recently fixed)

---

## 6. TP / SL / TRAILING STOP — HOW THEY WORK

### Key rule: ALL position_pct is % of ORIGINAL tokens, not remaining.
Example: bought 1000 tokens. TP1=50% → sells 500. TP2=50% → sells 500.

### Take Profit:
- Checked on every price tick AFTER SL check
- If price jumps past multiple TP levels at once → all fire simultaneously
- sells_in_progress set prevents double-fire
- tp_fired set prevents replay after on-chain confirmation

### Stop Loss:
- Checked BEFORE TP on every tick
- sl_fired set prevents replay

### Trailing Stop:
- peak tracked from position open time (even before activation)
- Activates when: (current_price - entry_price) / entry_price * 100 >= activation_pct
- Fires when: current_price <= peak * (1 - trail_size_pct / 100)
- Example: entry=1.0, activation=+10%, trail=20%, peak=1.5
  → fires at 1.5 × 0.80 = 1.20 (+20% from entry)

### TP + Trail PAIRING (dashboard shows pairs, code is independent):
- Dashboard: TP and Trail in same row share position_pct (Trail auto-copies from TP)
- Trail activation_pct cannot exceed TP price_pct (validation enforced in UI)
- In Python code: take_profits[] and trailing_stops[] are independent arrays
  (mutual cancellation NOT yet implemented in Python — open task)

### Settings snapshot:
TP/SL/Trail settings are snapshotted at position open time.
Changes in dashboard do NOT affect currently open positions.
Changes take effect on the NEXT new position only.

---

## 7. FILTER TOGGLES — COMPLETE ISOLATION RULE

When a filter toggle is OFF in dashboard:
1. The data FETCH for that filter is completely skipped
2. The filter CHECK is completely skipped
3. The Telegram message does NOT show data from that filter
4. Zero code runs for that filter — as if it doesn't exist

### FETCH DECISION TABLE (in scanner_runner.py):
```python
# GMGN fetch runs if: ath_enabled OR migrations_enabled
# Stream4 fetch runs if: tx_count_enabled OR lifetime_enabled
# BC_buy fetch runs if: dev_buy_check_enabled
# MC_check fetch runs if: entry_mc_enabled AND auto_trading=True
```

### Config reload:
Changes in dashboard take effect on the NEXT token (no restart needed).
_read_bot_config() is called 3 times per token: lines 1002, 1079, 1296.

### When filters are disabled, Telegram shows:
- No GMGN data (ATH/migrations): "📊 Dev history: not checked (ATH/migration filters off)"
- No dev buy data: "Dev First Buy: —"

---

## 8. FEE SYSTEM — THREE MODES

### Fee modes (fee_mode in preset config):

**AUTO** (default):
- Priority fee: Helius getPriorityFeeEstimate veryHigh, cap 3M µL/CU
- Jito tip: bundles.jito.wtf 75th percentile × 1.5, floor 0.001 SOL
- Both fetched in parallel ~50ms before buy
- Cost: ~0.002–0.006 SOL typical

**SUPER FAST** (zero API calls, zero latency):
- Priority fee: FIXED 10,000,000 µL/CU → 0.000850 SOL at 85k CU
- Jito tip: FIXED 0.010 SOL
- Total: ~0.010855 SOL
- Helius fee API call is SKIPPED entirely

**ULTRA** (maximum aggression):
- Priority fee: FIXED 50,000,000 µL/CU → 0.004250 SOL at 85k CU
- Jito tip: FIXED 0.025 SOL
- Total: ~0.029255 SOL
- Helius fee API call is SKIPPED entirely
- Based on real competitor analysis: top sniper bots pay $15–24 per buy

### Code constants (scanner_runner.py):
```python
SUPERFAST_PRIORITY_UL_PER_CU = 10_000_000
SUPERFAST_JITO_LAMPORTS      = 10_000_000
ULTRA_PRIORITY_UL_PER_CU     = 50_000_000
ULTRA_JITO_LAMPORTS          = 25_000_000
```

### Jito tip accounts (ALL 8 official — recently fixed 3 wrong addresses):
```
96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5
HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe  ← FIXED
Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY
ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49  ← FIXED
DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh
ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt
DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL
3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT  ← FIXED
```

### Jito is BUY-ONLY. Sells use priority + gas only (no Jito).

### SOL Price:
Fetched live from Binance API (CoinGecko as fallback).
Cached 60 seconds. Used for USD display in dashboard cost calculator.
Never hardcoded.

### /api/recommended-fees endpoint (bot_server.py):
Returns live priority fee recommendation + Jito tip floor + SOL price.
Congestion levels: low (<500k lamports) / medium / high (>2M lamports).

---

## 9. DASHBOARD — KEY FEATURES

### Preset structure:
- 3 presets with independent settings
- Each preset has: Entry params, Entry Filters, TP/Trail Pairs, SL
- Active preset controlled by active_preset field

### TP/Trail Pairs section:
- Up to 10 pairs per preset
- Each pair: TP (price_pct, position_pct) + Trail (enabled, activation_pct,
  trail_size_pct, position_pct auto-copied from TP)
- Rows numbered: TP 1/2/3, Trail 1/2/3 (renumberRows() called on add/remove)
- Trail activation_pct validation: cannot exceed TP price_pct (red border)
- Trail position_pct is readonly — always mirrors TP position_pct

### SL section:
- Up to 3 SL rows per preset
- Rows numbered: SL 1/2/3
- Validation: SL total % must not exceed 100% (independent from TP)

### Total exits summary line:
"TP exits: X% | SL exits: Y% | Moonbag: Z%"
- Moonbag = 100% - TP total only (SL not subtracted)
- Trail not counted (shares TP allocation)

### Filter toggles with latency hints:
Each filter shows latency cost when ON, savings when OFF.
Data source shown in parentheses: (gmgn.ai), (helius rpc), etc.

### New Wallet Dev toggle:
- When ON: tokens from devs with zero history processed normally
- When OFF: completely silently ignored (no Telegram, no logs)
- When token IS from new wallet and passes with toggle ON:
  Shows "🆕 New Dev Wallet — first token launch" in Telegram

### Fee Mode section:
Three buttons: AUTO / SUPER FAST / ULTRA
Live cost calculator updates as values change.
Color: green (<0.006 SOL) / yellow (0.006–0.020) / red (>0.020)
"Fetch Live Rates" button calls /api/recommended-fees.

---

## 10. TELEGRAM ALERTS — CURRENT STATE

### Messages in Russian (PARTIAL — redesign in progress):
Current messages still partially in Russian.
Full English redesign was specified but NOT YET IMPLEMENTED.

### Planned redesign (next task):
All messages to be redesigned in English with this format:

MESSAGE 1 — PASSED ALL FILTERS:
```
✅ PASSED ALL FILTERS [TEST/LIVE]
Token name, CA, GMGN link
Dev wallet, age, balance, launches, first buy
Recent dev tokens (up to 5, only if GMGN was called)
Token created time, auto-buy status
```

MESSAGE 2 — BUY EXECUTED:
```
🟢 BUY [TEST/LIVE]
Market cap at entry, buy amount + tokens
Fees breakdown (priority, jito if>0, gas)
Total cost, exit strategy (TP 1/2/3, Trail 1/2/3, SL 1/2/3)
```

MESSAGE 3 — TAKE PROFIT #N:
```
💚 TAKE PROFIT #N [TEST/LIVE]
Token, CA, GMGN link
Trigger: TP #N (+X%), MC at trigger, MC reference
Sold tokens, received SOL, fees, PnL gross/net
Time in position, remaining tokens
```

MESSAGE 4 — TRAIL STOP #N:
```
🔵 TRAIL STOP #N [TEST/LIVE]
Same structure + activation point, peak, fire point
```

MESSAGE 5 — STOP LOSS #N:
```
🔴 STOP LOSS #N [TEST/LIVE]
Same structure as TP
```

POSITION CLOSED SUMMARY (appended when last tokens sold):
```
Full exit history, total PnL gross/net, entry/exit fees,
total time in position.
```

### Implementation details for redesign:
- Track mc_at_tp[] array per position (Option A confirmed)
  TP#2 MC reference = MC when TP#1 fired (not entry MC)
- Jito fee shown only in BUY message (not in sell messages)
- Exit fees = priority + gas only (no Jito on sells)
- [TEST] or [LIVE] label on every message

---

## 11. FILE LOCKING

Three parts of the codebase write to bot_config.json.
All now use coordinated file locking:

- bot_server.py: FileLock("bot_config.lock") — threading context
- scanner_runner.py: AsyncFileLock("bot_config.lock") — asyncio context
- scanner_position_monitor.py: AsyncFileLock("bot_config.lock") — asyncio context

Library: filelock==3.29.0 (installed)

---

## 12. WHAT CURRENTLY WORKS (verified and tested)

| Feature | Status |
|---|---|
| Token detection (logsSubscribe) | ✅ Working |
| GMGN fetch (HTTP + CLI fallback) | ✅ Fixed (API key loading bug fixed) |
| All filter toggles — complete isolation | ✅ Fixed |
| New Wallet filter | ✅ Working |
| Dev Buy Check toggle | ✅ Working |
| ATH filter with GMGN skip when off | ✅ Working |
| Migrations filter | ✅ Working |
| TX Count filter | ✅ Fixed (was running even when disabled) |
| Token Lifetime filter | ✅ Fixed |
| Entry MC filter | ✅ Working |
| Auto-buy (PlatformAwareBuyer) | ✅ Working |
| Test mode simulation | ✅ Working |
| accountSubscribe position monitor | ✅ Working (0.5s backup poll) |
| TP multi-level partial sells | ✅ Working (% of original) |
| SL multi-level | ✅ Working |
| Trailing stop (activation→peak→fire) | ✅ Working |
| Timeout uses last known price | ✅ Fixed (was using entry_price) |
| Trail state key "active" everywhere | ✅ Fixed |
| TP/Trail position_pct pairing | ✅ In dashboard (code independent) |
| Row numbering TP1/Trail1/SL1 | ✅ Working |
| Toggle accessibility (re-enable) | ✅ Fixed |
| Panel width (no overlapping) | ✅ Fixed |
| Validation: TP and SL independent | ✅ Fixed |
| Moonbag calculation | ✅ Fixed |
| THREE fee modes AUTO/SUPERFAST/ULTRA | ✅ Working |
| Jito addresses (all 8 correct) | ✅ Fixed (3 were wrong) |
| Live SOL price (Binance/CoinGecko) | ✅ Working |
| /api/recommended-fees endpoint | ✅ Working |
| Fee cost calculator with colors | ✅ Working |
| filelock cross-process coordination | ✅ Working |
| PIPELINE log line per token | ✅ Added |
| New Wallet label in Telegram | ✅ Working |
| Config reload without restart | ✅ Confirmed |

---

## 13. KNOWN REMAINING ISSUES

1. **Telegram messages still in Russian** — full English redesign
   specified but not yet implemented. This is the NEXT TASK.

2. **TP + Trail mutual cancellation not in Python** — dashboard
   visually pairs them but in Python code they are independent arrays.
   If TP1 fires, Trail1 is NOT cancelled automatically.
   Documented here as open item.

3. **gas_fee_sol misleading name** — it's a display-only accounting
   field, not an actual on-chain fee. Rename to base_fee_estimate_sol
   recommended but not yet done.

4. **max_priority_fee_sol naming misleading** — setting 0.001 SOL
   doesn't mean you pay 0.001 SOL. It sets the µL/CU cap.
   Actual fee at 85k CU = cap_value × (85000/1e9) × 1e9 SOL.

5. **Position recovery after crash** — if bot crashes with open
   positions, monitors stop. No auto-recovery. Manual reset required.

6. **Geyser not configured** — code exists but no endpoint set.
   Would reduce detection latency from ~200ms to ~50ms.

7. **PnL in Telegram is approximate** — uses trigger price, not
   actual fill price. Difference = sell slippage amount.

---

## 14. NEXT TASKS (in priority order)

1. **[HIGH] Complete Telegram message redesign (English)**
   All messages to English per spec in section 10.
   Both telegram_reporter.py and scanner_position_monitor.py.
   Key requirement: position closed summary appended to last exit.
   MC tracking: mc_at_tp[] array per position (Option A).

2. **[MEDIUM] TP + Trail mutual cancellation in Python**
   When TP#N fires → cancel the paired Trail#N.
   Requires linking take_profits[i] to trailing_stops[i] by index.

3. **[LOW] Rename gas_fee_sol → base_fee_estimate_sol**

4. **[FUTURE] Geyser integration** — faster token detection

5. **[FUTURE] Bot 2 — Trusted Dev sniper**
   Concept: Bot 1 saves dev addresses that pass filters to trusted_devs.txt
   Bot 2 monitors those addresses and buys instantly without filters.
   MVP: fast path in existing scanner_runner.py (20 lines).
   Full: separate bot2_runner.py process.
   Risk: list quality degrades over time — needs TTL and scoring.

---

## 15. HOW TO RUN

```bash
# Start dashboard (open http://localhost:8765)
python bot_server.py

# Or run scanner directly
uv run src/scanner_runner.py bots/bot-scanner-telegram.yaml

# Push to GitHub
./push.sh
```

---

## 16. IMPORTANT PUMP.FUN PROTOCOL NOTES

- BC v2 PDA seed: ["bonding-curve-v2", mint] under pump program
- BC buy = 18 accounts (post 2026-04-28 upgrade)
- BC sell = 16 accounts (non-cashback) or 17 (cashback)
- Cashback detection: byte 82 of BC account data
- BondingCurve account = 83 bytes
- 8 fee recipients randomized per tx (BREAKING_FEE_RECIPIENTS)
- Token detection default: listener_type = logs (logsSubscribe)
- Active platform: pump_fun (not letsbonk)

---

## 17. HELIUS ACCOUNT

- Plan: Developer (check .env for actual plan details)
- SOLANA_NODE_RPC_ENDPOINT: Helius mainnet HTTP
- SOLANA_NODE_WSS_ENDPOINT: Helius mainnet WSS
- HELIUS_STAKED_URL: Used for all transaction sends (buys AND sells)
  CRITICAL: must be set in .env or bot falls back to regular RPC
- API key: d28868cd-5ac7-4f33-8db1-0a37e304c0a6
- Credits: 10M/month (Developer plan)
- Backup poll at 0.5s/position = 360 extra getAccountInfo/hour
  per 3 concurrent positions

---

*Last updated: 2026-05-26*
*Session: Major refactor session — filters, fees, dashboard, Jito fixes*
