"""
Position monitor for scanner_runner.py.

Uses accountSubscribe WebSocket instead of polling so every on-chain
trade triggers a price check within 50–200 ms rather than waiting up
to 1 second for the next poll tick.

Protocol:
  1. Open a Solana RPC WebSocket (derived from client.rpc_endpoint).
  2. Send accountSubscribe for the bonding-curve / pool-state address.
  3. On each accountNotification: decode raw account bytes → price.
  4. Run TP / SL / Trailing-Stop checks immediately.
  5. Reconnect automatically if the socket drops.

Settings are re-read from bot_config.json on every notification so
changes take effect without a restart.
"""

import asyncio
import base64
import json
import struct
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import websockets

from core.client import SolanaClient
from core.priority_fee.manager import PriorityFeeManager
from core.pubkeys import LAMPORTS_PER_SOL, TOKEN_DECIMALS
from core.wallet import Wallet
from interfaces.core import Platform, TokenInfo
from platforms import get_platform_implementations
from trading.platform_aware import PlatformAwareSeller
from utils.logger import get_logger

logger = get_logger(__name__)

# Below this threshold we treat the position as fully exited (dust)
_DUST_TOKENS: float = 1.0

# pump.fun BondingCurve binary layout (Anchor 8-byte discriminator prefix kept):
#   [0:8]   discriminator
#   [8:16]  virtual_token_reserves  u64
#   [16:24] virtual_sol_reserves    u64
_BC_VTOKEN_OFF: int = 8
_BC_VSOL_OFF: int = 16
_BC_MIN_LEN: int = 24

_PUMP_FUN_TOTAL_SUPPLY: float = 1_000_000_000.0

_SPM_CONFIG_PATH = Path(__file__).parent.parent / "bot_config.json"
_spm_config_lock = asyncio.Lock()


async def _update_test_stat(key: str, pnl_delta: float = 0.0) -> None:
    """Increment test_wins or test_losses and accumulate test_total_pnl_sol."""
    async with _spm_config_lock:
        try:
            if _SPM_CONFIG_PATH.exists():
                with open(_SPM_CONFIG_PATH) as f:
                    cfg = json.load(f)
            else:
                cfg = {}
            stats = cfg.setdefault("stats", {})
            stats[key] = stats.get(key, 0) + 1
            if pnl_delta != 0.0:
                stats["test_total_pnl_sol"] = stats.get("test_total_pnl_sol", 0.0) + pnl_delta
            with open(_SPM_CONFIG_PATH, "w") as f:
                json.dump(cfg, f, indent=2)
        except Exception as exc:
            logger.warning(f"[test monitor] Failed to update stat {key}: {exc}")


async def _update_live_stat(key: str, pnl_delta: float) -> None:
    """Increment real_wins or real_losses and accumulate real_total_pnl_sol."""
    async with _spm_config_lock:
        try:
            if _SPM_CONFIG_PATH.exists():
                with open(_SPM_CONFIG_PATH) as f:
                    cfg = json.load(f)
            else:
                cfg = {}
            stats = cfg.setdefault("stats", {})
            stats[key] = stats.get(key, 0) + 1
            stats["real_total_pnl_sol"] = stats.get("real_total_pnl_sol", 0.0) + pnl_delta
            with open(_SPM_CONFIG_PATH, "w") as f:
                json.dump(cfg, f, indent=2)
        except Exception as exc:
            logger.warning(f"[live monitor] Failed to update stat {key}: {exc}")


@dataclass
class SimulatedPosition:
    """Data container for a paper-trade (test mode) position."""

    mint: str
    symbol: str
    name: str
    entry_price_sol: float
    entry_mc_usd: float
    simulated_token_amount: float
    entry_sol: float
    priority_fee_sol: float
    jito_tip_sol: float
    gas_fee_sol: float
    total_cost_sol: float
    entry_timestamp: float  # time.time() at simulated buy
    preset_snapshot: dict = field(default_factory=dict)
    platform: Any = None  # Platform enum
    bonding_curve: str | None = None
    is_cashback_coin: bool = False
    sol_price_at_entry: float = 0.0
    active_preset_id: str = "1"


def _price_from_raw(
    data: bytes,
    platform: Platform,
    curve_manager: Any,
) -> float | None:
    """Decode token price (SOL/token) from raw account bytes.

    pump.fun: two struct.unpack calls at known offsets — no IDL needed.
    letsbonk: delegates to the curve manager's IDL decoder on the same bytes.

    Returns None when data is too short or reserves are zero/invalid.
    """
    if platform == Platform.PUMP_FUN:
        if len(data) < _BC_MIN_LEN:
            return None
        vtoken = struct.unpack_from("<Q", data, _BC_VTOKEN_OFF)[0]
        vsol = struct.unpack_from("<Q", data, _BC_VSOL_OFF)[0]
        if vtoken <= 0 or vsol <= 0:
            return None
        return (vsol / vtoken) * (10**TOKEN_DECIMALS) / LAMPORTS_PER_SOL

    if platform == Platform.LETS_BONK:
        try:
            pool_data = curve_manager._decode_pool_state_with_idl(data)
            return pool_data.get("price_per_token")
        except Exception:
            return None

    return None


def _fmt_mc(mc: float | None) -> str:
    if mc is None or mc <= 0:
        return "—"
    if mc < 100:
        return f"${mc:.2f}"
    return f"${mc:,.0f}"


def _fmt_duration(seconds: float) -> str:
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    m, s = divmod(s, 60)
    if m < 60:
        return f"{m}m {s}s"
    h, m = divmod(m, 60)
    return f"{h}h {m}m"


async def monitor_position(
    token_info: TokenInfo,
    token_amount: float,
    entry_price: float,
    client: SolanaClient,
    wallet: Wallet,
    read_config: Callable[[], dict[str, Any] | None],
    update_config: Callable[[dict[str, Any]], Any],
    preset_id: str,
    priority_fee_microlamports: int,
    notify_fn: Callable[[str], Any] | None = None,
    entry_time: float | None = None,
    sol_price_getter: Callable[[], float] | None = None,
    position_close_fn: Callable[[], None] | None = None,
) -> None:
    """Monitor an open position via accountSubscribe and execute exits.

    Each accountNotification (fired by every on-chain buy/sell of the token):
      1. Decode raw account bytes → current price (no extra RPC call).
      2. Re-read active preset from bot_config.json (live settings).
      3. Check stop-loss levels (ascending price_pct → least loss fires first).
      4. Check take-profit levels (ascending price_pct → smallest gain first).
      5. Check trailing stop (activate → trail peak → fire).
      6. Fire all triggered levels per notification (each as a background task).
      7. Sells run as background tasks and retry indefinitely until confirmed.
      8. Fired flags are set ONLY after on-chain sell confirmation.

    position_pct in every level is % of the *original* token_amount at buy time.
    Moonbag = whatever tokens remain when the monitor exits.

    Args:
        token_info: Token being held.
        token_amount: Tokens received from the buy (decimal).
        entry_price: SOL price per token at buy time.
        client: Shared Solana RPC client (rpc_endpoint used to build WSS URL).
        wallet: Trading wallet.
        read_config: Callable → current bot_config dict (sync, cheap).
        update_config: Async callable that writes changes to bot_config.
        preset_id: Active preset ID (str) at the time of the buy.
        priority_fee_microlamports: Fallback fee if not found in preset.
        notify_fn: Optional async callable for Telegram notifications.
        entry_time: monotonic() timestamp of the buy (for duration tracking).
        sol_price_getter: Optional callable returning current SOL/USD price.
    """
    t_start = entry_time if entry_time is not None else time.monotonic()
    label = f"{token_info.symbol}[{str(token_info.mint)[:8]}]"
    logger.info(
        f"[monitor] {label} started | "
        f"entry={entry_price:.8f} SOL | amount={token_amount:.4f}"
    )

    # Derive WebSocket URL from the HTTP RPC endpoint
    rpc_url: str = client.rpc_endpoint
    wss_url = rpc_url.replace("https://", "wss://").replace("http://", "ws://")

    impls = get_platform_implementations(token_info.platform, client)
    curve_manager = impls.curve_manager
    address_provider = impls.address_provider

    if token_info.platform == Platform.PUMP_FUN and token_info.bonding_curve:
        pool_address = token_info.bonding_curve
    elif token_info.platform == Platform.LETS_BONK and token_info.pool_state:
        pool_address = token_info.pool_state
    else:
        pool_address = address_provider.derive_pool_address(token_info.mint)

    pool_addr_str = str(pool_address)

    remaining: float = token_amount
    tp_fired: set[int] = set()   # original indices of fired TP levels
    sl_fired: set[int] = set()   # original indices of fired SL levels
    # Per-trailing-stop state dicts: {active, peak, fired}
    trail_states: list[dict] = []
    # Keys of sell tasks currently in flight: "tp_0", "sl_1", "trail_2", etc.
    sells_in_progress: set[str] = set()
    # Cumulative tracking for full-close PnL summary
    total_sol_received: float = 0.0
    total_fees_paid: float = 0.0

    async def _sell(sell_amount: float, reason: str, current_price: float) -> bool:
        """Execute a partial sell, decrement remaining, return True on success."""
        nonlocal remaining, total_sol_received, total_fees_paid

        if sell_amount < _DUST_TOKENS:
            logger.debug(f"[monitor] {label} skip dust sell {sell_amount:.2f} | {reason}")
            return False

        cfg = read_config() or {}
        active_pid = str(cfg.get("active_preset", preset_id))
        preset = (cfg.get("presets") or {}).get(active_pid, {})
        sell_slip = float(preset.get("sell_slippage", 25)) / 100
        fee_sol = float(
            preset.get("priority_fee_sol", priority_fee_microlamports / 1_000_000_000)
        )
        fee_ul = int(fee_sol * 1_000_000_000)
        jito_tip_sol = float(preset.get("jito_tip_sol", 0.003))
        jito_tip_ul = int(jito_tip_sol * 1_000_000_000) if jito_tip_sol > 0 else None

        pf = PriorityFeeManager(
            client=client,
            enable_dynamic_fee=False,
            enable_fixed_fee=True,
            fixed_fee=fee_ul,
            extra_fee=0.0,
            hard_cap=fee_ul,
        )
        seller = PlatformAwareSeller(
            client=client,
            wallet=wallet,
            priority_fee_manager=pf,
            slippage=sell_slip,
            max_retries=3,
            jito_tip_lamports=jito_tip_ul,
        )

        logger.info(
            f"[monitor] {label} SELL {sell_amount:.4f} @ {current_price:.8f} SOL | {reason}"
        )
        try:
            result = await seller.execute(token_info, sell_amount, current_price)
        except Exception as exc:
            logger.error(f"[monitor] {label} sell exception: {exc}")
            return False

        if result.success:
            sol_received = sell_amount * current_price
            fees_total = fee_sol + jito_tip_sol
            total_fees_paid += fees_total
            total_sol_received += sol_received
            remaining = max(0.0, remaining - sell_amount)

            logger.info(
                f"[monitor] {label} sell OK | remaining={remaining:.4f} | "
                f"tx={result.tx_signature}"
            )

            if notify_fn is not None:
                pnl_pct = ((current_price - entry_price) / entry_price) * 100
                sol_pnl = (current_price - entry_price) * sell_amount
                net_pnl_sol = sol_pnl - fees_total
                entry_total_sol = entry_price * token_amount
                net_pnl_pct = (net_pnl_sol / entry_total_sol * 100) if entry_total_sol > 0 else 0.0
                position_sold_pct = (sell_amount / token_amount * 100) if token_amount > 0 else 0.0
                remaining_pct = (remaining / token_amount * 100) if token_amount > 0 else 0.0

                sol_price = sol_price_getter() if sol_price_getter else 0.0
                duration_s = time.monotonic() - t_start

                tx_sig = str(result.tx_signature) if result.tx_signature else None
                mint_str = str(token_info.mint)
                token_url = f"https://gmgn.ai/sol/token/{mint_str}"
                tx_url = f"https://solscan.io/tx/{tx_sig}" if tx_sig else ""

                if reason.startswith("TP"):
                    trigger_emoji = "💚"
                    trigger_type = "ТЕЙК-ПРОФИТ"
                elif reason.startswith("SL"):
                    trigger_emoji = "🔴"
                    trigger_type = "СТОП-ЛОСС"
                else:
                    trigger_emoji = "🔶"
                    trigger_type = "ТРЕЙЛИНГ-СТОП"

                exit_mc = current_price * _PUMP_FUN_TOTAL_SUPPLY * sol_price if sol_price > 0 else None
                lines = [
                    f"{trigger_emoji} <b>ПРОДАЖА {trigger_type}</b> | {label}",
                    f"📌 {reason}",
                    f"📊 MC выхода: {_fmt_mc(exit_mc)}",
                    f"🪙 Продано: {sell_amount:.4f} токенов ({position_sold_pct:.1f}%) → {sol_received:.6f} SOL",
                    f"⏱ Время: {_fmt_duration(duration_s)}",
                    f"📈 Брутто PnL: {sol_pnl:+.6f} SOL ({pnl_pct:+.1f}%)" + (f" / ${sol_pnl * sol_price:+.2f}" if sol_price > 0 else ""),
                    f"💸 Комиссии: {fee_sol:.5f} + {jito_tip_sol:.5f} = {fees_total:.5f} SOL",
                    f"🏆 Чистый PnL: {net_pnl_sol:+.6f} SOL ({net_pnl_pct:+.1f}%)",
                ]

                if remaining > _DUST_TOKENS:
                    lines.append(f"📊 Остаток: {remaining:.4f} токенов ({remaining_pct:.1f}%)")
                else:
                    net_total = total_sol_received - entry_total_sol - total_fees_paid
                    net_total_pct = (net_total / entry_total_sol * 100) if entry_total_sol > 0 else 0.0
                    lines += [
                        "🔒 <b>ПОЗИЦИЯ ЗАКРЫТА</b>",
                        f"  Вложено: {entry_total_sol:.6f} SOL | Получено: {total_sol_received:.6f} SOL",
                        f"  Комиссии итого: {total_fees_paid:.6f} SOL",
                        f"  Итоговый PnL: {net_total:+.6f} SOL ({net_total_pct:+.1f}%)",
                    ]
                    real_stat_key = "real_wins" if net_total > 0 else "real_losses"
                    asyncio.create_task(_update_live_stat(real_stat_key, net_total))

                if tx_url:
                    lines.append(f"🔗 <a href='{token_url}'>GMGN</a> | <a href='{tx_url}'>TX</a>")
                else:
                    lines.append(f"🔗 <a href='{token_url}'>GMGN</a>")

                asyncio.create_task(notify_fn("\n".join(lines)))

            return True

        logger.warning(f"[monitor] {label} sell FAILED: {result.error_message}")
        if notify_fn is not None:
            asyncio.create_task(notify_fn(
                f"❌ <b>SELL FAILED</b> | {label}\n"
                f"📌 {reason}\n"
                f"⚠️ {result.error_message or 'unknown error'}"
            ))
        return False

    async def _sell_with_retry(
        sell_amount: float,
        reason: str,
        current_price: float,
        key: str,
        fired_set: set[int] | None,
        fired_idx: int | None,
        trail_state_dict: dict | None,
    ) -> None:
        """Background sell task. Retries until on-chain confirmation, then sets fired flag."""
        attempt = 0
        while True:
            attempt += 1
            try:
                ok = await _sell(sell_amount, reason, current_price)
            except Exception as exc:
                logger.error(f"[monitor] {label} sell task exception (attempt {attempt}): {exc}")
                ok = False

            if ok:
                if fired_set is not None and fired_idx is not None:
                    fired_set.add(fired_idx)
                if trail_state_dict is not None:
                    trail_state_dict["fired"] = True
                sells_in_progress.discard(key)
                return

            if attempt > 1:
                logger.warning(f"[monitor] {label} sell retry {attempt} | {reason}")
            await asyncio.sleep(0.5)

    async def _on_price(current_price: float) -> None:
        """Run TP/SL/trailing-stop checks for the given price tick.

        Every tick is fully evaluated — sells_in_progress only prevents
        re-firing the same level, not evaluation of other levels.
        """
        price_chg_pct = ((current_price - entry_price) / entry_price) * 100

        cfg = read_config() or {}
        active_pid = str(cfg.get("active_preset", preset_id))
        preset = (cfg.get("presets") or {}).get(active_pid, {})
        tp_levels: list[dict] = preset.get("take_profits", [])
        sl_levels: list[dict] = preset.get("stop_losses", [])

        # Support trailing_stops array (new) and trailing_stop single object (legacy)
        ts_list: list[dict] = list(preset.get("trailing_stops") or [])
        if not ts_list:
            legacy = preset.get("trailing_stop", {})
            if legacy.get("enabled"):
                ts_list = [legacy]

        # Lazily extend trail_states for any new stops added to config
        while len(trail_states) < len(ts_list):
            trail_states.append({"active": False, "peak": entry_price, "fired": False})

        # --- Stop-loss (ascending price_pct → least loss fires first) ---
        for orig_idx, level in sorted(
            enumerate(sl_levels), key=lambda x: x[1].get("price_pct", 0)
        ):
            if orig_idx in sl_fired:
                continue
            key = f"sl_{orig_idx}"
            if key in sells_in_progress:
                continue  # this level's sell already in flight, don't double-fire
            trigger = entry_price * (1 - level["price_pct"] / 100)
            if current_price <= trigger:
                amount = token_amount * (level["position_pct"] / 100)
                sells_in_progress.add(key)
                _sol = sol_price_getter() if sol_price_getter else 0.0
                asyncio.create_task(_sell_with_retry(
                    amount,
                    f"SL{orig_idx + 1} (-{level['price_pct']:.0f}%) | MC: {_fmt_mc(current_price * _PUMP_FUN_TOTAL_SUPPLY * _sol)}",
                    current_price,
                    key,
                    sl_fired,
                    orig_idx,
                    None,
                ))
                # no return — keep evaluating other levels this tick

        if remaining <= _DUST_TOKENS:
            return

        # --- Take-profit (ascending price_pct → smallest gain first) ---
        for orig_idx, level in sorted(
            enumerate(tp_levels), key=lambda x: x[1].get("price_pct", 0)
        ):
            if orig_idx in tp_fired:
                continue
            key = f"tp_{orig_idx}"
            if key in sells_in_progress:
                continue  # this level's sell already in flight, don't double-fire
            trigger = entry_price * (1 + level["price_pct"] / 100)
            if current_price >= trigger:
                amount = token_amount * (level["position_pct"] / 100)
                sells_in_progress.add(key)
                _sol = sol_price_getter() if sol_price_getter else 0.0
                asyncio.create_task(_sell_with_retry(
                    amount,
                    f"TP{orig_idx + 1} (+{level['price_pct']:.0f}%) | MC: {_fmt_mc(current_price * _PUMP_FUN_TOTAL_SUPPLY * _sol)}",
                    current_price,
                    key,
                    tp_fired,
                    orig_idx,
                    None,
                ))
                # no return — keep evaluating other levels this tick

        if remaining <= _DUST_TOKENS:
            return

        # --- Trailing stops (each independent; peak tracks from activation price) ---
        for i, ts_cfg in enumerate(ts_list):
            if not ts_cfg.get("enabled", True):
                continue
            state = trail_states[i]
            if state["fired"]:
                continue
            key = f"trail_{i}"
            if key in sells_in_progress:
                continue  # this trail's sell already in flight, don't double-fire

            act_pct = float(ts_cfg.get("activation_pct", 50))
            trail_size = float(ts_cfg.get("trail_size_pct", 20))
            trail_pos = float(ts_cfg.get("position_pct", 100))

            if not state["active"] and price_chg_pct >= act_pct:
                state["active"] = True
                state["peak"] = current_price
                logger.info(
                    f"[monitor] {label} trail[{i + 1}] ACTIVATED | "
                    f"activation=+{act_pct:.1f}% | floor={current_price * (1 - trail_size / 100):.8f} | "
                    f"price={current_price:.8f} ({price_chg_pct:+.1f}%)"
                )

            if state["active"]:
                if current_price > state["peak"]:
                    state["peak"] = current_price
                trail_trigger = state["peak"] * (1 - trail_size / 100)
                if current_price <= trail_trigger:
                    amount = token_amount * (trail_pos / 100)
                    sells_in_progress.add(key)
                    _sol = sol_price_getter() if sol_price_getter else 0.0
                    asyncio.create_task(_sell_with_retry(
                        amount,
                        f"Trail{i + 1} (-{trail_size:.0f}% от пика) | MC пика: {_fmt_mc(state['peak'] * _PUMP_FUN_TOTAL_SUPPLY * _sol)} | MC выхода: {_fmt_mc(current_price * _PUMP_FUN_TOTAL_SUPPLY * _sol)}",
                        current_price,
                        key,
                        None,
                        None,
                        state,
                    ))
                    # no return — keep evaluating other trailing stops this tick

    # ---------------------------------------------------------------------------
    # accountSubscribe WebSocket loop
    # ---------------------------------------------------------------------------
    try:
        while remaining > _DUST_TOKENS:
            try:
                async with websockets.connect(
                    wss_url,
                    ping_interval=20,
                    ping_timeout=10,
                ) as ws:
                    await ws.send(
                        json.dumps({
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "accountSubscribe",
                            "params": [
                                pool_addr_str,
                                {"encoding": "base64", "commitment": "processed"},
                            ],
                        })
                    )

                    # Wait for subscription confirmation
                    confirm_raw = await asyncio.wait_for(ws.recv(), timeout=10.0)
                    confirm = json.loads(confirm_raw)
                    if "result" not in confirm:
                        logger.warning(
                            f"[monitor] {label} accountSubscribe failed: {confirm}"
                        )
                        await asyncio.sleep(2)
                        continue

                    logger.info(
                        f"[monitor] {label} accountSubscribe OK "
                        f"(sub_id={confirm['result']}) | watching {pool_addr_str[:8]}"
                    )

                    # Notification loop — no polling, purely event-driven
                    while remaining > _DUST_TOKENS:
                        try:
                            msg_raw = await asyncio.wait_for(ws.recv(), timeout=60.0)
                        except asyncio.TimeoutError:
                            # No trade in 60 s — send a WS-level ping to keep alive
                            await ws.ping()
                            continue

                        msg = json.loads(msg_raw)
                        if msg.get("method") != "accountNotification":
                            continue

                        try:
                            data_b64 = msg["params"]["result"]["value"]["data"][0]
                        except (KeyError, IndexError, TypeError):
                            continue

                        raw = base64.b64decode(data_b64)
                        price = _price_from_raw(raw, token_info.platform, curve_manager)
                        if price and price > 0:
                            await _on_price(price)

            except websockets.exceptions.ConnectionClosed as exc:
                logger.warning(
                    f"[monitor] {label} WS closed ({exc.code}), reconnecting..."
                )
                await asyncio.sleep(1)
            except asyncio.TimeoutError:
                logger.warning(f"[monitor] {label} WS timeout, reconnecting...")
                await asyncio.sleep(1)
            except Exception as exc:
                logger.error(f"[monitor] {label} WS error: {exc}")
                await asyncio.sleep(2)

    except Exception as exc:
        logger.error(f"[monitor] {label} UNHANDLED CRASH: {exc}", exc_info=True)
        if notify_fn is not None:
            try:
                await notify_fn(
                    f"🚨 <b>Live position monitor crashed</b>\n"
                    f"💊 {label}\n"
                    f"📋 <code>{str(token_info.mint)}</code>\n"
                    f"⚠️ Check wallet manually. open_positions may need manual reset.\n"
                    f"<code>{type(exc).__name__}: {exc}</code>"
                )
            except Exception:
                pass
        raise

    finally:
        moonbag = remaining if remaining > _DUST_TOKENS else 0.0
        logger.info(
            f"[monitor] {label} position closed | moonbag={moonbag:.4f} tokens"
        )
        cur_cfg = read_config() or {}
        await update_config(
            {"open_positions": max(0, cur_cfg.get("open_positions", 1) - 1)}
        )
        if position_close_fn is not None:
            position_close_fn()


async def monitor_position_test(
    sim_pos: SimulatedPosition,
    rpc_endpoint: str,
    notify_fn: Callable[[str], Any] | None = None,
    sol_price_getter: Callable[[], float] | None = None,
    position_close_fn: Callable[[], None] | None = None,
) -> None:
    """Monitor a paper-trade position via accountSubscribe.

    Uses sim_pos.preset_snapshot for ALL TP/SL/trailing decisions — never
    re-reads bot_config.json. Sells are instant simulations (no blockchain).
    On full close: increments test_wins or test_losses in bot_config.json.
    Does NOT touch open_positions counter.
    """
    original_amount: float = sim_pos.simulated_token_amount  # never changes
    remaining: float = sim_pos.simulated_token_amount
    entry_price: float = sim_pos.entry_price_sol
    entry_time: float = sim_pos.entry_timestamp
    tp_fired: set[int] = set()
    sl_fired: set[int] = set()
    sells_in_progress: set[str] = set()
    total_sim_sol_received: float = 0.0
    total_sim_fees: float = 0.0
    preset = sim_pos.preset_snapshot

    # Build trail states from preset snapshot at entry time (not re-read later)
    trail_states: list[dict] = []
    for i, t in enumerate(preset.get("trailing_stops", [])):
        trail_states.append({
            "activated": False,
            "peak": None,
            "fired": False,
            "enabled": bool(t.get("enabled", True)),
            "activation_pct": float(t.get("activation_pct", 50)),
            "trail_size_pct": float(t.get("trail_size_pct", 20)),
            "position_pct": float(t.get("position_pct", 100)),
            "index": i,
        })

    logger.info(
        f"[TEST] Starting position monitor for {sim_pos.mint[:8]}, "
        f"entry={sim_pos.entry_price_sol:.8f}, tokens={sim_pos.simulated_token_amount:.0f}"
    )

    if not sim_pos.bonding_curve:
        logger.warning(f"[TEST] No bonding_curve for {sim_pos.mint[:8]} — cannot subscribe")
        return

    wss_url = rpc_endpoint.replace("https://", "wss://").replace("http://", "ws://")

    async def _simulate_sell_task(
        amount: float,
        reason: str,
        current_price: float,
        key: str,
        fired_set: set[int] | None,
        fired_idx: int | None,
        trail_state: dict | None,
    ) -> None:
        nonlocal remaining, total_sim_sol_received, total_sim_fees
        try:
            sol_received = amount * current_price
            entry_sol_portion = (
                sim_pos.entry_sol * (amount / sim_pos.simulated_token_amount)
                if sim_pos.simulated_token_amount > 0
                else 0.0
            )
            gross_pnl_sol = sol_received - entry_sol_portion
            gross_pnl_pct = (
                gross_pnl_sol / entry_sol_portion * 100
                if entry_sol_portion > 0
                else 0.0
            )
            sim_fee = sim_pos.priority_fee_sol
            net_pnl_sol = gross_pnl_sol - sim_fee
            net_pnl_pct = (
                net_pnl_sol / entry_sol_portion * 100
                if entry_sol_portion > 0
                else 0.0
            )

            current_sol_price = sol_price_getter() if sol_price_getter else sim_pos.sol_price_at_entry
            exit_mc_usd = current_price * _PUMP_FUN_TOTAL_SUPPLY * current_sol_price if current_sol_price > 0 else None
            duration = time.time() - entry_time
            minutes = int(duration // 60)
            seconds = int(duration % 60)
            duration_str = f"{minutes}m {seconds}s"

            remaining = max(0.0, remaining - amount)
            total_sim_sol_received += sol_received
            total_sim_fees += sim_fee

            if fired_set is not None and fired_idx is not None:
                fired_set.add(fired_idx)
            if trail_state is not None:
                trail_state["fired"] = True

            if "tp" in key:
                header = "🧪 💚 TEST MODE — ТЕЙК-ПРОФИТ"
            elif "sl" in key:
                header = "🧪 🔴 TEST MODE — СТОП-ЛОСС"
            else:
                header = "🧪 🟡 TEST MODE — ТРЕЙЛИНГ СТОП"

            sold_pct = (
                amount / sim_pos.simulated_token_amount * 100
                if sim_pos.simulated_token_amount > 0
                else 0.0
            )

            msg = (
                f"{header}\n\n"
                f"🔥 {sim_pos.name} ({sim_pos.symbol})\n"
                f"📋 <code>{sim_pos.mint}</code>\n"
                f"🔗 <a href='https://gmgn.ai/sol/token/{sim_pos.mint}'>GMGN</a>\n\n"
                f"📊 <b>Симулированный выход</b>\n"
                f"├ Триггер: <b>{reason}</b>\n"
                f"├ MC выхода: <b>{_fmt_mc(exit_mc_usd)}</b>\n"
                f"├ Продано токенов: <b>{amount:.0f}</b> ({sold_pct:.1f}% позиции)\n"
                f"└ SOL получено (симуляция): <b>{sol_received:.6f} SOL</b>\n\n"
                f"⏱ <b>Время в позиции: {duration_str}</b>\n\n"
                f"📈 <b>Симулированный PnL</b>\n"
                f"├ Брутто PnL: <b>{gross_pnl_sol:+.6f} SOL ({gross_pnl_pct:+.1f}%)</b>\n"
                f"├ Комиссии (симуляция): <b>{sim_fee:.5f} SOL</b>\n"
                f"└ Чистый PnL: <b>{net_pnl_sol:+.6f} SOL ({net_pnl_pct:+.1f}%)</b>\n"
            )

            if remaining > _DUST_TOKENS:
                remaining_pct = (
                    remaining / sim_pos.simulated_token_amount * 100
                    if sim_pos.simulated_token_amount > 0
                    else 0.0
                )
                msg += (
                    f"\n📦 <b>Остаток (симуляция): "
                    f"{remaining:.0f} токенов ({remaining_pct:.1f}%)</b>"
                )
            else:
                total_entry = sim_pos.entry_sol
                final_net = total_sim_sol_received - total_entry - total_sim_fees
                final_pct = (final_net / total_entry * 100) if total_entry > 0 else 0.0
                msg += (
                    f"\n\n🏁 <b>ПОЗИЦИЯ ЗАКРЫТА (СИМУЛЯЦИЯ)</b>\n"
                    f"├ Вложено: <b>{total_entry:.6f} SOL</b>\n"
                    f"├ Получено: <b>{total_sim_sol_received:.6f} SOL</b>\n"
                    f"├ Комиссии итого: <b>{total_sim_fees:.6f} SOL</b>\n"
                    f"└ Итоговый PnL: <b>{final_net:+.6f} SOL ({final_pct:+.1f}%)</b>"
                )
                stat_key = "test_wins" if final_net > 0 else "test_losses"
                asyncio.create_task(_update_test_stat(stat_key, pnl_delta=final_net))

            if notify_fn is not None:
                asyncio.create_task(notify_fn(msg))

            logger.info(
                f"[TEST] SIM SELL {amount:.0f} tokens @ {current_price:.8f} SOL "
                f"| {reason} | remaining={remaining:.0f}"
            )

        except Exception as exc:
            logger.error(f"[TEST] _simulate_sell_task error: {exc}", exc_info=True)
        finally:
            sells_in_progress.discard(key)

    def _on_price_test(current_price: float) -> None:
        nonlocal remaining
        if remaining <= _DUST_TOKENS:
            return

        price_chg_pct = (current_price - entry_price) / entry_price * 100

        # --- Stop-loss (ascending price_pct → least loss fires first) ---
        for orig_idx, sl in sorted(
            enumerate(preset.get("stop_losses", [])),
            key=lambda x: x[1].get("price_pct", 0),
        ):
            if orig_idx in sl_fired:
                continue
            key = f"sl_{orig_idx}"
            if key in sells_in_progress:
                continue
            trigger = entry_price * (1 - sl["price_pct"] / 100)
            if current_price <= trigger:
                if remaining <= _DUST_TOKENS:
                    break
                amount = original_amount * (sl["position_pct"] / 100)
                sells_in_progress.add(key)
                _sol = sol_price_getter() if sol_price_getter else 0.0
                asyncio.create_task(_simulate_sell_task(
                    amount,
                    f"SL{orig_idx + 1} (-{sl['price_pct']:.0f}%) | MC: {_fmt_mc(current_price * _PUMP_FUN_TOTAL_SUPPLY * _sol)}",
                    current_price, key, sl_fired, orig_idx, None,
                ))

        if remaining <= _DUST_TOKENS:
            return

        # --- Take-profit (ascending price_pct → smallest gain first) ---
        for orig_idx, tp in sorted(
            enumerate(preset.get("take_profits", [])),
            key=lambda x: x[1].get("price_pct", 0),
        ):
            if orig_idx in tp_fired:
                continue
            key = f"tp_{orig_idx}"
            if key in sells_in_progress:
                continue
            trigger = entry_price * (1 + tp["price_pct"] / 100)
            if current_price >= trigger:
                if remaining <= _DUST_TOKENS:
                    break
                amount = original_amount * (tp["position_pct"] / 100)
                sells_in_progress.add(key)
                _sol = sol_price_getter() if sol_price_getter else 0.0
                asyncio.create_task(_simulate_sell_task(
                    amount,
                    f"TP{orig_idx + 1} (+{tp['price_pct']:.0f}%) | MC: {_fmt_mc(current_price * _PUMP_FUN_TOTAL_SUPPLY * _sol)}",
                    current_price, key, tp_fired, orig_idx, None,
                ))

        if remaining <= _DUST_TOKENS:
            return

        # --- Trailing stops (each independent) ---
        for state in trail_states:
            if not state["enabled"] or state["fired"]:
                continue
            key = f"trail_{state['index']}"
            if key in sells_in_progress:
                continue

            if not state["activated"]:
                if price_chg_pct >= state["activation_pct"]:
                    state["activated"] = True
                    state["peak"] = current_price
                    logger.info(
                        f"[TEST] Trail {state['index'] + 1} activated at "
                        f"{current_price:.8f} ({price_chg_pct:+.1f}%)"
                    )
            else:
                if current_price > state["peak"]:
                    state["peak"] = current_price
                trail_trigger = state["peak"] * (1 - state["trail_size_pct"] / 100)
                if current_price <= trail_trigger:
                    if remaining <= _DUST_TOKENS:
                        break
                    amount = original_amount * (state["position_pct"] / 100)
                    sells_in_progress.add(key)
                    _sol = sol_price_getter() if sol_price_getter else 0.0
                    asyncio.create_task(_simulate_sell_task(
                        amount,
                        f"Trail{state['index'] + 1} (-{state['trail_size_pct']:.0f}% от пика) | MC пика: {_fmt_mc(state['peak'] * _PUMP_FUN_TOTAL_SUPPLY * _sol)} | MC выхода: {_fmt_mc(current_price * _PUMP_FUN_TOTAL_SUPPLY * _sol)}",
                        current_price, key, None, None, state,
                    ))

    # -----------------------------------------------------------------------
    # accountSubscribe WebSocket loop
    # -----------------------------------------------------------------------
    try:
        while remaining > _DUST_TOKENS:
            try:
                async with websockets.connect(
                    wss_url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    await ws.send(json.dumps({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "accountSubscribe",
                        "params": [
                            sim_pos.bonding_curve,
                            {"encoding": "base64", "commitment": "processed"},
                        ],
                    }))
                    logger.info(
                        f"[TEST] accountSubscribe connected for {sim_pos.mint[:8]}"
                    )

                    async for raw_msg in ws:
                        try:
                            data = json.loads(raw_msg)
                        except Exception:
                            continue
                        if data.get("method") != "accountNotification":
                            continue
                        try:
                            account_data_b64 = data["params"]["result"]["value"]["data"][0]
                        except (KeyError, IndexError, TypeError):
                            continue
                        account_bytes = base64.b64decode(account_data_b64)
                        current_price = _price_from_raw(
                            account_bytes, sim_pos.platform, None
                        )
                        if current_price is None or current_price <= 0:
                            continue
                        _on_price_test(current_price)
                        if remaining <= _DUST_TOKENS:
                            break

            except Exception as exc:
                logger.warning(
                    f"[TEST] WS error for {sim_pos.mint[:8]}: {exc}, reconnecting in 2s"
                )
                await asyncio.sleep(2)

    except Exception as exc:
        logger.error(f"[TEST] monitor_position_test crashed: {exc}", exc_info=True)
        if notify_fn is not None:
            asyncio.create_task(notify_fn(
                f"🧪 ⚠️ TEST MODE: Position monitor crashed\n"
                f"Mint: <code>{sim_pos.mint}</code>\n"
                f"Error: {exc}"
            ))

    finally:
        logger.info(
            f"[TEST] monitor_position_test ended for {sim_pos.mint[:8]} "
            f"| remaining={remaining:.0f}"
        )
        if position_close_fn is not None:
            position_close_fn()
