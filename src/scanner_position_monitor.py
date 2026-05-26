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
  6. A backup getAccountInfo poll fires every 0.5s in parallel (Bug #9).

Exit strategy (TP/SL/TrailingStop) is snapshotted from bot_config.json
FRESH at position open time and never re-read mid-trade.

Sell amounts at every exit level are calculated as a percentage of
ORIGINAL entry tokens (not remaining). If calculated amount > remaining:
sell all remaining. (Bug #8)
"""

import asyncio
import base64
import json
import struct
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import aiohttp
import websockets
from filelock import AsyncFileLock

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
_SPM_CONFIG_LOCK_PATH = _SPM_CONFIG_PATH.with_suffix(".lock")
_spm_config_lock = AsyncFileLock(_SPM_CONFIG_LOCK_PATH)

MAX_INACTIVITY_SECONDS: int = 5 * 60  # Force-close if no accountNotification for 5 min

# Backup poll interval in seconds — 0.5s cuts max blind window during WS reconnect from ~4s to ~1s
# 3 concurrent positions → 3 × 120 = 360 getAccountInfo calls/hour (negligible on Helius)
_POLL_INTERVAL: float = 0.5


class _InactivityTimeout(Exception):
    """Raised when no accountNotification arrives for MAX_INACTIVITY_SECONDS."""


def _safe_url(url: str) -> str:
    """Redact API key from RPC/WSS URL for safe logging."""
    if "api-key=" in url:
        return url.split("api-key=", 1)[0] + "api-key=[REDACTED]"
    return url


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


async def _fetch_price_via_rpc(rpc_url: str, pool_addr_str: str) -> bytes | None:
    """Single getAccountInfo call, returns raw bytes or None on any error."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [pool_addr_str, {"encoding": "base64", "commitment": "processed"}],
    }
    try:
        timeout = aiohttp.ClientTimeout(total=3.0)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(rpc_url, json=payload) as resp:
                data = await resp.json(content_type=None)
        value = (data.get("result") or {}).get("value") or {}
        raw_list = value.get("data")
        if raw_list and isinstance(raw_list, list):
            return base64.b64decode(raw_list[0])
    except Exception:
        pass
    return None


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
    buy_commission_sol: float = 0.0,
    entry_mc_usd: float = 0.0,
) -> None:
    """Monitor an open position via accountSubscribe and execute exits.

    Bug #1: Preset is read FRESH from disk at position open time.
    Bug #8: All sell amounts are % of ORIGINAL entry tokens, capped by remaining.
    Bug #9: Backup poll every 0.5s runs in parallel with accountSubscribe.

    Args:
        buy_commission_sol: Total buy-side fees (priority + jito + gas) for PnL reporting.
        entry_mc_usd: Market cap at entry in USD for close message.
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

    # ------------------------------------------------------------------
    # Bug #1: Read bot_config.json FRESH from disk at exact position open.
    # Snapshot ALL exit strategy values — zero reads from config mid-trade.
    # ------------------------------------------------------------------
    try:
        with open(_SPM_CONFIG_PATH) as _f:
            _entry_cfg = json.load(_f)
    except Exception:
        _entry_cfg = read_config() or {}
    _active_pid = str(_entry_cfg.get("active_preset", preset_id))
    _preset_snap: dict = (_entry_cfg.get("presets") or {}).get(_active_pid, {})

    snap_tp_levels: list[dict] = sorted(
        list(_preset_snap.get("take_profits", [])), key=lambda x: x.get("price_pct", 0)
    )
    snap_sl_levels: list[dict] = sorted(
        list(_preset_snap.get("stop_losses", [])), key=lambda x: x.get("price_pct", 0)
    )
    _snap_ts_raw: list[dict] = list(_preset_snap.get("trailing_stops") or [])
    if not _snap_ts_raw:
        _legacy_ts = _preset_snap.get("trailing_stop", {})
        if _legacy_ts.get("enabled"):
            _snap_ts_raw = [_legacy_ts]
    snap_ts_raw: list[dict] = sorted(_snap_ts_raw, key=lambda x: x.get("activation_pct", 0))

    # Accept both old and new field names for sell priority fee
    snap_priority_fee_sol: float = float(
        _preset_snap.get("max_priority_fee_sol")
        or _preset_snap.get("priority_fee_sol")
        or (priority_fee_microlamports / 1_000_000_000)
    )
    snap_gas_fee_sol: float = float(_preset_snap.get("gas_fee_sol", 0.00005))
    snap_sell_slippage: float = float(_preset_snap.get("sell_slippage", 25)) / 100

    logger.info(
        f"[monitor] {label} snapshot: {len(snap_tp_levels)} TP, "
        f"{len(snap_sl_levels)} SL, {len(snap_ts_raw)} Trail | "
        f"sell_fee={snap_priority_fee_sol + snap_gas_fee_sol:.6f} SOL"
    )

    # CHANGE 5 — position_pct is % of ORIGINAL entry tokens, set ONCE here and never changed.
    # Every sell amount = round(original_tokens * level.position_pct / 100), capped by remaining.
    # It is NOT % of remaining balance. Verified at each call site below.
    original_tokens: float = token_amount
    remaining: float = token_amount

    tp_fired: set[int] = set()
    sl_fired: set[int] = set()

    # Bug #1: trail_states initialized with peak=entry_price from T+0.
    # Peak is updated on EVERY tick (even before activation_pct reached).
    trail_states: list[dict] = [
        {"active": False, "peak": entry_price, "fired": False}
        for _ in snap_ts_raw
    ]
    sells_in_progress: set[str] = set()
    total_sol_received: float = 0.0
    total_sell_fees_paid: float = 0.0
    close_emitted: bool = False
    _position_closed: list[bool] = [False]  # mutable flag for backup poller

    # Bug #3B: track exit history for close message
    _exit_history: list[dict] = []
    _last_exit_mc: list[float] = [entry_mc_usd]
    _last_known_price: list[float] = [entry_price]

    async def _sell(sell_amount: float, reason: str, current_price: float, trigger_label: str) -> bool:
        """Execute a partial sell, decrement remaining, return True on success."""
        nonlocal remaining, total_sol_received, total_sell_fees_paid, close_emitted

        if sell_amount < _DUST_TOKENS:
            logger.debug(f"[monitor] {label} skip dust sell {sell_amount:.2f} | {reason}")
            return False

        # BUG #3B + BUG #2 FIX: use snapshotted preset values; no jito on sells
        fee_ul = int(snap_priority_fee_sol * 1_000_000_000)
        sell_fee_sol = snap_priority_fee_sol + snap_gas_fee_sol

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
            slippage=snap_sell_slippage,
            max_retries=3,
            jito_tip_lamports=None,  # no jito on sells
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
            total_sell_fees_paid += sell_fee_sol
            total_sol_received += sol_received
            remaining = max(0.0, remaining - sell_amount)

            logger.info(
                f"[monitor] {label} sell OK | remaining={remaining:.4f} | "
                f"tx={result.tx_signature}"
            )

            if notify_fn is not None:
                sol_price = sol_price_getter() if sol_price_getter else 0.0
                duration_s = time.monotonic() - t_start

                # Bug #8: pct from ORIGINAL tokens
                pct_of_original = (sell_amount / original_tokens * 100) if original_tokens > 0 else 0.0
                remaining_pct = (remaining / original_tokens * 100) if original_tokens > 0 else 0.0

                # Gross PnL on this exit
                entry_sol_this_exit = entry_price * sell_amount
                gross_pnl_sol = sol_received - entry_sol_this_exit
                gross_pnl_pct = (gross_pnl_sol / entry_sol_this_exit * 100) if entry_sol_this_exit > 0 else 0.0
                net_pnl_sol = gross_pnl_sol - sell_fee_sol
                entry_total_sol = entry_price * original_tokens
                net_pnl_pct = (net_pnl_sol / entry_total_sol * 100) if entry_total_sol > 0 else 0.0

                exit_mc = current_price * _PUMP_FUN_TOTAL_SUPPLY * sol_price if sol_price > 0 else None
                _last_exit_mc[0] = exit_mc or _last_exit_mc[0]
                _last_known_price[0] = current_price

                # Track exit for close message (Bug #3B)
                _exit_history.append({
                    "label": trigger_label,
                    "mc": exit_mc,
                    "sol_received": sol_received,
                    "gross_sol": gross_pnl_sol,
                    "gross_pct": gross_pnl_pct,
                    "net_sol": net_pnl_sol,
                })

                tx_sig = str(result.tx_signature) if result.tx_signature else None
                mint_str = str(token_info.mint)
                token_url = f"https://gmgn.ai/sol/token/{mint_str}"
                tx_url = f"https://solscan.io/tx/{tx_sig}" if tx_sig else ""

                # Bug #3B: Full commission reporting on exit messages
                lines = [
                    f"{'💚' if trigger_label.startswith('TP') else ('🔴' if trigger_label.startswith('SL') else '🔶')} "
                    f"<b>ВЫХОД {trigger_label}</b> | {label}",
                    "",
                    f"📊 Выход:",
                    f"├ Триггер: <b>{reason}</b>",
                    f"├ МС выхода: <b>{_fmt_mc(exit_mc)}</b>",
                    f"├ МС входа: {_fmt_mc(entry_mc_usd)}",
                    f"├ Продано: <b>{sell_amount:,.0f}</b> ({pct_of_original:.1f}% от входа)",
                    f"├ SOL получено: <b>{sol_received:.6f} SOL</b>",
                    f"├ Комиссия: priority {snap_priority_fee_sol:.6f} + gas {snap_gas_fee_sol:.6f} = {sell_fee_sol:.6f} SOL",
                    f"├ Брутто PnL: {gross_pnl_sol:+.6f} SOL ({gross_pnl_pct:+.1f}%)",
                    f"└ Чистый PnL: <b>{net_pnl_sol:+.6f} SOL ({net_pnl_pct:+.1f}%)</b>",
                    "",
                    f"⏱ Время в позиции: {_fmt_duration(duration_s)}",
                ]

                if remaining > _DUST_TOKENS:
                    lines.append(f"📦 Остаток: {remaining:,.0f} токенов ({remaining_pct:.1f}% от входа)")
                elif not close_emitted:
                    close_emitted = True
                    _position_closed[0] = True

                    # Bug #3B: full close message
                    entry_total_sol_inv = entry_price * original_tokens
                    total_gross = total_sol_received - entry_total_sol_inv
                    total_fees_all = buy_commission_sol + total_sell_fees_paid
                    total_net = total_gross - total_sell_fees_paid - buy_commission_sol
                    total_gross_pct = (total_gross / entry_total_sol_inv * 100) if entry_total_sol_inv > 0 else 0.0
                    total_net_pct = (total_net / entry_total_sol_inv * 100) if entry_total_sol_inv > 0 else 0.0

                    exit_hist_lines = []
                    for j, eh in enumerate(_exit_history):
                        prefix = "└" if j == len(_exit_history) - 1 else "├"
                        exit_hist_lines.append(
                            f"{prefix} {eh['label']} @ {_fmt_mc(eh['mc'])}: "
                            f"{eh['gross_sol']:+.6f} SOL ({eh['gross_pct']:+.1f}%)"
                        )

                    lines += [
                        "",
                        f"🏁 <b>ПОЗИЦИЯ ЗАКРЫТА</b>",
                        "",
                        f"🔥 {token_info.name} ({token_info.symbol})",
                        f"📍 МС входа: {_fmt_mc(entry_mc_usd)}",
                        f"📍 МС выхода (посл): {_fmt_mc(_last_exit_mc[0])}",
                        "",
                        "📋 История выходов:",
                    ]
                    lines.extend(exit_hist_lines)
                    lines += [
                        "",
                        "💰 Итоговый PnL:",
                        f"├ Вложено:           {entry_total_sol_inv:.6f} SOL",
                        f"├ Получено:          {total_sol_received:.6f} SOL",
                        f"├ Брутто PnL:        {total_gross:+.6f} SOL ({total_gross_pct:+.1f}%)",
                        f"├ ── Комиссии итого: {total_fees_all:.6f} SOL",
                        f"│     ├ Вход:   {buy_commission_sol:.6f} SOL",
                        f"│     └ Выходы: {total_sell_fees_paid:.6f} SOL",
                        f"└ Чистый PnL:        <b>{total_net:+.6f} SOL ({total_net_pct:+.1f}%)</b>",
                    ]

                    real_stat_key = "real_wins" if total_net > 0 else "real_losses"
                    asyncio.create_task(_update_live_stat(real_stat_key, total_net))

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
        trigger_label: str,
        fired_set: set[int] | None,
        fired_idx: int | None,
        trail_state_dict: dict | None,
    ) -> None:
        """Background sell task. Retries until on-chain confirmation, then sets fired flag."""
        attempt = 0
        while True:
            attempt += 1
            try:
                ok = await _sell(sell_amount, reason, current_price, trigger_label)
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

        Bug #1: Peak is updated on every tick, even before trail activation.
        Bug #8: sell amount = min(original * pct%, remaining).
        """
        if remaining <= _DUST_TOKENS:
            return

        _last_known_price[0] = current_price
        price_chg_pct = ((current_price - entry_price) / entry_price) * 100

        tp_levels: list[dict] = snap_tp_levels
        sl_levels: list[dict] = snap_sl_levels
        ts_list: list[dict] = snap_ts_raw

        # --- Stop-loss (ascending price_pct → least loss fires first) ---
        for orig_idx, level in sorted(
            enumerate(sl_levels), key=lambda x: x[1].get("price_pct", 0)
        ):
            if orig_idx in sl_fired:
                continue
            key = f"sl_{orig_idx}"
            if key in sells_in_progress:
                continue
            trigger = entry_price * (1 - level["price_pct"] / 100)
            if current_price <= trigger:
                # CHANGE 5 ✓ — position_pct applied to original_tokens (not remaining)
                amount = min(
                    round(original_tokens * level["position_pct"] / 100),
                    int(remaining),
                )
                if amount < _DUST_TOKENS:
                    sl_fired.add(orig_idx)
                    continue
                sells_in_progress.add(key)
                _sol = sol_price_getter() if sol_price_getter else 0.0
                trig_lbl = f"SL{orig_idx + 1}"
                asyncio.create_task(_sell_with_retry(
                    amount,
                    f"SL{orig_idx + 1} (-{level['price_pct']:.0f}%) | MC: {_fmt_mc(current_price * _PUMP_FUN_TOTAL_SUPPLY * _sol)}",
                    current_price,
                    key,
                    trig_lbl,
                    sl_fired,
                    orig_idx,
                    None,
                ))

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
                continue
            trigger = entry_price * (1 + level["price_pct"] / 100)
            if current_price >= trigger:
                # CHANGE 5 ✓ — position_pct applied to original_tokens (not remaining)
                amount = min(
                    round(original_tokens * level["position_pct"] / 100),
                    int(remaining),
                )
                if amount < _DUST_TOKENS:
                    tp_fired.add(orig_idx)
                    continue
                sells_in_progress.add(key)
                _sol = sol_price_getter() if sol_price_getter else 0.0
                trig_lbl = f"TP{orig_idx + 1}"
                asyncio.create_task(_sell_with_retry(
                    amount,
                    f"TP{orig_idx + 1} (+{level['price_pct']:.0f}%) | MC: {_fmt_mc(current_price * _PUMP_FUN_TOTAL_SUPPLY * _sol)}",
                    current_price,
                    key,
                    trig_lbl,
                    tp_fired,
                    orig_idx,
                    None,
                ))

        if remaining <= _DUST_TOKENS:
            return

        # --- Trailing stops ---
        # Bug #1: peak is updated on EVERY tick (before AND after activation).
        for i, ts_cfg in enumerate(ts_list):
            if not ts_cfg.get("enabled", True):
                continue
            state = trail_states[i]
            if state["fired"]:
                continue
            key = f"trail_{i}"
            if key in sells_in_progress:
                continue

            act_pct = float(ts_cfg.get("activation_pct", 50))
            trail_size = float(ts_cfg.get("trail_size_pct", 20))
            trail_pos = float(ts_cfg.get("position_pct", 100))

            # Bug #1 FIX: Always update peak from T+0, even before activation.
            if current_price > state["peak"]:
                state["peak"] = current_price

            if not state["active"] and price_chg_pct >= act_pct:
                state["active"] = True
                logger.info(
                    f"[monitor] {label} trail[{i + 1}] ACTIVATED | "
                    f"activation=+{act_pct:.1f}% | peak={state['peak']:.8f} | "
                    f"floor={state['peak'] * (1 - trail_size / 100):.8f} | "
                    f"price={current_price:.8f} ({price_chg_pct:+.1f}%)"
                )

            if state["active"]:
                trail_trigger = state["peak"] * (1 - trail_size / 100)
                if current_price <= trail_trigger:
                    # CHANGE 5 ✓ — position_pct applied to original_tokens (not remaining)
                    amount = min(
                        round(original_tokens * trail_pos / 100),
                        int(remaining),
                    )
                    if amount < _DUST_TOKENS:
                        state["fired"] = True
                        continue
                    sells_in_progress.add(key)
                    _sol = sol_price_getter() if sol_price_getter else 0.0
                    trig_lbl = f"Trail{i + 1}"
                    asyncio.create_task(_sell_with_retry(
                        amount,
                        f"Trail{i + 1} (-{trail_size:.0f}% от пика) | MC пика: {_fmt_mc(state['peak'] * _PUMP_FUN_TOTAL_SUPPLY * _sol)} | MC выхода: {_fmt_mc(current_price * _PUMP_FUN_TOTAL_SUPPLY * _sol)}",
                        current_price,
                        key,
                        trig_lbl,
                        None,
                        None,
                        state,
                    ))

    # Bug #9: Backup price poll every 0.5s in parallel with accountSubscribe.
    async def _poll_price_backup() -> None:
        """Periodic getAccountInfo fallback — catches dumps between WS notifications."""
        await asyncio.sleep(_POLL_INTERVAL)
        while not _position_closed[0] and remaining > _DUST_TOKENS:
            try:
                raw = await _fetch_price_via_rpc(rpc_url, pool_addr_str)
                if raw:
                    price = _price_from_raw(raw, token_info.platform, curve_manager)
                    if price and price > 0:
                        sol_price = sol_price_getter() if sol_price_getter else 0.0
                        mc = price * _PUMP_FUN_TOTAL_SUPPLY * sol_price if sol_price > 0 else 0.0
                        logger.debug(f"[monitor] {label} Poll: MC {_fmt_mc(mc if mc else None)}")
                        if abs(price - _last_known_price[0]) / max(_last_known_price[0], 1e-12) > 1e-6:
                            await _on_price(price)
            except Exception as e:
                logger.debug(f"[monitor] {label} poll backup error: {e}")
            await asyncio.sleep(_POLL_INTERVAL)

    # ---------------------------------------------------------------------------
    # accountSubscribe WebSocket loop
    # ---------------------------------------------------------------------------
    last_activity_time = time.time()
    poll_task: asyncio.Task | None = None
    try:
        poll_task = asyncio.create_task(_poll_price_backup())
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

                    while remaining > _DUST_TOKENS:
                        time_since = time.time() - last_activity_time
                        remaining_inactivity = MAX_INACTIVITY_SECONDS - time_since
                        if remaining_inactivity <= 0:
                            raise _InactivityTimeout()
                        try:
                            msg_raw = await asyncio.wait_for(
                                ws.recv(), timeout=min(remaining_inactivity, 60.0)
                            )
                        except asyncio.TimeoutError:
                            if time.time() - last_activity_time >= MAX_INACTIVITY_SECONDS:
                                raise _InactivityTimeout()
                            await ws.ping()
                            continue

                        msg = json.loads(msg_raw)
                        if msg.get("method") != "accountNotification":
                            continue
                        last_activity_time = time.time()

                        try:
                            data_b64 = msg["params"]["result"]["value"]["data"][0]
                        except (KeyError, IndexError, TypeError):
                            continue

                        raw = base64.b64decode(data_b64)
                        price = _price_from_raw(raw, token_info.platform, curve_manager)
                        if price and price > 0:
                            await _on_price(price)

            except _InactivityTimeout:
                raise
            except websockets.exceptions.ConnectionClosed as exc:
                logger.warning(
                    f"[monitor] {label} WS closed ({exc.code}), reconnecting..."
                )
                await asyncio.sleep(1)
            except asyncio.TimeoutError:
                logger.warning(f"[monitor] {label} WS timeout, reconnecting...")
                await asyncio.sleep(1)
            except Exception as exc:
                logger.error(f"[monitor] {label} WS error: {_safe_url(str(exc))}")
                await asyncio.sleep(2)

    except _InactivityTimeout:
        elapsed = time.monotonic() - t_start
        mint_str = str(token_info.mint)
        logger.warning(
            f"[monitor] {label} INACTIVITY TIMEOUT — "
            f"{MAX_INACTIVITY_SECONDS // 60} min with no activity, force-closing"
        )
        _position_closed[0] = True
        sell_ok = False
        if remaining > _DUST_TOKENS:
            try:
                exit_price = _last_known_price[0] if _last_known_price[0] > 0 else entry_price
                if _last_known_price[0] <= 0:
                    logger.warning(
                        f"[monitor] {label} [TIMEOUT] No price updates received — "
                        "using entry_price for sell, PnL may be inaccurate"
                    )
                sell_ok = await _sell(remaining, "Inactivity timeout", exit_price, "Timeout")
            except Exception as sell_exc:
                logger.error(f"[monitor] {label} timeout sell failed: {sell_exc}")
        else:
            exit_price = _last_known_price[0] if _last_known_price[0] > 0 else entry_price

        sol_price = sol_price_getter() if sol_price_getter else 0.0
        exit_mc = exit_price * _PUMP_FUN_TOTAL_SUPPLY * sol_price if sol_price > 0 else None

        if sell_ok:
            net = total_sol_received - (entry_price * original_tokens) - total_sell_fees_paid - buy_commission_sol
            alert = (
                f"⏱ <b>ТАЙМАУТ {MAX_INACTIVITY_SECONDS // 60} МИН — ПРОДАН</b>\n\n"
                f"🔥 {token_info.name} ({token_info.symbol})\n"
                f"📋 <code>{mint_str}</code>\n\n"
                f"❌ Токен мёртв — {MAX_INACTIVITY_SECONDS // 60} минут без активности\n\n"
                f"📊 MC: {_fmt_mc(exit_mc)}\n"
                f"💸 PnL: {net:+.6f} SOL\n\n"
                f"⏱ Время в позиции: {_fmt_duration(elapsed)}"
            )
            real_stat_key = "real_wins" if net > 0 else "real_losses"
            asyncio.create_task(_update_live_stat(real_stat_key, net))
        else:
            alert = (
                f"⏱ ⚠️ <b>ТАЙМАУТ {MAX_INACTIVITY_SECONDS // 60} МИН — ПРОДАЖА НЕ УДАЛАСЬ</b>\n\n"
                f"🔥 {token_info.name} ({token_info.symbol})\n"
                f"📋 <code>{mint_str}</code>\n\n"
                f"❌ Токен мёртв — {MAX_INACTIVITY_SECONDS // 60} минут без активности\n"
                f"⚠️ Автоматическая продажа не удалась\n"
                f"Проверь кошелёк вручную\n\n"
                f"📊 MC: {_fmt_mc(exit_mc)}\n"
                f"⏱ Время в позиции: {_fmt_duration(elapsed)}"
            )
        if notify_fn is not None:
            try:
                await notify_fn(alert)
            except Exception:
                pass

    except Exception as exc:
        logger.error(f"[monitor] {label} UNHANDLED CRASH: {exc}", exc_info=True)
        _position_closed[0] = True
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
        _position_closed[0] = True
        if poll_task and not poll_task.done():
            poll_task.cancel()
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

    Bug #1: Snapshot preset is used for ALL TP/SL/trailing decisions.
    Bug #8: All sell amounts are % of ORIGINAL entry tokens.
    Bug #9: Backup poll every 0.5s runs in parallel.
    """
    # CHANGE 5 — position_pct is % of ORIGINAL entry tokens, set ONCE here and never changed.
    # Every sell amount = round(original_tokens * level.position_pct / 100), capped by remaining.
    original_tokens: float = sim_pos.simulated_token_amount
    remaining: float = sim_pos.simulated_token_amount
    entry_price: float = sim_pos.entry_price_sol
    entry_time: float = sim_pos.entry_timestamp
    tp_fired: set[int] = set()
    sl_fired: set[int] = set()
    total_sim_sol_received: float = 0.0
    # Total fees start with buy fees; sell fees added per exit
    buy_fees_sol: float = sim_pos.priority_fee_sol + sim_pos.jito_tip_sol + sim_pos.gas_fee_sol
    total_sim_fees: float = buy_fees_sol
    preset = sim_pos.preset_snapshot

    _snap_sl: list[dict] = sorted(
        preset.get("stop_losses", []), key=lambda x: x.get("price_pct", 0)
    )
    _snap_tp: list[dict] = sorted(
        preset.get("take_profits", []), key=lambda x: x.get("price_pct", 0)
    )
    _snap_ts: list[dict] = sorted(
        preset.get("trailing_stops", []) or [], key=lambda x: x.get("activation_pct", 0)
    )

    # Bug #1: peak initialized to entry_price, updated on EVERY tick.
    trail_states: list[dict] = []
    for i, t in enumerate(_snap_ts):
        trail_states.append({
            "active": False,
            "peak": entry_price,  # track from T+0
            "fired": False,
            "enabled": bool(t.get("enabled", True)),
            "activation_pct": float(t.get("activation_pct", 50)),
            "trail_size_pct": float(t.get("trail_size_pct", 20)),
            "position_pct": float(t.get("position_pct", 100)),
            "index": i,
        })

    _position_closed: list[bool] = [False]
    _last_known_price: list[float] = [entry_price]
    _exit_history: list[dict] = []  # Bug #3B: for close message
    _last_sell_fees: list[float] = [0.0]

    rpc_url = rpc_endpoint.replace("wss://", "https://").replace("ws://", "http://")
    wss_url = rpc_endpoint.replace("https://", "wss://").replace("http://", "ws://")

    logger.info(
        f"[TEST] Starting position monitor for {sim_pos.mint[:8]}, "
        f"entry={sim_pos.entry_price_sol:.8f}, tokens={sim_pos.simulated_token_amount:.0f}"
    )

    if not sim_pos.bonding_curve:
        logger.warning(f"[TEST] No bonding_curve for {sim_pos.mint[:8]} — cannot subscribe")
        return

    def _on_price_test(current_price: float) -> None:
        nonlocal remaining, total_sim_sol_received, total_sim_fees

        if remaining <= _DUST_TOKENS:
            return

        _last_known_price[0] = current_price
        price_chg_pct = (current_price - entry_price) / entry_price * 100
        _sell_msgs: list[str] = []

        def _do_sell(amount: float, reason: str, header: str, trigger_label: str) -> None:
            nonlocal remaining, total_sim_sol_received, total_sim_fees

            if amount < _DUST_TOKENS:
                return

            sol_received = amount * current_price
            # Entry SOL portion for this sell (proportional to entry investment)
            entry_sol_this_exit = sim_pos.entry_price_sol * amount
            gross_pnl_sol = sol_received - entry_sol_this_exit
            gross_pnl_pct = (
                gross_pnl_sol / entry_sol_this_exit * 100
                if entry_sol_this_exit > 0
                else 0.0
            )
            sim_fee = sim_pos.priority_fee_sol + sim_pos.gas_fee_sol
            net_pnl_sol = gross_pnl_sol - sim_fee
            entry_total_sol = sim_pos.entry_sol
            net_pnl_pct = (
                net_pnl_sol / entry_total_sol * 100
                if entry_total_sol > 0
                else 0.0
            )

            remaining = max(0.0, remaining - amount)
            total_sim_sol_received += sol_received
            total_sim_fees += sim_fee
            _last_sell_fees[0] = sim_fee

            current_sol_price = sol_price_getter() if sol_price_getter else sim_pos.sol_price_at_entry
            exit_mc_usd = (
                current_price * _PUMP_FUN_TOTAL_SUPPLY * current_sol_price
                if current_sol_price > 0
                else None
            )

            # Bug #8: % from ORIGINAL tokens
            sold_pct = (
                amount / original_tokens * 100
                if original_tokens > 0
                else 0.0
            )
            remaining_pct = (
                remaining / original_tokens * 100
                if original_tokens > 0
                else 0.0
            )

            # Track exit history for close message (Bug #3B)
            _exit_history.append({
                "label": trigger_label,
                "mc": exit_mc_usd,
                "gross_sol": gross_pnl_sol,
                "gross_pct": gross_pnl_pct,
                "net_sol": net_pnl_sol,
            })

            # Bug #3B: Full commission reporting format
            msg = (
                f"{header}\n\n"
                f"🔥 {sim_pos.name} ({sim_pos.symbol})\n"
                f"📋 <code>{sim_pos.mint}</code>\n"
                f"🔗 <a href='https://gmgn.ai/sol/token/{sim_pos.mint}'>GMGN</a>\n\n"
                f"📊 <b>Симулированный выход</b>\n"
                f"├ Триггер: <b>{reason}</b>\n"
                f"├ МС выхода: <b>{_fmt_mc(exit_mc_usd)}</b>\n"
                f"├ МС входа: {_fmt_mc(sim_pos.entry_mc_usd)}\n"
                f"├ Продано: <b>{amount:,.0f}</b> ({sold_pct:.1f}% от входа)\n"
                f"├ SOL получено (симуляция): <b>{sol_received:.6f} SOL</b>\n"
                f"├ Комиссия: priority {sim_pos.priority_fee_sol:.6f} + gas {sim_pos.gas_fee_sol:.6f} = {sim_fee:.6f} SOL\n"
                f"├ Брутто PnL: <b>{gross_pnl_sol:+.6f} SOL ({gross_pnl_pct:+.1f}%)</b>\n"
                f"└ Чистый PnL: <b>{net_pnl_sol:+.6f} SOL ({net_pnl_pct:+.1f}%)</b>\n\n"
                f"⏱ <b>Время в позиции: {_fmt_duration(time.time() - entry_time)}</b>"
            )

            if remaining > _DUST_TOKENS:
                msg += (
                    f"\n\n📦 <b>Остаток (симуляция): "
                    f"{remaining:,.0f} токенов ({remaining_pct:.1f}% от входа)</b>"
                )

            _sell_msgs.append(msg)
            logger.info(
                f"[TEST] SIM SELL {amount:.0f} tokens @ {current_price:.8f} SOL "
                f"| {reason} | remaining={remaining:.0f}"
            )

        # --- Stop-loss ---
        for orig_idx, sl in enumerate(_snap_sl):
            if orig_idx in sl_fired:
                continue
            trigger = entry_price * (1 - sl["price_pct"] / 100)
            if current_price <= trigger:
                if remaining <= _DUST_TOKENS:
                    break
                sl_fired.add(orig_idx)
                _sol = sol_price_getter() if sol_price_getter else 0.0
                # CHANGE 5 ✓ — position_pct applied to original_tokens (not remaining)
                amount = min(
                    round(original_tokens * sl["position_pct"] / 100),
                    int(remaining),
                )
                _do_sell(
                    amount,
                    f"SL{orig_idx + 1} (-{sl['price_pct']:.0f}%) | MC: {_fmt_mc(current_price * _PUMP_FUN_TOTAL_SUPPLY * _sol)}",
                    "🧪 🔴 TEST MODE — СТОП-ЛОСС",
                    f"SL{orig_idx + 1}",
                )

        if remaining > _DUST_TOKENS:
            # --- Take-profit ---
            for orig_idx, tp in enumerate(_snap_tp):
                if orig_idx in tp_fired:
                    continue
                trigger = entry_price * (1 + tp["price_pct"] / 100)
                if current_price >= trigger:
                    if remaining <= _DUST_TOKENS:
                        break
                    tp_fired.add(orig_idx)
                    _sol = sol_price_getter() if sol_price_getter else 0.0
                    # CHANGE 5 ✓ — position_pct applied to original_tokens (not remaining)
                    amount = min(
                        round(original_tokens * tp["position_pct"] / 100),
                        int(remaining),
                    )
                    _do_sell(
                        amount,
                        f"TP{orig_idx + 1} (+{tp['price_pct']:.0f}%) | MC: {_fmt_mc(current_price * _PUMP_FUN_TOTAL_SUPPLY * _sol)}",
                        "🧪 💚 TEST MODE — ТЕЙК-ПРОФИТ",
                        f"TP{orig_idx + 1}",
                    )

        if remaining > _DUST_TOKENS:
            # --- Trailing stops ---
            # Bug #1: peak updated on EVERY tick, even before activation.
            for state in trail_states:
                if not state["enabled"] or state["fired"]:
                    continue

                # Bug #1 FIX: always update peak before activation check
                if current_price > state["peak"]:
                    state["peak"] = current_price

                if not state["active"]:
                    if price_chg_pct >= state["activation_pct"]:
                        state["active"] = True
                        logger.info(
                            f"[TEST] Trail {state['index'] + 1} activated at "
                            f"{current_price:.8f} ({price_chg_pct:+.1f}%), peak={state['peak']:.8f}"
                        )
                else:
                    trail_trigger = state["peak"] * (1 - state["trail_size_pct"] / 100)
                    if current_price <= trail_trigger:
                        if remaining <= _DUST_TOKENS:
                            break
                        state["fired"] = True
                        _sol = sol_price_getter() if sol_price_getter else 0.0
                        # CHANGE 5 ✓ — position_pct applied to original_tokens (not remaining)
                        amount = min(
                            round(original_tokens * state["position_pct"] / 100),
                            int(remaining),
                        )
                        _do_sell(
                            amount,
                            f"Trail{state['index'] + 1} (-{state['trail_size_pct']:.0f}% от пика) | MC пика: {_fmt_mc(state['peak'] * _PUMP_FUN_TOTAL_SUPPLY * _sol)} | MC выхода: {_fmt_mc(current_price * _PUMP_FUN_TOTAL_SUPPLY * _sol)}",
                            "🧪 🟡 TEST MODE — ТРЕЙЛИНГ СТОП",
                            f"Trail{state['index'] + 1}",
                        )

        # Bug #3B: append ПОЗИЦИЯ ЗАКРЫТА only once, after the full loop
        if _sell_msgs and remaining <= _DUST_TOKENS:
            _position_closed[0] = True
            total_entry = sim_pos.entry_sol
            sell_fees_total = total_sim_fees - buy_fees_sol
            total_fees_all = total_sim_fees  # buy + sell fees
            final_gross = total_sim_sol_received - total_entry
            final_net = final_gross - total_sim_fees
            final_gross_pct = (final_gross / total_entry * 100) if total_entry > 0 else 0.0
            final_net_pct = (final_net / total_entry * 100) if total_entry > 0 else 0.0

            exit_hist_lines = []
            for j, eh in enumerate(_exit_history):
                prefix = "└" if j == len(_exit_history) - 1 else "├"
                exit_hist_lines.append(
                    f"{prefix} {eh['label']} @ {_fmt_mc(eh['mc'])}: "
                    f"{eh['gross_sol']:+.6f} SOL ({eh['gross_pct']:+.1f}%)"
                )

            close_msg = (
                f"\n\n🏁 <b>ПОЗИЦИЯ ЗАКРЫТА (СИМУЛЯЦИЯ)</b>\n\n"
                f"🔥 {sim_pos.name} ({sim_pos.symbol})\n"
                f"📍 МС входа: {_fmt_mc(sim_pos.entry_mc_usd)}\n\n"
                "📋 История выходов:\n"
                + "\n".join(exit_hist_lines)
                + "\n\n💰 Итоговый PnL:\n"
                f"├ Вложено:           {total_entry:.6f} SOL\n"
                f"├ Получено:          {total_sim_sol_received:.6f} SOL\n"
                f"├ Брутто PnL:        {final_gross:+.6f} SOL ({final_gross_pct:+.1f}%)\n"
                f"├ ── Комиссии итого: {total_fees_all:.6f} SOL\n"
                f"│     ├ Вход:   {buy_fees_sol:.6f} SOL\n"
                f"│     └ Выходы: {sell_fees_total:.6f} SOL\n"
                f"└ Чистый PnL: <b>{final_net:+.6f} SOL ({final_net_pct:+.1f}%)</b>"
            )
            _sell_msgs[-1] += close_msg

            stat_key = "test_wins" if final_net > 0 else "test_losses"
            asyncio.create_task(_update_test_stat(stat_key, pnl_delta=final_net))

        if notify_fn is not None:
            for msg in _sell_msgs:
                asyncio.create_task(notify_fn(msg))

    # Bug #9: Backup poll for test mode too
    async def _poll_price_backup_test() -> None:
        await asyncio.sleep(_POLL_INTERVAL)
        while not _position_closed[0] and remaining > _DUST_TOKENS:
            try:
                raw = await _fetch_price_via_rpc(rpc_url, sim_pos.bonding_curve)
                if raw:
                    price = _price_from_raw(raw, sim_pos.platform, None)
                    if price and price > 0:
                        sol_price = sol_price_getter() if sol_price_getter else 0.0
                        mc = price * _PUMP_FUN_TOTAL_SUPPLY * sol_price if sol_price > 0 else 0.0
                        logger.debug(f"[TEST] Poll: MC {_fmt_mc(mc if mc else None)}")
                        if abs(price - _last_known_price[0]) / max(_last_known_price[0], 1e-12) > 1e-6:
                            _on_price_test(price)
            except Exception as e:
                logger.debug(f"[TEST] poll backup error: {e}")
            await asyncio.sleep(_POLL_INTERVAL)

    # -----------------------------------------------------------------------
    # accountSubscribe WebSocket loop
    # -----------------------------------------------------------------------
    last_activity_time = time.time()
    poll_task: asyncio.Task | None = None
    try:
        poll_task = asyncio.create_task(_poll_price_backup_test())
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

                    while remaining > _DUST_TOKENS:
                        time_since = time.time() - last_activity_time
                        remaining_inactivity = MAX_INACTIVITY_SECONDS - time_since
                        if remaining_inactivity <= 0:
                            raise _InactivityTimeout()
                        try:
                            raw_msg = await asyncio.wait_for(
                                ws.recv(), timeout=min(remaining_inactivity, 60.0)
                            )
                        except asyncio.TimeoutError:
                            if time.time() - last_activity_time >= MAX_INACTIVITY_SECONDS:
                                raise _InactivityTimeout()
                            continue
                        try:
                            data = json.loads(raw_msg)
                        except Exception:
                            continue
                        if data.get("method") != "accountNotification":
                            continue
                        last_activity_time = time.time()
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

            except _InactivityTimeout:
                raise
            except Exception as exc:
                logger.warning(
                    f"[TEST] WS error for {sim_pos.mint[:8]}: {_safe_url(str(exc))}, reconnecting in 2s"
                )
                await asyncio.sleep(2)

    except _InactivityTimeout:
        elapsed = time.time() - entry_time
        logger.warning(
            f"[TEST] {sim_pos.mint[:8]} INACTIVITY TIMEOUT — "
            f"{MAX_INACTIVITY_SECONDS // 60} min with no activity, force-closing"
        )
        _position_closed[0] = True
        sol_price = sol_price_getter() if sol_price_getter else sim_pos.sol_price_at_entry
        exit_mc = entry_price * _PUMP_FUN_TOTAL_SUPPLY * sol_price if sol_price > 0 else None

        alert = (
            f"⏱ 🧪 TEST MODE — ТАЙМАУТ {MAX_INACTIVITY_SECONDS // 60} МИН\n\n"
            f"🔥 {sim_pos.name} ({sim_pos.symbol})\n"
            f"📋 <code>{sim_pos.mint}</code>\n\n"
            f"❌ Токен мёртв — {MAX_INACTIVITY_SECONDS // 60} минут без активности\n"
            f"Позиция закрыта принудительно по цене входа\n\n"
            f"📊 MC входа: {_fmt_mc(sim_pos.entry_mc_usd)}\n"
            f"📊 MC выхода: {_fmt_mc(exit_mc)} (оценка)\n"
            f"💸 PnL: ~0 SOL (выход по цене входа)\n\n"
            f"⏱ Время в позиции: {_fmt_duration(elapsed)}"
        )
        asyncio.create_task(_update_test_stat("test_losses", pnl_delta=0.0))
        if notify_fn is not None:
            try:
                await notify_fn(alert)
            except Exception:
                pass

    except Exception as exc:
        logger.error(f"[TEST] monitor_position_test crashed: {exc}", exc_info=True)
        _position_closed[0] = True
        if notify_fn is not None:
            asyncio.create_task(notify_fn(
                f"🧪 ⚠️ TEST MODE: Position monitor crashed\n"
                f"Mint: <code>{sim_pos.mint}</code>\n"
                f"Error: {exc}"
            ))

    finally:
        _position_closed[0] = True
        if poll_task and not poll_task.done():
            poll_task.cancel()
        logger.info(
            f"[TEST] monitor_position_test ended for {sim_pos.mint[:8]} "
            f"| remaining={remaining:.0f}"
        )
        if position_close_fn is not None:
            position_close_fn()
