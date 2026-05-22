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
) -> None:
    """Monitor an open position via accountSubscribe and execute exits.

    Each accountNotification (fired by every on-chain buy/sell of the token):
      1. Decode raw account bytes → current price (no extra RPC call).
      2. Re-read active preset from bot_config.json (live settings).
      3. Check stop-loss levels (ascending price_pct → least loss fires first).
      4. Check take-profit levels (ascending price_pct → smallest gain first).
      5. Check trailing stop (activate → trail peak → fire).
      6. Execute at most one sell action per notification.

    position_pct in every level is % of *remaining* tokens at sell time.
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
    """
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
    trail_fired: bool = False
    trail_active: bool = False
    peak_price: float = entry_price

    async def _sell(amount: float, reason: str, current_price: float) -> bool:
        """Execute a partial sell, decrement remaining, return True on success."""
        nonlocal remaining

        if amount < _DUST_TOKENS:
            logger.debug(f"[monitor] {label} skip dust sell {amount:.2f} | {reason}")
            return False

        cfg = read_config() or {}
        active_pid = str(cfg.get("active_preset", preset_id))
        preset = (cfg.get("presets") or {}).get(active_pid, {})
        sell_slip = float(preset.get("sell_slippage", 25)) / 100
        fee_sol = float(
            preset.get("priority_fee_sol", priority_fee_microlamports / 1_000_000_000)
        )
        fee_ul = int(fee_sol * 1_000_000_000)

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
        )

        logger.info(
            f"[monitor] {label} SELL {amount:.4f} @ {current_price:.8f} SOL | {reason}"
        )
        try:
            result = await seller.execute(token_info, amount, current_price)
        except Exception as exc:
            logger.error(f"[monitor] {label} sell exception: {exc}")
            return False

        if result.success:
            remaining = max(0.0, remaining - amount)
            logger.info(
                f"[monitor] {label} sell OK | remaining={remaining:.4f} | "
                f"tx={result.tx_signature}"
            )
            if notify_fn is not None:
                pnl_pct = ((current_price - entry_price) / entry_price) * 100
                tg_msg = (
                    f"💰 <b>ПРОДАЖА</b> | {label}\n"
                    f"📌 {reason}\n"
                    f"💱 Цена: {current_price:.8f} SOL ({pnl_pct:+.1f}%)\n"
                    f"🪙 Продано: {amount:.4f} | Остаток: {remaining:.4f}"
                )
                asyncio.create_task(notify_fn(tg_msg))
            return True

        logger.warning(f"[monitor] {label} sell FAILED: {result.error_message}")
        if notify_fn is not None:
            asyncio.create_task(notify_fn(
                f"❌ <b>ПРОДАЖА FAILED</b> | {label}\n"
                f"📌 {reason}\n"
                f"⚠️ {result.error_message or 'unknown error'}"
            ))
        return False

    async def _on_price(current_price: float) -> None:
        """Run TP/SL/trailing-stop checks for the given price tick."""
        nonlocal peak_price, trail_active, trail_fired

        if current_price > peak_price:
            peak_price = current_price

        price_chg_pct = ((current_price - entry_price) / entry_price) * 100

        cfg = read_config() or {}
        active_pid = str(cfg.get("active_preset", preset_id))
        preset = (cfg.get("presets") or {}).get(active_pid, {})
        tp_levels: list[dict] = preset.get("take_profits", [])
        sl_levels: list[dict] = preset.get("stop_losses", [])
        trail_cfg: dict = preset.get("trailing_stop", {})

        # --- Stop-loss (ascending price_pct → least loss fires first) ---
        for orig_idx, level in sorted(
            enumerate(sl_levels), key=lambda x: x[1].get("price_pct", 0)
        ):
            if orig_idx in sl_fired:
                continue
            trigger = entry_price * (1 - level["price_pct"] / 100)
            if current_price <= trigger:
                sl_fired.add(orig_idx)  # mark before sell to prevent double-sell
                await _sell(
                    remaining * (level["position_pct"] / 100),
                    f"SL{orig_idx + 1}(-{level['price_pct']:.1f}% "
                    f"price={current_price:.8f})",
                    current_price,
                )
                return  # one action per notification

        if remaining <= _DUST_TOKENS:
            return

        # --- Take-profit (ascending price_pct → smallest gain first) ---
        for orig_idx, level in sorted(
            enumerate(tp_levels), key=lambda x: x[1].get("price_pct", 0)
        ):
            if orig_idx in tp_fired:
                continue
            trigger = entry_price * (1 + level["price_pct"] / 100)
            if current_price >= trigger:
                tp_fired.add(orig_idx)
                await _sell(
                    remaining * (level["position_pct"] / 100),
                    f"TP{orig_idx + 1}(+{level['price_pct']:.1f}% "
                    f"price={current_price:.8f})",
                    current_price,
                )
                return  # one action per notification

        if remaining <= _DUST_TOKENS:
            return

        # --- Trailing stop ---
        if trail_cfg.get("enabled") and not trail_fired:
            act_pct = float(trail_cfg.get("activation_pct", 50))
            trail_size = float(trail_cfg.get("trail_size_pct", 20))
            trail_pos = float(trail_cfg.get("position_pct", 100))

            if not trail_active and price_chg_pct >= act_pct:
                trail_active = True
                logger.info(
                    f"[monitor] {label} trailing stop ACTIVATED | "
                    f"price={current_price:.8f} ({price_chg_pct:+.1f}%)"
                )

            if trail_active:
                trail_trigger = peak_price * (1 - trail_size / 100)
                if current_price <= trail_trigger:
                    trail_fired = True
                    await _sell(
                        remaining * (trail_pos / 100),
                        f"Trail(peak={peak_price:.8f} -{trail_size:.1f}% "
                        f"price={current_price:.8f})",
                        current_price,
                    )

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

    finally:
        moonbag = remaining if remaining > _DUST_TOKENS else 0.0
        logger.info(
            f"[monitor] {label} position closed | moonbag={moonbag:.4f} tokens"
        )
        cur_cfg = read_config() or {}
        await update_config(
            {"open_positions": max(0, cur_cfg.get("open_positions", 1) - 1)}
        )
