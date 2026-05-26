"""
Сканер новых токенов с уведомлениями в Telegram.

Слушает новые токены и отправляет алерты.
При AUTO_BUY_ENABLED=true — автоматически покупает токены через PlatformAwareBuyer.

Запуск:
    uv run src/scanner_runner.py
    uv run src/scanner_runner.py bots/bot-scanner-telegram.yaml
"""

import asyncio
import base64
import json
import logging
import os
import re
import struct
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import aiohttp
import yaml
from dotenv import load_dotenv
from filelock import AsyncFileLock

try:
    import uvloop

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

from solders.pubkey import Pubkey

from core.client import SolanaClient
from core.priority_fee.helius_fee import HeliusFeeEstimator
from core.priority_fee.manager import PriorityFeeManager
from core.wallet import Wallet
from interfaces.core import Platform, TokenInfo
from platforms.pumpfun.address_provider import PumpFunAddresses
from monitoring.dev_checker import DevWalletInfo, check_dev_wallet
from monitoring.listener_factory import ListenerFactory
from notifications.telegram_reporter import TelegramReporter
from scanner_position_monitor import SimulatedPosition, monitor_position, monitor_position_test
from trading.platform_aware import PlatformAwareBuyer
from utils.logger import get_logger, setup_file_logging

logger = get_logger(__name__)

# Tokens that are permanently ignored — no Telegram message, no processing.
BLACKLISTED_MINTS: frozenset[str] = frozenset({
    "DjrHJeQSrNQ31GEraBm3xmo3Eer963EiXuX7hpCuHnbm",  # phantom "USDC" garbage token
})

# Fee mode constants
SUPERFAST_PRIORITY_UL_PER_CU = 10_000_000   # 850k lamports @ 85k CU = 0.000850 SOL
SUPERFAST_JITO_LAMPORTS = 10_000_000         # 0.010 SOL
ULTRA_PRIORITY_UL_PER_CU = 50_000_000        # 4.25M lamports @ 85k CU = 0.004250 SOL
ULTRA_JITO_LAMPORTS = 25_000_000             # 0.025 SOL

_BOT_CONFIG_PATH = Path(__file__).parent.parent / "bot_config.json"
_BOT_CONFIG_LOCK_PATH = _BOT_CONFIG_PATH.with_suffix(".lock")


def _read_bot_config() -> dict[str, Any] | None:
    if not _BOT_CONFIG_PATH.exists():
        return None
    try:
        with open(_BOT_CONFIG_PATH) as f:
            return json.load(f)
    except Exception:
        return None


async def _update_bot_config(updates: dict[str, Any]) -> None:
    """Merge top-level keys and nested stats into bot_config.json (cross-process safe)."""
    async with AsyncFileLock(_BOT_CONFIG_LOCK_PATH):
        cfg = _read_bot_config() or {}
        for k, v in updates.items():
            if k == "stats" and isinstance(v, dict):
                cfg.setdefault("stats", {}).update(v)
            else:
                cfg[k] = v
        try:
            with open(_BOT_CONFIG_PATH, "w") as f:
                json.dump(cfg, f, indent=2)
        except Exception as e:
            logger.warning(f"[config] Не удалось записать bot_config.json: {e}")


# ---------------------------------------------------------------------------
# Локальные типы
# ---------------------------------------------------------------------------


@dataclass
class TokenHistory:
    mint: str
    name: str
    ath_market_cap: float | None = None
    create_timestamp: int | None = None

    @property
    def migrated(self) -> bool | None:
        if self.ath_market_cap is None:
            return None
        return self.ath_market_cap >= 35_000


# ---------------------------------------------------------------------------
# Загрузка конфига
# ---------------------------------------------------------------------------


def _resolve_env_vars(value: object) -> object:
    if isinstance(value, str):
        def _replace(match: re.Match) -> str:
            var_name = match.group(1)
            result = os.environ.get(var_name, "")
            if not result:
                logger.warning(f"Переменная окружения ${{{var_name}}} не задана в .env")
            return result

        return re.sub(r"\$\{([^}]+)\}", _replace, value)

    elif isinstance(value, dict):
        return {k: _resolve_env_vars(v) for k, v in value.items()}

    elif isinstance(value, list):
        return [_resolve_env_vars(item) for item in value]

    return value


def load_scanner_config(config_path: str) -> dict:
    config_file = Path(config_path)

    if not config_file.exists():
        raise FileNotFoundError(
            f"Файл конфига не найден: {config_path}\n"
            f"Убедись, что файл существует и путь указан правильно."
        )

    with config_file.open(encoding="utf-8") as f:
        raw_config = yaml.safe_load(f)

    if not isinstance(raw_config, dict):
        raise ValueError(f"Конфиг должен быть YAML-словарём: {config_path}")

    env_file = raw_config.get("env_file", ".env")
    env_path = Path(env_file)

    if env_path.exists():
        load_dotenv(env_path)
        logger.info(f"Загружен .env файл: {env_path.resolve()}")
    else:
        logger.warning(
            f".env файл не найден по пути: {env_path.resolve()}\n"
            "Переменные окружения будут читаться из системы."
        )

    return _resolve_env_vars(raw_config)


# ---------------------------------------------------------------------------
# Утилиты форматирования
# ---------------------------------------------------------------------------


def _escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _now_ms() -> str:
    now = datetime.now()
    return now.strftime("%H:%M:%S.") + f"{now.microsecond // 1000:03d}"


def _format_mc(mc: float | None) -> str:
    if mc is None or mc <= 0:
        return "—"
    if mc < 100:
        return f"${mc:.2f}"
    return f"${mc:,.0f}"


# ---------------------------------------------------------------------------
# On-chain проверки (Поток 1 + BC buy)
# ---------------------------------------------------------------------------

_VIRTUAL_SOL_INITIAL = 30_000_000_000
_PUMP_FUN_TOTAL_SUPPLY = 1_000_000_000


async def _fetch_bc_dev_buy(bc_address: str, rpc: str) -> float | None:
    """Read virtual_sol_reserves from BC account to compute dev buy amount.

    Retries when vsol == INITIAL (30 SOL) since the RPC node may not yet have
    propagated the dev buy state even though the transaction is confirmed.
    Runs concurrently with Phase 1 (GMGN, ~1.3 s), so retries cost no latency.
    """
    _MAX_ATTEMPTS = 5
    _RETRY_DELAY_S = 0.25

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [bc_address, {"encoding": "base64", "commitment": "confirmed"}],
    }

    for attempt in range(_MAX_ATTEMPTS):
        try:
            timeout = aiohttp.ClientTimeout(total=2.0)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(rpc, json=payload) as resp:
                    data = await resp.json()

            value = data.get("result", {}).get("value")
            if value is None:
                logger.debug(f"[dev_buy] attempt {attempt+1}: account not found {bc_address[:8]}")
                if attempt < _MAX_ATTEMPTS - 1:
                    await asyncio.sleep(_RETRY_DELAY_S)
                continue

            raw_data = value.get("data")
            if not raw_data or not isinstance(raw_data, list):
                logger.warning(f"[dev_buy] no data field in account: {bc_address[:8]}")
                return None

            account_bytes = base64.b64decode(raw_data[0])
            if len(account_bytes) < 24:
                logger.warning(
                    f"[dev_buy] account data too short ({len(account_bytes)}B): {bc_address[:8]}"
                )
                return None

            vsol = struct.unpack_from("<Q", account_bytes, 16)[0]
            dev_buy = max(0.0, (vsol - _VIRTUAL_SOL_INITIAL) / 1_000_000_000)
            logger.info(
                f"[DEV_BUY] _fetch_bc_dev_buy attempt {attempt+1}: vsol={vsol} → {dev_buy:.4f} SOL"
            )

            if dev_buy > 0.0 or attempt == _MAX_ATTEMPTS - 1:
                return dev_buy

            # vsol == INITIAL: may be reading stale pre-dev-buy state, retry
            logger.debug(
                f"[dev_buy] attempt {attempt+1}: vsol=INITIAL, retrying in {_RETRY_DELAY_S}s"
            )
            await asyncio.sleep(_RETRY_DELAY_S)

        except Exception as e:
            logger.warning(f"[dev_buy] attempt {attempt+1} failed for {bc_address[:8]}: {e}")
            if attempt < _MAX_ATTEMPTS - 1:
                await asyncio.sleep(_RETRY_DELAY_S)

    return 0.0


# ---------------------------------------------------------------------------
# SOL price cache + entry MC helper
# ---------------------------------------------------------------------------

_sol_price_usd: float = 0.0
_position_entry_lock: asyncio.Lock = asyncio.Lock()


async def _sol_price_updater() -> None:
    """Background task: refresh SOL/USD from Binance every 30 s."""
    global _sol_price_usd
    while True:
        try:
            timeout = aiohttp.ClientTimeout(total=3.0)
            async with aiohttp.ClientSession(timeout=timeout) as s:
                async with s.get(
                    "https://api.binance.com/api/v3/ticker/price?symbol=SOLUSDT"
                ) as r:
                    data = await r.json()
                    _sol_price_usd = float(data["price"])
        except Exception:
            pass
        await asyncio.sleep(30)


async def _get_current_token_price_sol(bc_address: str, rpc: str) -> float | None:
    """Read bonding-curve reserves and return current price in SOL per whole token.

    Retries when the BC account looks like it's in initial (pre-dev-buy) state,
    using the same backoff pattern as _fetch_bc_dev_buy.
    Runs concurrently with Phase 1 (GMGN ~1.3 s) so retries cost zero latency.
    """
    _MAX_ATTEMPTS = 5
    _RETRY_DELAY_S = 0.25
    # Below this MC estimate the account is likely wrong / corrupted / not yet
    # created. Real pump.fun initial MC is ~$1,400-$5,600 at SOL $50-$200,
    # so $500 is safely below any legitimate reading. The $388 bug case gives
    # mc_estimate ≈ $337 (pre-slippage), which falls below this floor.
    _MC_STALE_THRESHOLD_USD = 500.0

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [bc_address, {"encoding": "base64", "commitment": "confirmed"}],
    }

    last_price: float | None = None

    for attempt in range(_MAX_ATTEMPTS):
        try:
            timeout = aiohttp.ClientTimeout(total=2.0)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(rpc, json=payload) as resp:
                    data = await resp.json()

            value = data.get("result", {}).get("value")
            if not value:
                logger.debug(
                    f"[MC_FIX] attempt {attempt+1}: BC account not found {bc_address[:8]}"
                )
                if attempt < _MAX_ATTEMPTS - 1:
                    await asyncio.sleep(_RETRY_DELAY_S)
                continue

            raw_data = value.get("data")
            if not raw_data or not isinstance(raw_data, list):
                return None

            account_bytes = base64.b64decode(raw_data[0])
            if len(account_bytes) < 24:
                return None

            vtoken = struct.unpack_from("<Q", account_bytes, 8)[0]
            vsol = struct.unpack_from("<Q", account_bytes, 16)[0]
            if vtoken <= 0 or vsol <= 0:
                if attempt < _MAX_ATTEMPTS - 1:
                    await asyncio.sleep(_RETRY_DELAY_S)
                continue

            # price in SOL per full token (6 decimals)
            price = (vsol / vtoken) * (10**6) / 1_000_000_000
            last_price = price

            mc_estimate = price * _PUMP_FUN_TOTAL_SUPPLY * _sol_price_usd
            logger.info(
                f"[MC_FIX] attempt {attempt+1}: vsol={vsol}, vtoken={vtoken}, "
                f"mc_estimate=${mc_estimate:.0f}"
            )

            if _sol_price_usd > 0 and mc_estimate <= _MC_STALE_THRESHOLD_USD:
                # BC is in initial / pre-dev-buy state — retry
                logger.debug(
                    f"[MC_FIX] attempt {attempt+1}: mc=${mc_estimate:.0f} <= "
                    f"${_MC_STALE_THRESHOLD_USD:.0f} threshold, retrying"
                )
                if attempt < _MAX_ATTEMPTS - 1:
                    await asyncio.sleep(_RETRY_DELAY_S)
                continue

            return price

        except Exception as e:
            logger.debug(f"[MC_FIX] attempt {attempt+1} failed for {bc_address[:8]}: {e}")
            if attempt < _MAX_ATTEMPTS - 1:
                await asyncio.sleep(_RETRY_DELAY_S)

    # Return last read value even if below threshold (avoids silently dropping
    # tokens whose genuine MC is near the floor)
    return last_price


async def _check_entry_mc(
    filt: dict,
    bc_address: str | None,
    rpc: str | None,
    token_label: str,
) -> tuple[bool, str]:
    """Return (passes, skip_reason_or_empty). Skips check when both limits are 0."""
    min_mc = float(filt.get("min_entry_mc_usd", 0))
    max_mc = float(filt.get("max_entry_mc_usd", 0))
    if min_mc <= 0 and max_mc <= 0:
        return True, ""
    if not bc_address or not rpc:
        return True, ""
    if _sol_price_usd <= 0:
        return True, ""
    cur_price_sol = await _get_current_token_price_sol(bc_address, rpc)
    if cur_price_sol is None or cur_price_sol <= 0:
        return True, ""
    cur_mc_usd = cur_price_sol * 1_000_000_000 * _sol_price_usd
    if min_mc > 0 and cur_mc_usd < min_mc:
        logger.info(
            f"[MC filter] {token_label}: MC=${cur_mc_usd:.0f} < min=${min_mc:.0f} → skip"
        )
        return False, f"\n⛔ MC ${cur_mc_usd:,.0f} — ниже min MC ${min_mc:,.0f}"
    if max_mc > 0 and cur_mc_usd > max_mc:
        logger.info(
            f"[MC filter] {token_label}: MC=${cur_mc_usd:.0f} > max=${max_mc:.0f} → skip"
        )
        return False, f"\n⛔ MC ${cur_mc_usd:,.0f} — выше max MC ${max_mc:,.0f}"
    return True, ""


async def _check_mint_freeze(mint_str: str, rpc: str) -> tuple[bool, bool]:
    """Check mint and freeze authorities. Returns (has_mint_auth, has_freeze_auth)."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [mint_str, {"encoding": "base64", "commitment": "confirmed"}],
    }
    timeout = aiohttp.ClientTimeout(total=2.0)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for attempt in range(2):
            async with session.post(rpc, json=payload) as resp:
                data = await resp.json()
            value = data.get("result", {}).get("value")
            if value is not None:
                break
            if attempt == 0:
                await asyncio.sleep(0.3)
        else:
            return False, False

    raw_data = value.get("data")
    if not raw_data or not isinstance(raw_data, list):
        return False, False
    raw = base64.b64decode(raw_data[0])
    if len(raw) < 82:
        return False, False
    has_mint = struct.unpack_from("<I", raw, 0)[0] == 1
    has_freeze = struct.unpack_from("<I", raw, 46)[0] == 1
    return has_mint, has_freeze


# ---------------------------------------------------------------------------
# Helius BC signatures batch (Stream 4)
# ---------------------------------------------------------------------------


async def _fetch_signatures_for_bc(bc_address: str, rpc: str) -> tuple[int, int | None]:
    """Fetch tx count and last blockTime for one bonding-curve address.

    Uses limit=1000 — count is capped but sufficient to distinguish rugs (5–50)
    from real tokens (100+).  Returns (tx_count, last_blocktime_unix).
    last_blocktime is signatures[0].blockTime — i.e. the *most recent* trade.
    """
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSignaturesForAddress",
        "params": [bc_address, {"limit": 1000, "commitment": "confirmed"}],
    }
    timeout = aiohttp.ClientTimeout(total=2.0)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(rpc, json=payload) as resp:
                data = await resp.json()
        sigs = data.get("result") or []
        last_blocktime: int | None = sigs[0].get("blockTime") if sigs else None
        return len(sigs), last_blocktime
    except Exception as exc:
        logger.debug(f"[sigs] fetch failed for {bc_address[:8]}: {exc}")
        return 0, None


async def _fetch_token_signatures_batch(
    histories: list[TokenHistory],
    rpc: str,
) -> dict[str, tuple[int, int | None]]:
    """Fetch (tx_count, last_blocktime) for each history token's BC v2 in parallel.

    Derives each bonding-curve-v2 PDA from the mint address.
    Returns {mint_str: (tx_count, last_blocktime)}.
    Entries that fail silently return (0, None).
    """

    async def _one(h: TokenHistory) -> tuple[str, int, int | None]:
        try:
            bc_v2 = str(PumpFunAddresses.find_bonding_curve_v2(Pubkey.from_string(h.mint)))
            tx_count, last_bt = await _fetch_signatures_for_bc(bc_v2, rpc)
        except Exception as exc:
            logger.debug(f"[sigs] BC derivation failed for {h.mint[:8]}: {exc}")
            tx_count, last_bt = 0, None
        return h.mint, tx_count, last_bt

    raw = await asyncio.gather(*[_one(h) for h in histories], return_exceptions=True)
    result: dict[str, tuple[int, int | None]] = {}
    for item in raw:
        if isinstance(item, tuple):
            mint, cnt, bt = item
            result[mint] = (cnt, bt)
    return result


# ---------------------------------------------------------------------------
# Helius-native migrations check (Поток 3b — fallback when GMGN unavailable)
# ---------------------------------------------------------------------------


async def _check_bc_migrations_native(mints: list[str], rpc: str) -> int:
    """Count how many of dev's last N tokens have migrated (bonding curve complete=True).

    Derives bonding-curve-v2 PDA for each mint, fetches BC account via Helius RPC,
    reads the `complete` bool at byte 48 (BC v2 account is 83 bytes per CONTEXT §16).
    All fetches run in parallel — latency ≈ single getAccountInfo call (~300-500 ms).
    Returns migration count (0 if all fetches fail).
    """
    async def _one(mint_str: str) -> bool:
        try:
            bc_addr = str(PumpFunAddresses.find_bonding_curve_v2(Pubkey.from_string(mint_str)))
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getAccountInfo",
                "params": [bc_addr, {"encoding": "base64", "commitment": "confirmed"}],
            }
            timeout = aiohttp.ClientTimeout(total=2.0)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(rpc, json=payload) as resp:
                    data = await resp.json()
            value = data.get("result", {}).get("value")
            if not value:
                return False  # BC not found → token dead / never existed
            raw_data = value.get("data")
            if not raw_data or not isinstance(raw_data, list):
                return False
            account_bytes = base64.b64decode(raw_data[0])
            if len(account_bytes) < 56:
                return False
            # complete flag at byte 48 (bool, 8-byte padded field — see CONTEXT §16)
            return account_bytes[48] == 1
        except Exception as exc:
            logger.debug(f"[mig_native] BC check failed for {mint_str[:8]}: {exc}")
            return False

    results = await asyncio.gather(*[_one(m) for m in mints[:5]], return_exceptions=True)
    return sum(1 for r in results if r is True)


# ---------------------------------------------------------------------------
# GMGN CLI (Поток 3)
# ---------------------------------------------------------------------------


_GMGN_BASE_URL: str = "https://openapi.gmgn.ai/v1/user/created_tokens"


def _parse_gmgn_tokens(
    raw_tokens: list[dict], current_mint: str
) -> list[TokenHistory]:
    """Sort desc by create_timestamp, exclude current_mint, return up to 5."""
    sorted_tokens = sorted(
        [t for t in raw_tokens if t.get("token_address") != current_mint],
        key=lambda t: t.get("create_timestamp") or 0,
        reverse=True,
    )
    histories: list[TokenHistory] = []
    for t in sorted_tokens[:5]:
        ath_raw = t.get("token_ath_mc")
        ts_raw = t.get("create_timestamp")
        histories.append(TokenHistory(
            mint=t.get("token_address") or "",
            name=t.get("symbol") or "—",
            ath_market_cap=float(ath_raw) if ath_raw else None,
            create_timestamp=int(ts_raw) if ts_raw else None,
        ))
    return histories


async def _get_gmgn_dev_tokens(dev_wallet: str, current_mint: str = "") -> tuple[list[TokenHistory], str | None]:
    """Fetch tokens created by dev wallet.

    Primary: GMGN OpenAPI HTTP (~200 ms when reachable).
    4xx responses (Cloudflare IP/ASN block, invalid key) → skip HTTP retry immediately
    and go straight to CLI, saving ~500 ms per token.
    Fallback: gmgn-cli subprocess — reads ~/.config/gmgn/.env for auth, bypasses Cloudflare.
    CLI output format: {"tokens": [...], "inner_count": N, ...} (dict, not bare list).
    """
    import uuid

    label = dev_wallet[:8]
    api_key = os.environ.get("GMGN_API_KEY", "")
    params = {
        "chain": "sol",
        "wallet_address": dev_wallet,
        "timestamp": int(time.time()),
        "client_id": str(uuid.uuid4()),
    }
    headers = {"X-APIKEY": api_key, "Content-Type": "application/json"}

    async def _http_attempt() -> tuple[list[TokenHistory], str | None]:
        t0 = time.perf_counter()
        async with aiohttp.ClientSession() as session:
            async with session.get(
                _GMGN_BASE_URL,
                params=params,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=3.5),
            ) as resp:
                ms = int((time.perf_counter() - t0) * 1000)
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status}")
                payload = await resp.json(content_type=None)

        if payload.get("code") != 0:
            raise RuntimeError(f"API code={payload.get('code')} msg={payload.get('message')}")

        raw_tokens = (payload.get("data") or {}).get("tokens") or []
        total_field = (payload.get("data") or {}).get("total")
        if isinstance(total_field, int) and total_field > 0:
            gmgn_total: str = str(total_field)
        else:
            gmgn_total = f"{len(raw_tokens)}+" if raw_tokens else "0"
        logger.info(f"[GMGN HTTP] {label}: {ms}ms, {len(raw_tokens)} tokens")
        return _parse_gmgn_tokens(raw_tokens, current_mint), gmgn_total

    async def _cli_fallback() -> tuple[list[TokenHistory], str | None]:
        """npx gmgn-cli — reads ~/.config/gmgn/.env, bypasses Cloudflare IP block.
        Returns {"tokens":[...],"inner_count":N,...} — a dict, not a bare list.
        """
        import json as _json
        t0 = time.perf_counter()
        proc = await asyncio.create_subprocess_exec(
            "npx", "gmgn-cli", "portfolio", "created-tokens",
            "--chain", "sol", "--wallet", dev_wallet, "--raw",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=6.0)
        except asyncio.TimeoutError:
            proc.kill()
            raise RuntimeError("gmgn-cli timed out after 6s")
        ms = int((time.perf_counter() - t0) * 1000)
        if proc.returncode != 0:
            raise RuntimeError(f"gmgn-cli exit {proc.returncode}: {stderr.decode()[:200]}")
        result = _json.loads(stdout.decode())
        # CLI returns a dict {"tokens": [...], "inner_count": N} — extract the list
        if isinstance(result, list):
            raw_tokens = result
            inner_count = len(result)
        elif isinstance(result, dict) and "tokens" in result:
            raw_tokens = result.get("tokens") or []
            inner_count = result.get("inner_count") or len(raw_tokens)
        else:
            raise RuntimeError(f"gmgn-cli unexpected output type: {type(result).__name__}: {stdout[:80]}")
        gmgn_total = (
            str(inner_count) if isinstance(inner_count, int) and inner_count > 0
            else (f"{len(raw_tokens)}+" if raw_tokens else "0")
        )
        logger.info(f"[GMGN CLI] {label}: {ms}ms, {len(raw_tokens)} tokens (total={gmgn_total})")
        return _parse_gmgn_tokens(raw_tokens, current_mint), gmgn_total

    # HTTP attempt — Cloudflare blocks many IPs with 403/401 (error 1010 = ASN block).
    # On any 4xx: skip retry immediately (retrying a blocked IP wastes ~500 ms).
    _http_blocked = False
    try:
        return await _http_attempt()
    except Exception as exc:
        _http_blocked = "HTTP 4" in str(exc)  # 401, 403, 404, etc.
        if _http_blocked:
            logger.warning(f"[GMGN HTTP] {label}: blocked ({exc}) — going straight to CLI")
        else:
            logger.warning(f"[GMGN HTTP] {label}: attempt 1 failed ({exc}), retrying in 200ms")

    if not _http_blocked:
        await asyncio.sleep(0.2)
        try:
            return await _http_attempt()
        except Exception as exc:
            logger.warning(f"[GMGN HTTP] {label}: attempt 2 failed ({exc}), falling back to CLI")

    try:
        return await _cli_fallback()
    except Exception as exc:
        raise RuntimeError(f"GMGN: HTTP + CLI failed: {exc}") from exc


# ---------------------------------------------------------------------------
# Форматирование алерта
# ---------------------------------------------------------------------------


def format_token_alert(
    token_info: TokenInfo,
    count: int,
    dev: DevWalletInfo | None = None,
    dev_buy_sol: float | None = None,
    token_history: list[TokenHistory] | None = None,
    gmgn_total: str | None = None,
    gmgn_was_called: bool = True,
    is_new_wallet: bool = False,
) -> str:
    mint_str = str(token_info.mint)
    creator = token_info.creator or token_info.user
    creator_str = str(creator) if creator else "неизвестен"

    if token_info.platform == Platform.PUMP_FUN:
        token_url = f"https://gmgn.ai/sol/token/{mint_str}"
    else:
        token_url = f"https://letsbonk.fun/token/{mint_str}"

    name = _escape_html(token_info.name)
    symbol = _escape_html(token_info.symbol)
    dev_addr = _escape_html(creator_str)

    if dev_buy_sol is None or dev_buy_sol < 0.001:
        dev_buy_str = "—"
    else:
        dev_buy_str = f"{dev_buy_sol:.3f} SOL"

    if dev is None or dev.timed_out or dev.error:
        bal = "—"
        age = "—"
        launches = "—"
    else:
        bal = f"{dev.sol_balance:.3f} SOL" if dev.sol_balance is not None else "—"
        age = dev.wallet_age_str or "—"
        if gmgn_total is not None:
            launches = gmgn_total
        elif dev.total_launches is not None:
            launches = f"{dev.total_launches}+" if dev.launches_truncated else str(dev.total_launches)
        else:
            launches = "—"

    lines = [
        f"⚡ ПРОВЕРКА #{count}",
        f"🔥 {name} (${symbol})",
        f"📋 <code>{mint_str}</code>",
        f"🔗 {token_url}",
        "",
        f"👤 ДЕВ: <code>{dev_addr}</code>",
        f"📅 Возраст кошелька: {age}",
        f"💰 Баланс: {bal}",
        f"🚀 Всего запусков: {launches}",
    ]
    if is_new_wallet:
        lines.append("🆕 <b>New Dev Wallet</b> — first token launch")
    lines.append(f"💵 Дев купил при запуске: {dev_buy_str}")

    if gmgn_was_called:
        lines.append("📦 Последние токены дева:")
        if token_history:
            for i, th in enumerate(token_history, 1):
                mig = (
                    "✅ Мигрировал" if th.migrated is True
                    else "❌ Нет" if th.migrated is False
                    else "—"
                )
                ath_str = _format_mc(th.ath_market_cap)
                ath_highlight = (
                    " 💚" if th.ath_market_cap is not None and th.ath_market_cap >= 20_000
                    else ""
                )
                lines.append(
                    f"  {i}. {_escape_html(th.name)} — {mig} | ATH: {ath_str}{ath_highlight}"
                )
        else:
            lines.append("  —")
    else:
        lines.append("📊 История дева: не проверялась (ATH/мигр. выключены)")

    lines.append("")
    lines.append(f"🕐 {_now_ms()}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Основной цикл сканера
# ---------------------------------------------------------------------------


async def run_scanner(config_path: str) -> None:
    logger.info(f"Загружаю конфиг: {config_path}")
    cfg = load_scanner_config(config_path)

    if not cfg.get("enabled", True):
        logger.info("Сканер отключён в конфиге (enabled: false). Выход.")
        return

    scanner_name = cfg.get("name", "scanner")

    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"{scanner_name}_{timestamp}.log"
    setup_file_logging(str(log_file))
    logger.info(f"Логи пишутся в файл: {log_file}")

    logger.info(f"=== Запуск сканера: {scanner_name} ===")

    telegram_section = cfg.get("telegram", {})
    telegram_token = telegram_section.get("bot_token") or os.environ.get(
        "TELEGRAM_BOT_TOKEN", ""
    )
    telegram_chat_id = telegram_section.get("chat_id") or os.environ.get(
        "TELEGRAM_CHAT_ID", ""
    )

    if not telegram_token or not telegram_chat_id:
        logger.error(
            "❌ Не найдены TELEGRAM_BOT_TOKEN и/или TELEGRAM_CHAT_ID!\n"
            "Добавь их в файл .env:\n"
            "  TELEGRAM_BOT_TOKEN=1234567890:ABCdef...\n"
            "  TELEGRAM_CHAT_ID=-1001234567890"
        )
        return

    token_preview = telegram_token[:10] + "..."
    logger.info(f"Telegram token: {token_preview} | Chat ID: {telegram_chat_id}")

    telegram = TelegramReporter(
        bot_token=telegram_token,
        chat_id=telegram_chat_id,
    )

    platform_str = cfg.get("platform", "pump_fun")
    try:
        platform = Platform(platform_str)
    except ValueError:
        logger.error(
            f"Неизвестная платформа: '{platform_str}'\n"
            "Доступные значения: pump_fun, lets_bonk"
        )
        return

    platform_display = "pump.fun" if platform == Platform.PUMP_FUN else "letsbonk.fun"
    logger.info(f"Платформа: {platform_display}")

    logger.info("Отправляю стартовое сообщение в Telegram...")
    await telegram.send_startup_message(platform=platform_display)

    rpc_endpoint = cfg.get("rpc_endpoint") or os.environ.get(
        "SOLANA_NODE_RPC_ENDPOINT", ""
    )
    if rpc_endpoint:
        logger.info("On-chain проверка: RPC эндпоинт настроен")
    else:
        logger.warning(
            "⚠️  SOLANA_NODE_RPC_ENDPOINT не задан — on-chain проверка будет пропущена"
        )

    logger.info("GMGN: используется HTTP API (2.5s timeout, 1 retry)")

    # -----------------------------------------------------------------------
    # Инициализация покупателя
    # -----------------------------------------------------------------------
    private_key = os.environ.get("SOLANA_PRIVATE_KEY", "")
    helius_staked_url = os.environ.get("HELIUS_STAKED_URL", "")
    buy_amount_sol = float(os.environ.get("BUY_AMOUNT_SOL", "0.01"))
    buy_slippage = float(os.environ.get("BUY_SLIPPAGE", "0.3"))
    priority_fee_sol = float(os.environ.get("PRIORITY_FEE_SOL", "0.001"))
    auto_buy_enabled = os.environ.get("AUTO_BUY_ENABLED", "false").lower() == "true"
    priority_fee_microlamports = int(priority_fee_sol * 1_000_000_000)

    buyer: PlatformAwareBuyer | None = None
    buyer_client: SolanaClient | None = None

    if not private_key:
        logger.warning("SOLANA_PRIVATE_KEY не задан — покупки отключены")
    elif not rpc_endpoint:
        logger.warning("RPC endpoint не задан — покупки отключены")
    else:
        try:
            wallet = Wallet(private_key)
            send_url = helius_staked_url or rpc_endpoint
            buyer_client = SolanaClient(
                rpc_endpoint,
                send_rpc_endpoint=send_url,
            )
            priority_fee_manager = PriorityFeeManager(
                client=buyer_client,
                enable_dynamic_fee=False,
                enable_fixed_fee=True,
                fixed_fee=priority_fee_microlamports,
                extra_fee=0.0,
                hard_cap=priority_fee_microlamports,
            )
            buyer = PlatformAwareBuyer(
                client=buyer_client,
                wallet=wallet,
                priority_fee_manager=priority_fee_manager,
                amount=buy_amount_sol,
                slippage=buy_slippage,
                max_retries=1,
                extreme_fast_mode=True,
            )
            logger.info(
                f"Покупатель инициализирован: {buy_amount_sol} SOL, "
                f"slippage={buy_slippage}, priority_fee={priority_fee_sol} SOL"
            )
        except Exception as e:
            logger.error(f"Ошибка инициализации покупателя: {e}")
            buyer = None

    logger.info(
        f"Автопокупка: {'ВКЛЮЧЕНА' if auto_buy_enabled and buyer else 'ВЫКЛЮЧЕНА'}"
    )

    listener_type = cfg.get("listener_type", "pumpportal")
    wss_endpoint = cfg.get("wss_endpoint", "")

    logger.info(f"Метод прослушивания: {listener_type}")

    if listener_type == "blocks":
        logger.warning(
            "⚠️  Метод 'blocks' использует blockSubscribe — этот метод может быть "
            "недоступен на бесплатном тарифе Chainstack/Helius.\n"
            "Если бот не получает токены — переключись на listener_type: pumpportal"
        )

    try:
        listener = ListenerFactory.create_listener(
            listener_type=listener_type,
            wss_endpoint=wss_endpoint if wss_endpoint else None,
            platforms=[platform],
        )
    except ValueError as e:
        logger.error(f"Ошибка создания слушателя: {e}")
        await telegram.send_error_message(f"Ошибка конфига: {e}")
        return

    filters = cfg.get("filters", {})
    match_string = filters.get("match_string") or None
    creator_address = filters.get("creator_address") or None

    if match_string:
        logger.info(f"Фильтр по имени/символу: {match_string}")
    if creator_address:
        logger.info(f"Фильтр по кошельку разработчика: {creator_address}")

    # -----------------------------------------------------------------------
    # N-trades mode: read mode/max_trades from bot_config.json at startup
    # -----------------------------------------------------------------------
    _initial_cfg = _read_bot_config() or {}
    bot_mode: str = str(_initial_cfg.get("mode", "infinite"))
    max_trades_count: int = int(_initial_cfg.get("max_trades", 10))
    successful_buys_this_session: int = 0
    _stop_scanning: bool = False
    logger.info(f"Mode: {bot_mode} | Max trades: {max_trades_count}")

    token_count = 0

    # -----------------------------------------------------------------------
    # BUG 1 FIX: deduplication
    # logsSubscribe fires multiple log entries per transaction.
    # processed_mints prevents the same mint from being handled more than once.
    # -----------------------------------------------------------------------
    processed_mints: dict[str, float] = {}  # mint_str -> monotonic time first seen

    async def _cleanup_processed_mints() -> None:
        """Remove entries older than 5 minutes every 60 seconds."""
        while True:
            await asyncio.sleep(60)
            cutoff = time.monotonic() - 1800
            stale = [m for m, t in list(processed_mints.items()) if t < cutoff]
            for m in stale:
                processed_mints.pop(m, None)
            if stale:
                logger.debug(f"[dedup] Removed {len(stale)} stale mint(s)")

    asyncio.create_task(_cleanup_processed_mints())
    asyncio.create_task(_sol_price_updater())

    async def on_new_token(token_info: TokenInfo) -> None:
        nonlocal token_count

        # N-trades mode: stop dispatching new tokens when limit reached
        if _stop_scanning:
            return

        mint_str = str(token_info.mint)

        # --- Deduplication: drop every duplicate from the same transaction ---
        now = time.monotonic()
        if mint_str in processed_mints:
            logger.debug(f"[dedup] Duplicate dropped: {mint_str[:8]}")
            return
        processed_mints[mint_str] = now

        token_count += 1
        current_count = token_count

        logger.info(
            f"[#{current_count}] Новый токен: {token_info.name} ({token_info.symbol})"
            f" | {mint_str}"
        )

        asyncio.create_task(_update_bot_config({
            "stats": {"tokens_found_today": (
                (_read_bot_config() or {}).get("stats", {}).get("tokens_found_today", 0) + 1
            )}
        }))

        asyncio.create_task(
            _check_and_notify(token_info, current_count, rpc_endpoint, telegram)
        )

    async def _check_and_notify(
        token_info: TokenInfo,
        count: int,
        rpc: str,
        tg: TelegramReporter,
    ) -> None:
        nonlocal successful_buys_this_session, _stop_scanning

        # STEP 1: Capacity check — must be first, before ANY data fetch or Telegram output
        _cap_cfg = _read_bot_config() or {}
        _open_pos_cap = int(_cap_cfg.get("open_positions", 0))
        _max_concurrent_cap = int(_cap_cfg.get("max_concurrent_positions", 1))
        _any_position_open = _open_pos_cap > 0
        _at_capacity = _open_pos_cap >= _max_concurrent_cap
        if _at_capacity:
            logger.debug(
                f"[SILENT] {token_info.mint} — at capacity ({_open_pos_cap}/{_max_concurrent_cap}), skipping"
            )
            return

        t_total = time.perf_counter()
        creator = token_info.creator or token_info.user
        creator_str = str(creator) if creator else ""
        mint_str = str(token_info.mint)

        if mint_str in BLACKLISTED_MINTS:
            logger.debug(f"[#{count}] Blacklisted mint — silent skip: {mint_str[:8]}")
            return

        bc_str = str(token_info.bonding_curve) if token_info.bonding_curve else None

        # Extract dev_buy from CreateEvent virtual_sol_reserves (zero-cost, no RPC).
        # NOTE: CreateEvent fires at BC initialization (pre-dev-buy), so vsr == 30e9 is
        # the normal initial state — do NOT set dev_buy_sol=0.0 in that case, because that
        # would prevent the RPC fallback from running. Only set from event when vsr > 30e9.
        logger.info(
            f"[DEV_BUY] virtual_sol_reserves={token_info.virtual_sol_reserves} "
            f"dev_buy_sol(pre)={token_info.dev_buy_sol}"
        )
        if token_info.dev_buy_sol is None and token_info.virtual_sol_reserves is not None:
            vsr = token_info.virtual_sol_reserves
            if isinstance(vsr, int) and vsr > _VIRTUAL_SOL_INITIAL:
                token_info.dev_buy_sol = (vsr - _VIRTUAL_SOL_INITIAL) / 1_000_000_000
                logger.info(f"[DEV_BUY] set from event: {token_info.dev_buy_sol:.4f} SOL")
            # else: vsr == INITIAL (pre-dev-buy) or unexpected type — leave as None
            # so _fetch_bc_dev_buy RPC fallback runs

        if token_info.platform == Platform.PUMP_FUN:
            token_url = f"https://gmgn.ai/sol/token/{mint_str}"
        else:
            token_url = f"https://letsbonk.fun/token/{mint_str}"

        name = _escape_html(token_info.name)
        symbol = _escape_html(token_info.symbol)

        # Short rejection message helper
        def _reject(reason: str) -> str:
            return (
                f"❌ ${symbol} — {name}\n"
                f"📋 <code>{mint_str}</code>\n"
                f"🔗 {token_url}\n"
                f"🚫 Not passed: {reason}"
            )

        # -------------------------------------------------------------------
        # FILTER 1: Mayhem mode — silent skip, no Telegram
        # -------------------------------------------------------------------
        if token_info.is_mayhem_mode:
            logger.debug(f"[#{count}] Mayhem mode — silent skip")
            return

        # -------------------------------------------------------------------
        # BUG 2 FIX: log clearly when key addresses are missing
        # -------------------------------------------------------------------
        if not creator_str:
            logger.warning(
                f"[#{count}] DEV address not extracted from event — "
                f"dev wallet check and GMGN will be skipped"
            )
        if not bc_str:
            logger.warning(
                f"[#{count}] bonding_curve address missing from event — "
                f"dev_buy will be None (token: {mint_str[:8]})"
            )

        # Pre-read config to decide whether to run the signatures batch (stream 4).
        _pre_cfg = _read_bot_config() or {}
        _pre_filt = (
            (_pre_cfg.get("presets") or {})
            .get(str(_pre_cfg.get("active_preset", 1)), {})
            .get("filters", {})
        )
        _ath_enabled: bool = bool(_pre_filt.get("ath_enabled", True))
        _mig_enabled: bool = bool(_pre_filt.get("migrations_enabled", True))
        _tx_count_enabled: bool = bool(_pre_filt.get("tx_count_enabled", True))
        _lifetime_enabled: bool = bool(_pre_filt.get("lifetime_enabled", True))
        _pre_preset = ((_pre_cfg.get("presets") or {}).get(str(_pre_cfg.get("active_preset", 1)), {}))
        _fee_mode_pre: str = str(_pre_preset.get("fee_mode", "auto")).lower()

        # -----------------------------------------------------------------------
        # FILTER TOGGLE ISOLATION — FETCH TRUTH TABLE
        # Which underlying data fetches fire for each toggle combination:
        #
        # ath  mig  lt   tx  | GMGN call | Stream4 sigs | BC dev buy
        # ─────────────────────────────────────────────────────────────────────
        # ON   *    *    *   |   YES      |  if tx/lt ON |  if check ON
        # OFF  ON   *    *   |   YES      |  if tx/lt ON |  if check ON
        # OFF  OFF  ON   *   |   NO       |  if values>0 |  if check ON  ← lt uses Stream4 sigs
        # OFF  OFF  OFF  ON  |   NO       |  if values>0 |  if check ON  ← TX uses dev.recent_tokens
        # OFF  OFF  OFF  OFF |   NO       |      NO      |  if check ON
        #
        # GMGN fetch:    ath OR mig ONLY (need per-token ATH MC or migration status)
        #                lifetime/TX use on-chain sigs (Stream4) — no GMGN needed
        # Stream4 sigs:  (tx_count OR lifetime) AND has non-zero threshold
        #                Source: GMGN histories when available, dev.recent_tokens as fallback
        # BC buy fetch:  dev_buy_check_enabled=True AND dev_buy_sol not already from event
        # MC check:      entry_mc_enabled=True AND auto_trading=True
        # mig_task:      GMGN failed AND mig_enabled AND dev.recent_tokens available
        # -----------------------------------------------------------------------

        # skip_gmgn: GMGN needed ONLY for ATH (per-token ATH MC) and migrations (ATH≥35k proxy).
        # Lifetime uses Stream4 create_timestamp; TX count only needs mint addresses.
        skip_gmgn: bool = not (_ath_enabled or _mig_enabled)
        # Stream 4 only needed when tx_count or lifetime filter is active AND has non-zero values
        _need_sigs: bool = bool(rpc) and bool(creator_str) and (_tx_count_enabled or _lifetime_enabled) and (
            int(_pre_filt.get("min_tx_count", 0)) > 0
            or int(_pre_filt.get("max_tx_count", 0)) > 0
            or float(_pre_filt.get("min_lifetime_minutes", 0)) > 0
        )
        _dev_buy_check: bool = bool(_pre_filt.get("dev_buy_check_enabled", True))
        logger.info(f"[#{count}] [PIPELINE] skip_gmgn={skip_gmgn} | need_sigs={_need_sigs} | dev_buy_check={_dev_buy_check}")

        # -------------------------------------------------------------------
        # Data fetch — all four tasks fire immediately in parallel at mint time.
        # Phase 1: GMGN + dev wallet (gates filters 3-6, ~1.3 s typical).
        # Phase 2: dev buy collected LAST after other filters pass — the RPC
        #   call runs during Phase1+filters so it's usually already done.
        # Net result: total latency = max(GMGN, dev_buy) → not GMGN + dev_buy.
        # -------------------------------------------------------------------
        t_fetch = time.perf_counter()
        # If the listener already provided dev_buy_sol (e.g. from event when vsr > 30e9),
        # skip the BC account RPC call. Also skip when dev_buy_check_enabled=False to avoid
        # a latency-free wasted RPC call.
        task_bc_buy: asyncio.Task = asyncio.create_task(
            asyncio.sleep(0, result=token_info.dev_buy_sol)
            if token_info.dev_buy_sol is not None
            else (
                _fetch_bc_dev_buy(bc_str, rpc)
                if (bc_str and rpc and _dev_buy_check)
                else asyncio.sleep(0, result=None)
            )
        )
        task_mint: asyncio.Task = asyncio.create_task(
            _check_mint_freeze(mint_str, rpc) if rpc
            else asyncio.sleep(0, result=(False, False)),
        )
        # Fire Helius dynamic fee estimation in parallel with Phase 1 (~200 ms).
        # superfast/ultra modes use fixed fees — skip the API call entirely.
        _helius_url = helius_staked_url or rpc
        task_helius_fee: asyncio.Task = asyncio.create_task(
            HeliusFeeEstimator().get_max_fee_microlamports(_helius_url)
            if (_helius_url and _fee_mode_pre == "auto") else asyncio.sleep(0, result=None)
        )

        dev = DevWalletInfo(address=creator_str, timed_out=True)
        dev_buy_sol: float | None = None
        has_mint = has_freeze = False
        histories: list[TokenHistory] = []
        gmgn_failed = False
        gmgn_total_launches: str | None = None
        sig_data: dict[str, tuple[int, int | None]] = {}
        phase1_ok = False
        mig_task: asyncio.Task | None = None
        helius_mig_count: int | None = None

        try:
            # Phase 1: GMGN + dev wallet run concurrently; task_bc_buy fires in background.
            # When skip_gmgn=True (all dev-history filters disabled) GMGN call is skipped.
            dev_r, gmgn_r = await asyncio.wait_for(
                asyncio.gather(
                    check_dev_wallet(creator_str, rpc) if (creator_str and rpc)
                    else asyncio.sleep(0, result=DevWalletInfo(address=creator_str or "")),
                    asyncio.sleep(0, result=([], None)) if skip_gmgn
                    else (
                        _get_gmgn_dev_tokens(creator_str, current_mint=mint_str) if creator_str
                        else asyncio.sleep(0, result=([], None))
                    ),
                    return_exceptions=True,
                ),
                timeout=4.0,
            )
            phase1_ok = True

            dev = (
                dev_r if isinstance(dev_r, DevWalletInfo)
                else DevWalletInfo(address=creator_str or "")
            )
            if isinstance(dev_r, Exception):
                logger.warning(f"[#{count}] Dev wallet check failed: {dev_r}")

            gmgn_failed = (not skip_gmgn) and isinstance(gmgn_r, Exception)
            if isinstance(gmgn_r, Exception) and not skip_gmgn:
                logger.warning(f"[#{count}] GMGN fetch failed for {mint_str[:8]}: {gmgn_r}")
                histories = []
            elif isinstance(gmgn_r, tuple):
                histories, gmgn_total_launches = gmgn_r
                histories = histories or []
            else:
                histories = []

            logger.info(
                f"[#{count}] Phase1 (dev{'[no GMGN]' if skip_gmgn else '+GMGN'}): "
                f"{(time.perf_counter()-t_fetch)*1000:.0f}ms"
                f" | GMGN: {len(histories)} tokens"
                f"{' [GMGN FAILED]' if gmgn_failed else ''}"
            )

            # BUG #2 FIX: GMGN returned empty (not error) but dev has recent tokens →
            # likely GMGN indexing lag for a recently-created token, not a truly new wallet.
            # One retry after 400ms. Skipped when dev has no recent activity (truly new wallet).
            if (
                not histories
                and not gmgn_failed
                and not skip_gmgn
                and creator_str
                and isinstance(dev, DevWalletInfo)
                and dev.recent_tokens
            ):
                logger.info(
                    f"[#{count}] GMGN empty + dev has {len(dev.recent_tokens)} recent token(s) "
                    f"— retrying in 400ms (indexing lag check)"
                )
                await asyncio.sleep(0.4)
                try:
                    _retry_histories, _retry_total = await asyncio.wait_for(
                        _get_gmgn_dev_tokens(creator_str, current_mint=mint_str),
                        timeout=2.5,
                    )
                    if _retry_histories:
                        histories = _retry_histories
                        gmgn_total_launches = _retry_total
                        logger.info(f"[#{count}] GMGN retry: {len(histories)} tokens (was empty — indexing lag confirmed)")
                    else:
                        logger.info(f"[#{count}] GMGN retry: still empty — new wallet confirmed")
                except Exception as _retry_exc:
                    logger.warning(f"[#{count}] GMGN retry failed: {_retry_exc}")

            # Fire Helius-native migrations check immediately after Phase 1 using
            # dev.recent_tokens (mints from getTransaction batch in check_dev_wallet).
            # Runs concurrently with Stream 4 — ~300-500 ms, usually done before filter check.
            if (
                gmgn_failed
                and _mig_enabled
                and isinstance(dev, DevWalletInfo)
                and dev.recent_tokens
                and rpc
            ):
                _mig_mints = [t.mint for t in dev.recent_tokens[:5]]
                mig_task = asyncio.create_task(_check_bc_migrations_native(_mig_mints, rpc))
                logger.info(f"[#{count}] Helius migrations check started ({len(_mig_mints)} mints)")

            # When GMGN was skipped (ATH+mig both OFF) and Stream4 needs mint sources,
            # use dev.recent_tokens. Entries have no ATH/migration data — safe because
            # those filter checks are disabled when skip_gmgn=True.
            if skip_gmgn and _need_sigs and not histories and isinstance(dev, DevWalletInfo) and dev.recent_tokens:
                histories = [TokenHistory(mint=t.mint, name=t.name) for t in dev.recent_tokens[:5]]
                logger.debug(f"[#{count}] TX count: using dev.recent_tokens ({len(histories)} mints, GMGN skipped)")

            # Stream 4: BC signatures — sequential after GMGN, own 1.5 s timeout
            if _need_sigs and histories and rpc:
                t_sig = time.perf_counter()
                try:
                    sig_data = await asyncio.wait_for(
                        _fetch_token_signatures_batch(histories, rpc),
                        timeout=1.5,
                    )
                except TimeoutError:
                    logger.warning(f"[#{count}] Stream4 (sigs) timed out after 1.5s")
                logger.info(
                    f"[#{count}] Stream4 (sigs x{len(sig_data)}): "
                    f"{(time.perf_counter() - t_sig) * 1000:.0f}ms"
                )

        except TimeoutError:
            logger.warning(f"[#{count}] Phase1 timeout (4s) — sending with dashes")
            task_bc_buy.cancel()
            task_mint.cancel()
            task_helius_fee.cancel()
        except Exception as e:
            logger.error(f"[#{count}] Phase1 fetch error: {e}")
            task_bc_buy.cancel()
            task_mint.cancel()
            task_helius_fee.cancel()

        # Bug #3A: Collect Helius fee (usually done by now; was fired in parallel with Phase 1)
        _helius_fee_ul: int | None = None
        if not task_helius_fee.cancelled():
            try:
                _helius_fee_ul = await asyncio.wait_for(task_helius_fee, timeout=0.1)
            except (TimeoutError, asyncio.TimeoutError):
                logger.debug(f"[#{count}] Helius fee still pending — will use cap")
            except Exception as exc:
                logger.debug(f"[#{count}] Helius fee collection error: {exc}")

        logger.info(f"[#{count}] Phase1+sigs: {(time.perf_counter()-t_total)*1000:.0f}ms")

        # Read live bot_config for per-token decisions
        bot_cfg = _read_bot_config()
        active_preset_id = str((bot_cfg or {}).get("active_preset", 1))
        preset = ((bot_cfg or {}).get("presets") or {}).get(active_preset_id, {})
        filt = preset.get("filters", {})

        # -------------------------------------------------------------------
        # DATA QUALITY GUARDS — must run before any filter check.
        # A ✅ is only sent when ALL data is real. Any missing source → reject.
        # -------------------------------------------------------------------
        if dev.timed_out:
            logger.info(f"[#{count}] Data fetch timed out — rejecting")
            if not _any_position_open:
                await tg.send_message(_reject("data fetch timeout"))
            return

        # ATH filter requires GMGN — no Helius fallback exists for historical ATH data.
        # Migrations filter can fall back to Helius BC complete-flag check (mig_task).
        # tx_count / lifetime use on-chain sigs (stream 4), not GMGN histories.
        if gmgn_failed and _ath_enabled and float(filt.get("min_ath_last5", 0)) > 0:
            logger.info(f"[#{count}] GMGN failed + ATH filter active — rejecting (no Helius fallback for ATH)")
            if not _any_position_open:
                await tg.send_message(_reject("GMGN data unavailable"))
            return
        if gmgn_failed:
            _fallback_info = "using Helius for migrations" if mig_task else "no ath/mig filters need it"
            logger.info(f"[#{count}] GMGN unavailable — {_fallback_info}, continuing")

        if not skip_gmgn and not gmgn_failed and not histories:
            # Only reject on empty history when a filter with a non-zero threshold needs GMGN data.
            # Lifetime and TX count use Stream4 — not GMGN — so they're excluded here.
            _gmgn_data_required = (
                (bool(filt.get("ath_enabled", True)) and float(filt.get("min_ath_last5", 0)) > 0)
                or (bool(filt.get("migrations_enabled", True)) and int(filt.get("min_migrations_last5", 0)) > 0)
            )
            if _gmgn_data_required:
                logger.info(f"[#{count}] New wallet — no previous token history (required for active filters)")
                if not _any_position_open:
                    await tg.send_message(_reject("New wallet — no previous tokens"))
                return
            else:
                logger.info(f"[#{count}] GMGN: new wallet, no history — no filter requires it, continuing")

        dev_data_missing = (
            dev.sol_balance is None
            and dev.wallet_age_str is None
            and dev.total_launches is None
        )
        if dev_data_missing or dev.error:
            logger.info(f"[#{count}] Dev wallet data missing — rejecting")
            if not _any_position_open:
                await tg.send_message(_reject("dev data unavailable"))
            return

        # -------------------------------------------------------------------
        # FILTER: New Wallet (new_wallet_enabled)
        # is_new_wallet = histories is empty AND dev.total_launches == 0
        # OFF → silent discard (debug log only, zero extra RPC calls)
        # ON  → new wallets pass through and continue to remaining filters
        # -------------------------------------------------------------------
        is_new_wallet = (not histories) and (dev.total_launches == 0)
        if is_new_wallet and not bool(filt.get("new_wallet_enabled", True)):
            logger.info(f"[#{count}] Silent skip: new wallet, toggle OFF — {mint_str[:8]}")
            return

        min_ath = float(filt.get("min_ath_last5", 0))
        min_mig = int(filt.get("min_migrations_last5", 0))

        # -------------------------------------------------------------------
        # FILTER 3: ATH of last 5 tokens
        # ath_require_all=False → at least one token must meet the threshold
        # ath_require_all=True  → every token must meet the threshold
        # -------------------------------------------------------------------
        if bool(filt.get("ath_enabled", True)) and min_ath > 0 and histories:
            ath_require_all = bool(filt.get("ath_require_all", False))
            if ath_require_all:
                failing = [h for h in histories if (h.ath_market_cap or 0.0) < min_ath]
                if failing:
                    logger.info(
                        f"[#{count}] Filter: {len(failing)}/{len(histories)} tokens below "
                        f"ATH {min_ath:.0f} → skip"
                    )
                    if not _any_position_open:
                        await tg.send_message(_reject(
                            f"ATH: {len(failing)} of {len(histories)} tokens below {_format_mc(min_ath)}"
                        ))
                    return
            else:
                best_ath = max((h.ath_market_cap or 0.0) for h in histories)
                if best_ath < min_ath:
                    logger.info(
                        f"[#{count}] Filter: best ATH {best_ath:.0f} < {min_ath:.0f} → skip"
                    )
                    if not _any_position_open:
                        await tg.send_message(_reject(
                            f"ATH {_format_mc(best_ath)} best of 5 (min {_format_mc(min_ath)})"
                        ))
                    return

        # Collect Helius-native migrations result (fired after Phase 1 using dev.recent_tokens).
        # Task has been running concurrently with Stream 4 — usually already done here.
        if mig_task is not None and not mig_task.cancelled():
            try:
                helius_mig_count = (
                    mig_task.result() if mig_task.done()
                    else await asyncio.wait_for(mig_task, timeout=1.0)
                )
                logger.info(f"[#{count}] Helius migrations: {helius_mig_count}/5")
            except (TimeoutError, asyncio.TimeoutError):
                logger.warning(f"[#{count}] Helius migrations check timed out after 1s")
            except Exception as exc:
                logger.warning(f"[#{count}] Helius migrations check error: {exc}")

        # -------------------------------------------------------------------
        # FILTER 4: Migrations count
        # Priority: GMGN histories → Helius native → skip (no data → pass)
        # -------------------------------------------------------------------
        if bool(filt.get("migrations_enabled", True)) and min_mig > 0:
            if histories:
                mig_count = sum(1 for h in histories if h.migrated is True)
                mig_src = f"{len(histories)} GMGN tokens"
            elif helius_mig_count is not None:
                mig_count = helius_mig_count
                mig_src = "Helius BC check"
            else:
                mig_count = min_mig  # no data → treat as passing (don't reject on missing data)
                mig_src = "no data"
            if mig_count < min_mig:
                logger.info(
                    f"[#{count}] Filter: migrations {mig_count} < {min_mig} ({mig_src}) → skip"
                )
                if not _any_position_open:
                    await tg.send_message(_reject(
                        f"Migrations {mig_count}/5 (min {min_mig})"
                    ))
                return

        # -------------------------------------------------------------------
        # FILTER 5: TX count range for dev's last 5 tokens
        # min_tx_count=0 → no lower bound; max_tx_count=0 → no upper bound
        # -------------------------------------------------------------------
        min_tx = int(filt.get("min_tx_count", 0))
        max_tx = int(filt.get("max_tx_count", 0))
        if not bool(filt.get("tx_count_enabled", True)):
            logger.debug(f"[#{count}] TX COUNT FILTER: SKIPPED (toggle OFF)")
        if bool(filt.get("tx_count_enabled", True)) and (min_tx > 0 or max_tx > 0) and sig_data and histories:
            tx_require_all = bool(filt.get("tx_count_require_all", False))

            def _tx_range_str() -> str:
                if min_tx > 0 and max_tx > 0:
                    return f"{min_tx}–{max_tx}"
                return f">={min_tx}" if min_tx > 0 else f"<={max_tx}"

            def _tx_ok(h: TokenHistory) -> bool:
                cnt = sig_data.get(h.mint, (0, None))[0]
                if min_tx > 0 and cnt < min_tx:
                    return False
                if max_tx > 0 and cnt > max_tx:
                    return False
                return True

            if tx_require_all:
                failing_tx = [h for h in histories if not _tx_ok(h)]
                if failing_tx:
                    h0 = failing_tx[0]
                    cnt0 = sig_data.get(h0.mint, (0, None))[0]
                    logger.info(
                        f"[#{count}] Filter: tx count {cnt0} out of range "
                        f"{_tx_range_str()} for {h0.name} → skip"
                    )
                    if not _any_position_open:
                        await tg.send_message(_reject(
                            f"TX count {cnt0} (range {_tx_range_str()}) for {h0.name}"
                        ))
                    return
            else:
                if not any(_tx_ok(h) for h in histories):
                    counts = [sig_data.get(h.mint, (0, None))[0] for h in histories]
                    logger.info(
                        f"[#{count}] Filter: no token in tx range "
                        f"{_tx_range_str()}, counts={counts} → skip"
                    )
                    if not _any_position_open:
                        await tg.send_message(_reject(
                            f"TX count {counts} (range {_tx_range_str()}), none passed"
                        ))
                    return

        # -------------------------------------------------------------------
        # FILTER 6: Token lifetime in minutes for dev's last 5 tokens
        # -------------------------------------------------------------------
        min_lifetime = float(filt.get("min_lifetime_minutes", 0))
        if not bool(filt.get("lifetime_enabled", True)):
            logger.debug(f"[#{count}] LIFETIME FILTER: SKIPPED (toggle OFF)")
        if bool(filt.get("lifetime_enabled", True)) and min_lifetime > 0 and sig_data and histories:
            lifetime_require_all = bool(filt.get("lifetime_require_all", False))

            def _lifetime_minutes(h: TokenHistory) -> float | None:
                last_bt = sig_data.get(h.mint, (0, None))[1]
                if last_bt is None or h.create_timestamp is None:
                    return None
                return (last_bt - h.create_timestamp) / 60.0

            def _lifetime_ok(h: TokenHistory) -> bool:
                lt = _lifetime_minutes(h)
                return lt is None or lt >= min_lifetime  # None = no data → don't reject

            if lifetime_require_all:
                failing_lt = [h for h in histories if not _lifetime_ok(h)]
                if failing_lt:
                    h0 = failing_lt[0]
                    lt0 = _lifetime_minutes(h0)
                    lt_str = f"{lt0:.1f}" if lt0 is not None else "?"
                    logger.info(
                        f"[#{count}] Filter: lifetime {lt_str}min < "
                        f"{min_lifetime:.1f}min for {h0.name} → skip"
                    )
                    if not _any_position_open:
                        await tg.send_message(_reject(
                            f"Token lifetime {lt_str} min (min {min_lifetime:.1f} min) for {h0.name}"
                        ))
                    return
            else:
                if not any(_lifetime_ok(h) for h in histories):
                    lt_strs = [
                        f"{_lifetime_minutes(h):.1f}" if _lifetime_minutes(h) is not None else "?"
                        for h in histories
                    ]
                    logger.info(
                        f"[#{count}] Filter: no token with lifetime >= "
                        f"{min_lifetime:.1f}min, values={lt_strs} → skip"
                    )
                    if not _any_position_open:
                        await tg.send_message(_reject(
                            f"Token lifetimes {lt_strs} min (min {min_lifetime:.1f} min), none passed"
                        ))
                    return

        # -------------------------------------------------------------------
        # Phase 2: collect dev buy result (task fired at token detection).
        # By the time we reach here, GMGN + filters have taken ~1.3 s, so
        # the dev buy RPC call is usually already complete — await is instant.
        # -------------------------------------------------------------------
        if phase1_ok and not task_bc_buy.cancelled():
            t_bc = time.perf_counter()
            try:
                bc_result = await asyncio.wait_for(task_bc_buy, timeout=0.5)
                if isinstance(bc_result, (int, float)):
                    dev_buy_sol = bc_result
                elif bc_result is None and bc_str:
                    logger.debug(f"[#{count}] DEV BUY returned None for {bc_str[:8]}")
            except TimeoutError:
                logger.warning(f"[#{count}] Dev buy still pending after filters — treating as None")
            except Exception as exc:
                logger.warning(f"[#{count}] DEV BUY fetch failed for {(bc_str or '')[:8]}: {exc}")
            buy_str = f"{dev_buy_sol:.3f} SOL" if dev_buy_sol is not None else "—"
            logger.info(
                f"[#{count}] Phase2 (dev buy={buy_str}): "
                f"{(time.perf_counter()-t_bc)*1000:.0f}ms"
            )

        # Collect mint/freeze — informational, non-blocking
        if not task_mint.cancelled():
            if task_mint.done():
                try:
                    mint_r = task_mint.result()
                    if isinstance(mint_r, tuple):
                        has_mint, has_freeze = mint_r
                        if has_mint or has_freeze:
                            logger.info(f"[#{count}] Mint auth={has_mint}, Freeze auth={has_freeze}")
                except Exception:
                    pass
            else:
                task_mint.cancel()

        logger.info(f"[#{count}] Total: {(time.perf_counter()-t_total)*1000:.0f}ms | dev_buy={dev_buy_sol}")

        # -------------------------------------------------------------------
        # FILTER 2 (last): Dev buy amount
        # dev_buy_check_enabled=False → dev buy shown in alert, filter skipped
        # -------------------------------------------------------------------
        if filt.get("dev_buy_check_enabled", True):
            min_dev_buy = float(filt.get("min_dev_buy_sol", 0.1))
            if dev_buy_sol is not None and dev_buy_sol < min_dev_buy:
                logger.info(
                    f"[#{count}] Filter: dev bought {dev_buy_sol:.3f} SOL < {min_dev_buy} → skip"
                )
                if not _any_position_open:
                    await tg.send_message(_reject(
                        f"Dev buy {dev_buy_sol:.3f} SOL (min {min_dev_buy:.3f} SOL)"
                    ))
                return

        # -------------------------------------------------------------------
        # ALL FILTERS PASSED
        # -------------------------------------------------------------------
        t_filters_passed = time.perf_counter()
        logger.info(
            f"[#{count}] [TIMING] mint→filters: {(t_filters_passed - t_total) * 1000:.0f}ms"
        )
        asyncio.create_task(_update_bot_config({
            "stats": {"tokens_passed_filters": (
                ((bot_cfg or {}).get("stats") or {}).get("tokens_passed_filters", 0) + 1
            )}
        }))

        # Build the full message with a ✅ header
        full_message = (
            "✅ ПРОШЁЛ ВСЕ ФИЛЬТРЫ\n"
            + format_token_alert(
                token_info, count, dev, dev_buy_sol, histories,
                gmgn_total=gmgn_total_launches,
                gmgn_was_called=not skip_gmgn,
                is_new_wallet=is_new_wallet,
            )
        )

        # Auto-buy block
        buy_block = ""
        effective_auto_buy = (bot_cfg or {}).get("auto_trading", auto_buy_enabled)

        if buyer is None:
            pass
        elif not effective_auto_buy:
            buy_block = "\n🔴 Автопокупка выключена"
        else:
            max_pos = int((bot_cfg or {}).get("max_concurrent_positions", 1))
            open_pos = int((bot_cfg or {}).get("open_positions", 0))
            if open_pos >= max_pos:
                logger.info(
                    f"[#{count}] At capacity ({open_pos}/{max_pos}) when reaching buy — silent drop"
                )
                return
            else:
                cur_buy_amount = float(preset.get("buy_amount_sol", buy_amount_sol))
                cur_slippage = float(preset.get("buy_slippage", buy_slippage * 100)) / 100
                fee_mode = str(preset.get("fee_mode", "auto")).lower()
                if fee_mode == "superfast":
                    cur_priority_ul = SUPERFAST_PRIORITY_UL_PER_CU
                    cur_jito_tip_ul = SUPERFAST_JITO_LAMPORTS
                    logger.info(
                        f"[#{count}] 🚀 SUPER FAST: priority={cur_priority_ul}µL/CU "
                        f"jito={cur_jito_tip_ul} lamports total≈0.010855 SOL"
                    )
                elif fee_mode == "ultra":
                    cur_priority_ul = ULTRA_PRIORITY_UL_PER_CU
                    cur_jito_tip_ul = ULTRA_JITO_LAMPORTS
                    logger.info(
                        f"[#{count}] ⚡ ULTRA: priority={cur_priority_ul}µL/CU "
                        f"jito={cur_jito_tip_ul} lamports total≈0.029255 SOL"
                    )
                else:  # "auto" — Helius dynamic + cap
                    cur_max_priority_sol = float(
                        preset.get("max_priority_fee_sol")
                        or preset.get("priority_fee_sol")
                        or priority_fee_sol
                    )
                    cap_ul = int(cur_max_priority_sol * 1_000_000_000)
                    if _helius_fee_ul is not None:
                        final_fee_ul = min(_helius_fee_ul, cap_ul)
                        logger.info(
                            f"[#{count}] Priority fee: {final_fee_ul} µL/CU "
                            f"(helius={_helius_fee_ul}, cap={cap_ul})"
                        )
                    else:
                        final_fee_ul = cap_ul
                        logger.info(
                            f"[#{count}] Priority fee: {final_fee_ul} µL/CU (helius=N/A, cap={cap_ul})"
                        )
                    cur_priority_ul = final_fee_ul
                    cur_jito_tip_sol = float(preset.get("jito_tip_sol", 0.003))
                    cur_jito_tip_ul = int(cur_jito_tip_sol * 1_000_000_000) if cur_jito_tip_sol > 0 else None
                cur_priority_sol = cur_priority_ul / 1_000_000_000

                from core.priority_fee.manager import PriorityFeeManager as _PFM
                fresh_pf = _PFM(
                    client=buyer_client,
                    enable_dynamic_fee=False,
                    enable_fixed_fee=True,
                    fixed_fee=cur_priority_ul,
                    extra_fee=0.0,
                    hard_cap=cur_priority_ul,
                )
                from trading.platform_aware import PlatformAwareBuyer as _PAB
                fresh_buyer = _PAB(
                    client=buyer_client,
                    wallet=wallet,
                    priority_fee_manager=fresh_pf,
                    amount=cur_buy_amount,
                    slippage=cur_slippage,
                    max_retries=int(preset.get("max_retries", 1)),
                    jito_tip_lamports=cur_jito_tip_ul,
                    extreme_fast_mode=True,
                )

                # Atomic gate: claim position slot before any awaits
                async with _position_entry_lock:
                    _gate_cfg = _read_bot_config() or {}
                    _gate_open = int(_gate_cfg.get("open_positions", 0))
                    _gate_max = int(_gate_cfg.get("max_concurrent_positions", 1))
                    if _gate_open >= _gate_max:
                        logger.info(
                            f"[#{count}] [GATE] Race: position limit {_gate_open}/{_gate_max} already consumed, skip"
                        )
                        return
                    if _stop_scanning:
                        return
                    await _update_bot_config({"open_positions": _gate_open + 1})
                    open_pos = _gate_open  # snapshot before increment, for failure path decrement

                # Bug #5: skip Entry MC check when entry_mc_enabled=False
                _entry_mc_enabled = bool(filt.get("entry_mc_enabled", True))
                if _entry_mc_enabled:
                    mc_passes, mc_skip_msg = await _check_entry_mc(
                        filt, bc_str, rpc, f"#{count}"
                    )
                else:
                    mc_passes, mc_skip_msg = True, ""
                if not mc_passes:
                    await _update_bot_config({"open_positions": max(0, open_pos)})
                    buy_block = mc_skip_msg
                else:
                    t_send = time.perf_counter()
                    logger.info(
                        f"[#{count}] [TIMING] filters→send: {(t_send - t_filters_passed) * 1000:.0f}ms"
                    )
                    logger.info(f"[#{count}] Buying token {str(token_info.mint)[:8]}...")
                    try:
                        buy_result = await fresh_buyer.execute(token_info)
                        t_confirm = time.perf_counter()
                        if buy_result.success:
                            logger.info(
                                f"[#{count}] [TIMING] send→confirm: {(t_confirm - t_send) * 1000:.0f}ms"
                                f" | mint→confirm: {(t_confirm - t_total) * 1000:.0f}ms"
                            )
                            logger.info(f"[#{count}] Buy success: {buy_result.tx_signature}")
                            await _update_bot_config({
                                "stats": {
                                    "buys_executed": (
                                        ((bot_cfg or {}).get("stats") or {}).get("buys_executed", 0) + 1
                                    )
                                }
                            })

                            # N-trades mode: increment counter and check limit
                            successful_buys_this_session += 1
                            if bot_mode == "n" and successful_buys_this_session >= max_trades_count:
                                _stop_scanning = True
                                asyncio.create_task(tg.send_message(
                                    f"✅ <b>Лимит торгов достигнут!</b>\n"
                                    f"🎯 Выполнено {successful_buys_this_session}/{max_trades_count} покупок.\n"
                                    f"🔴 Новые токены больше не обрабатываются."
                                ))

                            # Build rich BUY alert
                            entry_p = buy_result.price or 0.0
                            entry_tokens = buy_result.amount or 0.0
                            entry_sol_spent = entry_p * entry_tokens
                            gas_fee_sol = float(preset.get("gas_fee_sol", 0.00005))
                            total_fees_sol = cur_priority_sol + cur_jito_tip_sol + gas_fee_sol
                            total_out_sol = entry_sol_spent + total_fees_sol
                            sol_usd = _sol_price_usd

                            mc_at_entry = entry_p * 1_000_000_000 * sol_usd if sol_usd > 0 and entry_p > 0 else None
                            tx_sig_str = str(buy_result.tx_signature) if buy_result.tx_signature else None
                            tx_url = f"https://solscan.io/tx/{tx_sig_str}" if tx_sig_str else ""

                            exit_lines: list[str] = []
                            for i, tp in enumerate(preset.get("take_profits", []), 1):
                                exit_lines.append(
                                    f"  TP{i}: +{tp['price_pct']:.0f}% → {tp['position_pct']:.0f}% позиции"
                                )
                            for i, sl in enumerate(preset.get("stop_losses", []), 1):
                                exit_lines.append(
                                    f"  SL{i}: -{sl['price_pct']:.0f}% → {sl['position_pct']:.0f}% позиции"
                                )
                            ts_list = list(preset.get("trailing_stops") or [])
                            if not ts_list and preset.get("trailing_stop", {}).get("enabled"):
                                ts_list = [preset["trailing_stop"]]
                            for i, ts in enumerate(ts_list, 1):
                                if ts.get("enabled", True):
                                    exit_lines.append(
                                        f"  Trail{i}: act +{ts['activation_pct']:.0f}% | trail -{ts['trail_size_pct']:.0f}% | {ts['position_pct']:.0f}%"
                                    )

                            buy_lines = [
                                "\n✅ <b>КУПЛЕНО</b>",
                                f"💰 Вложено: {entry_sol_spent:.6f} SOL" + (f" (${entry_sol_spent * sol_usd:.2f})" if sol_usd > 0 else "") + f" → {entry_tokens:.4f} токенов",
                                f"📊 MC входа: {_format_mc(mc_at_entry)}",
                            ]
                            buy_lines += [
                                f"💸 Комиссии входа: priority {cur_priority_sol:.5f} + jito {cur_jito_tip_sol:.5f} + gas {gas_fee_sol:.5f} = {total_fees_sol:.5f} SOL" + (f" (${total_fees_sol * sol_usd:.3f})" if sol_usd > 0 else ""),
                                f"💼 Итого: {total_out_sol:.6f} SOL" + (f" (${total_out_sol * sol_usd:.2f})" if sol_usd > 0 else ""),
                                "⚠️ Gap risk: цена обновляется только при торгах. Резервный опрос каждые 2с.",
                            ]
                            if exit_lines:
                                buy_lines.append("📈 Стратегия выхода:")
                                buy_lines.extend(exit_lines)
                            if tx_url:
                                buy_lines.append(f"🔗 <a href='{tx_url}'>Solscan TX</a>")
                            buy_block = "\n".join(buy_lines)

                            if (
                                buy_result.amount is not None
                                and buy_result.price is not None
                                and buyer_client is not None
                            ):
                                asyncio.create_task(
                                    monitor_position(
                                        token_info=token_info,
                                        token_amount=buy_result.amount,
                                        entry_price=buy_result.price,
                                        client=buyer_client,
                                        wallet=wallet,
                                        read_config=_read_bot_config,
                                        update_config=_update_bot_config,
                                        preset_id=active_preset_id,
                                        priority_fee_microlamports=cur_priority_ul,
                                        notify_fn=tg.send_message,
                                        entry_time=time.monotonic(),
                                        sol_price_getter=lambda: _sol_price_usd,
                                        position_close_fn=None,
                                        buy_commission_sol=total_fees_sol,
                                        entry_mc_usd=mc_at_entry or 0.0,
                                    )
                                )
                            else:
                                # Buy returned success=True but no amount/price — release slot
                                await _update_bot_config({"open_positions": max(0, open_pos)})
                        else:
                            err = _escape_html(buy_result.error_message or "unknown error")
                            buy_block = f"\n\n❌ Покупка не удалась: {err}"
                            logger.warning(f"[#{count}] Buy failed: {buy_result.error_message}")
                            await _update_bot_config({"open_positions": max(0, open_pos)})
                    except Exception as e:
                        buy_block = f"\n\n❌ Покупка не удалась: {_escape_html(str(e))}"
                        logger.error(f"[#{count}] Buy exception: {e}")
                        await _update_bot_config({"open_positions": max(0, open_pos)})

        full_message += buy_block
        success = await tg.send_message(full_message)

        if not success:
            logger.warning(f"[#{count}] Failed to send Telegram notification")

        # -------------------------------------------------------------------
        # TEST MODE: paper-trade simulation (independent of auto_trading)
        # Shares successful_buys_this_session with real buys for N-trades limit.
        # Participates in open_positions counter (same capacity rules as live mode).
        # -------------------------------------------------------------------
        test_mode_enabled = bool((bot_cfg or {}).get("test_mode", False))
        if test_mode_enabled and bc_str and rpc:
            # Atomic gate: claim position slot before price fetch
            async with _position_entry_lock:
                _tgate_cfg = _read_bot_config() or {}
                _tgate_open = int(_tgate_cfg.get("open_positions", 0))
                _tgate_max = int(_tgate_cfg.get("max_concurrent_positions", 1))
                if _tgate_open >= _tgate_max:
                    logger.info(
                        f"[#{count}] [GATE] Race: test position limit {_tgate_open}/{_tgate_max} already consumed, skip"
                    )
                    return
                if _stop_scanning:
                    return
                await _update_bot_config({"open_positions": _tgate_open + 1})
                logger.info(f"[POSITION] open_positions → {_tgate_open + 1} for {mint_str} (TEST) [GATE]")

            logger.info(
                f"[#{count}] [TEST MC DEBUG] virtual_sol_reserves from event: {token_info.virtual_sol_reserves}"
            )
            cur_price_test = await _get_current_token_price_sol(bc_str, rpc)
            logger.info(f"[#{count}] [TEST MC DEBUG] cur_price_test (RPC fetch result): {cur_price_test}")
            if cur_price_test is None or cur_price_test <= 0:
                _cfg_revert = _read_bot_config() or {}
                await _update_bot_config({"open_positions": max(0, int(_cfg_revert.get("open_positions", 1)) - 1)})
                logger.warning(
                    f"[#{count}] Test mode: could not fetch entry price for {mint_str[:8]}"
                )
                return
            if True:
                # TEST MODE ONLY — slippage simulation. Live path is unchanged below.
                simulated_entry_price = cur_price_test * 1.15
                sol_usd_test = _sol_price_usd
                simulated_entry_mc = (
                    simulated_entry_price * _PUMP_FUN_TOTAL_SUPPLY * sol_usd_test
                    if sol_usd_test > 0 else 0.0
                )
                logger.info(f"[#{count}] [TEST MC DEBUG] simulated_entry_mc: ${simulated_entry_mc:.0f}")

                # MC range check — same filter as live mode; skip when entry_mc_enabled=False
                _mc_min_t = float(filt.get("min_entry_mc_usd", 0)) if bool(filt.get("entry_mc_enabled", True)) else 0
                _mc_max_t = float(filt.get("max_entry_mc_usd", 0)) if bool(filt.get("entry_mc_enabled", True)) else 0
                if _mc_min_t > 0 and simulated_entry_mc < _mc_min_t:
                    _cfg_revert = _read_bot_config() or {}
                    await _update_bot_config({"open_positions": max(0, int(_cfg_revert.get("open_positions", 1)) - 1)})
                    logger.info(
                        f"[#{count}] [TEST] MC entry ${simulated_entry_mc:.0f} < min ${_mc_min_t:.0f} → skip"
                    )
                    asyncio.create_task(tg.send_message(
                        f"🧪 <b>TEST: симуляция отменена</b>\n"
                        f"⛔ MC входа {_format_mc(simulated_entry_mc)} ниже минимума {_format_mc(_mc_min_t)}"
                    ))
                    return
                if _mc_max_t > 0 and simulated_entry_mc > _mc_max_t:
                    _cfg_revert = _read_bot_config() or {}
                    await _update_bot_config({"open_positions": max(0, int(_cfg_revert.get("open_positions", 1)) - 1)})
                    logger.info(
                        f"[#{count}] [TEST] MC entry ${simulated_entry_mc:.0f} > max ${_mc_max_t:.0f} → skip"
                    )
                    asyncio.create_task(tg.send_message(
                        f"🧪 <b>TEST: симуляция отменена</b>\n"
                        f"⛔ MC входа {_format_mc(simulated_entry_mc)} выше максимума {_format_mc(_mc_max_t)}"
                    ))
                    return

                test_buy_amount = float(preset.get("buy_amount_sol", 0.01))
                sim_tokens = test_buy_amount / simulated_entry_price
                _test_fee_mode = str(preset.get("fee_mode", "auto")).lower()
                if _test_fee_mode == "superfast":
                    test_priority = SUPERFAST_PRIORITY_UL_PER_CU / 1_000_000_000
                    test_jito = SUPERFAST_JITO_LAMPORTS / 1_000_000_000
                elif _test_fee_mode == "ultra":
                    test_priority = ULTRA_PRIORITY_UL_PER_CU / 1_000_000_000
                    test_jito = ULTRA_JITO_LAMPORTS / 1_000_000_000
                else:
                    test_priority = float(
                        preset.get("max_priority_fee_sol")
                        or preset.get("priority_fee_sol", 0.001)
                    )
                    test_jito = float(preset.get("jito_tip_sol", 0.003))
                test_gas = float(preset.get("gas_fee_sol", 0.00005))
                test_fees = test_priority + test_jito + test_gas
                test_total = test_buy_amount + test_fees

                test_exit_tp: list[str] = []
                for _i, _tp in enumerate(preset.get("take_profits", []), 1):
                    test_exit_tp.append(f"  TP{_i}: +{_tp['price_pct']:.0f}% → {_tp['position_pct']:.0f}% позиции")
                test_exit_sl: list[str] = []
                for _i, _sl in enumerate(preset.get("stop_losses", []), 1):
                    test_exit_sl.append(f"  SL{_i}: -{_sl['price_pct']:.0f}% → {_sl['position_pct']:.0f}% позиции")
                _ts_test = list(preset.get("trailing_stops") or [])
                if not _ts_test and preset.get("trailing_stop", {}).get("enabled"):
                    _ts_test = [preset["trailing_stop"]]
                test_exit_trail: list[str] = []
                for _i, _ts in enumerate(_ts_test, 1):
                    if _ts.get("enabled", True):
                        test_exit_trail.append(
                            f"  Trail{_i}: act +{_ts['activation_pct']:.0f}% | trail -{_ts['trail_size_pct']:.0f}% | {_ts['position_pct']:.0f}%"
                        )

                test_buy_lines = [
                    "🧪 <b>TEST MODE — СИМУЛИРОВАННАЯ ПОКУПКА</b>",
                    f"🔥 {_escape_html(token_info.name)} (${_escape_html(token_info.symbol)})",
                    f"📍 MC входа (с учётом слиппеджа 15%): {_format_mc(simulated_entry_mc)}",
                ]
                test_buy_lines += [
                    f"💰 Вложено (симуляция): {test_buy_amount:.6f} SOL → {sim_tokens:.4f} токенов",
                    f"💸 Комиссии входа (симуляция): priority {test_priority:.5f} + jito {test_jito:.5f} + gas {test_gas:.5f} = {test_fees:.5f} SOL",
                    f"💼 Итого (симуляция): {test_total:.6f} SOL",
                    "⚠️ Gap risk: цена обновляется только при торгах. Резервный опрос каждые 2с.",
                    "📈 Стратегия выхода:",
                ]
                if test_exit_tp:
                    test_buy_lines.extend(test_exit_tp)
                else:
                    test_buy_lines.append("  Take Profit: не настроено")
                if test_exit_sl:
                    test_buy_lines.extend(test_exit_sl)
                else:
                    test_buy_lines.append("  Stop Loss: не настроено")
                if test_exit_trail:
                    test_buy_lines.extend(test_exit_trail)
                else:
                    test_buy_lines.append("  Trailing Stop: не настроено")

                asyncio.create_task(tg.send_message("\n".join(test_buy_lines)))

                sim_pos = SimulatedPosition(
                    mint=mint_str,
                    symbol=token_info.symbol,
                    name=token_info.name,
                    entry_price_sol=simulated_entry_price,
                    entry_mc_usd=simulated_entry_mc,
                    simulated_token_amount=sim_tokens,
                    entry_sol=test_buy_amount,
                    priority_fee_sol=test_priority,
                    jito_tip_sol=test_jito,
                    gas_fee_sol=test_gas,
                    total_cost_sol=test_total,
                    entry_timestamp=time.time(),
                    preset_snapshot=dict(preset),
                    platform=token_info.platform,
                    bonding_curve=bc_str,
                    is_cashback_coin=bool(getattr(token_info, "is_cashback_coin", False)),
                    sol_price_at_entry=sol_usd_test,
                    active_preset_id=active_preset_id,
                )

                await _update_bot_config({
                    "stats": {
                        "test_buys_executed": (
                            ((bot_cfg or {}).get("stats") or {}).get("test_buys_executed", 0) + 1
                        )
                    }
                })

                # N-trades mode: test buys count toward the same limit as real buys
                successful_buys_this_session += 1
                if bot_mode == "n" and successful_buys_this_session >= max_trades_count:
                    _stop_scanning = True
                    asyncio.create_task(tg.send_message(
                        f"✅ <b>Сессия завершена.</b>\n"
                        f"🎯 Выполнено {successful_buys_this_session}/{max_trades_count} сделок.\n"
                        f"🔴 Бот приостановлен. Нажми Stop → Start для новой сессии."
                    ))

                def _on_test_close() -> None:
                    async def _decrement() -> None:
                        _c = _read_bot_config() or {}
                        await _update_bot_config(
                            {"open_positions": max(0, int(_c.get("open_positions", 1)) - 1)}
                        )
                        logger.info("[POSITION] test position closed, open_positions decremented")
                    asyncio.get_running_loop().create_task(_decrement())

                asyncio.create_task(
                    monitor_position_test(
                        sim_pos=sim_pos,
                        rpc_endpoint=rpc,
                        notify_fn=tg.send_message,
                        sol_price_getter=lambda: _sol_price_usd,
                        position_close_fn=_on_test_close,
                    )
                )
                logger.info(
                    f"[#{count}] Test mode: simulated buy {test_buy_amount} SOL @ "
                    f"{cur_price_test:.8f} → {sim_tokens:.4f} tokens"
                )

    logger.info(
        "Слушаю новые токены...\n"
        "Нажми Ctrl+C для остановки."
    )

    try:
        await listener.listen_for_tokens(
            token_callback=on_new_token,
            match_string=match_string,
            creator_address=creator_address,
        )
    except asyncio.CancelledError:
        logger.info("Сканер остановлен (CancelledError)")
    except Exception as e:
        error_msg = f"Сканер остановлен из-за ошибки: {e}"
        logger.exception(error_msg)
        try:
            crash_cfg = _read_bot_config() or {}
            open_pos = crash_cfg.get("open_positions", 0)
            crash_msg = (
                f"🚨 <b>КРАШ СКАНЕРА</b>\n"
                f"<code>{_escape_html(str(e))}</code>\n"
                f"📊 Открытых позиций: {open_pos}\n"
                f"🔄 Покупок в сессии: {successful_buys_this_session}"
            )
            await telegram.send_message(crash_msg)
        except Exception:
            pass

    if buyer_client:
        await buyer_client.close()

    logger.info(f"=== Сканер завершил работу. Обнаружено токенов: {token_count} ===")


# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S",
    )

    if len(sys.argv) >= 2:
        config_path = sys.argv[1]
    else:
        config_path = "bots/bot-scanner-telegram.yaml"

    logger.info(f"Запуск с конфигом: {config_path}")

    try:
        asyncio.run(run_scanner(config_path))
    except KeyboardInterrupt:
        logger.info("Остановлено пользователем (Ctrl+C)")


if __name__ == "__main__":
    main()
