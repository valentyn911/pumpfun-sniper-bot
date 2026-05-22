"""
Проверка дев-кошелька: баланс, возраст, история запусков на pump.fun.

Два запроса к Helius (параллельно с getBalance):
  1. getSignaturesForAddress(limit=1000) — возраст кошелька + счётчик активности,
     без единого getTransaction.
  2. Batch getTransaction × RECENT_TX_LIMIT для первых N сигнатур из п.1 —
     парсим CreateEvent, собираем mint-адреса последних запусков.

Таймаут: 1.5 секунды.
Concurrency guard: _HELIUS_SEM(2) — не более 2 одновременных batch-запросов.
"""

import asyncio
import base64
import struct
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import aiohttp
from solders.pubkey import Pubkey

from utils.logger import get_logger

logger = get_logger(__name__)

TIMEOUT_SECONDS = 1.5
LAMPORTS_PER_SOL = 1_000_000_000
PUMP_FUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
CREATE_EVENT_DISC = bytes.fromhex("1b72a94ddeeb6376")

SIG_LIMIT_FULL = 1000   # для счётчика и возраста
RECENT_TX_LIMIT = 10    # getTransaction для последних N сигнатур

_PUMP_PROGRAM_PUBKEY = Pubkey.from_string(PUMP_FUN_PROGRAM)
_HELIUS_SEM = asyncio.Semaphore(2)


@dataclass
class RecentToken:
    mint: str
    name: str


@dataclass
class DevWalletInfo:
    address: str
    sol_balance: float | None = None
    wallet_age_str: str | None = None
    total_launches: int | None = None
    launches_truncated: bool = False
    recent_tokens: list[RecentToken] = field(default_factory=list)
    elapsed_ms: float = 0.0
    timed_out: bool = False
    error: str | None = None


async def check_dev_wallet(
    wallet_address: str,
    rpc_endpoint: str,
) -> DevWalletInfo:
    t_start = time.perf_counter()
    info = DevWalletInfo(address=wallet_address)

    try:
        info = await asyncio.wait_for(
            _gather_all(wallet_address, rpc_endpoint),
            timeout=TIMEOUT_SECONDS,
        )
    except TimeoutError:
        info.timed_out = True
        logger.warning(f"[dev] Таймаут для {wallet_address[:8]}...")
    except Exception as e:
        info.error = str(e)
        logger.error(f"[dev] Ошибка {wallet_address[:8]}...: {e}")

    info.elapsed_ms = (time.perf_counter() - t_start) * 1000
    return info


async def _gather_all(wallet: str, rpc: str) -> DevWalletInfo:
    http_timeout = aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)

    async with aiohttp.ClientSession(timeout=http_timeout) as session:
        r_balance, r_pumpfun = await asyncio.gather(
            _get_sol_balance(session, wallet, rpc),
            _get_pumpfun_data(session, wallet, rpc),
            return_exceptions=True,
        )

    info = DevWalletInfo(address=wallet)

    if isinstance(r_balance, Exception):
        logger.debug(f"[dev] balance: {r_balance}")
    else:
        info.sol_balance = r_balance

    if isinstance(r_pumpfun, Exception):
        logger.warning(f"[dev] pumpfun data error: {r_pumpfun}")
    else:
        age_str, count, truncated, creates = r_pumpfun
        info.wallet_age_str = age_str
        info.total_launches = count
        info.launches_truncated = truncated
        info.recent_tokens = creates

    return info


async def _get_sol_balance(
    session: aiohttp.ClientSession,
    wallet: str,
    rpc: str,
) -> float:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBalance",
        "params": [wallet, {"commitment": "confirmed"}],
    }
    async with session.post(rpc, json=payload) as resp:
        data = await resp.json()
    return data["result"]["value"] / LAMPORTS_PER_SOL


async def _get_pumpfun_data(
    session: aiohttp.ClientSession,
    wallet: str,
    rpc: str,
) -> tuple[str | None, int, bool, list[RecentToken]]:
    """Запрос 1: getSignaturesForAddress(1000) — возраст + счётчик.
    Запрос 2: batch getTransaction × RECENT_TX_LIMIT — последние создания токенов.

    Returns:
        (wallet_age_str, total_count, truncated, recent_creates)
    """
    # Запрос 1 — только подписи, без getTransaction
    sig_payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSignaturesForAddress",
        "params": [wallet, {"limit": SIG_LIMIT_FULL, "commitment": "confirmed"}],
    }
    async with session.post(rpc, json=sig_payload) as resp:
        sig_data = await resp.json()

    all_sigs = sig_data.get("result", [])

    # Возраст: из самой старой доступной подписи
    age_str: str | None = None
    if all_sigs:
        oldest_time = all_sigs[-1].get("blockTime")
        if oldest_time:
            age_str = _format_age(oldest_time)

    total = len(all_sigs)
    truncated = total == SIG_LIMIT_FULL

    if total == 0:
        return age_str, 0, False, []

    # Запрос 2 — getTransaction только для первых RECENT_TX_LIMIT подписей
    recent_sigs = [
        x["signature"]
        for x in all_sigs[:RECENT_TX_LIMIT]
        if not x.get("err")
    ]

    if not recent_sigs:
        logger.debug(f"[dev] {wallet[:8]}...: все последние {RECENT_TX_LIMIT} tx завершились ошибкой")
        return age_str, total, truncated, []

    batch = [
        {
            "jsonrpc": "2.0",
            "id": i,
            "method": "getTransaction",
            "params": [
                sig,
                {
                    "encoding": "json",
                    "maxSupportedTransactionVersion": 0,
                    "commitment": "confirmed",
                },
            ],
        }
        for i, sig in enumerate(recent_sigs)
    ]

    async with _HELIUS_SEM:
        async with session.post(rpc, json=batch) as resp:
            tx_results = await resp.json()

    logger.debug(
        f"[dev] {wallet[:8]}...: getTransaction × {len(recent_sigs)} "
        f"(из {total} сигнатур)"
    )

    creates: list[RecentToken] = []

    if not isinstance(tx_results, list):
        logger.warning(f"[dev] batch error: {str(tx_results)[:120]}")
        return age_str, total, truncated, []

    for item in tx_results:
        tx = item.get("result")
        if not tx or tx.get("meta", {}).get("err"):
            continue

        logs: list[str] = tx.get("meta", {}).get("logMessages") or []

        if not any(PUMP_FUN_PROGRAM in log for log in logs):
            continue
        if not any(
            "Program log: Instruction: Create" in log
            or "Program log: Instruction: Create_v2" in log
            for log in logs
        ):
            continue
        if any("Instruction: CreateTokenAccount" in log for log in logs):
            continue

        name, mint = _parse_createevent(logs)
        if mint:
            creates.append(RecentToken(mint=mint, name=name or "—"))

    return age_str, total, truncated, creates


def _format_age(oldest_block_time: int) -> str:
    """Форматирует возраст кошелька из blockTime самой старой транзакции."""
    dt = datetime.fromtimestamp(oldest_block_time, tz=timezone.utc)
    days = (datetime.now(tz=timezone.utc) - dt).days

    if days == 0:
        return "менее 1 дня"
    if days < 30:
        return f"{days} дн."
    if days < 365:
        months = days // 30
        rem = days % 30
        return f"{months} мес. {rem} дн." if rem else f"{months} мес."
    years = days // 365
    months = (days % 365) // 30
    return f"{years} г. {months} мес." if months else f"{years} г."


def _parse_createevent(logs: list[str]) -> tuple[str, str]:
    """Extract (name, mint) from CreateEvent binary payload in transaction logs."""
    for log in logs:
        if "Program data:" not in log:
            continue

        encoded = log.split("Program data: ")[1].strip()
        try:
            raw = base64.b64decode(encoded)
        except Exception:
            continue

        if len(raw) < 8 or raw[:8] != CREATE_EVENT_DISC:
            continue

        offset = 8
        try:
            name_len = struct.unpack_from("<I", raw, offset)[0]
            offset += 4
            if offset + name_len > len(raw):
                continue
            name = raw[offset : offset + name_len].decode("utf-8", errors="replace")
            offset += name_len

            sym_len = struct.unpack_from("<I", raw, offset)[0]
            offset += 4 + sym_len

            uri_len = struct.unpack_from("<I", raw, offset)[0]
            offset += 4 + uri_len

            if offset + 32 > len(raw):
                continue
            mint = str(Pubkey.from_bytes(raw[offset : offset + 32]))

            return name, mint

        except Exception:
            continue

    return "", ""
