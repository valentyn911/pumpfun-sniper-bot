"""
Быстрая on-chain проверка токена сразу после обнаружения.

Что проверяем:
- Mint Authority  — может ли кто-то выпускать новые монеты
- Freeze Authority — может ли кто-то заморозить кошельки

SPL Token mint account layout (82 bytes):
  [0:4]   mintAuthorityOption  — COption<Pubkey>: 0 = None/revoked, 1 = Some
  [4:36]  mintAuthority        — Pubkey (32 bytes)
  [36:44] supply               — u64
  [44]    decimals             — u8
  [45]    isInitialized        — bool
  [46:50] freezeAuthorityOption — COption<Pubkey>: 0 = None/revoked, 1 = Some
  [50:82] freezeAuthority      — Pubkey (32 bytes)
"""

import asyncio
import base64
import struct
import time
from dataclasses import dataclass

import aiohttp

from utils.logger import get_logger

logger = get_logger(__name__)

MINT_ACCOUNT_SIZE = 82
TIMEOUT_SECONDS = 2.0


@dataclass
class MintCheckResult:
    """Результат on-chain проверки минт-аккаунта."""

    mint_authority_revoked: bool
    freeze_authority_revoked: bool
    timed_out: bool = False
    error: str | None = None
    elapsed_ms: float = 0.0

    @property
    def is_clean(self) -> bool:
        return (
            not self.timed_out
            and self.error is None
            and self.mint_authority_revoked
            and self.freeze_authority_revoked
        )

    @property
    def red_flags(self) -> list[str]:
        flags = []
        if self.timed_out:
            flags.append("таймаут проверки")
            return flags
        if self.error:
            flags.append(f"ошибка RPC: {self.error}")
            return flags
        if not self.mint_authority_revoked:
            flags.append("Mint Authority активна")
        if not self.freeze_authority_revoked:
            flags.append("Freeze Authority активна")
        return flags


async def check_mint_authorities(
    mint_address: str,
    rpc_endpoint: str,
) -> MintCheckResult:
    """
    Проверить Mint Authority и Freeze Authority токена через Helius RPC.

    Оба запроса отправляются одновременно (параллельно).
    Таймаут: 2 секунды на всю операцию.

    Args:
        mint_address: Адрес минт-аккаунта токена (строка base58)
        rpc_endpoint: URL Helius RPC эндпоинта

    Returns:
        MintCheckResult с результатами проверки
    """
    t_start = time.perf_counter()

    try:
        result = await asyncio.wait_for(
            _fetch_and_parse_mint(mint_address, rpc_endpoint),
            timeout=TIMEOUT_SECONDS,
        )
        result.elapsed_ms = (time.perf_counter() - t_start) * 1000
        return result

    except TimeoutError:
        elapsed = (time.perf_counter() - t_start) * 1000
        logger.warning(
            f"[checker] Таймаут проверки {mint_address[:8]}... за {elapsed:.0f}мс"
        )
        return MintCheckResult(
            mint_authority_revoked=False,
            freeze_authority_revoked=False,
            timed_out=True,
            elapsed_ms=elapsed,
        )

    except Exception as e:
        elapsed = (time.perf_counter() - t_start) * 1000
        logger.error(f"[checker] Ошибка проверки {mint_address[:8]}...: {e}")
        return MintCheckResult(
            mint_authority_revoked=False,
            freeze_authority_revoked=False,
            error=str(e),
            elapsed_ms=elapsed,
        )


async def _fetch_and_parse_mint(
    mint_address: str,
    rpc_endpoint: str,
) -> MintCheckResult:
    """getAccountInfo → парсинг SPL mint layout. Один retry через 300мс если аккаунт не найден."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [
            mint_address,
            {"encoding": "base64", "commitment": "processed"},
        ],
    }

    timeout = aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for attempt in range(2):
            async with session.post(rpc_endpoint, json=payload) as resp:
                data = await resp.json()

            if "error" in data:
                raise RuntimeError(data["error"].get("message", str(data["error"])))

            value = data.get("result", {}).get("value")
            if value is not None:
                break

            if attempt == 0:
                # Аккаунт ещё не проиндексирован — ждём 300мс и повторяем
                await asyncio.sleep(0.3)
        else:
            raise RuntimeError("аккаунт не найден")

    raw_data = value.get("data")
    if not raw_data or not isinstance(raw_data, list):
        raise RuntimeError("нет данных аккаунта")

    account_bytes = base64.b64decode(raw_data[0])

    if len(account_bytes) < MINT_ACCOUNT_SIZE:
        raise RuntimeError(
            f"неожиданный размер аккаунта: {len(account_bytes)} байт"
        )

    # COption: struct.unpack("<I", ...) → 0 = None/revoked, 1 = Some/active
    mint_authority_option = struct.unpack_from("<I", account_bytes, 0)[0]
    freeze_authority_option = struct.unpack_from("<I", account_bytes, 46)[0]

    return MintCheckResult(
        mint_authority_revoked=(mint_authority_option == 0),
        freeze_authority_revoked=(freeze_authority_option == 0),
    )
