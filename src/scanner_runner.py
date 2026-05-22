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
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import aiohttp
import yaml
from dotenv import load_dotenv

try:
    import uvloop

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

from core.client import SolanaClient
from core.priority_fee.manager import PriorityFeeManager
from core.wallet import Wallet
from interfaces.core import Platform, TokenInfo
from monitoring.dev_checker import DevWalletInfo, check_dev_wallet
from monitoring.listener_factory import ListenerFactory
from notifications.telegram_reporter import TelegramReporter
from scanner_position_monitor import monitor_position
from trading.platform_aware import PlatformAwareBuyer
from utils.logger import get_logger, setup_file_logging

logger = get_logger(__name__)

# Tokens that are permanently ignored — no Telegram message, no processing.
BLACKLISTED_MINTS: frozenset[str] = frozenset({
    "DjrHJeQSrNQ31GEraBm3xmo3Eer963EiXuX7hpCuHnbm",  # phantom "USDC" garbage token
})

_BOT_CONFIG_PATH = Path(__file__).parent.parent / "bot_config.json"
_config_lock = asyncio.Lock()


def _read_bot_config() -> dict[str, Any] | None:
    if not _BOT_CONFIG_PATH.exists():
        return None
    try:
        with open(_BOT_CONFIG_PATH) as f:
            return json.load(f)
    except Exception:
        return None


async def _update_bot_config(updates: dict[str, Any]) -> None:
    """Merge top-level keys and nested stats into bot_config.json."""
    async with _config_lock:
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
    return f"${mc:,.0f}"


# ---------------------------------------------------------------------------
# On-chain проверки (Поток 1 + BC buy)
# ---------------------------------------------------------------------------

_VIRTUAL_SOL_INITIAL = 30_000_000_000


async def _fetch_bc_dev_buy(bc_address: str, rpc: str) -> float | None:
    """
    Получить virtual_sol_reserves из bonding curve аккаунта.
    Возвращает SOL, потраченный девом при запуске (0.0 если не покупал).
    """
    try:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [bc_address, {"encoding": "base64", "commitment": "confirmed"}],
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
                logger.warning(
                    f"[dev_buy] account not found after 2 attempts: {bc_address[:8]}"
                )
                return None

        raw_data = value.get("data")
        if not raw_data or not isinstance(raw_data, list):
            logger.warning(f"[dev_buy] no data field in account: {bc_address[:8]}")
            return None
        account_bytes = base64.b64decode(raw_data[0])
        if len(account_bytes) < 24:
            logger.warning(
                f"[dev_buy] account data too short ({len(account_bytes)} bytes): {bc_address[:8]}"
            )
            return None

        vsol = struct.unpack_from("<Q", account_bytes, 16)[0]
        return max(0.0, (vsol - _VIRTUAL_SOL_INITIAL) / 1_000_000_000)

    except Exception as e:
        logger.warning(f"[dev_buy] RPC fetch failed for {bc_address[:8]}: {e}")
        return None


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
# GMGN CLI (Поток 3)
# ---------------------------------------------------------------------------


async def _get_gmgn_dev_tokens(dev_wallet: str, current_mint: str = "") -> list[TokenHistory]:
    """Fetch tokens created by dev wallet via gmgn-cli subprocess.

    Raises RuntimeError on timeout or subprocess failure so the caller can
    distinguish a communication error from a dev with no token history.
    Returns [] (empty list) when GMGN succeeded but dev has no prior tokens.
    """
    loop = asyncio.get_event_loop()
    try:
        result = await asyncio.wait_for(
            loop.run_in_executor(
                None,
                lambda: subprocess.run(
                    [
                        "npx", "gmgn-cli", "portfolio", "created-tokens",
                        "--chain", "sol",
                        "--wallet", dev_wallet,
                        "--raw",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=3,
                ),
            ),
            timeout=4.0,
        )
    except (TimeoutError, asyncio.TimeoutError) as e:
        raise RuntimeError("gmgn-cli timeout") from e
    except Exception as e:
        raise RuntimeError(f"gmgn-cli subprocess error: {e}") from e

    if result.returncode != 0:
        raise RuntimeError(
            f"gmgn-cli exit {result.returncode}: {result.stderr[:120]}"
        )

    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"gmgn-cli invalid JSON: {e}") from e

    histories: list[TokenHistory] = []
    raw_tokens = [
        t for t in data.get("tokens", [])
        if t.get("token_address") != current_mint
    ]
    for t in raw_tokens[:5]:
        ath_raw = t.get("token_ath_mc")
        histories.append(TokenHistory(
            mint=t.get("token_address") or "",
            name=t.get("symbol") or "—",
            ath_market_cap=float(ath_raw) if ath_raw else None,
        ))
    return histories


# ---------------------------------------------------------------------------
# Форматирование алерта
# ---------------------------------------------------------------------------


def format_token_alert(
    token_info: TokenInfo,
    count: int,
    dev: DevWalletInfo | None = None,
    dev_buy_sol: float | None = None,
    token_history: list[TokenHistory] | None = None,
) -> str:
    mint_str = str(token_info.mint)
    creator = token_info.creator or token_info.user
    creator_str = str(creator) if creator else "неизвестен"

    if token_info.platform == Platform.PUMP_FUN:
        token_url = f"https://pump.fun/{mint_str}"
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
        launches = (
            f"{dev.total_launches}+" if dev.launches_truncated and dev.total_launches
            else str(dev.total_launches) if dev.total_launches is not None
            else "—"
        )

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
        f"💵 Дев купил при запуске: {dev_buy_str}",
        "📦 Последние токены дева:",
    ]

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

    logger.info("GMGN: используется gmgn-cli (ключ из ~/.config/gmgn/.env)")

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

    async def on_new_token(token_info: TokenInfo) -> None:
        nonlocal token_count

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
        t_total = time.perf_counter()
        creator = token_info.creator or token_info.user
        creator_str = str(creator) if creator else ""
        mint_str = str(token_info.mint)

        if mint_str in BLACKLISTED_MINTS:
            logger.debug(f"[#{count}] Blacklisted mint — silent skip: {mint_str[:8]}")
            return

        bc_str = str(token_info.bonding_curve) if token_info.bonding_curve else None

        if token_info.platform == Platform.PUMP_FUN:
            token_url = f"https://pump.fun/{mint_str}"
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

        # -------------------------------------------------------------------
        # Parallel data fetch: BC dev buy (stream 1) + dev wallet + GMGN (streams 2+3)
        # -------------------------------------------------------------------
        async def _run_stream1_bc() -> float | None:
            """Stream 1: mint/freeze check (silent) + BC dev buy."""
            t0 = time.perf_counter()
            mint_r, bc_r = await asyncio.gather(
                _check_mint_freeze(mint_str, rpc) if rpc else asyncio.sleep(0, result=(False, False)),
                _fetch_bc_dev_buy(bc_str, rpc) if (bc_str and rpc) else asyncio.sleep(0, result=None),
                return_exceptions=True,
            )
            elapsed = (time.perf_counter() - t0) * 1000

            has_mint = has_freeze = False
            if isinstance(mint_r, tuple):
                has_mint, has_freeze = mint_r

            if isinstance(bc_r, Exception):
                logger.warning(f"[#{count}] DEV BUY fetch failed for {(bc_str or '')[:8]}: {bc_r}")
                bc_buy = None
            elif bc_r is None and bc_str:
                logger.warning(
                    f"[#{count}] DEV BUY returned None for {bc_str[:8]} "
                    f"(account not found or data too short)"
                )
                bc_buy = None
            else:
                bc_buy = bc_r if isinstance(bc_r, (int, float)) else None

            buy_str = f"{bc_buy:.3f} SOL" if bc_buy is not None else "—"
            logger.info(
                f"[#{count}] Stream1 (mint={has_mint} freeze={has_freeze} buy={buy_str}): {elapsed:.0f}ms"
            )
            return bc_buy

        async def _run_stream2_then_3() -> tuple[DevWalletInfo, list[TokenHistory], bool]:
            """Streams 2+3: dev wallet check and GMGN in parallel.

            Returns (dev_info, histories, gmgn_failed).
            gmgn_failed=True means a communication error occurred — the histories
            list will be empty but that is NOT the same as 'dev has no tokens'.
            """
            t0 = time.perf_counter()

            dev_coro = (
                check_dev_wallet(creator_str, rpc)
                if creator_str and rpc
                else asyncio.sleep(0, result=DevWalletInfo(address=creator_str or ""))
            )
            gmgn_coro = (
                _get_gmgn_dev_tokens(creator_str, current_mint=mint_str)
                if creator_str
                else asyncio.sleep(0, result=[])
            )

            dev_result, histories_result = await asyncio.gather(
                dev_coro, gmgn_coro, return_exceptions=True
            )

            elapsed = (time.perf_counter() - t0) * 1000

            dev = (
                dev_result
                if isinstance(dev_result, DevWalletInfo)
                else DevWalletInfo(address=creator_str or "")
            )
            if isinstance(dev_result, Exception):
                logger.warning(f"[#{count}] Dev wallet check failed: {dev_result}")

            gmgn_failed = isinstance(histories_result, Exception)
            if gmgn_failed:
                logger.warning(
                    f"[#{count}] GMGN fetch failed for {mint_str[:8]}: {histories_result}"
                )
                histories: list[TokenHistory] = []
            else:
                histories = histories_result if isinstance(histories_result, list) else []

            logger.info(
                f"[#{count}] Stream2+3 (dev+GMGN): {elapsed:.0f}ms"
                f" | GMGN tokens: {len(histories)}"
                f"{' [GMGN FAILED]' if gmgn_failed else ''}"
            )
            return dev, histories, gmgn_failed

        dev = DevWalletInfo(address=creator_str, timed_out=True)
        dev_buy_sol: float | None = None
        histories: list[TokenHistory] = []
        gmgn_failed = False

        try:
            stream1_result, stream23_result = await asyncio.wait_for(
                asyncio.gather(_run_stream1_bc(), _run_stream2_then_3()),
                timeout=4.0,
            )
            dev_buy_sol = stream1_result
            dev, histories, gmgn_failed = stream23_result
        except TimeoutError:
            logger.warning(f"[#{count}] Overall fetch timeout (4s) — sending with dashes")
        except Exception as e:
            logger.error(f"[#{count}] Fetch error: {e}")

        t_elapsed = (time.perf_counter() - t_total) * 1000
        logger.info(f"[#{count}] Total: {t_elapsed:.0f}ms | dev_buy={dev_buy_sol}")

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
            await tg.send_message(_reject("data fetch timeout"))
            return

        if gmgn_failed:
            logger.info(f"[#{count}] GMGN unavailable — rejecting")
            await tg.send_message(_reject("GMGN data unavailable"))
            return

        if not histories:
            logger.info(f"[#{count}] GMGN returned no token history — rejecting")
            await tg.send_message(_reject("GMGN: no token history for this dev"))
            return

        dev_data_missing = (
            dev.sol_balance is None
            and dev.wallet_age_str is None
            and dev.total_launches is None
        )
        if dev_data_missing or dev.error:
            logger.info(f"[#{count}] Dev wallet data missing — rejecting")
            await tg.send_message(_reject("dev data unavailable"))
            return

        # -------------------------------------------------------------------
        # FILTER 2: Dev buy amount
        # -------------------------------------------------------------------
        min_dev_buy = float(filt.get("min_dev_buy_sol", 0.1))
        if dev_buy_sol is not None and dev_buy_sol < min_dev_buy:
            logger.info(
                f"[#{count}] Filter: dev bought {dev_buy_sol:.3f} SOL < {min_dev_buy} → skip"
            )
            await tg.send_message(_reject(
                f"Dev buy {dev_buy_sol:.3f} SOL (min {min_dev_buy:.3f} SOL)"
            ))
            return

        min_ath = float(filt.get("min_ath_last5", 0))
        min_mig = int(filt.get("min_migrations_last5", 0))

        # -------------------------------------------------------------------
        # FILTER 3: ATH of last 5 tokens
        # ath_require_all=False → at least one token must meet the threshold
        # ath_require_all=True  → every token must meet the threshold
        # -------------------------------------------------------------------
        if min_ath > 0 and histories:
            ath_require_all = bool(filt.get("ath_require_all", False))
            if ath_require_all:
                failing = [h for h in histories if (h.ath_market_cap or 0.0) < min_ath]
                if failing:
                    logger.info(
                        f"[#{count}] Filter: {len(failing)}/{len(histories)} tokens below "
                        f"ATH {min_ath:.0f} → skip"
                    )
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
                    await tg.send_message(_reject(
                        f"ATH {_format_mc(best_ath)} best of 5 (min {_format_mc(min_ath)})"
                    ))
                    return

        # -------------------------------------------------------------------
        # FILTER 4: Migrations count
        # -------------------------------------------------------------------
        if min_mig > 0 and histories:
            mig_count = sum(1 for h in histories if h.migrated is True)
            if mig_count < min_mig:
                logger.info(
                    f"[#{count}] Filter: migrations {mig_count} < {min_mig} → skip"
                )
                await tg.send_message(_reject(
                    f"Migrations {mig_count}/{len(histories)} (min {min_mig})"
                ))
                return

        # -------------------------------------------------------------------
        # ALL FILTERS PASSED
        # -------------------------------------------------------------------
        asyncio.create_task(_update_bot_config({
            "stats": {"tokens_passed_filters": (
                ((bot_cfg or {}).get("stats") or {}).get("tokens_passed_filters", 0) + 1
            )}
        }))

        # Build the full message with a ✅ header
        full_message = (
            "✅ ПРОШЁЛ ВСЕ ФИЛЬТРЫ\n"
            + format_token_alert(token_info, count, dev, dev_buy_sol, histories)
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
                    f"[#{count}] Position limit: {open_pos}/{max_pos} → skip buy"
                )
                buy_block = f"\n\n⏸ Лимит позиций: {open_pos}/{max_pos}"
            else:
                cur_buy_amount = float(preset.get("buy_amount_sol", buy_amount_sol))
                cur_slippage = float(preset.get("buy_slippage", buy_slippage * 100)) / 100
                cur_priority_sol = float(preset.get("priority_fee_sol", priority_fee_sol))
                cur_priority_ul = int(cur_priority_sol * 1_000_000_000)

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
                )

                await _update_bot_config({"open_positions": open_pos + 1})

                logger.info(f"[#{count}] Buying token {str(token_info.mint)[:8]}...")
                try:
                    buy_result = await fresh_buyer.execute(token_info)
                    if buy_result.success:
                        buy_block = (
                            f"\n✅ КУПЛЕНО: {cur_buy_amount:.4f} SOL | Fee: {cur_priority_sol:.4f} SOL"
                        )
                        logger.info(f"[#{count}] Buy success: {buy_result.tx_signature}")
                        await _update_bot_config({
                            "stats": {
                                "buys_executed": (
                                    ((bot_cfg or {}).get("stats") or {}).get("buys_executed", 0) + 1
                                )
                            }
                        })
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
                                )
                            )
                    else:
                        err = _escape_html(buy_result.error_message or "unknown error")
                        buy_block = f"\n\n❌ Покупка не удалась: {err}"
                        logger.warning(f"[#{count}] Buy failed: {buy_result.error_message}")
                        cur_cfg = _read_bot_config() or {}
                        await _update_bot_config({"open_positions": max(0, cur_cfg.get("open_positions", 1) - 1)})
                except Exception as e:
                    buy_block = f"\n\n❌ Покупка не удалась: {_escape_html(str(e))}"
                    logger.error(f"[#{count}] Buy exception: {e}")
                    cur_cfg = _read_bot_config() or {}
                    await _update_bot_config({"open_positions": max(0, cur_cfg.get("open_positions", 1) - 1)})

        full_message += buy_block
        success = await tg.send_message(full_message)

        if not success:
            logger.warning(f"[#{count}] Failed to send Telegram notification")

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
            await telegram.send_error_message(str(e))
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
