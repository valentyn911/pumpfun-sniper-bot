"""
Модуль для отправки уведомлений в Telegram.

Использует простые HTTP-запросы через aiohttp — без дополнительных библиотек,
aiohttp уже входит в зависимости проекта.

Документация Telegram Bot API: https://core.telegram.org/bots/api
"""

import aiohttp

from utils.logger import get_logger

logger = get_logger(__name__)

# Таймаут на один HTTP-запрос к Telegram (секунды)
TELEGRAM_REQUEST_TIMEOUT = 10


class TelegramReporter:
    """Отправляет сообщения в Telegram-чат через Bot API."""

    def __init__(self, bot_token: str, chat_id: str) -> None:
        """
        Args:
            bot_token: Токен бота, полученный от @BotFather
            chat_id:   ID чата/канала куда слать сообщения
        """
        # Базовый URL для метода sendMessage
        self._api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        self._chat_id = chat_id

    async def send_message(self, text: str) -> bool:
        """
        Отправить текстовое сообщение в Telegram.

        Поддерживается HTML-разметка: <b>жирный</b>, <i>курсив</i>,
        <code>моноширинный</code>, <a href="...">ссылка</a>.

        Args:
            text: Текст сообщения

        Returns:
            True — сообщение отправлено, False — ошибка
        """
        payload = {
            "chat_id": self._chat_id,
            "text": text,
            "parse_mode": "HTML",
            # Не показывать превью для ссылок (чтобы не загромождать чат)
            "disable_web_page_preview": True,
        }

        timeout = aiohttp.ClientTimeout(total=TELEGRAM_REQUEST_TIMEOUT)

        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(self._api_url, json=payload) as response:
                    if response.status == 200:
                        logger.debug("Telegram: сообщение отправлено успешно")
                        return True

                    # Если статус не 200 — читаем ответ и логируем ошибку
                    error_body = await response.text()
                    logger.error(
                        f"Telegram API вернул ошибку {response.status}: {error_body}"
                    )
                    return False

        except TimeoutError:
            logger.error("Telegram: запрос истёк по таймауту")
            return False
        except aiohttp.ClientError as e:
            logger.error(f"Telegram: сетевая ошибка — {e}")
            return False
        except Exception as e:
            logger.error(f"Telegram: неожиданная ошибка — {e}")
            return False

    async def send_startup_message(self, platform: str = "pump.fun") -> None:
        """
        Отправить сообщение о том, что бот запущен.
        Это первое сообщение — подтверждает, что Telegram работает.

        Args:
            platform: Название платформы для отображения в сообщении
        """
        text = (
            "✅ <b>Сканер запущен!</b>\n"
            "\n"
            f"👀 Слежу за новыми токенами на <b>{platform}</b>\n"
            "📩 Как только появится новый токен — сразу пришлю сюда"
        )
        success = await self.send_message(text)

        if success:
            logger.info("Telegram: стартовое сообщение отправлено")
        else:
            logger.warning(
                "Telegram: не удалось отправить стартовое сообщение — "
                "проверь TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID"
            )

    async def send_error_message(self, error_text: str) -> None:
        """
        Отправить уведомление об ошибке.

        Args:
            error_text: Описание ошибки
        """
        text = f"❌ <b>Ошибка сканера:</b>\n<code>{error_text}</code>"
        await self.send_message(text)
