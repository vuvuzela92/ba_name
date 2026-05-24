"""Объекты конфигурации для runtime-настроек Telegram-бота."""

from __future__ import annotations

from dataclasses import dataclass
import os

from dotenv import load_dotenv


@dataclass(slots=True)
class BotConfig:
    """Настройки, необходимые для запуска polling Telegram-бота."""

    telegram_bot_token: str

    @classmethod
    def from_env(cls) -> "BotConfig":
        """Загружает конфиг из переменных окружения и валидирует обязательные поля."""
        load_dotenv()
        token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        if not token:
            raise RuntimeError("Не найден TELEGRAM_BOT_TOKEN в .env")
        return cls(telegram_bot_token=token)
