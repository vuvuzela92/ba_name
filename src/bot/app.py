from __future__ import annotations

import asyncio

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import BotCommand

from src.bot.config import BotConfig
from src.bot.handlers import router
from src.core.logging_utils import bind_context, setup_logging
from src.core.runtime_config import load_runtime_settings


def _get_bot_token() -> str:
    return BotConfig.from_env().telegram_bot_token


async def _set_commands(bot: Bot) -> None:
    commands = [
        BotCommand(command="start", description="Старт"),
        BotCommand(command="help", description="Помощь"),
        BotCommand(command="prices", description="Текущие цены"),
        BotCommand(command="ad_stats", description="Рекламная статистика"),
        BotCommand(command="fin_report", description="Финансовый отчет"),
        BotCommand(command="top_products", description="Топ товаров"),
        BotCommand(command="problem_products", description="Проблемные товары"),
        BotCommand(command="summary", description="Краткая сводка"),
    ]
    await bot.set_my_commands(commands)


def build_dispatcher() -> Dispatcher:
    log = bind_context(task_name="bot_init", endpoint="dispatcher")
    dp = Dispatcher()
    dp.include_router(router)
    log.info("Handlers registered")
    return dp


async def run_polling_async() -> None:
    runtime_settings = load_runtime_settings()
    setup_logging(runtime_settings.logging)
    log = bind_context(task_name="bot_run", endpoint="polling")

    token = _get_bot_token()
    bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = build_dispatcher()

    await _set_commands(bot)
    log.info("Bot polling started")

    try:
        await dp.start_polling(bot)
    except TelegramBadRequest as exc:
        log.exception("Telegram API bad request: {}", exc)
        raise
    except Exception as exc:
        log.exception("Polling crashed: {}", exc)
        raise


def run_polling() -> None:
    asyncio.run(run_polling_async())
