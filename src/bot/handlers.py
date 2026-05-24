"""Обработчики команд Telegram-бота аналитики (слой маршрутизации)."""

from __future__ import annotations

from time import perf_counter
from typing import Callable

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import Message

from src.bot.command_service import BotCommandService
from src.bot.keyboards import main_menu_keyboard
from src.bot.message_service import TelegramMessageSender
from src.core.logging_utils import bind_context

router = Router(name="analytics_bot")
command_service = BotCommandService()
message_sender = TelegramMessageSender(max_message_len=4000)


def _safe_text(handler: Callable[[], str], fallback: str) -> str:
    """Выполняет callback форматтера и возвращает fallback при ошибке."""
    try:
        return handler()
    except Exception as exc:
        log = bind_context(task_name="tg_handler", endpoint="safe_text")
        log.exception("Handler formatting failed: {}", exc)
        return f"{fallback}\n\nТехническая ошибка: {exc}"


async def _run_with_logging(message: Message, command_name: str, producer: Callable[[], str], parse_mode: str | None = None) -> None:
    """Единая обвязка выполнения команды, отправки и логирования времени."""
    start = perf_counter()
    log = bind_context(task_name=command_name, endpoint="telegram", account=str(message.from_user.id if message.from_user else "-"))
    log.info("Incoming command text='{}'", message.text)
    try:
        text = _safe_text(producer, f"Не удалось выполнить команду {command_name}.")
        await message_sender.send_long_message(message, text, parse_mode=parse_mode)
    except Exception as exc:
        log.exception("Command failed: {}", exc)
        await message.answer("Произошла ошибка при обработке команды. Проверьте логи.")
    finally:
        duration_ms = round((perf_counter() - start) * 1000, 2)
        log.info("Command finished duration_ms={}", duration_ms)


@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    """Показывает стартовое сообщение и основную клавиатуру."""
    text = (
        "Здравствуйте. Это бот аналитики (read-only).\n"
        "Бот не запускает WB API и не изменяет данные в таблицах.\n"
        "Используйте кнопки ниже или команды /help."
    )
    await message.answer(text, reply_markup=main_menu_keyboard())


@router.message(Command("help"))
@router.message(F.text == "Помощь")
async def cmd_help(message: Message) -> None:
    """Показывает список доступных команд."""
    text = (
        "Доступные команды:\n"
        "/prices — текущие цены\n"
        "/ad_stats — рекламная статистика\n"
        "/fin_report — финансовый отчет\n"
        "/top_products — топ товаров\n"
        "/problem_products — проблемные товары\n"
        "/summary — краткая сводка"
    )
    await message.answer(text)


@router.message(Command("prices"))
@router.message(F.text == "Текущие цены")
async def cmd_prices(message: Message) -> None:
    """Возвращает превью таблицы текущих цен."""
    await _run_with_logging(
        message,
        "prices",
        command_service.get_prices_text,
        parse_mode="HTML",
    )


@router.message(Command("ad_stats"))
@router.message(F.text == "Рекламная статистика")
async def cmd_ad_stats(message: Message) -> None:
    """Возвращает превью рекламной статистики."""
    await _run_with_logging(
        message,
        "ad_stats",
        command_service.get_ad_stats_text,
        parse_mode="HTML",
    )


@router.message(Command("fin_report"))
@router.message(F.text == "Финансовый отчет")
async def cmd_fin_report(message: Message) -> None:
    """Возвращает компактный финансовый отчет с итогами и TOP строками."""
    await _run_with_logging(
        message,
        "fin_report",
        command_service.get_fin_report_text,
        parse_mode="HTML",
    )


@router.message(Command("top_products"))
@router.message(F.text == "Топ товаров")
async def cmd_top_products(message: Message) -> None:
    """Возвращает рейтинг топовых товаров."""
    await _run_with_logging(
        message,
        "top_products",
        command_service.get_top_products_text,
        parse_mode="HTML",
    )


@router.message(Command("problem_products"))
@router.message(F.text == "Проблемные товары")
async def cmd_problem_products(message: Message) -> None:
    """Возвращает товары с низкой эффективностью по текущим порогам."""
    await _run_with_logging(
        message,
        "problem_products",
        command_service.get_problem_products_text,
        parse_mode="HTML",
    )


@router.message(Command("summary"))
@router.message(F.text == "Сводка за день")
async def cmd_summary(message: Message) -> None:
    """Возвращает краткую сводку по подготовленным данным."""
    await _run_with_logging(
        message,
        "summary",
        command_service.get_summary_text,
        parse_mode="HTML",
    )
