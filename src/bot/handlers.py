from __future__ import annotations

from dataclasses import asdict
from time import perf_counter
from typing import Callable

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import Message

from src.analytics import AnalyticsQueryService
from src.bot.keyboards import main_menu_keyboard
from src.bot.message_service import TelegramMessageSender
from src.core.logging_utils import bind_context

router = Router(name="analytics_bot")
service = AnalyticsQueryService()
message_sender = TelegramMessageSender(max_message_len=4000)


def _safe_text(handler: Callable[[], str], fallback: str) -> str:
    try:
        return handler()
    except Exception as exc:
        log = bind_context(task_name="tg_handler", endpoint="safe_text")
        log.exception("Handler formatting failed: {}", exc)
        return f"{fallback}\n\nТехническая ошибка: {exc}"


def _render_table_preview(df, title: str, limit: int = 10) -> str:
    if df is None or df.empty:
        return f"{title}: данных нет."
    preview = df.head(limit).to_string(index=False)
    return f"{title} (показаны первые {min(len(df), limit)} строк):\n<pre>{preview}</pre>"


def _format_top_products() -> str:
    items = service.get_top_products(top_n=10)
    if not items:
        return "Топ товаров: данных недостаточно."
    lines = ["Топ товаров:"]
    for idx, item in enumerate(items, start=1):
        lines.append(
            f"{idx}. Артикул {item.nm_id} | аккаунт: {item.account} | "
            f"выручка: {item.revenue:.2f} | заказы: {item.orders:.0f} | расход: {item.spend:.2f}"
        )
    return "\n".join(lines)


def _format_problem_products() -> str:
    items = service.get_problem_products(min_spend=500.0, max_orders=0.0, max_cr=0.5, top_n=10)
    if not items:
        return "Проблемные товары: не найдено по текущим порогам."
    lines = ["Проблемные товары:"]
    for idx, item in enumerate(items, start=1):
        lines.append(
            f"{idx}. Артикул {item.nm_id} | аккаунт: {item.account} | "
            f"расход: {item.spend:.2f} | заказы: {item.orders:.0f} | CR: {item.cr:.2f}"
        )
    return "\n".join(lines)


def _format_summary() -> str:
    summary = service.get_daily_summary()
    data = asdict(summary)
    return (
        "Краткая сводка:\n"
        f"Период: {data['date']}\n"
        f"Строк в рекламе: {data['adverts_count']}\n"
        f"Расходы: {data['total_spend']:.2f}\n"
        f"Выручка: {data['total_revenue']:.2f}\n"
        f"Заказы: {data['total_orders']:.0f}\n"
        f"Строк с ценами: {data['sku_prices_count']}\n"
        f"Строк в фин. отчете: {data['fin_rows_count']}"
    )


def _format_fin_report_compact() -> str:
    compact = service.get_fin_report_compact(top_n=10)
    total_records = compact["total_records"]
    if total_records == 0:
        return "Финансовый отчет: данных нет."

    top_df = compact["top"]
    lines = [
        "Финансовый отчет (кратко):",
        f"Записей: {total_records}",
        f"Сумма продаж: {compact['sum_sales']:.2f}",
        f"К перечислению: {compact['sum_payout']:.2f}",
        f"Штрафы: {compact['sum_penalty']:.2f}",
        "",
        "TOP 10 записей по продажам:",
    ]
    lines.append(top_df.to_string(index=False) if not top_df.empty else "Нет строк для TOP 10")
    return "\n".join(lines)


async def _run_with_logging(message: Message, command_name: str, producer: Callable[[], str], parse_mode: str | None = None) -> None:
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
    text = (
        "Здравствуйте. Это бот аналитики (read-only).\n"
        "Бот не запускает WB API и не изменяет данные в таблицах.\n"
        "Используйте кнопки ниже или команды /help."
    )
    await message.answer(text, reply_markup=main_menu_keyboard())


@router.message(Command("help"))
@router.message(F.text == "Помощь")
async def cmd_help(message: Message) -> None:
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
    await _run_with_logging(
        message,
        "prices",
        lambda: _render_table_preview(service.get_current_prices(limit=10), "Текущие цены"),
        parse_mode="HTML",
    )


@router.message(Command("ad_stats"))
@router.message(F.text == "Рекламная статистика")
async def cmd_ad_stats(message: Message) -> None:
    await _run_with_logging(
        message,
        "ad_stats",
        lambda: _render_table_preview(service.get_advert_stats().head(10), "Рекламная статистика"),
        parse_mode="HTML",
    )


@router.message(Command("fin_report"))
@router.message(F.text == "Финансовый отчет")
async def cmd_fin_report(message: Message) -> None:
    await _run_with_logging(message, "fin_report", _format_fin_report_compact)


@router.message(Command("top_products"))
@router.message(F.text == "Топ товаров")
async def cmd_top_products(message: Message) -> None:
    await _run_with_logging(message, "top_products", _format_top_products)


@router.message(Command("problem_products"))
@router.message(F.text == "Проблемные товары")
async def cmd_problem_products(message: Message) -> None:
    await _run_with_logging(message, "problem_products", _format_problem_products)


@router.message(Command("summary"))
@router.message(F.text == "Сводка за день")
async def cmd_summary(message: Message) -> None:
    await _run_with_logging(message, "summary", _format_summary)
