import asyncio
import inspect
from typing import Any, Callable, Dict

# --- ИМПОРТЫ ЗАДАЧ ---
from src.price.run import get_current_prices_async
from src.fin_report.run import fin_rep_weekly_async
from src.advert.run import advert_stat


def smart_run(func: Callable):
    """
    Автоматически определяет, как запускать функцию:
    как обычную или через asyncio.run().
    """
    if inspect.iscoroutinefunction(func):
        return lambda: asyncio.run(func())
    return func


# --- РЕЕСТР ЗАДАЧ ---
# Формат: "команда_в_консоли": {"original_func": функция, "desc": описание}
TASKS: Dict[str, Dict[str, Any]] = {
    "fin_rep_weekly": {
        "original_func": fin_rep_weekly_async,
        "desc": "💵 Запуск обновления данных по еженедельным финансовым отчетам",
    },
    "get_current_prices": {
        "original_func": get_current_prices_async,
        "desc": "💵 Запуск обновления данных по текущим ценам",
    },
    # python main.py advert_stat --date_from 2026-06-01 --date_to 2026-06-05 Забрать данные за период
    "advert_stat": {
        "original_func": advert_stat,
        "desc": "📊 Запуск обновления данных по рекламной статистике",
    },
}
