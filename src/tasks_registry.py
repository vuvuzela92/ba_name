import asyncio
import inspect
from typing import Callable, Dict, Any

# --- ИМПОРТЫ ЗАДАЧ ---
from src.price.run import get_current_prices
from src.fin_report.run import fin_rep_weekly
from src.advert.run import advert_stat


def smart_run(func: Callable):
    """
    Автоматически определяет, как запускать функцию:
    как обычную или через asyncio.run()
    """
    if inspect.iscoroutinefunction(func):
        return lambda: asyncio.run(func())
    return func

# --- РЕЕСТР ЗАДАЧ ---
# Формат: "команда_в_консоли": (функция, "текст_описания")

# Упростим реестр, чтобы main.py сам решал, как запускать
TASKS: Dict[str, Dict[str, Any]] = {
    "fin_rep_weekly": {
        "original_func": fin_rep_weekly,
        "desc": "💵 Запуск обновления данных о еженедельных финансовых отчетах"
    },
    "get_current_prices": {
        "original_func": get_current_prices,
        "desc": "💵 Запуск обновления данных о текущих ценах"
    },
    "advert_stat": {
        "original_func": advert_stat,
        "desc": "📊 Запуск обновления данных о рекламной статистике"
    },
}