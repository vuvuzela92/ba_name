import asyncio
import inspect
from typing import Callable, Dict, Any

# --- ИМПОРТЫ ЗАДАЧ ---
from src.price.run import get_current_prices
from src.fin_report.run import fin_rep_weekly


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
TASKS: Dict[str, Dict[str, Any]] = {
    # Раздел: фин отчеты
    "fin_rep_weekly": {
        "func": smart_run(fin_rep_weekly),
        "desc": "💵 Запуск обновления данных о еженедельных финансовых отчетах"
    },
    # Раздел: фин отчеты
    "get_current_prices": {
        "func": smart_run(get_current_prices),
        "desc": "💵 Запуск обновления данных о текущих ценах"
    },
}