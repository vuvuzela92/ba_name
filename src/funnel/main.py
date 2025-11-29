from utils_funnel import main_funnel_daily
import asyncio

if __name__ == "__main__":
    # Выбираем нужное количество дней
    days_count = 1
    # Получаем данные
    asyncio.run(main_funnel_daily(days_count))