# Импорт внутренних модулей
from src.core.utils_general import load_api_tokens
from src.fin_report.finance_report import FinRep
# Импорт внешних библиотек
import aiohttp
import asyncio
from datetime import datetime, timedelta

async def fetch_fin_reps_weekly(count_weeks: int):
    today = datetime.today()
    weekday = today.weekday()
    base_sunday = today - timedelta(days=(weekday + 1)) 
    tokens = load_api_tokens()
    
    all_tasks = [] # Один общий список для всех задач
    
    async with aiohttp.ClientSession() as session:
        for week in range(count_weeks):
            target_sunday = base_sunday - timedelta(weeks=week)
            target_monday = target_sunday - timedelta(days=6)
            
            date_from = target_monday.strftime('%Y-%m-%d')
            date_to = target_sunday.strftime('%Y-%m-%d')
            
            for name, token in tokens.items():
                client = FinRep(token, session, name) 
                task = client.get_fin_report_daily(date_from, date_to)
                all_tasks.append(task)
        
        print(f"🚀 Запускаем ПОЛНЫЙ сбор: {len(all_tasks)} задач одновременно...")
        
        # Запускаем вообще всё разом
        results = await asyncio.gather(*all_tasks, return_exceptions=True)
        
        # Собираем все результаты в один плоский список
        all_final_results = []
        for res in results:
            if isinstance(res, Exception):
                print(f"💥 Ошибка в одной из задач: {res}")
            elif res is not None:
                all_final_results.extend(res)
                
    print(f"🏁 Сбор завершен. Итого записей: {len(all_final_results)}")
    return all_final_results