from src.wb.api.wb_client import WildberriesClient
from src.wb.core.utils_general import batchify, load_api_tokens

# from wb_client import WildberriesClient
# from advert_stat import WbAdverStat
# from utils_general import batchify, load_api_tokens

from datetime import datetime, timedelta
import asyncio
import aiohttp
import logging

logger = logging.getLogger(__name__)

class WBScraper():
    """
    Оркестратор сбора данных с Wildberries.
    Управляет датами, аккаунтами, параллелизмом.
    """

    def __init__(self, max_concurrent: int = 16):
        """
        Args:
            tokens: Словарь {account_name: api_key}
            max_concurrent: Максимум одновременных запросов (semaphore)
        """
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)

    async def _fetch_funnel(self, days_count=1):
        tokens = load_api_tokens()
        dates = [(datetime.now() - timedelta(days=day)).strftime('%Y-%m-%d') 
                for day in range(days_count)]
        
        async with aiohttp.ClientSession() as session:
            # 1. Создаём всех клиентов
            clients = {
                name: WildberriesClient(token, session, name)
                for name, token in tokens.items()
            }
            
            # 2. Создаём все задачи с контекстом
            tasks = []    
            for name, client in clients.items():
                for date in dates:
                    print(f"Начался сбор данных за {date} по ЛК {name}")
                    task = client.get_funnel(start_date=date, end_date=date)
                    print(f"Собраны данные за {date} по ЛК {name}")
                    tasks.append(task)
            
            # 3. Запускаем ВСЕ задачи одновременно с gather()
            results = await asyncio.gather(*tasks, return_exceptions=True)
            return results
        

    async def _fetch_adv_camp_list(self):
        """ Создает и получает данные по спискам по всем ЛК асинхронно"""
        # Используем ленивый импорт, чтобы не вызывать циклических зависимостей
        from src.wb.services.advert_service import WbAdverStat
        tokens = load_api_tokens()
        # Определяем перечень статусов РК, по которым будем запращивать данные
        campaign_statuses = 9, 11
        async with aiohttp.ClientSession() as session:
            tasks = []
            for account, token in tokens.items():
                # Создаем экземпляр класса для каждого ЛК
                client = WbAdverStat(token, session, account)
                for campaign_status in campaign_statuses:
                    # Создаем корутину
                    task = client.get_camp_list(campaign_status)
                    tasks.append(task)
            print(f"🚀 Запускаем сбор {len(tasks)} задач одновременно...")
            # 3. Запускаем всё разом
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Небольшая фильтрация: убираем ошибки, если они возникли
            clean_results = []
            for res in results:
                if isinstance(res, Exception):
                    print(f"💥 Одна из задач завершилась ошибкой: {res}")
                elif res is not None:
                    clean_results.extend(res) # Распаковываем списки данных в один общий список
                    
            print(f"🏁 Сбор завершен. Итого записей: {len(clean_results)}")
            return clean_results

    async def _fetch_advert_stat(self, adverts = None, date_from: str = None, date_to: str = None)->list:
        # Используем ленивый импорт, чтобы не вызывать циклических зависимостей
        from src.wb.services.advert_service import WbAdverStat
        """ Функция для сбора статистической информации по общей статистике по всем ЛК.
        adverts: кортеж из словарей с ключом в качестве названия ЛК и списком рекламных кампаний по типу Единая и Ручная."""
        if date_from is None:
            date_from = date_to = (datetime.now()-timedelta(days=1)).strftime('%Y-%m-%d')   
        async with aiohttp.ClientSession() as session:
            tasks = []
            for account, token in load_api_tokens().items():
                # Создаем экземпляры клиентского класса для каждого ЛК
                client = WbAdverStat(token, session, account)
                try:
                    for advert in adverts:
                        for batch in batchify(advert[account], 50):
                            task = client.get_advert_stat(batch, date_from, date_to)
                            tasks.append(task)
                except KeyError:
                    print(f"Отсутствует ключ для {account}")
            print(f"🚀 Запускаем сбор {len(tasks)} задач одновременно...")
            # 3. Запускаем всё разом
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Небольшая фильтрация: убираем ошибки, если они возникли
            clean_results = []
            for res in results:
                if isinstance(res, Exception):
                    print(f"💥 Одна из задач завершилась ошибкой: {res}")
                elif res is not None:
                    clean_results.extend(res) # Распаковываем списки данных в один общий список
                    
            print(f"🏁 Сбор завершен. Итого записей: {len(clean_results)}")
            return clean_results
        
    async def _fetch_advert_spend(self, date_from: str = None, date_to: str = None)->list:
        """ Функция для сбора информации о рекламных затратах по всем ЛК.
        date_from: дата формата "2026-05-10", задает начала диапазона сбора данных
        date_to: дата формата "2026-05-10", задает конец диапазона сбора данных 
        """
        # Используем ленивый импорт, чтобы не вызывать циклических зависимостей
        from src.wb.services.advert_service import WbAdverStat

        if date_from is None:
            date_from = date_to = (datetime.now()-timedelta(days=1)).strftime('%Y-%m-%d') 
        async with aiohttp.ClientSession() as session:
            tasks = []
            for account, token in load_api_tokens().items():
                # Создаем экземпляры клиентского класса для каждого ЛК
                client = WbAdverStat(token, session, account)
                try:
                    task = client.get_advert_spend(date_from, date_to)
                    tasks.append(task)
                except KeyError:
                    print(f"Отсутствует ключ для {account}")
            print(f"🚀 Запускаем сбор {len(tasks)} задач одновременно...")
            # 3. Запускаем всё разом
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Небольшая фильтрация: убираем ошибки, если они возникли
            clean_results = []
            for res in results:
                if isinstance(res, Exception):
                    print(f"💥 Одна из задач завершилась ошибкой: {res}")
                elif res is not None:
                    clean_results.extend(res) # Распаковываем списки данных в один общий список
                    
            print(f"🏁 Сбор завершен. Итого записей: {len(clean_results)}")
            return clean_results