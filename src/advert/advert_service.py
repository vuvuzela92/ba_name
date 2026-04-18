# Импорт внутренних модулей
from src.core.base_scraper import WBScraper
from src.core.utils_general import load_api_tokens, batchify
from src.advert.advert_stat import WbAdverStat
# Импорт внешних библиотек
import aiohttp
import asyncio
from datetime import datetime, timedelta

class ScraperAdvert(WBScraper):
    """Класс для получения рекламных данных со всех доступных кабинетов ВБ"""
    def __init__(self, max_concurrent = 16):
        super().__init__(max_concurrent)

    async def _fetch_adv_camp_list(self):
        """ Создает и получает данные по спискам по всем ЛК асинхронно"""
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