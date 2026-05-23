from src.core.wb_client import WildberriesClient
from src.core.http_runtime import ConcurrencyLimiter, HttpRuntimeConfig, RetryPolicy, SessionManager
from src.core.utils_general import batchify, load_api_tokens

from datetime import datetime, timedelta
import asyncio
import logging

logger = logging.getLogger(__name__)


class WBScraper:
    """Оркестратор сбора данных с Wildberries."""

    def __init__(self, max_concurrent: int = 16):
        self.max_concurrent = max_concurrent
        self.runtime_config = HttpRuntimeConfig(global_concurrency_limit=max_concurrent)
        self.retry_policy = RetryPolicy()
        self.limiter = ConcurrencyLimiter(self.runtime_config.global_concurrency_limit)

    async def _fetch_funnel(self, days_count=1):
        tokens = load_api_tokens()
        dates = [
            (datetime.now() - timedelta(days=day)).strftime('%Y-%m-%d')
            for day in range(days_count)
        ]

        async with SessionManager(self.runtime_config) as session:
            clients = {
                name: WildberriesClient(
                    token,
                    session,
                    name,
                    timeout=int(self.runtime_config.request_timeout_sec),
                    retry_policy=self.retry_policy,
                    limiter=self.limiter,
                )
                for name, token in tokens.items()
            }

            tasks = []
            for name, client in clients.items():
                for date in dates:
                    print(f"Начался сбор данных за {date} по ЛК {name}")
                    task = client.get_funnel(start_date=date, end_date=date)
                    print(f"Собраны данные за {date} по ЛК {name}")
                    tasks.append(task)

            results = await asyncio.gather(*tasks, return_exceptions=True)
            return results

    async def _fetch_adv_camp_list(self):
        from src.advert.advert_service import WbAdverStat

        tokens = load_api_tokens()
        campaign_statuses = 9, 11

        async with SessionManager(self.runtime_config) as session:
            tasks = []
            for account, token in tokens.items():
                client = WbAdverStat(
                    token,
                    session,
                    account,
                    timeout=int(self.runtime_config.request_timeout_sec),
                    retry_policy=self.retry_policy,
                    limiter=self.limiter,
                )
                for campaign_status in campaign_statuses:
                    tasks.append(client.get_camp_list(campaign_status))
            print(f"🚀 Запускаем сбор {len(tasks)} задач одновременно...")
            results = await asyncio.gather(*tasks, return_exceptions=True)

            clean_results = []
            for res in results:
                if isinstance(res, Exception):
                    print(f"💥 Одна из задач завершилась ошибкой: {res}")
                elif res is not None:
                    clean_results.extend(res)

            print(f"🏁 Сбор завершен. Итого записей: {len(clean_results)}")
            return clean_results

    async def _fetch_advert_stat(self, adverts=None, date_from: str = None, date_to: str = None) -> list:
        from src.advert.advert_service import WbAdverStat

        if date_from is None:
            date_from = date_to = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        async with SessionManager(self.runtime_config) as session:
            tasks = []
            for account, token in load_api_tokens().items():
                client = WbAdverStat(
                    token,
                    session,
                    account,
                    timeout=int(self.runtime_config.request_timeout_sec),
                    retry_policy=self.retry_policy,
                    limiter=self.limiter,
                )
                try:
                    for advert in adverts:
                        for batch in batchify(advert[account], 50):
                            tasks.append(client.get_advert_stat(batch, date_from, date_to))
                except KeyError:
                    print(f"Отсутствует ключ для {account}")
            print(f"🚀 Запускаем сбор {len(tasks)} задач одновременно...")
            results = await asyncio.gather(*tasks, return_exceptions=True)

            clean_results = []
            for res in results:
                if isinstance(res, Exception):
                    print(f"💥 Одна из задач завершилась ошибкой: {res}")
                elif res is not None:
                    clean_results.extend(res)

            print(f"🏁 Сбор завершен. Итого записей: {len(clean_results)}")
            return clean_results

    async def _fetch_advert_spend(self, date_from: str = None, date_to: str = None) -> list:
        from src.advert.advert_service import WbAdverStat

        if date_from is None:
            date_from = date_to = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        async with SessionManager(self.runtime_config) as session:
            tasks = []
            for account, token in load_api_tokens().items():
                client = WbAdverStat(
                    token,
                    session,
                    account,
                    timeout=int(self.runtime_config.request_timeout_sec),
                    retry_policy=self.retry_policy,
                    limiter=self.limiter,
                )
                try:
                    tasks.append(client.get_advert_spend(date_from, date_to))
                except KeyError:
                    print(f"Отсутствует ключ для {account}")
            print(f"🚀 Запускаем сбор {len(tasks)} задач одновременно...")
            results = await asyncio.gather(*tasks, return_exceptions=True)

            clean_results = []
            for res in results:
                if isinstance(res, Exception):
                    print(f"💥 Одна из задач завершилась ошибкой: {res}")
                elif res is not None:
                    clean_results.extend(res)

            print(f"🏁 Сбор завершен. Итого записей: {len(clean_results)}")
            return clean_results
