from src.core.base_scraper import WBScraper
from src.core.http_runtime import ConcurrencyLimiter, HttpRuntimeConfig, RetryPolicy, SessionManager
from src.core.utils_general import load_api_tokens, batchify
from src.advert.advert_stat import WbAdverStat
import asyncio
from datetime import datetime, timedelta


class ScraperAdvert(WBScraper):
    """Класс для получения рекламных данных со всех доступных кабинетов WB."""

    def __init__(self, max_concurrent: int = 16):
        super().__init__(max_concurrent)

    async def _fetch_adv_camp_list(self):
        tokens = load_api_tokens()
        campaign_statuses = 9, 11

        runtime_config = HttpRuntimeConfig(global_concurrency_limit=self.max_concurrent)
        limiter = ConcurrencyLimiter(runtime_config.global_concurrency_limit)
        retry_policy = RetryPolicy()

        async with SessionManager(runtime_config) as session:
            tasks = []
            for account, token in tokens.items():
                client = WbAdverStat(
                    token,
                    session,
                    account,
                    timeout=int(runtime_config.request_timeout_sec),
                    retry_policy=retry_policy,
                    limiter=limiter,
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
        if date_from is None:
            date_from = date_to = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        runtime_config = HttpRuntimeConfig(global_concurrency_limit=self.max_concurrent)
        limiter = ConcurrencyLimiter(runtime_config.global_concurrency_limit)
        retry_policy = RetryPolicy()

        async with SessionManager(runtime_config) as session:
            tasks = []
            for account, token in load_api_tokens().items():
                client = WbAdverStat(
                    token,
                    session,
                    account,
                    timeout=int(runtime_config.request_timeout_sec),
                    retry_policy=retry_policy,
                    limiter=limiter,
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
        if date_from is None:
            date_from = date_to = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        runtime_config = HttpRuntimeConfig(global_concurrency_limit=self.max_concurrent)
        limiter = ConcurrencyLimiter(runtime_config.global_concurrency_limit)
        retry_policy = RetryPolicy()

        async with SessionManager(runtime_config) as session:
            tasks = []
            for account, token in load_api_tokens().items():
                client = WbAdverStat(
                    token,
                    session,
                    account,
                    timeout=int(runtime_config.request_timeout_sec),
                    retry_policy=retry_policy,
                    limiter=limiter,
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
