"""Общий оркестратор scraper-задач для асинхронного сбора данных WB."""

from src.core.wb_client import WildberriesClient
from src.core.http_runtime import (
    ConcurrencyLimiter,
    HttpRuntimeConfig,
    RetryPolicy,
    RuntimeMetrics,
    SessionManager,
)
from src.core.logging_utils import bind_context
from src.core.runtime_config import load_runtime_settings
from src.core.utils_general import batchify, load_api_tokens

from datetime import datetime, timedelta
import asyncio


class WBScraper:
    """Оркестратор сбора данных Wildberries.

    Централизует сетевые аспекты: общая сессия, retry-политика,
    limiter конкурентности и обработка partial failures после gather.
    """

    def __init__(self, max_concurrent: int = 16):
        self.max_concurrent = max_concurrent
        runtime_settings = load_runtime_settings()
        self.runtime_config = HttpRuntimeConfig.from_http_settings(runtime_settings.http)
        self.runtime_config.global_concurrency_limit = max_concurrent
        self.retry_policy = RetryPolicy.from_http_settings(runtime_settings.http)
        self.limiter = ConcurrencyLimiter(self.runtime_config.global_concurrency_limit)

    async def _fetch_funnel(self, days_count=1):
        """Собирает funnel-данные за последние дни по всем аккаунтам."""
        tokens = load_api_tokens()
        dates = [
            (datetime.now() - timedelta(days=day)).strftime("%Y-%m-%d")
            for day in range(days_count)
        ]
        metrics = RuntimeMetrics()

        async with SessionManager(self.runtime_config) as session:
            clients = {
                name: WildberriesClient(
                    token,
                    session,
                    name,
                    timeout=int(self.runtime_config.request_timeout_sec),
                    retry_policy=self.retry_policy,
                    limiter=self.limiter,
                    metrics=metrics,
                )
                for name, token in tokens.items()
            }

            tasks = []
            for name, client in clients.items():
                for date in dates:
                    bind_context(task_name="funnel", account=name).info(
                        f"Queue funnel fetch for date={date}"
                    )
                    tasks.append(client.get_funnel(start_date=date, end_date=date))

            results = await asyncio.gather(*tasks, return_exceptions=True)
            failed_tasks = sum(1 for r in results if isinstance(r, Exception))
            succeeded_tasks = len(results) - failed_tasks
            bind_context(
                task_name="funnel",
                failed_tasks=failed_tasks,
                succeeded_tasks=succeeded_tasks,
            ).info(f"Funnel gather complete metrics={metrics.snapshot()}")
            return results

    async def _fetch_adv_camp_list(self):
        """Собирает списки рекламных кампаний по нужным статусам."""
        from src.advert.advert_service import WbAdverStat

        tokens = load_api_tokens()
        campaign_statuses = 9, 11
        metrics = RuntimeMetrics()

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
                    metrics=metrics,
                )
                for campaign_status in campaign_statuses:
                    tasks.append(client.get_camp_list(campaign_status))

            bind_context(task_name="adv_camp_list").info(
                f"Starting gather tasks={len(tasks)}"
            )
            results = await asyncio.gather(*tasks, return_exceptions=True)

            clean_results = []
            failed_tasks = 0
            for idx, res in enumerate(results):
                if isinstance(res, Exception):
                    failed_tasks += 1
                    bind_context(task_name="adv_camp_list", attempt=idx + 1).exception(
                        f"Task failed: {res}"
                    )
                elif res is not None:
                    clean_results.extend(res)

            succeeded_tasks = len(results) - failed_tasks
            bind_context(
                task_name="adv_camp_list",
                failed_tasks=failed_tasks,
                succeeded_tasks=succeeded_tasks,
            ).info(f"Gather complete rows={len(clean_results)} metrics={metrics.snapshot()}")
            return clean_results

    async def _fetch_advert_stat(self, adverts=None, date_from: str = None, date_to: str = None) -> list:
        """Собирает детальную рекламную статистику батчами campaign id."""
        from src.advert.advert_service import WbAdverStat

        if date_from is None:
            date_from = date_to = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        metrics = RuntimeMetrics()

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
                    metrics=metrics,
                )
                try:
                    for advert in adverts:
                        for batch in batchify(advert[account], 50):
                            tasks.append(client.get_advert_stat(batch, date_from, date_to))
                except KeyError:
                    bind_context(task_name="advert_stat", account=account).warning(
                        "Missing account key in adverts payload"
                    )

            bind_context(task_name="advert_stat").info(
                f"Starting gather tasks={len(tasks)}"
            )
            results = await asyncio.gather(*tasks, return_exceptions=True)

            clean_results = []
            failed_tasks = 0
            for idx, res in enumerate(results):
                if isinstance(res, Exception):
                    failed_tasks += 1
                    bind_context(task_name="advert_stat", attempt=idx + 1).exception(
                        f"Task failed: {res}"
                    )
                elif res is not None:
                    clean_results.extend(res)

            succeeded_tasks = len(results) - failed_tasks
            bind_context(
                task_name="advert_stat",
                failed_tasks=failed_tasks,
                succeeded_tasks=succeeded_tasks,
            ).info(f"Gather complete rows={len(clean_results)} metrics={metrics.snapshot()}")
            return clean_results

    async def _fetch_advert_spend(self, date_from: str = None, date_to: str = None) -> list:
        """Собирает агрегированные рекламные затраты по всем аккаунтам."""
        from src.advert.advert_service import WbAdverStat

        if date_from is None:
            date_from = date_to = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        metrics = RuntimeMetrics()

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
                    metrics=metrics,
                )
                try:
                    tasks.append(client.get_advert_spend(date_from, date_to))
                except KeyError:
                    bind_context(task_name="advert_spend", account=account).warning(
                        "Missing account key for advert spend"
                    )

            bind_context(task_name="advert_spend").info(
                f"Starting gather tasks={len(tasks)}"
            )
            results = await asyncio.gather(*tasks, return_exceptions=True)

            clean_results = []
            failed_tasks = 0
            for idx, res in enumerate(results):
                if isinstance(res, Exception):
                    failed_tasks += 1
                    bind_context(task_name="advert_spend", attempt=idx + 1).exception(
                        f"Task failed: {res}"
                    )
                elif res is not None:
                    clean_results.extend(res)

            succeeded_tasks = len(results) - failed_tasks
            bind_context(
                task_name="advert_spend",
                failed_tasks=failed_tasks,
                succeeded_tasks=succeeded_tasks,
            ).info(f"Gather complete rows={len(clean_results)} metrics={metrics.snapshot()}")
            return clean_results
