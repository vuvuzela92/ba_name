from src.core.base_scraper import WBScraper
from src.core.http_runtime import (
    ConcurrencyLimiter,
    HttpRuntimeConfig,
    RetryPolicy,
    RuntimeMetrics,
    SessionManager,
)
from src.core.logging_utils import bind_context
from src.core.utils_general import load_api_tokens, batchify
from src.advert.advert_stat import WbAdverStat
import asyncio
from datetime import datetime, timedelta


class ScraperAdvert(WBScraper):
    """Collect advertising data from all available WB accounts."""

    def __init__(self, max_concurrent: int = 16):
        super().__init__(max_concurrent)

    async def _fetch_adv_camp_list(self):
        tokens = load_api_tokens()
        campaign_statuses = 9, 11

        runtime_config = HttpRuntimeConfig(global_concurrency_limit=self.max_concurrent)
        limiter = ConcurrencyLimiter(runtime_config.global_concurrency_limit)
        retry_policy = RetryPolicy()
        metrics = RuntimeMetrics()

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
                    metrics=metrics,
                )
                for campaign_status in campaign_statuses:
                    tasks.append(client.get_camp_list(campaign_status))

            bind_context(task_name="advert_camp_list").info(
                f"Starting advert campaign list collection tasks={len(tasks)}"
            )
            results = await asyncio.gather(*tasks, return_exceptions=True)

            clean_results = []
            failed_tasks = 0
            for idx, res in enumerate(results):
                if isinstance(res, Exception):
                    failed_tasks += 1
                    bind_context(task_name="advert_camp_list", attempt=idx + 1).exception(
                        f"Task failed: {res}"
                    )
                elif res is not None:
                    clean_results.extend(res)

            succeeded_tasks = len(results) - failed_tasks
            bind_context(
                task_name="advert_camp_list",
                failed_tasks=failed_tasks,
                succeeded_tasks=succeeded_tasks,
            ).info(
                f"Advert campaign list finished. rows={len(clean_results)} metrics={metrics.snapshot()}"
            )
            return clean_results

    async def _fetch_advert_stat(self, adverts=None, date_from: str = None, date_to: str = None) -> list:
        if date_from is None:
            date_from = date_to = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        runtime_config = HttpRuntimeConfig(global_concurrency_limit=self.max_concurrent)
        limiter = ConcurrencyLimiter(runtime_config.global_concurrency_limit)
        retry_policy = RetryPolicy()
        metrics = RuntimeMetrics()

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
                f"Starting advert statistics collection tasks={len(tasks)}"
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
            ).info(
                f"Advert statistics finished. rows={len(clean_results)} metrics={metrics.snapshot()}"
            )
            return clean_results

    async def _fetch_advert_spend(self, date_from: str = None, date_to: str = None) -> list:
        if date_from is None:
            date_from = date_to = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        runtime_config = HttpRuntimeConfig(global_concurrency_limit=self.max_concurrent)
        limiter = ConcurrencyLimiter(runtime_config.global_concurrency_limit)
        retry_policy = RetryPolicy()
        metrics = RuntimeMetrics()

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
                    metrics=metrics,
                )
                try:
                    tasks.append(client.get_advert_spend(date_from, date_to))
                except KeyError:
                    bind_context(task_name="advert_spend", account=account).warning(
                        "Missing account key for advert spend"
                    )

            bind_context(task_name="advert_spend").info(
                f"Starting advert spend collection tasks={len(tasks)}"
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
            ).info(
                f"Advert spend finished. rows={len(clean_results)} metrics={metrics.snapshot()}"
            )
            return clean_results
