from src.core.http_runtime import (
    ConcurrencyLimiter,
    HttpRuntimeConfig,
    RetryPolicy,
    RuntimeMetrics,
    SessionManager,
)
from src.core.logging_utils import bind_context
from src.core.runtime_config import load_runtime_settings
from src.core.utils_general import load_api_tokens
from src.price.price import PriceWB
import asyncio


async def fetch_get_price(nm_list: list):
    tokens = load_api_tokens()
    all_tasks = []

    runtime_settings = load_runtime_settings()
    runtime_config = HttpRuntimeConfig.from_http_settings(runtime_settings.http)
    limiter = ConcurrencyLimiter(runtime_config.global_concurrency_limit)
    retry_policy = RetryPolicy.from_http_settings(runtime_settings.http)
    metrics = RuntimeMetrics()

    async with SessionManager(runtime_config) as session:
        for name, token in tokens.items():
            client = PriceWB(
                token,
                session,
                name,
                timeout=int(runtime_config.request_timeout_sec),
                retry_policy=retry_policy,
                limiter=limiter,
                metrics=metrics,
            )
            all_tasks.append(client.get_price(nm_list))

        bind_context(task_name="get_current_prices").info(
            f"Starting price collection for {len(all_tasks)} accounts"
        )
        results = await asyncio.gather(*all_tasks, return_exceptions=True)

    all_final_results = []
    failed_tasks = 0

    for idx, res in enumerate(results):
        if isinstance(res, Exception):
            failed_tasks += 1
            bind_context(task_name="get_current_prices", attempt=idx + 1).exception(
                f"Task failed: {res}"
            )
        elif isinstance(res, list):
            all_final_results.extend(res)

    succeeded_tasks = len(results) - failed_tasks
    bind_context(
        task_name="get_current_prices",
        failed_tasks=failed_tasks,
        succeeded_tasks=succeeded_tasks,
    ).info(
        f"Price collection finished. rows={len(all_final_results)} metrics={metrics.snapshot()}"
    )
    return all_final_results
