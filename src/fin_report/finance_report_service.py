from src.core.http_runtime import (
    ConcurrencyLimiter,
    HttpRuntimeConfig,
    RetryPolicy,
    RuntimeMetrics,
    SessionManager,
)
from src.core.logging_utils import bind_context
from src.core.utils_general import load_api_tokens
from src.fin_report.finance_report import FinRep
import asyncio
from datetime import datetime, timedelta


async def fetch_fin_reps_weekly(count_weeks: int):
    today = datetime.today()
    weekday = today.weekday()
    base_sunday = today - timedelta(days=(weekday + 1))
    tokens = load_api_tokens()

    all_tasks = []

    runtime_config = HttpRuntimeConfig()
    limiter = ConcurrencyLimiter(runtime_config.global_concurrency_limit)
    retry_policy = RetryPolicy()
    metrics = RuntimeMetrics()

    async with SessionManager(runtime_config) as session:
        for week in range(count_weeks):
            target_sunday = base_sunday - timedelta(weeks=week)
            target_monday = target_sunday - timedelta(days=6)

            date_from = target_monday.strftime("%Y-%m-%d")
            date_to = target_sunday.strftime("%Y-%m-%d")

            for name, token in tokens.items():
                client = FinRep(
                    token,
                    session,
                    name,
                    timeout=int(runtime_config.request_timeout_sec),
                    retry_policy=retry_policy,
                    limiter=limiter,
                    metrics=metrics,
                )
                all_tasks.append(client.get_fin_report_daily(date_from, date_to))

        bind_context(task_name="fin_rep_weekly").info(
            f"Starting fin report collection tasks={len(all_tasks)}"
        )
        results = await asyncio.gather(*all_tasks, return_exceptions=True)

    all_final_results = []
    failed_tasks = 0

    for idx, res in enumerate(results):
        if isinstance(res, Exception):
            failed_tasks += 1
            bind_context(task_name="fin_rep_weekly", attempt=idx + 1).exception(
                f"Task failed: {res}"
            )
        elif res is not None:
            all_final_results.extend(res)

    succeeded_tasks = len(results) - failed_tasks
    bind_context(
        task_name="fin_rep_weekly",
        failed_tasks=failed_tasks,
        succeeded_tasks=succeeded_tasks,
    ).info(
        f"Fin report collection finished. rows={len(all_final_results)} metrics={metrics.snapshot()}"
    )
    return all_final_results
