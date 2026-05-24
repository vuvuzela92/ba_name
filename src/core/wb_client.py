import asyncio
import json as json_lib
from contextlib import asynccontextmanager
from time import perf_counter
from urllib.parse import urlparse

import aiohttp

from src.core.http_runtime import ConcurrencyLimiter, RetryPolicy, RuntimeMetrics
from src.core.logging_utils import bind_context
from src.core.runtime_config import load_runtime_settings


@asynccontextmanager
async def _noop_async_context():
    yield


class WildberriesClient:
    """Base WB API client with retry, limiter, and structured logging."""

    def __init__(
        self,
        api_key,
        session: aiohttp.ClientSession,
        account: str,
        timeout: int = 30,
        retry_policy: RetryPolicy | None = None,
        limiter: ConcurrencyLimiter | None = None,
        metrics: RuntimeMetrics | None = None,
    ) -> None:
        self.api_key = api_key
        self.session = session
        self.account = account
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.headers = {"Authorization": self.api_key}
        self.base_url = "https://advert-api.wildberries.ru"
        default_retry = RetryPolicy.from_http_settings(load_runtime_settings().http)
        self.retry_policy = retry_policy or default_retry
        self.limiter = limiter
        self.metrics = metrics

    async def _make_aiohttp_request(
        self,
        method: str,
        url: str,
        params=None,
        json=None,
        retries: int = 3,
        delay: int = 1,
    ):
        attempts = max(retries, self.retry_policy.max_attempts)
        endpoint = urlparse(url).path

        for attempt in range(attempts):
            started = perf_counter()
            try:
                lock_ctx = self.limiter.slot() if self.limiter else _noop_async_context()
                async with lock_ctx:
                    async with self.session.request(
                        method,
                        url,
                        headers=self.headers,
                        params=params,
                        json=json,
                        timeout=self.timeout,
                    ) as res:
                        duration_ms = (perf_counter() - started) * 1000

                        if res.status == 200:
                            if self.metrics:
                                self.metrics.observe_request(duration_ms, status_code=200, success=True)
                            bind_context(
                                task_name="http_request",
                                endpoint=endpoint,
                                account=self.account,
                                status_code=res.status,
                                attempt=attempt + 1,
                                duration_ms=round(duration_ms, 2),
                                retries=attempt,
                            ).info("HTTP request succeeded")
                            return await res.json()

                        error_text = await res.text()
                        try:
                            err_data = json_lib.loads(error_text)
                            detail = err_data.get("detail", error_text)
                        except Exception:
                            detail = error_text

                        if res.status in self.retry_policy.retry_statuses:
                            sleep_for = self.retry_policy.backoff_with_jitter(attempt, float(delay))
                            if self.metrics:
                                self.metrics.observe_request(duration_ms, status_code=res.status, success=False)
                                self.metrics.observe_retry()
                            bind_context(
                                task_name="http_retry",
                                endpoint=endpoint,
                                account=self.account,
                                status_code=res.status,
                                attempt=attempt + 1,
                                duration_ms=round(duration_ms, 2),
                                retries=attempt + 1,
                                retry_sleep=round(sleep_for, 2),
                            ).warning(f"Retry scheduled: {detail}")
                            await asyncio.sleep(sleep_for)
                            continue

                        if self.metrics:
                            self.metrics.observe_request(duration_ms, status_code=res.status, success=False)
                        bind_context(
                            task_name="http_request",
                            endpoint=endpoint,
                            account=self.account,
                            status_code=res.status,
                            attempt=attempt + 1,
                            duration_ms=round(duration_ms, 2),
                            retries=attempt,
                        ).error(f"HTTP request failed without retry: {detail}")
                        return None

            except Exception as exc:
                duration_ms = (perf_counter() - started) * 1000
                if self.metrics:
                    self.metrics.observe_exception()
                if attempt < attempts - 1:
                    sleep_for = self.retry_policy.backoff_with_jitter(attempt, float(delay))
                    if self.metrics:
                        self.metrics.observe_retry()
                    bind_context(
                        task_name="http_retry",
                        endpoint=endpoint,
                        account=self.account,
                        attempt=attempt + 1,
                        duration_ms=round(duration_ms, 2),
                        retries=attempt + 1,
                        retry_sleep=round(sleep_for, 2),
                    ).warning(f"Retry after exception: {exc}")
                    await asyncio.sleep(sleep_for)
                else:
                    bind_context(
                        task_name="http_request",
                        endpoint=endpoint,
                        account=self.account,
                        attempt=attempt + 1,
                        duration_ms=round(duration_ms, 2),
                        retries=attempt,
                    ).exception(f"Retries exhausted: {exc}")
                    return None

        bind_context(
            task_name="http_request",
            endpoint=endpoint,
            account=self.account,
            retries=attempts,
        ).error("HTTP request exhausted all retry attempts")
        return None
