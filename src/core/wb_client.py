import asyncio
import json as json_lib
from contextlib import asynccontextmanager

import aiohttp

from src.core.http_runtime import ConcurrencyLimiter, RetryPolicy


@asynccontextmanager
async def _noop_async_context():
    yield


class WildberriesClient:
    """Класс для работы с API WB."""

    def __init__(
        self,
        api_key,
        session: aiohttp.ClientSession,
        account: str,
        timeout: int = 30,
        retry_policy: RetryPolicy | None = None,
        limiter: ConcurrencyLimiter | None = None,
    ) -> None:
        self.api_key = api_key
        self.session = session
        self.account = account
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.headers = {"Authorization": self.api_key}
        self.base_url = "https://advert-api.wildberries.ru"
        self.retry_policy = retry_policy or RetryPolicy(max_attempts=3, base_delay=1.0)
        self.limiter = limiter

    async def _make_aiohttp_request(
        self,
        method: str,
        url: str,
        params=None,
        json=None,
        retries: int = 3,
        delay: int = 1,
    ):
        """Унифицированный async-запрос с retry/timeout и обработкой статусов."""
        attempts = max(retries, self.retry_policy.max_attempts)

        for attempt in range(attempts):
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
                        if res.status == 200:
                            return await res.json()

                        error_text = await res.text()
                        try:
                            err_data = json_lib.loads(error_text)
                            detail = err_data.get("detail", error_text)
                        except Exception:
                            detail = error_text

                        if res.status in self.retry_policy.retry_statuses:
                            sleep_for = self.retry_policy.backoff_with_jitter(attempt, float(delay))
                            print(
                                f"⏳ [{self.account}] Статус {res.status} {detail}. "
                                f"Ждем {sleep_for:.2f} сек. (Попытка {attempt + 1})"
                            )
                            await asyncio.sleep(sleep_for)
                            continue

                        if res.status in (400, 401, 403):
                            print(f"⚠️ Ошибка {res.status} для {self.account}: {detail}")
                            return None

                        print(f"❓ Неизвестный статус {res.status} для {self.account}: {detail}")
                        return None

            except Exception as exc:
                print(f"💥 [{self.account}] Сетевая ошибка (попытка {attempt + 1}): {exc}")
                if attempt < attempts - 1:
                    sleep_for = self.retry_policy.backoff_with_jitter(attempt, float(delay))
                    await asyncio.sleep(sleep_for)
                else:
                    return None

        return None
