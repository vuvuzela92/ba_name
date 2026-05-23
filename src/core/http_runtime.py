import asyncio
import random
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

import aiohttp


@dataclass(slots=True)
class RetryPolicy:
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 30.0
    jitter_ratio: float = 0.2
    retry_statuses: set[int] = field(default_factory=lambda: {429, 500, 502, 503, 504})

    def backoff_with_jitter(self, attempt: int, delay_override: float | None = None) -> float:
        base = delay_override if delay_override is not None else self.base_delay
        backoff = min(base * (2 ** attempt), self.max_delay)
        jitter_window = backoff * self.jitter_ratio
        return max(0.0, backoff + random.uniform(-jitter_window, jitter_window))


@dataclass(slots=True)
class HttpRuntimeConfig:
    request_timeout_sec: float = 30.0
    global_concurrency_limit: int = 16
    connector_limit: int = 100


class ConcurrencyLimiter:
    def __init__(self, limit: int) -> None:
        self._semaphore = asyncio.Semaphore(limit)

    @asynccontextmanager
    async def slot(self):
        async with self._semaphore:
            yield


class SessionManager:
    def __init__(self, config: HttpRuntimeConfig | None = None) -> None:
        self.config = config or HttpRuntimeConfig()
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> aiohttp.ClientSession:
        timeout = aiohttp.ClientTimeout(total=self.config.request_timeout_sec)
        connector = aiohttp.TCPConnector(limit=self.config.connector_limit)
        self._session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        return self._session

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session is not None:
            await self._session.close()
        self._session = None
