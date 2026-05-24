import asyncio
import random
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

import aiohttp

from src.core.runtime_config import HttpSettings, load_runtime_settings


@dataclass(slots=True)
class RetryPolicy:
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 30.0
    jitter_ratio: float = 0.2
    retry_statuses: set[int] = field(default_factory=lambda: {429, 500, 502, 503, 504})

    @classmethod
    def from_http_settings(cls, settings: HttpSettings) -> "RetryPolicy":
        return cls(
            max_attempts=settings.retry_max_attempts,
            base_delay=settings.retry_base_delay,
            max_delay=settings.retry_max_delay,
            jitter_ratio=settings.retry_jitter_ratio,
            retry_statuses=set(settings.retry_statuses),
        )

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

    @classmethod
    def from_http_settings(cls, settings: HttpSettings) -> "HttpRuntimeConfig":
        return cls(
            request_timeout_sec=settings.request_timeout_sec,
            global_concurrency_limit=settings.global_concurrency_limit,
            connector_limit=settings.connector_limit,
        )

    @classmethod
    def from_runtime_defaults(cls) -> "HttpRuntimeConfig":
        return cls.from_http_settings(load_runtime_settings().http)


@dataclass(slots=True)
class RuntimeMetrics:
    requests_total: int = 0
    requests_failed: int = 0
    retries_total: int = 0
    status_429_total: int = 0
    exceptions_total: int = 0
    duration_total_ms: float = 0.0
    avg_duration_ms: float = 0.0
    max_duration_ms: float = 0.0

    def observe_request(self, duration_ms: float, status_code: int | None = None, success: bool = True) -> None:
        self.requests_total += 1
        if not success:
            self.requests_failed += 1
        if status_code == 429:
            self.status_429_total += 1
        self.duration_total_ms += duration_ms
        if duration_ms > self.max_duration_ms:
            self.max_duration_ms = duration_ms
        self.avg_duration_ms = self.duration_total_ms / max(1, self.requests_total)

    def observe_retry(self) -> None:
        self.retries_total += 1

    def observe_exception(self) -> None:
        self.exceptions_total += 1

    def snapshot(self) -> dict:
        return {
            "requests_total": self.requests_total,
            "requests_failed": self.requests_failed,
            "retries_total": self.retries_total,
            "status_429_total": self.status_429_total,
            "exceptions_total": self.exceptions_total,
            "duration_total_ms": round(self.duration_total_ms, 2),
            "avg_duration_ms": round(self.avg_duration_ms, 2),
            "max_duration_ms": round(self.max_duration_ms, 2),
        }


class ConcurrencyLimiter:
    def __init__(self, limit: int) -> None:
        self._semaphore = asyncio.Semaphore(limit)

    @asynccontextmanager
    async def slot(self):
        async with self._semaphore:
            yield


class SessionManager:
    def __init__(self, config: HttpRuntimeConfig | None = None) -> None:
        self.config = config or HttpRuntimeConfig.from_runtime_defaults()
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
