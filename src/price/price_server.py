from src.core.http_runtime import ConcurrencyLimiter, HttpRuntimeConfig, RetryPolicy, SessionManager
from src.core.utils_general import load_api_tokens
from src.price.price import PriceWB
import asyncio


async def fetch_get_price(nm_list: list):
    tokens = load_api_tokens()
    all_tasks = []

    runtime_config = HttpRuntimeConfig()
    limiter = ConcurrencyLimiter(runtime_config.global_concurrency_limit)
    retry_policy = RetryPolicy()

    async with SessionManager(runtime_config) as session:
        for name, token in tokens.items():
            client = PriceWB(
                token,
                session,
                name,
                timeout=int(runtime_config.request_timeout_sec),
                retry_policy=retry_policy,
                limiter=limiter,
            )
            task = client.get_price(nm_list)
            all_tasks.append(task)

        print(f"🚀 Запускаем сбор для {len(all_tasks)} аккаунтов...")
        results = await asyncio.gather(*all_tasks, return_exceptions=True)

    all_final_results = []

    for res in results:
        if isinstance(res, Exception):
            print(f"💥 Критическая ошибка в одной из задач: {res}")
        elif isinstance(res, list):
            all_final_results.extend(res)
        elif res is None:
            continue

    print(f"🏁 Сбор завершен. Итого получено цен для {len(all_final_results)} позиций.")
    return all_final_results
