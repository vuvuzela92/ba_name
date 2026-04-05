import requests
import json
from time import time, sleep
from datetime import datetime, timedelta
import asyncio
import aiohttp
import os
from pathlib import Path


class WildberriesClient:
    """ Класс для работы с АПИ ВБ"""
    def __init__(self, api_key, session: aiohttp.ClientSession, account: str, timeout=30):
        self.api_key = api_key 
        self.session = session 
        self.account = account 
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.headers = {"Authorization": self.api_key}
        self.base_url = "https://advert-api.wildberries.ru" # Базовый URL 

    async def _make_aiohttp_request(self, method: str, url: str, params=None, json=None, retries: int = 3, delay: int = 1):  
        """ Метод, который позволяет генрить асинхронные запросы в зависимости от метода и передаваемы параметров, а так же обрабатывающий ошибки в зависимости от статус-кода ответа"""      
        for attempt in range(retries):
            try:
                async with self.session.request(
                    method, 
                    url, 
                    headers=self.headers, 
                    params=params, 
                    json=json,
                    timeout=self.timeout
                ) as res:
                    
                    # 1. Если всё хорошо (200)
                    if res.status == 200:
                        data = await res.json()
                        return data
                    
                    # 2. Если ошибка — сначала получаем текст ответа (это всегда безопасно)
                    error_text = await res.text()
                    
                    # Пытаемся распарсить как JSON для логов, если не выйдет — оставляем текст
                    try:
                        # Декодируем текст в json вручную, если это возможно
                        err_data = json.loads(error_text)
                        detail = err_data.get('detail', error_text)
                    except:
                        detail = error_text

                    # 3. Логика повторов (Retry)
                    if res.status == 429 or res.status >= 500:
                        print(f"⏳ [{self.account}] Статус {res.status} {detail}. Ждем {delay} сек. (Попытка {attempt+1})")
                        await asyncio.sleep(delay * (attempt+1))
                        continue 

                    # 4. Критические ошибки (не требующие повтора)
                    elif res.status in (400, 401, 403):
                        print(f"⚠️ Ошибка {res.status} для {self.account}: {detail}")
                        return None
                    
                    else:
                        print(f"❓ Неизвестный статус {res.status} для {self.account}: {detail}")
                        return None
                        
            except Exception as e:
                print(f"💥 [{self.account}] Сетевая ошибка (попытка {attempt + 1}): {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(delay)
                else:
                    return None
                    
        return None
