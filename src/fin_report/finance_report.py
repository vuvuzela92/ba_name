from src.core.wb_client import WildberriesClient
from datetime import datetime, timedelta
import asyncio

class FinRep(WildberriesClient):
    def __init__(self, api_key, session, account, timeout=30):
        super().__init__(api_key, session, account, timeout)

    async def get_fin_report_daily(self, date_from: datetime = None, date_to: datetime = None):
        """ Получения данных ежедневного финансового отчета"""
        url = "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod"
            #  указатель на последнюю обработанную строку, используется для пагинации.
        rrdid = 0
        # список, в который будут складываться данные
        all_data = []    

        # Если даты не переданы — используем вчера
        if date_from is None:
            date_from = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        if date_to is None:
            date_to = date_from

        limit = 100000
        rrdid = 0

        while True:  # Бесконечный цикл, пока не сработает break
                params = {
                    "dateFrom": date_from, 
                    "dateTo": date_to,
                    "limit": limit,
                    "rrdid": rrdid,
                    "period": "weekly"
                }

                print(f"📡 [{self.account}] Запрос части отчета с rrdid={rrdid}...")

                res = await self._make_aiohttp_request("GET", url, params=params, delay=60)

                # 3. Обработка результата
                if res is None:
                    print(f"⚠️ {self.account} Данные не получены или отчет пуст.")
                    # Если запрос вернул None после всех попыток — прерываем всё
                    break
                # Проверяем, что res - это список (JSON массив)
                if not isinstance(res, list) or len(res) == 0:
                    print(f"🏁 [{self.account}] Все данные успешно собраны.")
                    break
                
                # Добавляем данные об аккаунте в результат
                for r in res:
                    r['account'] = self.account                

                all_data.extend(res)

                print(f"✅ Получено {len(res)} строк. Всего: {len(all_data)}")

                # Обновляем rrdid из ПОСЛЕДНЕЙ строки полученных данных
                rrdid = res[-1].get('rrd_id')
                
                # Если данных пришло меньше лимита — это была последняя страница
                if len(res) < limit:
                    break
                    
                # Если строк ровно 100 000, значит есть еще.
                # Но помним про лимит 1 запрос в минуту!
                print("⏳ Ждем минуту перед следующей порцией (лимит API)...")
                await asyncio.sleep(60)
                    
        return all_data