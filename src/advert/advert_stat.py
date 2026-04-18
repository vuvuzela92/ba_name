from src.core.wb_client import WildberriesClient
from datetime import datetime, timedelta

class WbAdverStat(WildberriesClient):
    """ Класс для работы с рекламными методами ВБ"""
    def __init__(self, api_key, session, account, timeout=30):
        super().__init__(api_key, session, account, timeout)

    async def get_camp_list(self, campaign_status: int = 9)->list:
        """Асинхронный метод получения списка рекламных кампаний.
        Метод должен получить один из статусов РК
        Статусы кампаний:

            -1 — удалена, процесс удаления будет завершён в течение 10 минут
            4 — готова к запуску
            7 — завершена
            8 — отменена
            9 — активна
            11 — на паузе"""
        
        url = "https://advert-api.wildberries.ru/api/advert/v2/adverts"
        params = {"statuses": campaign_status} 
        # Делаем запрос. Метод возвращает список [{}, {}...]
        res = await self._make_aiohttp_request("GET", url, params=params, delay=1.1)

        adverts = res.get("adverts")
        for advert in adverts:
            advert["account"] = self.account # Добавляю инфо о кабинете в данные отвтет

        return adverts
    
    async def get_advert_stat(self, camp_batch_list: list, begin_date = None, end_date = None):
        """ Асинхронный метод получения данных по статистике РК.
        Параметр ids: ID кампаний, максимум 50 значений """
        # Счетчик для подсчета количества полученных с ВБ данных по батчам
        counter = 0
        if begin_date is None:
                begin_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        # Если конечная дата не передана аргументом, то запрашиваем один день
        if end_date is None:
            end_date = begin_date
        url = "https://advert-api.wildberries.ru/adv/v3/fullstats"
        params = {"ids": ",".join(map(str, camp_batch_list)),
                  "beginDate": begin_date,
                  "endDate": end_date} 
        # Делаем запрос. Метод возвращает список [{}, {}...]
        res = await self._make_aiohttp_request("GET", url, params=params, delay=20.1)

        if res and isinstance(res, list):
                for advert in res:
                    advert["account"] = self.account
                counter+=1
                print(f"Получен {counter}-й набор данных по кабинету {self.account} за {begin_date} число")
                return res
        return []
    
    async def get_advert_spend(self, date_from: str = None, date_to: str = None):
        """Асинхронный метод для получения данных по рекламным затратам"""
        if date_from is None:
                date_from = date_to = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d") 

        url = "https://advert-api.wildberries.ru/adv/v1/upd"    
        params = {
                  "from" : date_from,
                  "to": date_to
                  }
        # Делаем запрос. Метод возвращает список [{}, {}...]
        res = await self._make_aiohttp_request("GET", url, params=params, delay=1.1)

        if res and isinstance(res, list):
            for advert in res:
                advert['account'] = self.account
            return res
        return []
    
   
    