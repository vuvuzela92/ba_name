from src.core.wb_client import WildberriesClient
from datetime import datetime, timedelta


class WbAdverStat(WildberriesClient):
    """Класс для работы с рекламными методами WB."""

    def __init__(
        self,
        api_key,
        session,
        account,
        timeout=30,
        retry_policy=None,
        limiter=None,
        metrics=None,
    ):
        super().__init__(
            api_key,
            session,
            account,
            timeout,
            retry_policy=retry_policy,
            limiter=limiter,
            metrics=metrics,
        )

    async def get_camp_list(self, campaign_status: int = 9) -> list:
        """Асинхронный метод получения списка рекламных кампаний."""

        url = "https://advert-api.wildberries.ru/api/advert/v2/adverts"
        params = {"statuses": campaign_status}
        res = await self._make_aiohttp_request("GET", url, params=params, delay=1.1)

        adverts = res.get("adverts")
        for advert in adverts:
            advert["account"] = self.account

        return adverts

    async def get_advert_stat(self, camp_batch_list: list, begin_date=None, end_date=None):
        """Асинхронный метод получения данных по статистике РК."""

        counter = 0
        if begin_date is None:
            begin_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        if end_date is None:
            end_date = begin_date

        url = "https://advert-api.wildberries.ru/adv/v3/fullstats"
        params = {
            "ids": ",".join(map(str, camp_batch_list)),
            "beginDate": begin_date,
            "endDate": end_date,
        }
        res = await self._make_aiohttp_request("GET", url, params=params, delay=20.1)

        if res and isinstance(res, list):
            for advert in res:
                advert["account"] = self.account
            counter += 1
            print(f"Получен {counter}-й набор данных по кабинету {self.account} за {begin_date} число")
            return res
        return []

    async def get_advert_spend(self, date_from: str = None, date_to: str = None):
        """Асинхронный метод для получения данных по рекламным затратам."""

        if date_from is None:
            date_from = date_to = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        url = "https://advert-api.wildberries.ru/adv/v1/upd"
        params = {
            "from": date_from,
            "to": date_to,
        }
        res = await self._make_aiohttp_request("GET", url, params=params, delay=1.1)

        if res and isinstance(res, list):
            for advert in res:
                advert["account"] = self.account
            return res
        return []


