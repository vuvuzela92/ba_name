from src.core.wb_client import WildberriesClient
import pandas as pd

class PriceWB(WildberriesClient):
    def __init__(self, api_key, session, account, timeout=30):
        super().__init__(api_key, session, account, timeout)

    async def get_price(self, nm_list: list):
        """Функция для получения информации о цене товара"""
        url = 'https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter'
    
        payload = {
            "nmList": nm_list
        }      
        
        try:
            res = await self._make_aiohttp_request("POST", url, json=payload, delay=6)
            
            if res and "data" in res and "listGoods" in res["data"]:
                # Возвращаем именно список объектов товаров
                return res["data"]["listGoods"]
            
            return [] # Возвращаем пустой список, если данных нет
        
        except Exception as e:
            print(f"❌ [{self.account}] Ошибка при получении цен: {e}")
            return []
        


    