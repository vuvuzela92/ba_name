# Импорт внешних библиотек
import pandas as pd
from datetime import datetime

class AdvertProcessor:
    """Класс для обработки сырых рекламных данных"""
    def __init__(self):
        # Словарь-роутер для разных типов данных
        self._processors = {
            "_process_camp_list": self._process_camp_list,
            "_process_advert_stat": self._process_advert_stat,
            "_process_advert_spend": self._process_advert_spend
        }

    # === Обработчики для рекламных методов ===
    def _process_camp_list(self, data: dict = None)->dict:
        """ Метод обрабатывает словарь рекламных кампаний, полученный с ВБ"""
        # Отдельный словарь для ручных РК
        manual_dict = {}
        # Отдельный словарь для РК с единой ставкой
        unified_dict = {}
        # Проходим циклом по каждой РК
        for camp in data:
            # Определяем ключи, которые нужны для получения рекламной статистики
            bid_type = camp.get("bid_type")
            account = camp.get("account", "Unknown")
            camp_id = camp.get("id")

            # Раскидываем РК по словарям
            if bid_type == "manual":
                if account not in manual_dict:
                    manual_dict[account] = []
                manual_dict[account].append(camp_id)
                
            elif bid_type == "unified":
                if account not in unified_dict:
                    unified_dict[account] = []
                unified_dict[account].append(camp_id)
        return manual_dict, unified_dict    
    
    def _process_advert_stat(self, result):
        full_adv_stat = []
        for res in result:
            days = res.get('days', '1970-01-01')  
            advert_id = res.get('advertId', 0)  
            account = res.get('account', 'unknown')
            for day in days:
                adv_stat = {}
                # Сразу присваиваем аккаунт и advert_id
                adv_stat['account'] = account.upper()
                adv_stat['advert_id'] = advert_id
                # Находим артикул для РК
                nm_id = day.get('apps', 'no apps')[0].get('nms', 'no nms')[0].get('nmId', 'no nmId')
                # Добавляем в словарь артикул и дату
                adv_stat['nm_id'] = nm_id   
                adv_stat['date'] = day.get('date', 0)
                # Находим и вносим в словарь метрики  
                adv_stat['atbs'] = day.get('atbs', 0) # Добавление в корзину
                adv_stat['canceled'] = day.get('canceled', 0)
                adv_stat['clicks'] = day.get('clicks', 0)
                adv_stat['cpc'] = day.get('cpc', 0)
                adv_stat['cr'] = day.get('cr', 0)
                adv_stat['ctr'] = day.get('ctr', 0)
                adv_stat['orders'] = day.get('orders', 0)
                adv_stat['shks'] = day.get('shks', 0)
                adv_stat['sum_spend'] = day.get('sum', 0)
                adv_stat['sum_price'] = day.get('sum_price', 0)
                adv_stat['views'] = day.get('views', 0)
                full_adv_stat.append(adv_stat)
        if full_adv_stat:
            df = pd.DataFrame(full_adv_stat).sort_values(['date','clicks'], ascending=[True, False])
            df['date'] = pd.to_datetime(df['date']).dt.date
            df['updated_at'] = (datetime.now()).strftime("%Y-%m-%d %H:%M")
        else:
            return None
        return df
    
    def _process_advert_spend(self, data_spend: list[dict])->pd.DataFrame:
        """Обработка данных по рекламным затратам.
        data_spend: список словарей
        """
        if data_spend and isinstance(data_spend, list):
            try:
                df = pd.DataFrame(data_spend)
                # 🔍 Извлекаем первое число из строки
                df['nm_id'] = df['campName'].str.extract(r'(\d+)')
                # Приводим дату к удобному формату
                df['date'] = pd.to_datetime(df['updTime'],
                                        format='ISO8601'
                                        ).dt.date
                df['updated_at'] = (datetime.now()).strftime("%Y-%m-%d %H:%M")
                return df
            except Exception as e:
                print(f"Ошибка {e}")
                return pd.DataFrame()    