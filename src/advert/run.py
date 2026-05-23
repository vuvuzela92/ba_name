from src.advert.processor import AdvertProcessor
from src.advert.advert_service import ScraperAdvert
from src.core.my_gspread import GoogleTabs
from src.core.config import google_tabs
import numpy as np

import gspread
from datetime import datetime, timedelta

async def advert_stat(date_from: str = None, date_to: str = None):
    """ Функция для получения и обработки сведений по рекламным кампаниям, а также их добавлению в гугл таблицы.
    date_from и date_to - необязательные параметры. Передавать нужно в формате '2026-05-01'. Если не передать, то по умолчанию выгрузятся данные за один день. Для получения доступен максимум 31 день."""
    # Получаем данные по рекламным кампаниям в кабинетах
    data = await(ScraperAdvert()._fetch_adv_camp_list())
    # Обрабатываем полученные данные выделяю данные по Единой и Ручной кампаниям в отдельные списки
    manual, unified = AdvertProcessor()._process_camp_list(data)
    # Для удобства списки соединяем в кортеж
    adverts = (manual, unified)

    # Проверяем дату
    if date_from is None:
        date_from = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    if date_to is None:
        date_to = date_from

    # Запрашиваем статистические данные по рекламным кампаниям
    result = await(ScraperAdvert()._fetch_advert_stat(adverts, date_from, date_to))
    # Обрабатываем, приводя в удобный для вставки в гугл таблицы вид
    df = AdvertProcessor()._process_advert_stat(result)  
    # Для вставки в гугл таблицу приводим дату к строковому типу
    df['date'] = df['date'].astype(str)
    # Вычисляю cpm
    df['cpm'] = df['sum_spend'] / df['views'].replace(0, np.nan) * 1000
    df_gs = df[['account', 'advert_id', 'nm_id', 'date', 'atbs', 'canceled', 'clicks', 'cpc', 'cr', 'ctr', 'orders', 'shks', 'sum_spend', 'sum_price', 'views', 'cpm', 'updated_at']]
    # Замена NaN/inf перед запись в гугл таблицу
    df_gs = df_gs.replace([np.inf, -np.inf], np.nan).fillna("")
    # Создаем соединение с гугл-таблицей
    google_table = google_tabs.get("ba_name").get("title")
    table_sheet = google_tabs.get("ba_name").get("advert_stat")
    # Создаем соединение с гугл-таблицей
    try:
        # Создаем соединение с гугл-таблицей
        google_connect = GoogleTabs(table_title=google_table, sheet_title=table_sheet)
        # Вставляем данные в гугл-таблицу
        google_connect._send_df_to_google(df_gs, google_connect.sheet_title)
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Не найдена таблица {google_table}")
    except gspread.exceptions.WorksheetNotFound as e:
        print(f"Не найден лист {table_sheet} в таблице {google_table}")
    except StopIteration:
        print(f"Не найден лист {table_sheet} в таблице {google_table}")
    except RuntimeError as e:
        print(f"Ошибка подключения: {e}")


