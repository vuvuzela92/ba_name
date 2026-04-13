# импорт внутренних модулей
# Для работы с ценами
from src.price.price_server import fetch_get_price
from src.price.price_processor import process_prices, get_nm_ids
# Для работы с гугл таблицами
from src.core.my_gspread import GoogleTabs
from src.core.config import google_tabs
# импорт внешних библиотек
import asyncio
import gspread
from datetime import datetime

def get_current_prices():
    nm_ids = get_nm_ids()
    data = asyncio.run(fetch_get_price(nm_ids))
    df = process_prices(data)
    # Создаем соединение с гугл-таблицей
    google_table = google_tabs.get("ba_name").get("title")
    table_sheet = google_tabs.get("ba_name").get("current_price")
    df['upd_time'] = datetime.now().strftime('%d/%m/%Y, %H:%M:%S')
    # Создаем соединение с гугл-таблицей
    try:
        # Создаем соединение с гугл-таблицей
        google_connect = GoogleTabs(table_title=google_table, sheet_title=table_sheet)
        # Вставляем данные в гугл-таблицу
        google_connect._send_df_to_google(df, google_connect.sheet_title)
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Не найдена таблица {google_table}")
    except gspread.exceptions.WorksheetNotFound as e:
        print(f"Не найден лист {table_sheet} в таблице {google_table}")
    except StopIteration:
        print(f"Не найден лист {table_sheet} в таблице {google_table}")
    except RuntimeError as e:
        print(f"Ошибка подключения: {e}")   