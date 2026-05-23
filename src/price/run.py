# импорт внутренних модулей
from src.price.price_server import fetch_get_price
from src.price.price_processor import process_prices, get_nm_ids
from src.core.my_gspread import GoogleTabs
from src.core.config import google_tabs

# импорт внешних библиотек
import asyncio
import gspread
from datetime import datetime


def _column_to_letter(col_idx: int) -> str:
    letters = ""
    while col_idx > 0:
        col_idx, rem = divmod(col_idx - 1, 26)
        letters = chr(65 + rem) + letters
    return letters


def _replace_sheet_data(df, sheet) -> None:
    """Полностью обновляет диапазон выгрузки (без append) для текущих цен."""
    df_to_write = df.fillna("")
    last_col = _column_to_letter(len(df_to_write.columns))
    clear_range = f"A1:{last_col}{sheet.row_count}"

    print(f"Очищаем диапазон: {clear_range}")
    sheet.batch_clear([clear_range])

    values = [df_to_write.columns.values.tolist()] + df_to_write.values.tolist()
    print(f"Записываем данные в диапазон: A1:{last_col}{len(values)}")
    sheet.update("A1", values, value_input_option="USER_ENTERED")


def get_current_prices():
    return asyncio.run(get_current_prices_async())


async def get_current_prices_async():
    nm_ids = get_nm_ids()
    data = await fetch_get_price(nm_ids)
    df = process_prices(data)

    google_table = google_tabs.get("ba_name").get("title")
    table_sheet = google_tabs.get("ba_name").get("current_price")
    df["upd_time"] = datetime.now().strftime("%d/%m/%Y, %H:%M:%S")

    try:
        google_connect = GoogleTabs(table_title=google_table, sheet_title=table_sheet)
        _replace_sheet_data(df, google_connect.sheet_title)
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Не найдена таблица {google_table}")
    except gspread.exceptions.WorksheetNotFound:
        print(f"Не найден лист {table_sheet} в таблице {google_table}")
    except StopIteration:
        print(f"Не найден лист {table_sheet} в таблице {google_table}")
    except RuntimeError as e:
        print(f"Ошибка подключения: {e}")
