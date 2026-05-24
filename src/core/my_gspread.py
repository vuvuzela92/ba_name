"""Класс-утилита для подключения к Google Sheets и записи DataFrame."""

import time
import logging
import gspread
import pandas as pd

from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class GoogleTabs:
    """Класс для подключения к Google-таблице и выбранному листу."""

    def __init__(self, table_title: str, sheet_title: str):
        """Инициализация клиента Google Sheets.

        Args:
            table_title: Название Google-таблицы.
            sheet_title: Название листа внутри таблицы.
        """
        self.creds_file = Path(__file__).resolve().parents[2] / "creds/creds.json"
        self.table_title = table_title
        self.table = None
        self.sheet_title = sheet_title
        self._safe_connect()

    def _safe_connect(self, retries=5, delay=2):
        """Подключение к таблице и листу с повторными попытками при временных ошибках."""
        self.gc = gspread.service_account(filename=self.creds_file)

        for attempt in range(1, retries + 1):
            try:
                # Сначала открываем саму таблицу и сохраняем ссылку для повторного использования.
                table = self.gc.open(self.table_title)
                self.table = table

                # Затем открываем конкретный лист внутри этой таблицы.
                self.sheet_title = table.worksheet(self.sheet_title)

                print(f"✅ Успешное подключение к {self.table_title} -> {self.sheet_title.title}")
                return

            except gspread.exceptions.APIError as e:
                if "503" in str(e):
                    print(f"[Попытка {attempt}/{retries}] APIError 503 — повтор через {delay} сек.")
                    time.sleep(delay)
                else:
                    raise
            except gspread.exceptions.WorksheetNotFound:
                raise RuntimeError(f"Ошибка: Лист '{self.sheet_title}' не найден в таблице '{self.table_title}'")

        raise RuntimeError(f"Не удалось открыть таблицу '{self.table_title}' после {retries} попыток.")

    def _update_df_in_google(self, df: pd.DataFrame, sheet):
        """Полностью перезаписывает лист данными из DataFrame."""
        try:
            # gspread не принимает NaN напрямую, поэтому заменяем на пустые строки.
            df = df.fillna("")

            # Полная очистка листа перед записью нового состояния.
            sheet.clear()

            df_data_to_append = [df.columns.values.tolist()] + df.values.tolist()
            sheet.append_rows(df_data_to_append, value_input_option="USER_ENTERED")
            print("Данные успешно перезаписаны на лист.")

        except Exception as e:
            print(f"Произошла ошибка: {e}")
            if "APIError: [400]: This action would increase the number of cells in the workbook" in str(e):
                print("Превышен лимит ячеек Google Таблицы. Создание резервной копии в Excel...")

    def _send_df_to_google(self, df, sheet):
        """Добавляет DataFrame в лист (append-режим)."""
        try:
            df_data_to_append = [df.columns.values.tolist()] + df.values.tolist()
            existing_data = sheet.get_all_values()

            if len(existing_data) <= 1:
                print("Добавляем заголовки и данные")
                sheet.append_rows(df_data_to_append, value_input_option="USER_ENTERED")
            else:
                print("Добавляем только данные")
                sheet.append_rows(df_data_to_append[1:], value_input_option="USER_ENTERED")

        except Exception as e:
            print(f"An error occurred: {e}")
