"""Read-only слой доступа к подготовленной аналитике в Google Sheets."""

from __future__ import annotations

from typing import Iterable

import pandas as pd

from src.core.config import google_tabs
from src.core.logging_utils import bind_context
from src.core.my_gspread import GoogleTabs

# Для бота используется аналитический лист, а не сырой экспортный лист.
FIN_REPORT_READ_ONLY_SHEET = "Анализ фин отчета арт"


class GoogleSheetsReadService:
    """Загружает DataFrame из настроенных листов Google Sheets.

    Сервис работает только на чтение и безопасен для вызовов из Telegram-бота.
    """

    def __init__(self, project_key: str = "ba_name") -> None:
        tabs = google_tabs.get(project_key, {})
        self._table_title = tabs.get("title")
        self._tabs = tabs

    def get_current_prices(self) -> pd.DataFrame:
        """Возвращает лист текущих цен как DataFrame."""
        return self.read_tab_as_dataframe("current_price")

    def get_advert_stats(self) -> pd.DataFrame:
        """Возвращает лист рекламной статистики как DataFrame."""
        return self.read_tab_as_dataframe("advert_stat")

    def get_fin_report(self) -> pd.DataFrame:
        """Возвращает фин. отчет из отдельного аналитического read-only листа."""
        return self.read_tab_as_dataframe("fin_rep_weekly", sheet_override=FIN_REPORT_READ_ONLY_SHEET)

    def get_unit(self) -> pd.DataFrame:
        """Возвращает лист юнит-экономики как DataFrame (read-only)."""
        return self.read_tab_as_dataframe("unit")

    def read_tab_as_dataframe(self, tab_key: str, sheet_override: str | None = None) -> pd.DataFrame:
        """Читает один лист и нормализует строки в прямоугольный DataFrame."""
        log = bind_context(task_name="analytics_read", endpoint=tab_key)
        sheet_name = sheet_override or self._tabs.get(tab_key)
        if not self._table_title or not sheet_name:
            log.warning("Google tab config is missing")
            return pd.DataFrame()

        try:
            sheet = GoogleTabs(table_title=self._table_title, sheet_title=sheet_name).sheet_title
            values = sheet.get_all_values()
        except Exception as exc:
            log.exception("Google Sheets read failed: {}", exc)
            return pd.DataFrame()

        if not values:
            log.info("Sheet is empty")
            return pd.DataFrame()

        # Заголовок может начинаться не с первой строки, поэтому ищем лучший кандидат.
        header_index = self._detect_header_row(values)
        headers = self._make_unique_headers(values[header_index])
        data_rows = [row for row in values[header_index + 1 :] if any(str(cell).strip() for cell in row)]
        normalized_rows = [self._fit_row(row, len(headers)) for row in data_rows]

        df = pd.DataFrame(normalized_rows, columns=headers)
        log.info("Sheet loaded rows={} cols={} header_row={}", len(df.index), len(df.columns), header_index + 1)
        return df

    @staticmethod
    def _fit_row(row: Iterable[str], target_len: int) -> list[str]:
        """Дополняет или обрезает строку до целевого числа столбцов."""
        row_list = list(row)
        if len(row_list) < target_len:
            return row_list + [""] * (target_len - len(row_list))
        return row_list[:target_len]

    @staticmethod
    def _detect_header_row(values: list[list[str]], scan_rows: int = 10) -> int:
        """Выбирает строку заголовков по максимальной заполненности и уникальности."""
        upper = min(len(values), scan_rows)
        best_idx = 0
        best_score = -1

        for idx in range(upper):
            row = values[idx]
            non_empty = [str(cell).strip() for cell in row if str(cell).strip()]
            if not non_empty:
                continue
            unique_count = len(set(non_empty))
            score = len(non_empty) + unique_count
            if score > best_score:
                best_score = score
                best_idx = idx
        return best_idx

    @staticmethod
    def _make_unique_headers(raw_headers: list[str]) -> list[str]:
        """Делает имена колонок уникальными при дубликатах и пустых ячейках."""
        used: dict[str, int] = {}
        result: list[str] = []

        for idx, header in enumerate(raw_headers, start=1):
            base = str(header).strip() or f"column_{idx}"
            if base not in used:
                used[base] = 1
                result.append(base)
                continue
            used[base] += 1
            result.append(f"{base}_{used[base]}")
        return result
