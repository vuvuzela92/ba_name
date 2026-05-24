"""Сервис подготовки пользовательских текстов для команд Telegram-бота."""

from __future__ import annotations

from dataclasses import asdict

import pandas as pd

from src.analytics import AnalyticsQueryService, DailyReportService
from src.bot.daily_report_formatter import DailyReportFormatter


class BotCommandService:
    """Готовит тексты ответов команд на основе read-only аналитики.

    Сервис не отправляет сообщения сам, только формирует payload.
    """

    def __init__(self, analytics_service: AnalyticsQueryService | None = None) -> None:
        self._analytics = analytics_service or AnalyticsQueryService()
        self._daily_report_service = DailyReportService(query_service=self._analytics)
        self._daily_report_formatter = DailyReportFormatter()

    def get_prices_text(self) -> str:
        """Возвращает компактную таблицу с текущими ценами."""
        return self._render_table_preview(
            self._analytics.get_current_prices(limit=10),
            "Текущие цены",
            limit=10,
        )

    def get_ad_stats_text(self) -> str:
        """Возвращает человекочитаемую аналитику по товарам и рекламе."""
        df = self._analytics.get_advert_stats()
        if df.empty:
            return "Рекламная статистика: данных нет."
        return self._render_advert_cards(df, top_n=7)

    def get_fin_report_text(self) -> str:
        """Возвращает компактный фин. отчёт: summary + TOP записи.

        Полная таблица намеренно не отправляется, чтобы не превышать лимиты Telegram.
        """
        compact = self._analytics.get_fin_report_compact(top_n=10)
        total_records = compact["total_records"]
        if total_records == 0:
            return "Финансовый отчет: данных нет."

        top_df = compact["top"]
        lines = [
            "Финансовый отчет (кратко):",
            f"Записей: {total_records}",
            f"Сумма продаж: {compact['sum_sales']:.2f}",
            f"К перечислению: {compact['sum_payout']:.2f}",
            f"Штрафы: {compact['sum_penalty']:.2f}",
            "",
            "TOP 10 записей по продажам:",
        ]
        lines.append(top_df.to_string(index=False) if not top_df.empty else "Нет строк для TOP 10")
        return "\n".join(lines)

    def get_top_products_text(self) -> str:
        """Возвращает топ товаров, отсортированный по выручке."""
        items = self._analytics.get_top_products(top_n=10)
        if not items:
            return "Топ товаров: данных недостаточно."
        lines = ["<b>📈 Топ товаров</b>"]
        for idx, item in enumerate(items, start=1):
            lines.append(
                f"{idx}. <b>Артикул {item.nm_id}</b> ({item.account})\n"
                f"• Выручка: <b>{self._fmt_money(item.revenue)}</b>\n"
                f"• Заказы: <b>{self._fmt_int(item.orders)}</b>\n"
                f"• Расход: {self._fmt_money(item.spend)}"
            )
        return "\n".join(lines)

    def get_problem_products_text(self) -> str:
        """Возвращает товары с низкой эффективностью по текущим порогам."""
        items = self._analytics.get_problem_products(min_spend=500.0, max_orders=0.0, max_cr=0.5, top_n=10)
        if not items:
            return "Проблемные товары: не найдено по текущим порогам."
        lines = ["<b>⚠️ Проблемные товары</b>"]
        for idx, item in enumerate(items, start=1):
            status = "🔴 высокая ДРР / слабая конверсия"
            lines.append(
                f"{idx}. <b>Артикул {item.nm_id}</b> ({item.account})\n"
                f"• Расход: {self._fmt_money(item.spend)}\n"
                f"• Заказы: {self._fmt_int(item.orders)}\n"
                f"• CR: {self._fmt_percent(item.cr)}\n"
                f"• Статус: {status}"
            )
        return "\n".join(lines)

    def get_summary_text(self) -> str:
        """Возвращает расширенную ежедневную сводку (MVP Daily Analytics Report)."""
        report = self._daily_report_service.build_daily_report()
        return self._daily_report_formatter.format(report)

    @staticmethod
    def _render_table_preview(df, title: str, limit: int = 10) -> str:
        """Рендерит превью DataFrame в HTML-блоке <pre> для читаемости."""
        if df is None or df.empty:
            return f"{title}: данных нет."
        preview = df.head(limit).to_string(index=False)
        return f"{title} (показаны первые {min(len(df), limit)} строк):\n<pre>{preview}</pre>"

    def _render_advert_cards(self, df: pd.DataFrame, top_n: int = 7) -> str:
        """Формирует карточки аналитики по артикулам без raw-таблиц."""
        nm_col = self._find_column(df, ["nm_id", "nmId", "Артикул WB"])
        account_col = self._find_column(df, ["account", "Аккаунт"])
        clicks_col = self._find_column(df, ["clicks"])
        views_col = self._find_column(df, ["views"])
        spend_col = self._find_column(df, ["sum_spend", "spend", "Расход"])
        orders_col = self._find_column(df, ["orders", "Количество заказов"])
        revenue_col = self._find_column(df, ["sum_price", "revenue", "Выручка"])
        ctr_col = self._find_column(df, ["ctr", "CTR"])
        cpc_col = self._find_column(df, ["cpc", "CPC"])
        cpm_col = self._find_column(df, ["cpm", "CPM"])
        cr_col = self._find_column(df, ["cr", "CR"])
        avg_pos_col = self._find_column(df, ["avg_position", "position", "Средняя позиция"])

        required = [nm_col, account_col, spend_col, orders_col, revenue_col]
        if not all(required):
            return "Рекламная статистика: недостаточно полей для построения аналитических карточек."

        prepared = df.assign(
            _clicks=self._to_num(df[clicks_col]) if clicks_col else 0.0,
            _views=self._to_num(df[views_col]) if views_col else 0.0,
            _spend=self._to_num(df[spend_col]),
            _orders=self._to_num(df[orders_col]),
            _revenue=self._to_num(df[revenue_col]),
            _ctr=self._to_num(df[ctr_col]) if ctr_col else 0.0,
            _cpc=self._to_num(df[cpc_col]) if cpc_col else 0.0,
            _cpm=self._to_num(df[cpm_col]) if cpm_col else 0.0,
            _cr=self._to_num(df[cr_col]) if cr_col else 0.0,
            _avg_pos=self._to_num(df[avg_pos_col]) if avg_pos_col else 0.0,
        )

        grouped = (
            prepared.groupby([account_col, nm_col], as_index=False)[
                ["_clicks", "_views", "_spend", "_orders", "_revenue", "_ctr", "_cpc", "_cpm", "_cr", "_avg_pos"]
            ]
            .mean()
            .sort_values(["_spend", "_revenue"], ascending=[False, False])
            .head(top_n)
        )

        lines = [
            "<b>📣 Рекламная аналитика по товарам</b>",
            f"<i>Показаны ключевые товары: {len(grouped)}</i>",
            "",
        ]
        for _, row in grouped.iterrows():
            drr = (float(row["_spend"]) / float(row["_revenue"]) * 100.0) if float(row["_revenue"]) > 0 else 0.0
            status = self._build_status(drr=drr, ctr=float(row["_ctr"]), orders=float(row["_orders"]))
            lines.extend(
                [
                    f"<b>📦 Артикул {row[nm_col]}</b> ({row[account_col]})",
                    "<b>📣 Реклама</b>",
                    f"• Показы: {self._fmt_int(row['_views'])}",
                    f"• Клики: {self._fmt_int(row['_clicks'])}",
                    f"• CTR: {self._fmt_percent(row['_ctr'])}",
                    f"• CPC: {self._fmt_money(row['_cpc'])}",
                    f"• CPM: {self._fmt_money(row['_cpm'])}",
                    "<b>🛒 Продажи</b>",
                    f"• Заказы: {self._fmt_int(row['_orders'])}",
                    f"• Выручка: {self._fmt_money(row['_revenue'])}",
                    f"• Конверсия: {self._fmt_percent(row['_cr'])}",
                    "<b>📈 Эффективность</b>",
                    f"• Средняя позиция: {self._fmt_position(row['_avg_pos'])}",
                    f"• ДРР: {self._fmt_percent(drr)}",
                    f"• Статус: {status}",
                    "────────────",
                ]
            )
        return "\n".join(lines).rstrip("─\n")

    @staticmethod
    def _find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
        normalized = {str(col).strip().lower(): col for col in df.columns}
        for candidate in candidates:
            hit = normalized.get(candidate.strip().lower())
            if hit is not None:
                return hit
        return None

    @staticmethod
    def _to_num(series: pd.Series) -> pd.Series:
        s = series.astype(str)
        s = s.str.replace("\xa0", "", regex=False)
        s = s.str.replace(" ", "", regex=False)
        s = s.str.replace(",", ".", regex=False)
        return pd.to_numeric(s, errors="coerce").fillna(0.0)

    @staticmethod
    def _fmt_int(value: float) -> str:
        return f"{int(round(float(value))):,}".replace(",", " ")

    @staticmethod
    def _fmt_money(value: float) -> str:
        return f"{int(round(float(value))):,}".replace(",", " ") + " ₽"

    @staticmethod
    def _fmt_percent(value: float) -> str:
        rounded = round(float(value), 1)
        if float(rounded).is_integer():
            return f"{int(rounded)}%"
        return f"{rounded:.1f}%"

    @staticmethod
    def _fmt_position(value: float) -> str:
        if value <= 0:
            return "н/д"
        return f"{round(float(value), 1):.1f}"

    @staticmethod
    def _build_status(drr: float, ctr: float, orders: float) -> str:
        if orders <= 0 and drr > 25:
            return "🔴 высокая ДРР"
        if ctr < 1.0:
            return "🔻 падение CTR"
        if drr <= 15 and orders >= 1:
            return "🚀 рост заказов"
        if drr <= 25:
            return "🟢 стабильный"
        return "🟡 требует внимания"
