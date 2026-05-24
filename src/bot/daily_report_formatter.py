"""Форматирование ежедневного отчета в HTML-сообщение для Telegram."""

from __future__ import annotations

from src.analytics.schemas import DailyAnalyticsReport


class DailyReportFormatter:
    """Готовит компактный executive-summary HTML для Telegram."""

    _EMPTY_BLOCK_TEXT = "Пока недостаточно данных для анализа"

    def format(self, report: DailyAnalyticsReport) -> str:
        m = report.metrics
        a = report.advert_metrics

        lines: list[str] = [
            f"<b>📊 Ежедневный отчет за {m.date}</b>",
            "────────────",
            "<b>💼 Ключевые показатели</b>",
            f"• Выручка: <b>{self._fmt_money(m.revenue)}</b>",
            f"• Заказы: <b>{self._fmt_int(m.orders)}</b>",
            f"• Прибыль: <b>{self._fmt_money(m.profit)}</b>",
            f"• Рекламные расходы: <b>{self._fmt_money(m.ad_spend)}</b>",
            f"• ДРР: <b>{self._fmt_percent(m.drr_percent)}</b>",
            "",
            "<b>📣 Реклама</b>",
            f"• Клики: {self._fmt_int(a.clicks)}",
            f"• Показы: {self._fmt_int(a.views)}",
            f"• CTR: {self._fmt_percent(a.ctr_percent)}",
            f"• CPC: {self._fmt_money(a.cpc)}",
            f"• CR: {self._fmt_percent(a.cr_percent)}",
            "",
            "<b>📈 Топ товаров</b>",
        ]

        if report.top_products:
            for idx, item in enumerate(report.top_products, start=1):
                status = "🚀 рост заказов" if item.orders_growth_abs > 0 else "🟡 требует внимания"
                lines.append(
                    f"{idx}. <b>📦 {item.nm_id}</b> ({item.account})\n"
                    f"   Заказы: +{self._fmt_int(item.orders_growth_abs)} | "
                    f"Прибыль: +{self._fmt_money(item.profit_growth_abs)} | "
                    f"ΔДРР: {self._fmt_pp(item.drr_change_pp)}\n"
                    f"   Статус: {status}"
                )
        else:
            lines.append(self._EMPTY_BLOCK_TEXT)

        lines.extend(["", "<b>⚠️ Риски / проблемные товары</b>"])

        if report.problem_products:
            for idx, item in enumerate(report.problem_products, start=1):
                risk_status = self._risk_status(item.reason)
                lines.append(
                    f"{idx}. <b>📦 {item.nm_id}</b> ({item.account})\n"
                    f"   Прибыль: {self._fmt_money(item.profit)} | "
                    f"ДРР: {self._fmt_percent(item.drr_percent)}\n"
                    f"   Заказы: {self._fmt_int(item.orders_today)} → {self._fmt_int(item.orders_yesterday)} | "
                    f"Расход: {self._fmt_money(item.ad_spend)}\n"
                    f"   Причина: {item.reason}\n"
                    f"   Статус: {risk_status}"
                )
        else:
            lines.append(self._EMPTY_BLOCK_TEXT)

        lines.append("────────────")
        lines.append("<i>Отчет сформирован на основе подготовленных данных Google Sheets.</i>")

        return "\n".join(lines)

    @staticmethod
    def _fmt_int(value: float) -> str:
        """Форматирует целое число с разделителем тысяч."""
        return f"{int(round(value)):,}".replace(",", " ")

    @staticmethod
    def _fmt_money(value: float) -> str:
        """Форматирует сумму в рублях в человекочитаемом виде."""
        rounded = int(round(value))
        return f"{rounded:,}".replace(",", " ") + " ₽"

    @staticmethod
    def _fmt_percent(value: float) -> str:
        """Форматирует проценты с 0/1 знаком после запятой в зависимости от дробной части."""
        rounded_1 = round(value, 1)
        if float(rounded_1).is_integer():
            return f"{int(rounded_1)}%"
        return f"{rounded_1:.1f}%"

    @staticmethod
    def _fmt_pp(value: float) -> str:
        """Форматирует изменение в процентных пунктах."""
        sign = "+" if value > 0 else ""
        rounded_1 = round(value, 1)
        if float(rounded_1).is_integer():
            return f"{sign}{int(rounded_1)} п.п."
        return f"{sign}{rounded_1:.1f} п.п."

    @staticmethod
    def _risk_status(reason: str) -> str:
        """Преобразует техническую причину в короткий пользовательский статус."""
        text = reason.lower()
        if "дрр" in text:
            return "🔴 высокая ДРР"
        if "прибыл" in text:
            return "🔴 отрицательная прибыль"
        if "падение" in text:
            return "🔻 падение заказов"
        if "эффективност" in text:
            return "🟡 требует внимания"
        return "🟡 требует внимания"
