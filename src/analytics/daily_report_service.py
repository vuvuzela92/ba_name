"""Сервис формирования ежедневного аналитического отчета для Telegram-бота."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from src.analytics.query_service import AnalyticsQueryService
from src.analytics.read_service import GoogleSheetsReadService
from src.analytics.schemas import (
    DailyAnalyticsReport,
    DailyReportAdvertMetrics,
    DailyReportMetrics,
    DailyReportProblemProduct,
    DailyReportTopProduct,
)
from src.core.logging_utils import bind_context


class DailyReportService:
    """Формирует ежедневный отчет на основе уже подготовленных данных Google Sheets.

    Сервис не вызывает WB API и не изменяет данные в таблицах.
    """

    def __init__(
        self,
        query_service: AnalyticsQueryService | None = None,
        read_service: GoogleSheetsReadService | None = None,
    ) -> None:
        self._query = query_service or AnalyticsQueryService()
        self._reader = read_service or GoogleSheetsReadService()

    def build_daily_report(self, date: str | None = None) -> DailyAnalyticsReport:
        """Собирает отчет за день. Если date не задана, используется вчера."""
        started = datetime.now()
        report_date = date or (started - timedelta(days=1)).strftime("%Y-%m-%d")
        prev_date = (datetime.fromisoformat(report_date) - timedelta(days=1)).strftime("%Y-%m-%d")
        log = bind_context(task_name="daily_report", endpoint="build")

        advert_df = self._query.get_advert_stats(date_from=report_date, date_to=report_date)
        advert_prev_df = self._query.get_advert_stats(date_from=prev_date, date_to=prev_date)
        fin_df = self._query.get_fin_report(date_from=report_date, date_to=report_date)
        fin_prev_df = self._query.get_fin_report(date_from=prev_date, date_to=prev_date)
        price_df = self._reader.get_current_prices()
        unit_df = self._reader.get_unit()

        warnings: list[str] = []
        if advert_df.empty:
            warnings.append("Нет данных advert_stat за выбранный день")
        if fin_df.empty:
            warnings.append("Нет данных fin_rep_weekly за выбранный день")
        if price_df.empty:
            warnings.append("Пустой лист current_price")
        if unit_df.empty:
            warnings.append("Пустой лист unit")

        revenue = self._sum_col(advert_df, ["sum_price", "revenue", "Выручка"])
        orders = self._sum_col(advert_df, ["orders", "Количество заказов"])
        ad_spend = self._sum_col(advert_df, ["sum_spend", "spend", "Расход"])

        # Прибыль берем из фин. отчета, fallback — выручка минус рекламные расходы.
        profit = self._sum_col(fin_df, ["К перечислению продавцу", "ppvz_for_pay", "К выплате", "profit", "Прибыль"])
        if profit == 0.0 and revenue != 0.0:
            profit = revenue - ad_spend
            warnings.append("Прибыль рассчитана как выручка минус рекламные расходы (fallback)")

        drr_percent = (ad_spend / revenue * 100.0) if revenue > 0 else 0.0

        advert_metrics = self._build_advert_metrics(advert_df)
        top_products = self._build_top_products(advert_df, advert_prev_df, fin_df, fin_prev_df)
        problem_products = self._build_problem_products(advert_df, advert_prev_df, fin_df)

        metrics = DailyReportMetrics(
            date=report_date,
            revenue=round(revenue, 2),
            orders=round(orders, 2),
            profit=round(profit, 2),
            ad_spend=round(ad_spend, 2),
            drr_percent=round(drr_percent, 2),
        )

        report = DailyAnalyticsReport(
            metrics=metrics,
            advert_metrics=advert_metrics,
            top_products=top_products,
            problem_products=problem_products,
            source_rows={
                "advert_stat": len(advert_df.index),
                "advert_stat_prev": len(advert_prev_df.index),
                "fin_rep_weekly": len(fin_df.index),
                "fin_rep_weekly_prev": len(fin_prev_df.index),
                "current_price": len(price_df.index),
                "unit": len(unit_df.index),
            },
            warnings=warnings,
        )

        duration_ms = round((datetime.now() - started).total_seconds() * 1000, 2)
        log.info(
            "Daily report built duration_ms={} sku_today={} sku_prev={} warnings={}",
            duration_ms,
            self._sku_count(advert_df),
            self._sku_count(advert_prev_df),
            len(warnings),
        )
        if warnings:
            log.warning("Daily report warnings: {}", warnings)
        return report

    def _build_advert_metrics(self, advert_df: pd.DataFrame) -> DailyReportAdvertMetrics:
        clicks = self._sum_col(advert_df, ["clicks"])
        views = self._sum_col(advert_df, ["views"])
        spend = self._sum_col(advert_df, ["sum_spend", "spend", "Расход"])
        orders = self._sum_col(advert_df, ["orders", "Количество заказов"])

        ctr_percent = (clicks / views * 100.0) if views > 0 else 0.0
        cpc = (spend / clicks) if clicks > 0 else 0.0
        cr_percent = (orders / clicks * 100.0) if clicks > 0 else 0.0

        return DailyReportAdvertMetrics(
            clicks=round(clicks, 2),
            views=round(views, 2),
            ctr_percent=round(ctr_percent, 2),
            cpc=round(cpc, 2),
            cr_percent=round(cr_percent, 2),
        )

    def _build_top_products(
        self,
        advert_today: pd.DataFrame,
        advert_prev: pd.DataFrame,
        fin_today: pd.DataFrame,
        fin_prev: pd.DataFrame,
    ) -> list[DailyReportTopProduct]:
        today = self._build_sku_daily_frame(advert_today, fin_today)
        prev = self._build_sku_daily_frame(advert_prev, fin_prev)
        if today.empty:
            return []

        key_cols = ["account", "nm_id"]
        prev = prev.rename(
            columns={
                "orders": "orders_prev",
                "profit": "profit_prev",
                "drr_percent": "drr_prev",
            }
        )

        merged = today.merge(prev[key_cols + ["orders_prev", "profit_prev", "drr_prev"]], on=key_cols, how="left")
        merged[["orders_prev", "profit_prev", "drr_prev"]] = merged[["orders_prev", "profit_prev", "drr_prev"]].fillna(0.0)

        merged["orders_growth_abs"] = merged["orders"] - merged["orders_prev"]
        merged["profit_growth_abs"] = merged["profit"] - merged["profit_prev"]
        merged["drr_change_pp"] = merged["drr_prev"] - merged["drr_percent"]

        ranked = merged.sort_values(
            ["orders_growth_abs", "profit_growth_abs", "drr_change_pp"],
            ascending=[False, False, False],
        ).head(10)

        return [
            DailyReportTopProduct(
                nm_id=str(row["nm_id"]),
                account=str(row["account"]),
                orders_growth_abs=float(row["orders_growth_abs"]),
                profit_growth_abs=float(row["profit_growth_abs"]),
                drr_change_pp=float(row["drr_change_pp"]),
            )
            for _, row in ranked.iterrows()
        ]

    def _build_problem_products(
        self,
        advert_today: pd.DataFrame,
        advert_prev: pd.DataFrame,
        fin_today: pd.DataFrame,
    ) -> list[DailyReportProblemProduct]:
        today = self._build_sku_daily_frame(advert_today, fin_today)
        prev = self._build_sku_daily_frame(advert_prev, pd.DataFrame())
        if today.empty:
            return []

        key_cols = ["account", "nm_id"]
        prev = prev.rename(columns={"orders": "orders_prev"})
        merged = today.merge(prev[key_cols + ["orders_prev"]], on=key_cols, how="left")
        merged["orders_prev"] = merged["orders_prev"].fillna(0.0)

        # Проблемные признаки:
        # 1) отрицательная прибыль;
        # 2) высокий ДРР;
        # 3) падение заказов;
        # 4) низкая рекламная эффективность (расход есть, заказов нет).
        reasons = np.select(
            [
                merged["profit"] < 0,
                merged["drr_percent"] > 35,
                merged["orders"] < merged["orders_prev"],
                (merged["ad_spend"] >= 500) & (merged["orders"] <= 0),
            ],
            [
                "Отрицательная прибыль",
                "Высокий ДРР",
                "Падение заказов",
                "Низкая эффективность рекламы",
            ],
            default="",
        )
        merged["reason"] = reasons
        filtered = merged[merged["reason"] != ""].copy()

        if filtered.empty:
            return []

        filtered = filtered.sort_values(["profit", "drr_percent", "ad_spend"], ascending=[True, False, False]).head(10)

        return [
            DailyReportProblemProduct(
                nm_id=str(row["nm_id"]),
                account=str(row["account"]),
                profit=float(row["profit"]),
                drr_percent=float(row["drr_percent"]),
                orders_today=float(row["orders"]),
                orders_yesterday=float(row["orders_prev"]),
                ad_spend=float(row["ad_spend"]),
                reason=str(row["reason"]),
            )
            for _, row in filtered.iterrows()
        ]

    def _build_sku_daily_frame(self, advert_df: pd.DataFrame, fin_df: pd.DataFrame) -> pd.DataFrame:
        if advert_df.empty:
            return pd.DataFrame(columns=["account", "nm_id", "revenue", "orders", "ad_spend", "profit", "drr_percent"])

        nm_col = self._find_col(advert_df, ["nm_id", "nmId", "Артикул WB"])
        acc_col = self._find_col(advert_df, ["account", "Аккаунт"])
        revenue_col = self._find_col(advert_df, ["sum_price", "revenue", "Выручка"])
        orders_col = self._find_col(advert_df, ["orders", "Количество заказов"])
        spend_col = self._find_col(advert_df, ["sum_spend", "spend", "Расход"])
        if not all([nm_col, acc_col, revenue_col, orders_col, spend_col]):
            return pd.DataFrame(columns=["account", "nm_id", "revenue", "orders", "ad_spend", "profit", "drr_percent"])

        base = advert_df[[acc_col, nm_col, revenue_col, orders_col, spend_col]].copy()
        base.columns = ["account", "nm_id", "revenue", "orders", "ad_spend"]
        base["revenue"] = self._to_number(base["revenue"])
        base["orders"] = self._to_number(base["orders"])
        base["ad_spend"] = self._to_number(base["ad_spend"])

        grouped = base.groupby(["account", "nm_id"], as_index=False).sum(numeric_only=True)

        if fin_df.empty:
            grouped["profit"] = grouped["revenue"] - grouped["ad_spend"]
        else:
            fin_nm_col = self._find_col(fin_df, ["nm_id", "nmId", "Артикул WB"])
            fin_acc_col = self._find_col(fin_df, ["account", "Аккаунт"])
            fin_profit_col = self._find_col(fin_df, ["К перечислению продавцу", "ppvz_for_pay", "К выплате", "profit", "Прибыль"])
            if all([fin_nm_col, fin_acc_col, fin_profit_col]):
                fin_base = fin_df[[fin_acc_col, fin_nm_col, fin_profit_col]].copy()
                fin_base.columns = ["account", "nm_id", "profit"]
                fin_base["profit"] = self._to_number(fin_base["profit"])
                fin_grouped = fin_base.groupby(["account", "nm_id"], as_index=False).sum(numeric_only=True)
                grouped = grouped.merge(fin_grouped, on=["account", "nm_id"], how="left")
                grouped["profit"] = grouped["profit"].fillna(grouped["revenue"] - grouped["ad_spend"])
            else:
                grouped["profit"] = grouped["revenue"] - grouped["ad_spend"]

        grouped["drr_percent"] = np.where(grouped["revenue"] > 0, grouped["ad_spend"] / grouped["revenue"] * 100.0, 0.0)
        return grouped

    def _sum_col(self, df: pd.DataFrame, candidates: list[str]) -> float:
        if df.empty:
            return 0.0
        col = self._find_col(df, candidates)
        if not col:
            return 0.0
        return float(self._to_number(df[col]).sum())

    @staticmethod
    def _to_number(series: pd.Series) -> pd.Series:
        """Нормализует локальные числовые строки (пробелы, запятые) в float."""
        s = series.astype(str)
        s = s.str.replace("\xa0", "", regex=False)
        s = s.str.replace(" ", "", regex=False)
        s = s.str.replace(",", ".", regex=False)
        return pd.to_numeric(s, errors="coerce").fillna(0.0)

    @staticmethod
    def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
        norm = {str(col).strip().lower(): col for col in df.columns}
        for candidate in candidates:
            hit = norm.get(candidate.strip().lower())
            if hit is not None:
                return hit
        return None

    def _sku_count(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        nm_col = self._find_col(df, ["nm_id", "nmId", "Артикул WB"])
        acc_col = self._find_col(df, ["account", "Аккаунт"])
        if not all([nm_col, acc_col]):
            return 0
        return int(df[[acc_col, nm_col]].drop_duplicates().shape[0])
