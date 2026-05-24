from __future__ import annotations

from dataclasses import asdict
from typing import Any

import pandas as pd

from src.analytics.read_service import GoogleSheetsReadService
from src.analytics.schemas import DailySummary, ProblemProduct, TopProduct


class AnalyticsQueryService:
    """Read-only analytics queries for future bot commands."""

    def __init__(self, reader: GoogleSheetsReadService | None = None) -> None:
        self._reader = reader or GoogleSheetsReadService()

    def get_current_prices(self, limit: int | None = None) -> pd.DataFrame:
        df = self._reader.get_current_prices()
        if limit is not None and limit > 0:
            return df.head(limit)
        return df

    def get_advert_stats(
        self,
        date_from: str | None = None,
        date_to: str | None = None,
        account: str | None = None,
    ) -> pd.DataFrame:
        df = self._reader.get_advert_stats()
        return self._filter_advert_df(df, date_from=date_from, date_to=date_to, account=account)

    def get_fin_report(
        self,
        date_from: str | None = None,
        date_to: str | None = None,
        account: str | None = None,
    ) -> pd.DataFrame:
        df = self._reader.get_fin_report()
        if df.empty:
            return df

        account_col = self._find_column(df, ["account", "Аккаунт"])
        if account and account_col:
            df = df[df[account_col].astype(str).str.lower() == account.lower()]

        date_col = self._find_column(df, ["date", "Дата продажи", "Дата операции", "sale_dt", "rr_dt"])
        if date_col and (date_from or date_to):
            date_series = pd.to_datetime(df[date_col], errors="coerce")
            if date_from:
                df = df[date_series >= pd.to_datetime(date_from)]
            if date_to:
                df = df[date_series <= pd.to_datetime(date_to)]
        return df

    def get_fin_report_compact(self, top_n: int = 10) -> dict[str, Any]:
        df = self.get_fin_report()
        if df.empty:
            return {
                "total_records": 0,
                "sum_sales": 0.0,
                "sum_payout": 0.0,
                "sum_penalty": 0.0,
                "top": pd.DataFrame(),
            }

        sales_col = self._find_column(df, ["WB реализовал товар (продажа)", "retail_amount", "sum_price", "Выручка"])
        payout_col = self._find_column(df, ["К перечислению продавцу", "ppvz_for_pay", "К выплате"])
        penalty_col = self._find_column(df, ["Сумма штрафов", "penalty", "Штраф"])
        nm_col = self._find_column(df, ["Артикул WB", "nm_id", "nmId"])
        account_col = self._find_column(df, ["account", "Аккаунт"])

        result_df = df.copy()
        if sales_col:
            result_df["_sales"] = pd.to_numeric(result_df[sales_col], errors="coerce").fillna(0.0)
        else:
            result_df["_sales"] = 0.0
        if payout_col:
            result_df["_payout"] = pd.to_numeric(result_df[payout_col], errors="coerce").fillna(0.0)
        else:
            result_df["_payout"] = 0.0
        if penalty_col:
            result_df["_penalty"] = pd.to_numeric(result_df[penalty_col], errors="coerce").fillna(0.0)
        else:
            result_df["_penalty"] = 0.0

        top_cols = [col for col in [account_col, nm_col] if col]
        top = result_df.sort_values("_sales", ascending=False).head(top_n)
        top = top[top_cols + ["_sales", "_payout", "_penalty"]] if top_cols else top[["_sales", "_payout", "_penalty"]]

        return {
            "total_records": int(len(result_df.index)),
            "sum_sales": float(result_df["_sales"].sum()),
            "sum_payout": float(result_df["_payout"].sum()),
            "sum_penalty": float(result_df["_penalty"].sum()),
            "top": top,
        }

    def get_top_products(
        self,
        date_from: str | None = None,
        date_to: str | None = None,
        account: str | None = None,
        top_n: int = 10,
    ) -> list[TopProduct]:
        df = self.get_advert_stats(date_from=date_from, date_to=date_to, account=account)
        if df.empty:
            return []

        nm_col = self._find_column(df, ["nm_id", "nmId", "Артикул WB"])
        account_col = self._find_column(df, ["account", "Аккаунт"])
        revenue_col = self._find_column(df, ["sum_price", "revenue", "Выручка"])
        orders_col = self._find_column(df, ["orders", "Количество заказов"])
        spend_col = self._find_column(df, ["sum_spend", "spend", "Расход"])
        if not all([nm_col, account_col, revenue_col, orders_col, spend_col]):
            return []

        grouped = (
            df.assign(
                _revenue=pd.to_numeric(df[revenue_col], errors="coerce").fillna(0.0),
                _orders=pd.to_numeric(df[orders_col], errors="coerce").fillna(0.0),
                _spend=pd.to_numeric(df[spend_col], errors="coerce").fillna(0.0),
            )
            .groupby([account_col, nm_col], as_index=False)[["_revenue", "_orders", "_spend"]]
            .sum()
            .sort_values("_revenue", ascending=False)
            .head(top_n)
        )

        return [
            TopProduct(
                nm_id=str(row[nm_col]),
                account=str(row[account_col]),
                revenue=float(row["_revenue"]),
                orders=float(row["_orders"]),
                spend=float(row["_spend"]),
            )
            for _, row in grouped.iterrows()
        ]

    def get_problem_products(
        self,
        date_from: str | None = None,
        date_to: str | None = None,
        account: str | None = None,
        min_spend: float = 0.0,
        max_orders: float = 0.0,
        max_cr: float = 0.0,
        top_n: int = 10,
    ) -> list[ProblemProduct]:
        df = self.get_advert_stats(date_from=date_from, date_to=date_to, account=account)
        if df.empty:
            return []

        nm_col = self._find_column(df, ["nm_id", "nmId", "Артикул WB"])
        account_col = self._find_column(df, ["account", "Аккаунт"])
        spend_col = self._find_column(df, ["sum_spend", "spend", "Расход"])
        orders_col = self._find_column(df, ["orders", "Количество заказов"])
        cr_col = self._find_column(df, ["cr", "CR"])
        if not all([nm_col, account_col, spend_col, orders_col, cr_col]):
            return []

        prepared = df.assign(
            _spend=pd.to_numeric(df[spend_col], errors="coerce").fillna(0.0),
            _orders=pd.to_numeric(df[orders_col], errors="coerce").fillna(0.0),
            _cr=pd.to_numeric(df[cr_col], errors="coerce").fillna(0.0),
        )
        grouped = (
            prepared.groupby([account_col, nm_col], as_index=False)[["_spend", "_orders", "_cr"]]
            .mean()
            .query("_spend >= @min_spend and _orders <= @max_orders and _cr <= @max_cr")
            .sort_values("_spend", ascending=False)
            .head(top_n)
        )

        return [
            ProblemProduct(
                nm_id=str(row[nm_col]),
                account=str(row[account_col]),
                spend=float(row["_spend"]),
                orders=float(row["_orders"]),
                cr=float(row["_cr"]),
            )
            for _, row in grouped.iterrows()
        ]

    def get_daily_summary(self, date: str | None = None) -> DailySummary:
        advert_df = self.get_advert_stats(date_from=date, date_to=date) if date else self.get_advert_stats()
        price_df = self.get_current_prices()
        fin_df = self.get_fin_report(date_from=date, date_to=date) if date else self.get_fin_report()

        spend_col = self._find_column(advert_df, ["sum_spend", "spend", "Расход"])
        revenue_col = self._find_column(advert_df, ["sum_price", "revenue", "Выручка"])
        orders_col = self._find_column(advert_df, ["orders", "Количество заказов"])

        total_spend = self._sum_column(advert_df, spend_col)
        total_revenue = self._sum_column(advert_df, revenue_col)
        total_orders = self._sum_column(advert_df, orders_col)

        summary_date = date or "all"
        return DailySummary(
            date=summary_date,
            adverts_count=len(advert_df.index),
            total_spend=total_spend,
            total_revenue=total_revenue,
            total_orders=total_orders,
            sku_prices_count=len(price_df.index),
            fin_rows_count=len(fin_df.index),
        )

    @staticmethod
    def _sum_column(df: pd.DataFrame, column: str | None) -> float:
        if df.empty or not column:
            return 0.0
        return float(pd.to_numeric(df[column], errors="coerce").fillna(0.0).sum())

    def _filter_advert_df(
        self,
        df: pd.DataFrame,
        date_from: str | None = None,
        date_to: str | None = None,
        account: str | None = None,
    ) -> pd.DataFrame:
        if df.empty:
            return df

        account_col = self._find_column(df, ["account", "Аккаунт"])
        if account and account_col:
            df = df[df[account_col].astype(str).str.lower() == account.lower()]

        date_col = self._find_column(df, ["date", "Дата"])
        if date_col and (date_from or date_to):
            date_series = pd.to_datetime(df[date_col], errors="coerce")
            if date_from:
                df = df[date_series >= pd.to_datetime(date_from)]
            if date_to:
                df = df[date_series <= pd.to_datetime(date_to)]
        return df

    @staticmethod
    def _find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
        normalized = {str(col).strip().lower(): col for col in df.columns}
        for candidate in candidates:
            hit = normalized.get(candidate.strip().lower())
            if hit is not None:
                return hit
        return None

    @staticmethod
    def to_dict_list(items: list[Any]) -> list[dict[str, Any]]:
        return [asdict(item) for item in items]
