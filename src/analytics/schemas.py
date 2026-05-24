from dataclasses import dataclass


@dataclass(slots=True)
class TopProduct:
    nm_id: str
    account: str
    revenue: float
    orders: float
    spend: float


@dataclass(slots=True)
class ProblemProduct:
    nm_id: str
    account: str
    spend: float
    orders: float
    cr: float


@dataclass(slots=True)
class DailySummary:
    date: str
    adverts_count: int
    total_spend: float
    total_revenue: float
    total_orders: float
    sku_prices_count: int
    fin_rows_count: int
