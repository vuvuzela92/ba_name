from dataclasses import dataclass, field


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


@dataclass(slots=True)
class DailyReportAdvertMetrics:
    clicks: float
    views: float
    ctr_percent: float
    cpc: float
    cr_percent: float


@dataclass(slots=True)
class DailyReportMetrics:
    date: str
    revenue: float
    orders: float
    profit: float
    ad_spend: float
    drr_percent: float


@dataclass(slots=True)
class DailyReportTopProduct:
    nm_id: str
    account: str
    orders_growth_abs: float
    profit_growth_abs: float
    drr_change_pp: float


@dataclass(slots=True)
class DailyReportProblemProduct:
    nm_id: str
    account: str
    profit: float
    drr_percent: float
    orders_today: float
    orders_yesterday: float
    ad_spend: float
    reason: str


@dataclass(slots=True)
class DailyAnalyticsReport:
    metrics: DailyReportMetrics
    advert_metrics: DailyReportAdvertMetrics
    top_products: list[DailyReportTopProduct] = field(default_factory=list)
    problem_products: list[DailyReportProblemProduct] = field(default_factory=list)
    source_rows: dict[str, int] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
