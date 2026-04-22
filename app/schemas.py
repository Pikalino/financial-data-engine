from datetime import date, datetime
from pydantic import BaseModel


class TransactionResponse(BaseModel):
    id: int
    date: datetime
    description: str
    amount: float
    currency: str
    payment_method: str | None = None
    counterparty: str | None = None
    type: str
    category: str
    source_file: str
    raw_row_hash: str

    class Config:
        from_attributes = True


class SummaryResponse(BaseModel):
    total_income: float
    total_expense: float
    total_refund: float
    net_profit: float


class CategorySummaryResponse(BaseModel):
    category: str
    total_amount: float


class DailySummaryResponse(BaseModel):
    date: date
    total_amount: float

class MonthlySummaryResponse(BaseModel):
    month: str
    total_income: float
    total_expense: float
    total_refund: float
    net_profit: float
class PaymentMethodSummaryResponse(BaseModel):
    payment_method: str
    total_amount: float

class CounterpartySummaryResponse(BaseModel):
    counterparty: str
    total_amount: float
class CategoryMonthlyTrendResponse(BaseModel):
    month: str
    category: str
    total_amount: float
class WeeklySummaryResponse(BaseModel):
    week: str
    total_income: float
    total_expense: float
    total_refund: float
    net_profit: float
class KpiSummaryResponse(BaseModel):
    transaction_count: int
    income_transaction_count: int
    expense_transaction_count: int
    refund_transaction_count: int
    net_margin: float
    expense_ratio: float
    refund_rate: float
    average_transaction_amount: float
class AutoInsightResponse(BaseModel):
    message: str
class AnomalyResponse(BaseModel):
    transaction_id: int
    date: datetime
    description: str
    amount: float
    category: str
    counterparty: str | None = None
    reason: str
class ForecastResponse(BaseModel):
    projected_next_week_net: float
    projected_next_month_income: float
    projected_next_month_expense: float
    projected_next_month_refund: float
    projected_next_month_net: float