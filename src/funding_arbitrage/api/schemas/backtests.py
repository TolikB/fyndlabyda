from decimal import Decimal

from pydantic import BaseModel, Field


class BacktestRequest(BaseModel):
    initial_capital: Decimal = Field(default=Decimal("15000"), gt=0)
    dataset_version: str = "request"
    monthly_pnl: dict[str, Decimal] = {}


class IncomeTargetRequest(BaseModel):
    portfolio: Decimal = Field(gt=0)
    monthly_target: Decimal = Field(ge=0)
