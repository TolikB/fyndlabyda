from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field, model_validator


class BacktestRequest(BaseModel):
    initial_capital: Decimal = Field(default=Decimal("15000"), gt=0)
    dataset_version: str = "request"
    monthly_pnl: dict[str, Decimal] = {}


class PaperReplayRequest(BaseModel):
    initial_capital: Decimal = Field(default=Decimal("6250"), gt=0)
    simulation_version: str = "v26-oos-candidate"
    start: datetime | None = None
    end: datetime | None = None

    @model_validator(mode="after")
    def validate_range(self) -> "PaperReplayRequest":
        if self.start is not None and self.end is not None and self.start >= self.end:
            raise ValueError("start must be before end")
        return self


class MarketReplayRequest(BaseModel):
    initial_capital: Decimal = Field(default=Decimal("15000"), gt=0)
    start: datetime
    end: datetime

    @model_validator(mode="after")
    def validate_range(self) -> "MarketReplayRequest":
        if self.start >= self.end:
            raise ValueError("start must be before end")
        if (self.end - self.start).total_seconds() < 30 * 86400:
            raise ValueError("market replay requires at least 30 days")
        if (self.end - self.start).total_seconds() > 90 * 86400:
            raise ValueError("market replay supports at most 90 days")
        return self


class IncomeTargetRequest(BaseModel):
    portfolio: Decimal = Field(gt=0)
    monthly_target: Decimal = Field(ge=0)
