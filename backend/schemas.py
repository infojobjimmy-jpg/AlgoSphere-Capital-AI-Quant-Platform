from pydantic import BaseModel, Field


class AccountUpdate(BaseModel):
    balance: float = Field(..., ge=0)
    risk_limit: float = Field(0.02, gt=0, le=1)


class BotUpdate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    profit: float
    drawdown: float = Field(..., ge=0)
    win_rate: float = Field(..., ge=0, le=1)
    trades: int = Field(..., ge=0)


class BotOut(BaseModel):
    id: int
    name: str
    profit: float
    drawdown: float
    win_rate: float
    trades: int
    score: float
    risk_level: str
    capital_alloc: float
    decision: str


class BrainOut(BaseModel):
    regime: str
    message: str
