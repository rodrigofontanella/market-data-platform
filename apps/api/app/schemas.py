from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    database: str


class SymbolResponse(BaseModel):
    symbol: str