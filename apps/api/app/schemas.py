from pydantic import BaseModel


class SymbolResponse(BaseModel):
    symbol: str