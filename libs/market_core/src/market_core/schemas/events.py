from datetime import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator


class TradeEvent(BaseModel):
    """Canonical trade event published to Kafka."""

    model_config = ConfigDict(extra="forbid")

    event_id: UUID = Field(default_factory=uuid4)
    event_type: Literal["trade"] = "trade"

    symbol: str = Field(
        min_length=1,
        max_length=20,
    )
    price: Decimal = Field(gt=0, max_digits=18, decimal_places=6,)
    volume: int = Field(gt=0)
    timestamp: datetime

    source: str = Field(
        min_length=1,
        max_length=100,
    )
    schema_version: int = Field(
        default=1,
        ge=1,
    )

    @field_validator("symbol", mode="before")
    @classmethod
    def _uppercase_symbol(cls, value: object) -> object:
        """Uppercase at the schema, so storage and lookup cannot disagree.

        save_trade() inserts event.symbol verbatim, while the API's
        normalize_symbol() uppercases the incoming query. A lowercase symbol
        reaching the table is therefore written in a form no lookup can find.
        The rule belongs here for the same reason the price bounds do: both
        services inherit it from the schema instead of each remembering to
        apply it. normalize_symbol() still owns the query side, because a URL
        path parameter never passes through this model.

        mode="before" is load-bearing, not stylistic. An "after" transform runs
        AFTER max_length is checked, and case folding can lengthen a string --
        "ß".upper() is "SS" -- so a 20-character symbol could leave this model
        21 characters long and be truncated by a VARCHAR(20) column. Upper-
        casing first is what keeps max_length=20 a claim about storage.
        """
        if isinstance(value, str):
            return value.upper()

        return value
