"""Level 1: TradeEvent's price constraint mirrors NUMERIC(18,6).

No I/O. This is a contract rule and market_core owns it: storage holds
eighteen digits with six after the point, and the schema refuses anything
the column would silently round or loudly reject.
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from market_core import TradeEvent


def make_event(price: str) -> TradeEvent:
    return TradeEvent(
        symbol="AAPL",
        price=Decimal(price),
        volume=100,
        timestamp=datetime(2026, 9, 7, 12, 0, tzinfo=UTC),
        source="test",
    )


def test_price_at_the_column_limit_is_accepted() -> None:
    """The largest value NUMERIC(18,6) can hold. One digit more in either
    direction is a different test."""
    assert make_event("999999999999.999999").price == Decimal("999999999999.999999")


def test_price_with_too_many_decimal_places_is_rejected() -> None:
    """PostgreSQL rounds this silently -- 215.1234567 becomes 215.123457 with
    no error. The schema has to stop it before the database sees it."""
    with pytest.raises(ValidationError) as exc_info:
        make_event("215.1234567")

    assert exc_info.value.errors()[0]["type"] == "decimal_max_places"


def test_price_too_large_for_the_column_is_rejected() -> None:
    """PostgreSQL raises 'numeric field overflow' above 10^12. Failing here is
    the same verdict, reached without a round trip."""
    with pytest.raises(ValidationError) as exc_info:
        make_event("1000000000000.00")

    assert exc_info.value.errors()[0]["type"] == "decimal_whole_digits"