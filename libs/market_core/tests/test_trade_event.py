"""Level 1: the contract rules market_core owns.

No I/O. Two rules live here, and both exist because a service downstream
would otherwise have to remember them.

The price constraint mirrors NUMERIC(18,6): storage holds eighteen digits with
six after the point, and the schema refuses anything the column would silently
round or loudly reject.

The symbol constraint is case. Storage and lookup have to agree on one form,
and the schema is where the producer and the consumer both inherit it.
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from market_core import TradeEvent
from pydantic import ValidationError


def make_event(price: str, symbol: str = "AAPL") -> TradeEvent:
    return TradeEvent(
        symbol=symbol,
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


def test_a_lowercase_symbol_is_uppercased_by_the_schema() -> None:
    """Goes through model_validate, which is the consumer's actual path.

    save_trade() inserts event.symbol verbatim, and the API's
    normalize_symbol() uppercases only the incoming query -- so a lowercase
    symbol reaching the table is stored in a form no lookup can find. Asserted
    here rather than end to end because this is where the decision is
    expressed; a database cannot tell you which layer was meant to normalise.
    """
    event = TradeEvent.model_validate(
        {
            "symbol": "e2e-a1b2c3d4e5f6",
            "price": "215.00",
            "volume": 100,
            "timestamp": "2026-09-07T12:00:00Z",
            "source": "test",
        }
    )

    assert event.symbol == "E2E-A1B2C3D4E5F6"


def test_a_symbol_that_grows_when_uppercased_is_rejected() -> None:
    """Pins mode="before" on the symbol validator. Nothing else does.

    max_length is checked before an "after" transform runs, and case folding
    can lengthen a string -- "ss".upper() is "SS", two characters from one.
    With the transform running after the check, this twenty-character symbol
    leaves the model twenty-one characters long and meets a VARCHAR(20)
    column. Replace the validator with StringConstraints(to_upper=True) and
    this is the only test in the repo that goes red.
    """
    with pytest.raises(ValidationError) as exc_info:
        make_event("215.00", symbol="x" * 19 + "\N{LATIN SMALL LETTER SHARP S}")

    assert exc_info.value.errors()[0]["type"] == "string_too_long"
