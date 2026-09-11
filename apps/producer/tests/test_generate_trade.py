"""The rounding POLICY, tested where it is observable.

generate_trade authors the price, so it chooses the resolution. This is the
only place in the system where that choice can be seen: by the time the value
reaches Kafka or PostgreSQL, round(float, 2) and quantize(ROUND_HALF_UP) are
indistinguishable -- both produce two decimal places.
"""

from decimal import Decimal

import app.main
from app.main import generate_trade


def test_price_is_rounded_half_up_at_a_half_cent(monkeypatch) -> None:
    """215.00 + 0.665 lands exactly on half a cent.

    ROUND_HALF_UP gives 215.67. The previous implementation gave 215.66, for
    two compounding reasons: round() breaks ties to even, and 215.665 is not
    exactly 215.665 in binary. This test goes red against that implementation,
    which is the only reason it earns its place.
    """
    monkeypatch.setattr(app.main.random, "uniform", lambda a, b: 0.665)
    monkeypatch.setattr(app.main.random, "randint", lambda a, b: 100)

    trade = generate_trade("AAPL")

    assert trade.price == Decimal("215.67")


def test_price_carries_exactly_two_decimal_places(monkeypatch) -> None:
    """Scale is carried information in Decimal. If the quantize is ever removed,
    the value still validates -- six places is legal -- and only this fails."""
    monkeypatch.setattr(app.main.random, "uniform", lambda a, b: 0.1234567)
    monkeypatch.setattr(app.main.random, "randint", lambda a, b: 100)

    trade = generate_trade("AAPL")

    assert trade.price.as_tuple().exponent == -2