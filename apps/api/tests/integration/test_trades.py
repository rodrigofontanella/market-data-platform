"""Level 2b: the trade routes against a real PostgreSQL.

Rows go in through the ORM model directly rather than through the consumer's
save_trade -- that lives in apps/consumer's `app` package, which cannot be
imported from here. These tests are about the API's queries, not about the
write path, so constructing Trade objects is the honest choice anyway.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest

from market_core import Trade

pytestmark = pytest.mark.integration


BASE_TIME = datetime(2026, 9, 7, 12, 0, 0, tzinfo=UTC)


def add_trade(
    session,
    *,
    symbol: str = "AAPL",
    price: str = "215.00",
    volume: int = 100,
    minutes: int = 0,
) -> None:
    """Stage one trade. Does NOT commit -- the test decides when.

    `minutes` offsets event_time from BASE_TIME, so ordering is explicit at the
    call site rather than implied by insertion order.
    """
    session.add(
        Trade(
            event_id=uuid4(),
            event_type="trade",
            symbol=symbol,
            price=Decimal(price),
            volume=volume,
            event_time=BASE_TIME.replace(minute=BASE_TIME.minute + minutes),
            source="test",
            schema_version=1,
        )
    )


def test_trades_are_returned_newest_first(client, db_session) -> None:
    """The order_by(event_time.desc()) is load-bearing, not decorative.

    volume is the marker rather than price: it is an integer, so how the
    response serialises it is not in question. Whether price comes back as a
    JSON number or a string is a separate thing worth knowing, and asserting on
    it here would conflate two questions.
    """
    add_trade(db_session, volume=1, minutes=0)
    add_trade(db_session, volume=2, minutes=1)
    add_trade(db_session, volume=3, minutes=2)
    db_session.commit()

    response = client.get("/trades")

    assert response.status_code == 200
    assert [trade["volume"] for trade in response.json()] == [3, 2, 1]


def test_limit_above_the_maximum_is_rejected(client) -> None:
    """Query(le=1000) is the contract. Without the bound, one request can ask
    the database for every row in the table."""
    response = client.get("/trades", params={"limit": 1001})

    assert response.status_code == 422


def test_symbol_lookup_is_case_insensitive(client, db_session) -> None:
    """The route upper-cases the path parameter. Symbols are stored upper-case,
    so without that normalisation /trades/aapl would 404 on data that exists."""
    add_trade(db_session, symbol="AAPL", volume=7)
    add_trade(db_session, symbol="MSFT", volume=9)
    db_session.commit()

    response = client.get("/trades/aapl")

    assert response.status_code == 200
    assert [trade["symbol"] for trade in response.json()] == ["AAPL"]


def test_unknown_symbol_returns_404(client, db_session) -> None:
    """An empty result is not an error for /trades, but it is for /trades/{symbol}
    -- the route asserts the symbol exists. That is a deliberate difference and
    this pins it."""
    add_trade(db_session, symbol="AAPL")
    db_session.commit()

    response = client.get("/trades/ZZZZ")

    assert response.status_code == 404
    assert "ZZZZ" in response.json()["detail"]


def test_trades_with_identical_event_times_have_a_stable_order(client, db_session) -> None:
    add_trade(db_session, volume=1, minutes=0)
    add_trade(db_session, volume=2, minutes=0)
    add_trade(db_session, volume=3, minutes=0)
    db_session.commit()

    first = [t["volume"] for t in client.get("/trades").json()]
    second = [t["volume"] for t in client.get("/trades").json()]

    assert first == second
