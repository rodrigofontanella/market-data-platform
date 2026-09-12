"""Level 2b: save_trade against a real PostgreSQL.

These tests exist because ON CONFLICT DO NOTHING is a claim about a database,
not about Python. A fake session cannot refute it; only a real unique index
can. Everything here commits, because save_trade does not -- its caller does,
and the commit is part of what is under test.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select, text

from app.database import save_trade
from market_core import Trade, TradeEvent

pytestmark = pytest.mark.integration


FIXED_TIMESTAMP = datetime(2026, 9, 7, 12, 0, 0, tzinfo=UTC)


def make_event(
    price: str = "215.00",
    symbol: str = "AAPL",
    event_id: UUID | None = None,
) -> TradeEvent:
    """Local to this module, duplicating tests/conftest.py on purpose.

    Promote it to a shared helper when a third suite needs it -- not before.
    A test-support module that exists for two callers is indirection you pay
    for on every read.
    """
    return TradeEvent(
        event_id=event_id or uuid4(),
        symbol=symbol,
        price=Decimal(price),
        volume=100,
        timestamp=FIXED_TIMESTAMP,
        source="test",
    )


def count_trades(session) -> int:
    return session.execute(select(func.count()).select_from(Trade)).scalar_one()


def test_a_repeated_event_id_is_not_stored_twice(db_session) -> None:
    """The claim the whole at-least-once design rests on.

    Kafka redelivers. That is not a failure mode to be avoided, it is the
    contract. This is what makes redelivery harmless.
    """
    event = make_event()

    first = save_trade(db_session, event)
    db_session.commit()

    second = save_trade(db_session, event)
    db_session.commit()

    assert first is True
    assert second is False
    assert count_trades(db_session) == 1


def test_a_duplicate_does_not_overwrite_the_stored_row(db_session) -> None:
    """DO NOTHING, not DO UPDATE.

    The test above passes under either. This one is the only thing standing
    between you and someone 'fixing' the upsert into a last-write-wins.
    """
    event_id = uuid4()

    save_trade(db_session, make_event(price="215.00", event_id=event_id))
    db_session.commit()

    save_trade(db_session, make_event(price="999.99", event_id=event_id))
    db_session.commit()

    stored = db_session.execute(
        select(Trade).where(Trade.event_id == event_id)
    ).scalar_one()

    assert stored.price == Decimal("215.00")


def test_distinct_events_for_one_symbol_are_both_stored(db_session) -> None:
    """The opposite failure.

    If index_elements ever became [Trade.symbol], the two tests above would
    still pass and the pipeline would silently store three rows a day.
    """
    first = save_trade(db_session, make_event(symbol="AAPL"))
    second = save_trade(db_session, make_event(symbol="AAPL"))
    db_session.commit()

    assert first is True
    assert second is True
    assert count_trades(db_session) == 2


def test_price_is_read_back_at_the_column_scale(db_session) -> None:
    """NUMERIC(18,6) pads. Decimal carries the scale; equality does not see it.

    Decimal("215.000000") == Decimal("215.00") is True, so an equality
    assertion here would pass no matter what the database did. The exponent
    is the only thing that actually observes the padding.
    """
    event = make_event(price="215.00")

    save_trade(db_session, event)
    db_session.commit()

    stored = db_session.execute(
        select(Trade).where(Trade.event_id == event.event_id)
    ).scalar_one()

    assert stored.price.as_tuple().exponent == -6


def test_the_price_column_still_matches_what_TradeEvent_enforces(
    db_session,
) -> None:
    """The drift test.

    TradeEvent duplicates NUMERIC(18,6) as max_digits/decimal_places, because
    a Pydantic model cannot see a column type. This is what makes that
    duplication safe: widen the column without widening the schema and this
    goes red, instead of the pipeline quietly rejecting valid prices.
    """
    precision, scale = db_session.execute(
        text(
            """
            SELECT numeric_precision, numeric_scale
              FROM information_schema.columns
             WHERE table_name = 'trades'
               AND column_name = 'price'
            """
        )
    ).one()

    constraint = TradeEvent.model_fields["price"].metadata

    assert (precision, scale) == (18, 6)
    assert any(getattr(m, "max_digits", None) == precision for m in constraint)
    assert any(getattr(m, "decimal_places", None) == scale for m in constraint)