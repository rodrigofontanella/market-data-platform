from sqlalchemy.dialects import postgresql

from app.queries import select_trades


def test_trades_are_ordered_by_a_total_key() -> None:
    """event_time alone is not unique, and SQL sorts are not stable. Without a
    tiebreaker the order of equal timestamps is whatever the planner does, and
    OFFSET paging over it can skip or repeat rows."""
    compiled = str(
        select_trades(limit=100, offset=0).compile(dialect=postgresql.dialect())
    )
    normalised = " ".join(compiled.split())

    assert "ORDER BY trades.event_time DESC, trades.id DESC" in normalised