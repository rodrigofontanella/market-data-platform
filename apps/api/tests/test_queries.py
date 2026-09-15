import pytest
from app.queries import select_latest_trade, select_trades, select_trades_by_symbol
from sqlalchemy.dialects import postgresql


@pytest.mark.parametrize(
    "statement",
    [
        pytest.param(select_trades(limit=100, offset=0), id="trades"),
        pytest.param(
            select_trades_by_symbol(symbol="AAPL", limit=100), id="trades_by_symbol"
        ),
        pytest.param(select_latest_trade(symbol="AAPL"), id="latest_trade"),
    ],
)
def test_trade_queries_order_by_a_total_key(statement) -> None:
    """event_time is not unique and SQL sorts are not stable, so ordering by it
    alone leaves tied rows in planner order -- different between two identical
    queries. id is unique and monotonic, so it makes the order deterministic.

    For latest_trade this is not cosmetic: LIMIT 1 over tied timestamps returns
    a wrong answer, silently, and possibly a different one each call.
    """
    compiled = str(statement.compile(dialect=postgresql.dialect()))
    normalised = " ".join(compiled.split())

    assert "ORDER BY trades.event_time DESC, trades.id DESC" in normalised
