"""Query construction, separated from HTTP handling.

The routes own status codes, validation and response shapes. These functions
own what is asked of the database. Nothing here touches a Session: a query is a
value, so it can be compiled and asserted without a database, a client or a
container. That is what makes the ordering decision below testable at all.

The split to hold onto: queries.py builds statements and never executes them;
routes.py executes statements and never builds one.
"""

from __future__ import annotations

from market_core import Trade
from sqlalchemy import Select, distinct, select


def normalize_symbol(symbol: str) -> str:
    """The one definition of what a symbol looks like in storage.

    Called by the queries below, and by the routes that echo the symbol back in
    a 404 message. If the rule ever grows -- stripping whitespace, handling a
    class suffix like BRK.B -- it grows here and everywhere follows.
    """
    return symbol.upper()


def select_trades(*, limit: int, offset: int) -> Select:
    """Newest first, with a TOTAL order.

    event_time is not unique: two trades can share a timestamp to the
    microsecond. SQL sorts are not guaranteed stable, so ordering by event_time
    alone leaves tied rows in whatever order the planner produces, and two
    identical queries may disagree. id is unique and monotonic, so adding it as
    a tiebreaker makes the order deterministic. It is never consulted unless
    two event_times are equal.

    id is the SECOND key, not the first, on purpose. id records insertion
    order, which is not event order -- Kafka orders within a partition only, at
    least-once redelivery replays messages, and a restart or backfill writes
    old events with new ids. Ordering by id alone would answer "most recently
    stored" while claiming to answer "newest".

    This also makes OFFSET paging coherent over a fixed dataset. It does NOT
    fix the other half of offset pagination: a row inserted between a client's
    request for offset=0 and offset=100 shifts everything down by one, and the
    client never sees a row. Solving that needs keyset pagination --
    WHERE (event_time, id) < (:last_time, :last_id) -- which is a different API
    and a deliberate future change.
    """
    return (
        select(Trade)
        .order_by(Trade.event_time.desc(),Trade.id.desc())
        .offset(offset)
        .limit(limit)
    )


def select_trades_by_symbol(*, symbol: str, limit: int) -> Select:
    """Normalises the symbol itself, so this cannot be called with a raw one.

    The route normalises as well, for its 404 message -- but through the same
    normalize_symbol, so that is a second call site, not a second rule.
    """
    return (
        select(Trade)
        .where(Trade.symbol == normalize_symbol(symbol))
        .order_by(Trade.event_time.desc(),Trade.id.desc())
        .limit(limit)
    )


def select_latest_trade(*, symbol: str) -> Select:
    """The single most recent trade for a symbol.

    The total order matters more here than anywhere else. With LIMIT 1 over
    tied timestamps, an unstable sort does not produce a wobbly order -- it
    produces a WRONG ANSWER, silently, and a different one each call.
    """
    return (
        select(Trade)
        .where(Trade.symbol == normalize_symbol(symbol))
        .order_by(Trade.event_time.desc(),Trade.id.desc())
        .limit(1)
    )


def select_distinct_symbols() -> Select:
    """Every symbol that has at least one stored trade, alphabetically.

    No tiebreaker needed: the rows are distinct symbols, so the sort key is
    already unique.
    """
    return select(distinct(Trade.symbol)).order_by(Trade.symbol)