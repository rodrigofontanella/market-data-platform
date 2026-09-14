"""Query construction, separated from HTTP handling.

The routes own status codes, validation and response shapes. These functions
own what is asked of the database. Splitting them means a query can be compiled
and inspected without a session, a client or a container -- which is how the
ordering decision below gets tested at all.
"""

from __future__ import annotations

from sqlalchemy import Select, distinct, select

from market_core import Trade


def normalize_symbol(symbol: str) -> str:
    """Symbols are stored upper-case. One definition, used by the queries and
    by the routes that report the symbol back in a 404."""
    return symbol.upper()


def select_trades(*, limit: int, offset: int) -> Select:
    """Newest first, with a TOTAL order.

    event_time is not unique -- two trades can share a timestamp to the
    microsecond -- and SQL sorts are not guaranteed stable, so ordering by it
    alone leaves tied rows in whatever order the planner produces, which may
    differ between two identical queries. id is unique and monotonic, so adding
    it makes the order deterministic.

    This also makes OFFSET paging coherent for a fixed dataset. It does NOT fix
    the other half of offset pagination: a row inserted between a client's
    request for offset=0 and offset=100 shifts everything down, and the client
    never sees one row. Solving that needs keyset pagination
    (WHERE (event_time, id) < (:last_time, :last_id)), which is a different API.
    """
    return (
        select(Trade)
        .order_by(Trade.event_time.desc(), Trade.id.desc())
        .offset(offset)
        .limit(limit)
    )


def select_trades_by_symbol(*, symbol: str, limit: int) -> Select:
    """Takes an ALREADY-NORMALIZED symbol. Normalising here as well would put
    the rule in two places; the route needs the normalised value anyway for its
    404 message."""
    return (
        select(Trade)
        .where(Trade.symbol == symbol)
        .order_by(Trade.event_time.desc(), Trade.id.desc())
        .limit(limit)
    )


def select_latest_trade(*, symbol: str) -> Select:
    """Also already-normalized. The total order matters more here than anywhere
    else: with LIMIT 1 over tied timestamps, an unstable sort means "the latest
    trade" is whichever row the planner happened to put first."""
    return (
        select(Trade)
        .where(Trade.symbol == symbol)
        .order_by(Trade.event_time.desc(), Trade.desc())
        .limit(1)
    )


def select_distinct_symbols() -> Select:
    return select(distinct(Trade.symbol)).order_by(Trade.symbol)