from typing import Annotated

from fastapi import APIRouter, HTTPException, Query
from market_core import Trade, TradeResponse

from app.database import DatabaseSession
from app.queries import (
    normalize_symbol,
    select_distinct_symbols,
    select_latest_trade,
    select_trades,
    select_trades_by_symbol,
)
from app.schemas import SymbolResponse

router = APIRouter()


@router.get(
    "/symbols",
    response_model=list[SymbolResponse],
    tags=["market-data"],
)
def list_symbols(
    session: DatabaseSession,
) -> list[SymbolResponse]:
    symbols = session.scalars(select_distinct_symbols()).all()

    return [
        SymbolResponse(symbol=symbol)
        for symbol in symbols
    ]


@router.get(
    "/trades",
    response_model=list[TradeResponse],
    tags=["market-data"],
)
def list_trades(
    session: DatabaseSession,
    limit: Annotated[
        int,
        Query(ge=1, le=1000),
    ] = 100,
    offset: Annotated[
        int,
        Query(ge=0),
    ] = 0,
) -> list[Trade]:
    return list(
        session.scalars(select_trades(limit=limit, offset=offset)).all()
    )


@router.get(
    "/trades/{symbol}",
    response_model=list[TradeResponse],
    tags=["market-data"],
)
def list_trades_by_symbol(
    symbol: str,
    session: DatabaseSession,
    limit: Annotated[
        int,
        Query(ge=1, le=1000),
    ] = 100,
) -> list[Trade]:
    normalized_symbol = normalize_symbol(symbol)

    trades = list(
        session.scalars(
            select_trades_by_symbol(symbol=normalized_symbol, limit=limit)
        ).all()
    )


    if not trades:
        raise HTTPException(
            status_code=404,
            detail=f"No trades found for symbol {normalized_symbol}",
        )

    return trades


@router.get(
    "/trades/{symbol}/latest",
    response_model=TradeResponse,
    tags=["market-data"],
)
def get_latest_trade(
    symbol: str,
    session: DatabaseSession,
) -> Trade:
    normalized_symbol = normalize_symbol(symbol)

    trade = session.scalar(select_latest_trade(symbol=normalized_symbol))

    if trade is None:
        raise HTTPException(
            status_code=404,
            detail=f"No trades found for symbol {normalized_symbol}",
        )

    return trade