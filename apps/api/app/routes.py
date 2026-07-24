from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import distinct, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.database import get_session
from market_core import Trade, TradeResponse
from app.schemas import HealthResponse, SymbolResponse

router = APIRouter()

DatabaseSession = Annotated[Session, Depends(get_session)]


@router.get(
    "/health",
    response_model=HealthResponse,
    tags=["health"],
)
def health_check(session: DatabaseSession) -> HealthResponse:
    try:
        session.execute(text("SELECT 1"))

        return HealthResponse(
            status="healthy",
            database="connected",
        )

    except SQLAlchemyError as error:
        raise HTTPException(
            status_code=503,
            detail="Database connection failed",
        ) from error


@router.get(
    "/symbols",
    response_model=list[SymbolResponse],
    tags=["market-data"],
)
def list_symbols(
    session: DatabaseSession,
) -> list[SymbolResponse]:
    statement = (
        select(distinct(Trade.symbol))
        .order_by(Trade.symbol)
    )

    symbols = session.scalars(statement).all()

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
    statement = (
        select(Trade)
        .order_by(Trade.event_time.desc())
        .offset(offset)
        .limit(limit)
    )

    return list(session.scalars(statement).all())


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
    normalized_symbol = symbol.upper()

    statement = (
        select(Trade)
        .where(Trade.symbol == normalized_symbol)
        .order_by(Trade.event_time.desc())
        .limit(limit)
    )

    trades = list(session.scalars(statement).all())

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
    normalized_symbol = symbol.upper()

    statement = (
        select(Trade)
        .where(Trade.symbol == normalized_symbol)
        .order_by(Trade.event_time.desc())
        .limit(1)
    )

    trade = session.scalar(statement)

    if trade is None:
        raise HTTPException(
            status_code=404,
            detail=f"No trades found for symbol {normalized_symbol}",
        )

    return trade