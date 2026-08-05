from __future__ import annotations

import logging
import time
from typing import Literal

from fastapi import APIRouter, Response, status
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.database import SessionLocal


logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/health",
    tags=["health"],
)


class LivenessResponse(BaseModel):
    status: Literal["alive"]


class DependencyStatus(BaseModel):
    status: Literal["up", "down"]
    latency_ms: float | None = None


class ReadinessResponse(BaseModel):
    status: Literal["ready", "not_ready"]
    checks: dict[str, DependencyStatus]


@router.get(
    "/live",
    response_model=LivenessResponse,
    status_code=status.HTTP_200_OK,
)
def liveness() -> LivenessResponse:
    return LivenessResponse(status="alive")


@router.get(
    "/ready",
    response_model=ReadinessResponse,
    responses={
        status.HTTP_503_SERVICE_UNAVAILABLE: {
            "model": ReadinessResponse,
            "description": "The API is alive but not ready to serve traffic.",
        }
    },
)
def readiness(response: Response) -> ReadinessResponse:
    started_at = time.perf_counter()

    try:
        with SessionLocal() as session:
            session.execute(text("SELECT 1"))

    except SQLAlchemyError:
        latency_ms = round(
            (time.perf_counter() - started_at) * 1_000,
            2,
        )

        logger.exception(
            "readiness_check_failed",
            extra={
                "dependency": "postgresql",
                "latency_ms": latency_ms,
            },
        )

        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

        return ReadinessResponse(
            status="not_ready",
            checks={
                "postgresql": DependencyStatus(
                    status="down",
                    latency_ms=latency_ms,
                )
            },
        )

    latency_ms = round(
        (time.perf_counter() - started_at) * 1_000,
        2,
    )

    logger.debug(
        "readiness_check_passed",
        extra={
            "dependency": "postgresql",
            "latency_ms": latency_ms,
        },
    )

    return ReadinessResponse(
        status="ready",
        checks={
            "postgresql": DependencyStatus(
                status="up",
                latency_ms=latency_ms,
            )
        },
    )