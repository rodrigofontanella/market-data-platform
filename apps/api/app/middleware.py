from __future__ import annotations

import logging
import re
import time
from collections.abc import Awaitable, Callable
from typing import Final
from uuid import uuid4

from fastapi import Request, Response
from market_core.logging import reset_request_id, set_request_id
from starlette.middleware.base import BaseHTTPMiddleware


logger = logging.getLogger(__name__)


REQUEST_ID_HEADER: Final = "X-Request-ID"

_REQUEST_ID_PATTERN: Final = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"
)


def _resolve_request_id(request: Request) -> str:
    supplied_request_id = request.headers.get(REQUEST_ID_HEADER)

    if (
        supplied_request_id
        and _REQUEST_ID_PATTERN.fullmatch(supplied_request_id)
    ):
        return supplied_request_id

    return str(uuid4())


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Create and propagate request-scoped correlation information."""

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        request_id = _resolve_request_id(request)
        token = set_request_id(request_id)
        started_at = time.perf_counter()

        logger.info(
            "request_started",
            extra={
                "http_method": request.method,
                "http_path": request.url.path,
            },
        )

        try:
            response = await call_next(request)

        except Exception:
            duration_ms = round(
                (time.perf_counter() - started_at) * 1_000,
                2,
            )

            logger.exception(
                "request_failed",
                extra={
                    "http_method": request.method,
                    "http_path": request.url.path,
                    "duration_ms": duration_ms,
                },
            )
            raise

        else:
            duration_ms = round(
                (time.perf_counter() - started_at) * 1_000,
                2,
            )

            response.headers[REQUEST_ID_HEADER] = request_id

            logger.info(
                "request_completed",
                extra={
                    "http_method": request.method,
                    "http_path": request.url.path,
                    "http_status_code": response.status_code,
                    "duration_ms": duration_ms,
                },
            )

            return response

        finally:
            reset_request_id(token)