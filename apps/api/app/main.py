import logging
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

from fastapi import FastAPI

from app.config import settings
from app.routes import router
from market_core.logging import configure_logging
from app.middleware import RequestContextMiddleware


# Configure logging before the application starts producing logs.
configure_logging(
    service_name=settings.service_name,
    log_level=settings.log_level,
    log_format=settings.log_format,
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """
    Manage API startup and shutdown actions.

    Code before `yield` runs when the API starts.
    Code after `yield` runs when the API stops.
    """
    logger.info(
        "service_starting",
        extra={
            "environment": settings.environment,
            "version": settings.app_version,
        },
    )

    yield

    logger.info("service_stopping")


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description=(
        "Read-only API for querying market trade events "
        "stored by the Kafka consumer."
    ),
    lifespan=lifespan,
)

app.add_middleware(RequestContextMiddleware)

app.include_router(router)


@app.get(
    "/",
    tags=["root"],
)
def root() -> dict[str, str]:
    return {
        "service": settings.app_name,
        "version": settings.app_version,
        "docs": "/docs",
    }