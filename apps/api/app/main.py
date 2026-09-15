import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from market_core.logging import configure_logging
from prometheus_client import make_asgi_app

from app.config import settings
from app.health import router as health_router
from app.middleware import RequestContextMiddleware
from app.routes import router

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

metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

app.include_router(health_router)
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