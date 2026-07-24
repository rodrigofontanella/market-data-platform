import logging

from fastapi import FastAPI

from app.config import settings
from app.routes import router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description=(
        "Read-only API for querying market trade events "
        "stored by the Kafka consumer."
    ),
)

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