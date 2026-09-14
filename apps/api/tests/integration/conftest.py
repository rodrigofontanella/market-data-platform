"""Level 2b: a real PostgreSQL, no Kafka.

The container starts once per session and is migrated with Alembic, so the
schema under test is the schema production gets -- not one rebuilt from the
ORM models, which could silently diverge from migrations/.

Isolation between tests is TRUNCATE, not a rolled-back transaction. The code
under test commits for itself; redirecting those commits into a SAVEPOINT
would test a transaction nesting that never occurs in production.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from testcontainers.community.postgres import PostgresContainer
from fastapi.testclient import TestClient

# Pinned to the tag in docker/compose.postgres.yml. If those drift, these
# tests stop being evidence about the database you actually run.
POSTGRES_IMAGE = "postgres:16"


def _repo_root() -> Path:
    """Locate the repository root by its marker file.

    Counting parents ("../../../..") works right up until someone moves this
    file. Searching upward for alembic.ini does not care where the suite lives.
    """
    for candidate in Path(__file__).resolve().parents:
        if (candidate / "alembic.ini").is_file():
            return candidate

    raise RuntimeError(f"alembic.ini not found above {__file__}")


@pytest.fixture(scope="session")
def postgres_container():
    """One container for the whole session. Roughly two seconds, paid once."""
    with PostgresContainer(POSTGRES_IMAGE, driver="psycopg") as container:
        yield container


@pytest.fixture(scope="session")
def migrated_engine(postgres_container) -> Engine:
    """The container, with the full migration chain applied from zero."""
    url = postgres_container.get_connection_url()

    config = Config(str(_repo_root() / "alembic.ini"))
    config.set_main_option("sqlalchemy.url", url)

    command.upgrade(config, "head")

    engine = create_engine(url)

    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture(scope="session")
def session_factory(migrated_engine: Engine):
    """Mirrors app.database.SessionLocal, including expire_on_commit=False."""
    return sessionmaker(bind=migrated_engine, expire_on_commit=False)


@pytest.fixture
def db_session(migrated_engine: Engine, session_factory) -> Session:
    """A clean table and a fresh Session, per test.

    TRUNCATE runs BEFORE the test, not after. A test that crashes mid-way
    leaves rows behind; cleaning up front means the next test is unaffected
    by that, and the wreckage is still there to inspect.
    """
    with migrated_engine.begin() as connection:
        connection.execute(text("TRUNCATE TABLE trades RESTART IDENTITY"))

    with session_factory() as session:
        yield session


@pytest.fixture
def client(db_session):
    from app.main import app
    from app.database import get_session

    app.dependency_overrides[get_session] = lambda: db_session
    try:
        with TestClient(app) as test_client:
            yield test_client
    finally:
        app.dependency_overrides.clear()