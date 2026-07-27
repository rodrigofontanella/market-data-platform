from market_core.config.base import BaseServiceSettings


class DatabaseSettings(BaseServiceSettings):
    """Shared PostgreSQL connection settings."""

    database_url: str = (
        "postgresql+psycopg://"
        "market_user:market_password@localhost:5433/market_data"
    )
