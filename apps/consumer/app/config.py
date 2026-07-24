from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:29092"
    kafka_topic: str = "market.trades.raw"
    kafka_group_id: str = "market-trades-storage-v1"

    database_url: str = (
        "postgresql+psycopg://"
        "market_user:market_password@localhost:5433/market_data"
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()