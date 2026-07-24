from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Market Data API"
    app_version: str = "0.1.0"

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