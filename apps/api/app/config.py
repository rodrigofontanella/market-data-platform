from market_core.config import DatabaseSettings


class Settings(DatabaseSettings):
    service_name: str = "api"

    app_name: str = "Market Data API"
    app_version: str = "0.1.0"


settings = Settings()