from market_core.config import KafkaSettings


class Settings(KafkaSettings):
    producer_interval_seconds: float = 1.0


settings = Settings()
