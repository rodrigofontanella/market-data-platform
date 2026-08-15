from market_core.config import KafkaSettings


class Settings(KafkaSettings):
    service_name: str = "producer"

    producer_interval_seconds: float = 1.0


settings = Settings()