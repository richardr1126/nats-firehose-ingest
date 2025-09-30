import os
from dataclasses import dataclass
@dataclass(frozen=True)
class Settings:
    # NATS
    NATS_URL: str = os.getenv("NATS_URL", "nats://nats.nats.svc.cluster.local:4222")
    NATS_STREAM: str = os.getenv("NATS_STREAM", "bluesky-posts")
    NUM_STREAM_REPLICAS: int = int(os.getenv("NUM_STREAM_REPLICAS", 1))
    NATS_SUBJECT: str = os.getenv("NATS_SUBJECT", "bluesky.posts")
    NATS_CONSUMER_NAME: str = os.getenv("NATS_CONSUMER_NAME", "firehose-ingest")
    NATS_KV_BUCKET: str = os.getenv("NATS_KV_BUCKET", "firehose-state")

    # Service
    SERVICE_NAME: str = os.getenv("SERVICE_NAME", "nats-firehose-ingest")
    CURSOR_SAVE_INTERVAL: int = int(os.getenv("CURSOR_SAVE_INTERVAL", 1000))

    # Filtering
    ENABLE_TEXT_FILTERING: bool = os.getenv("ENABLE_TEXT_FILTERING", "true").lower() == "true"
    MIN_TEXT_LENGTH: int = int(os.getenv("MIN_TEXT_LENGTH", 10))
    MAX_TEXT_LENGTH: int = int(os.getenv("MAX_TEXT_LENGTH", 1000))

    # Performance
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", 3))

    # Health/metrics
    HEALTH_CHECK_PORT: int = int(os.getenv("HEALTH_CHECK_PORT", 8080))
    METRICS_ENABLED: bool = os.getenv("METRICS_ENABLED", "true").lower() == "true"

    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FORMAT: str = os.getenv("LOG_FORMAT", "json")


settings = Settings()
