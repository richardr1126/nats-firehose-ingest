"""Configuration module for NATS firehose ingest service."""

import os
from typing import List, Optional
from pydantic import BaseModel, Field


class Settings(BaseModel):
    """Application settings with environment variable support."""
    
    # NATS Configuration
    nats_url: str = Field(default="nats://nats.nats.svc.cluster.local:4222")
    nats_stream: str = Field(default="bluesky-posts")
    nats_subject: str = Field(default="bluesky.posts")
    nats_consumer_name: str = Field(default="firehose-ingest")
    nats_kv_bucket: str = Field(default="firehose-state")
    
    # Firehose Configuration
    service_name: str = Field(default="nats-firehose-ingest")
    cursor_save_interval: int = Field(default=1000)
    
    # Filtering Configuration
    enable_text_filtering: bool = Field(default=False)  # Temporarily disabled for testing
    min_text_length: int = Field(default=1)  # Very permissive
    max_text_length: int = Field(default=10000)  # Very permissive
    
    # Performance Configuration
    max_retries: int = Field(default=3)
    
    # Health Check Configuration
    health_check_port: int = Field(default=8080)
    metrics_enabled: bool = Field(default=True)
    
    # Logging
    log_level: str = Field(default="INFO")
    log_format: str = Field(default="console")
    
    def __init__(self, **kwargs):
        # Load from environment variables
        env_values = {}
        
        # Map environment variables to settings
        env_mapping = {
            "NATS_URL": "nats_url",
            "NATS_STREAM": "nats_stream", 
            "NATS_SUBJECT": "nats_subject",
            "NATS_CONSUMER_NAME": "nats_consumer_name",
            "NATS_KV_BUCKET": "nats_kv_bucket",
            "SERVICE_NAME": "service_name",
            "CURSOR_SAVE_INTERVAL": "cursor_save_interval",
            "ENABLE_TEXT_FILTERING": "enable_text_filtering",
            "MIN_TEXT_LENGTH": "min_text_length",
            "MAX_TEXT_LENGTH": "max_text_length",
            "MAX_RETRIES": "max_retries",
            "HEALTH_CHECK_PORT": "health_check_port",
            "METRICS_ENABLED": "metrics_enabled",
            "LOG_LEVEL": "log_level",
            "LOG_FORMAT": "log_format",
        }
        
        for env_var, field_name in env_mapping.items():
            if env_var in os.environ:
                value = os.environ[env_var]
                
                # Convert types
                if field_name in ["cursor_save_interval", "min_text_length", "max_text_length", 
                                "max_retries", "health_check_port"]:
                    try:
                        value = int(value)
                    except ValueError:
                        pass
                elif field_name in ["enable_text_filtering", "metrics_enabled"]:
                    value = value.lower() in ("true", "1", "yes", "on")
                
                env_values[field_name] = value
        
        # Merge kwargs with env values (kwargs take precedence)
        final_values = {**env_values, **kwargs}
        super().__init__(**final_values)


# Global settings instance
settings = Settings()
