import logging
import os
import structlog


def configure_logging(level: str = "INFO", log_format: str = "json") -> None:
    logging_level = getattr(logging, level.upper(), logging.INFO)

    timestamper = structlog.processors.TimeStamper(fmt="iso")

    if log_format == "console":
        processors = [
            structlog.contextvars.merge_contextvars,
            timestamper,
            structlog.processors.add_log_level,
            structlog.dev.ConsoleRenderer()
        ]
    else:
        processors = [
            structlog.contextvars.merge_contextvars,
            timestamper,
            structlog.processors.add_log_level,
            structlog.processors.EventRenamer("message"),
            structlog.processors.JSONRenderer()
        ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(logging_level),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    logging.basicConfig(level=logging_level)


def get_logger(name: str = None):
    return structlog.get_logger(name)
