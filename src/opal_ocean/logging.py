"""Structured logging helpers for the Opal to Ocean pipelines."""
from __future__ import annotations

import logging
from typing import Any, Dict

import structlog


def configure_logging() -> None:
    """Configure structlog for JSON-formatted logs."""
    logging.basicConfig(format="%(message)s", level=logging.INFO)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Return a structlog logger with a default name binding."""
    logger = structlog.get_logger(name)
    return logger.bind(logger=name)


def log_event(logger: structlog.stdlib.BoundLogger, event: str, **extra: Dict[str, Any]) -> None:
    """Helper to emit a structured log event with additional context."""
    logger.info(event, **extra)
