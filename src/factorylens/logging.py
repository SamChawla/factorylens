"""Structured logging via structlog.

The fields a stage logs are the same fields it puts on its OTel span — one
source of truth for "what happened here," used twice (see
the instrumentation conventions). Never use bare print() except for direct CLI output.
"""

from __future__ import annotations

import logging
import sys

import structlog


def configure_logging(*, level: int = logging.INFO, json: bool = False) -> None:
    """Configure structlog once at process start.

    json=True emits one JSON object per line (good for shipping/parsing);
    json=False uses a readable console renderer (good for local dev).
    """
    renderer = (
        structlog.processors.JSONRenderer()
        if json
        else structlog.dev.ConsoleRenderer()
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)
