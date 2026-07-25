"""Structured logging via structlog, bridged to OpenTelemetry logs.

The fields a stage logs are the same fields it puts on its OTel span — one
source of truth for "what happened here," used twice. Never use bare print()
except for direct CLI output.

Logs also ship to SigNoz as the third OTel signal. Rather than reroute
structlog through the stdlib and change how the console looks, a processor
forwards each event to OTel as a side-channel: console output is byte-for-byte
unchanged, and when no logger provider is attached the forward is a cheap no-op.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog

# Set by telemetry.attach_otel_logging once a provider exists. A stdlib
# logging.Handler from OTel — reused so structlog events get the SDK's own
# severity mapping, attribute extraction, and trace-context correlation.
_otel_handler: logging.Handler | None = None

# structlog level names -> stdlib levels, for the forwarded record.
_LEVEL_TO_STDLIB = {
    "critical": logging.CRITICAL,
    "error": logging.ERROR,
    "warning": logging.WARNING,
    "warn": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG,
    "notset": logging.NOTSET,
}

# LogRecord fields we must not copy onto the OTel record as attributes.
_STDLIB_RESERVED = frozenset(logging.makeLogRecord({}).__dict__)


def attach_otel_logging(logger_provider: Any) -> None:
    """Point the structlog->OTel bridge at ``logger_provider`` (or disable it).

    Called by ``setup_telemetry``. A no-op provider leaves the bridge inert, so
    unconfigured runs simply keep logging to the console.
    """
    global _otel_handler
    from opentelemetry.sdk._logs import LoggerProvider as SdkLoggerProvider
    from opentelemetry.sdk._logs import LoggingHandler

    if isinstance(logger_provider, SdkLoggerProvider):
        _otel_handler = LoggingHandler(
            level=logging.NOTSET, logger_provider=logger_provider
        )
    else:
        _otel_handler = None


def _forward_to_otel(_logger: Any, method_name: str, event_dict: dict) -> dict:
    """structlog processor: emit the event to OTel too, then pass it through.

    Runs just before the renderer, so ``event_dict`` still holds the raw
    structured fields (line_id, rows_in, …) rather than a formatted string.
    """
    handler = _otel_handler
    if handler is not None:
        record = logging.LogRecord(
            name="factorylens",
            level=_LEVEL_TO_STDLIB.get(method_name, logging.INFO),
            pathname="",
            lineno=0,
            msg=str(event_dict.get("event", "")),
            args=(),
            exc_info=event_dict.get("exc_info"),
        )
        # Carry the structured fields as record extras; OTel's handler turns
        # non-reserved attributes into log-record attributes on the wire.
        for key, value in event_dict.items():
            if key not in _STDLIB_RESERVED and key != "event":
                setattr(record, key, value)
        handler.emit(record)
    return event_dict


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
            _forward_to_otel,  # ship to SigNoz before rendering to the console
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)
