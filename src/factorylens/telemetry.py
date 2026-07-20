"""OpenTelemetry tracer setup and export to SigNoz Cloud.

Export path: OTLP over HTTP/proto to the region ingest endpoint on
:443, authenticating with the ``signoz-ingestion-key`` header. When telemetry is
disabled or unconfigured, spans go to the console instead, so the whole pipeline
is runnable and testable before SigNoz auth is wired up (the scope's Day-1 risk).
"""

from __future__ import annotations

from dataclasses import dataclass

from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SimpleSpanProcessor,
    SpanExporter,
)
from opentelemetry.trace import Tracer

from factorylens.config import Settings, get_settings
from factorylens.logging import get_logger

_log = get_logger("telemetry")


@dataclass
class Telemetry:
    """A configured tracer provider plus lifecycle helpers.

    Not installed as the global provider — held and passed explicitly, so tests
    can spin up isolated providers without leaking state into each other.
    """

    provider: TracerProvider
    exporting_to_signoz: bool

    def tracer(self, name: str = "factorylens") -> Tracer:
        return self.provider.get_tracer(name)

    def force_flush(self, timeout_millis: int = 5000) -> None:
        self.provider.force_flush(timeout_millis)

    def shutdown(self) -> None:
        self.provider.shutdown()


def _traces_endpoint(base: str) -> str:
    """Turn a SigNoz ingest base URL into the OTLP/HTTP traces endpoint."""
    return base.rstrip("/") + "/v1/traces"


def setup_telemetry(
    settings: Settings | None = None,
    *,
    exporter: SpanExporter | None = None,
) -> Telemetry:
    """Build a tracer provider.

    - ``exporter`` given (tests): use it via a SimpleSpanProcessor (spans export
      the instant they end, so assertions see them without flushing races).
    - telemetry enabled + SigNoz configured: OTLP/HTTP → SigNoz, batched.
    - otherwise: ConsoleSpanExporter, so runs still produce visible spans.
    """
    settings = settings or get_settings()
    resource = Resource.create({"service.name": settings.otel_service_name})
    provider = TracerProvider(resource=resource)

    if exporter is not None:
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        return Telemetry(provider=provider, exporting_to_signoz=False)

    if settings.telemetry_enabled and settings.signoz_configured:
        # Imported lazily so the package works without the OTLP exporter deps
        # installed in a minimal/test environment.
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter,
        )

        otlp = OTLPSpanExporter(
            endpoint=_traces_endpoint(settings.signoz_otlp_endpoint),
            headers={"signoz-ingestion-key": settings.signoz_ingestion_key},
        )
        provider.add_span_processor(BatchSpanProcessor(otlp))
        _log.info(
            "telemetry_configured",
            exporter="otlp_http",
            endpoint=settings.signoz_otlp_endpoint,
            service=settings.otel_service_name,
        )
        return Telemetry(provider=provider, exporting_to_signoz=True)

    provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
    _log.info(
        "telemetry_console_only",
        reason="disabled_or_unconfigured",
        telemetry_enabled=settings.telemetry_enabled,
        signoz_configured=settings.signoz_configured,
    )
    return Telemetry(provider=provider, exporting_to_signoz=False)
