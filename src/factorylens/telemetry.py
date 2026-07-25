"""OpenTelemetry tracer/meter/logger setup and export to SigNoz.

Export path: OTLP over HTTP/proto. The same code targets both
deployments — only the endpoint and auth differ, and both come from
env:

  - **Cloud**: region ingest endpoint on :443, ``signoz-ingestion-key`` header.
  - **Self-hosted** (Foundry/Docker Compose): ``http://localhost:4318``, no key.

When telemetry is disabled or unconfigured, spans go to the console instead, so
the whole pipeline is runnable and testable before any SigNoz exists (the scope's
Day-1 risk).

All three signals live here:
  - **Traces** carry per-*unit-of-work* facts — a pipeline stage, a closed
    window, a fired alert.
  - **Metrics** carry per-*reading* facts, too fast to be spans.
  - **Logs** are the structlog stream, shipped as OTel logs so the same
    structured fields land in SigNoz next to the spans they mirror.
A run with nothing configured gets no-op providers, so instrument calls are
always safe to make.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from opentelemetry._logs import LoggerProvider, NoOpLoggerProvider
from opentelemetry.metrics import Meter, MeterProvider, NoOpMeterProvider
from opentelemetry.sdk._logs import LoggerProvider as SdkLoggerProvider
from opentelemetry.sdk._logs.export import (
    BatchLogRecordProcessor,
    LogExporter,
    SimpleLogRecordProcessor,
)
from opentelemetry.sdk.metrics import MeterProvider as SdkMeterProvider
from opentelemetry.sdk.metrics.export import MetricReader, PeriodicExportingMetricReader
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
from factorylens.logging import attach_otel_logging, get_logger

_log = get_logger("telemetry")

# Metrics export interval. Well under the 60s SDK default on purpose: a demo
# stream runs for ~30 seconds, and a panel that stays empty until the process
# exits is worse than no panel at all.
METRIC_EXPORT_INTERVAL_MS = 5000


@dataclass
class Telemetry:
    """A configured tracer provider plus lifecycle helpers.

    Not installed as the global provider — held and passed explicitly, so tests
    can spin up isolated providers without leaking state into each other.
    """

    provider: TracerProvider
    exporting_to_signoz: bool
    meter_provider: MeterProvider = field(default_factory=NoOpMeterProvider)
    logger_provider: LoggerProvider = field(default_factory=NoOpLoggerProvider)

    def tracer(self, name: str = "factorylens") -> Tracer:
        return self.provider.get_tracer(name)

    def meter(self, name: str = "factorylens") -> Meter:
        return self.meter_provider.get_meter(name)

    def force_flush(self, timeout_millis: int = 5000) -> bool:
        flushed = self.provider.force_flush(timeout_millis)
        if isinstance(self.meter_provider, SdkMeterProvider):
            flushed = self.meter_provider.force_flush(timeout_millis) and flushed
        if isinstance(self.logger_provider, SdkLoggerProvider):
            flushed = self.logger_provider.force_flush(timeout_millis) and flushed
        return flushed

    def shutdown(self) -> None:
        self.provider.shutdown()
        if isinstance(self.meter_provider, SdkMeterProvider):
            self.meter_provider.shutdown()
        if isinstance(self.logger_provider, SdkLoggerProvider):
            self.logger_provider.shutdown()


def _traces_endpoint(base: str) -> str:
    """Turn a SigNoz ingest base URL into the OTLP/HTTP traces endpoint."""
    return base.rstrip("/") + "/v1/traces"


def _metrics_endpoint(base: str) -> str:
    """Turn a SigNoz ingest base URL into the OTLP/HTTP metrics endpoint."""
    return base.rstrip("/") + "/v1/metrics"


def _logs_endpoint(base: str) -> str:
    """Turn a SigNoz ingest base URL into the OTLP/HTTP logs endpoint."""
    return base.rstrip("/") + "/v1/logs"


def _build_meter_provider(
    settings: Settings, resource: Resource, reader: MetricReader | None
) -> MeterProvider:
    """Build a meter provider, or a no-op one when there is nowhere to send to.

    A ``reader`` is passed by tests (typically an ``InMemoryMetricReader``) and
    wins over the configured exporter, mirroring how ``exporter`` works for
    spans. Unconfigured runs get :class:`NoOpMeterProvider` rather than a console
    exporter: at streaming rates, console metrics would bury the CLI output the
    user is actually reading.
    """
    if reader is not None:
        return SdkMeterProvider(resource=resource, metric_readers=[reader])

    if settings.telemetry_enabled and settings.signoz_configured:
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
            OTLPMetricExporter,
        )

        otlp = OTLPMetricExporter(
            endpoint=_metrics_endpoint(settings.signoz_otlp_endpoint),
            headers=settings.otlp_headers,
        )
        periodic = PeriodicExportingMetricReader(
            otlp, export_interval_millis=METRIC_EXPORT_INTERVAL_MS
        )
        return SdkMeterProvider(resource=resource, metric_readers=[periodic])

    return NoOpMeterProvider()


def _build_logger_provider(
    settings: Settings, resource: Resource, exporter: LogExporter | None
) -> LoggerProvider:
    """Build a logger provider, or a no-op one when there is nowhere to send to.

    ``exporter`` is passed by tests (an ``InMemoryLogExporter``) and wins over
    the configured OTLP exporter, mirroring ``exporter``/``metric_reader`` for
    the other two signals. Unconfigured runs get a no-op provider: structlog
    already prints to the console, so there is nothing to duplicate.
    """
    if exporter is not None:
        provider = SdkLoggerProvider(resource=resource)
        provider.add_log_record_processor(SimpleLogRecordProcessor(exporter))
        return provider

    if settings.telemetry_enabled and settings.signoz_configured:
        from opentelemetry.exporter.otlp.proto.http._log_exporter import (
            OTLPLogExporter,
        )

        provider = SdkLoggerProvider(resource=resource)
        otlp = OTLPLogExporter(
            endpoint=_logs_endpoint(settings.signoz_otlp_endpoint),
            headers=settings.otlp_headers,
        )
        provider.add_log_record_processor(BatchLogRecordProcessor(otlp))
        return provider

    return NoOpLoggerProvider()


def setup_telemetry(
    settings: Settings | None = None,
    *,
    exporter: SpanExporter | None = None,
    capture: SpanExporter | None = None,
    metric_reader: MetricReader | None = None,
    log_exporter: LogExporter | None = None,
) -> Telemetry:
    """Build tracer, meter, and logger providers together.

    - ``exporter`` given (tests): use it via a SimpleSpanProcessor (spans export
      the instant they end, so assertions see them without flushing races).
    - telemetry enabled + SigNoz configured (Cloud key OR self-hosted): OTLP/HTTP
      → SigNoz, batched, auth header only when a key is set.
    - otherwise: ConsoleSpanExporter, so runs still produce visible spans.

    ``capture`` adds a second span exporter *alongside* the primary one. The Q&A
    agent uses this to read the very same spans that were shipped to SigNoz.
    ``metric_reader`` and ``log_exporter`` are the metrics/logs equivalents of
    ``exporter``: tests pass in-memory ones and assert on what was recorded.
    """
    settings = settings or get_settings()
    resource = Resource.create({"service.name": settings.otel_service_name})
    provider = TracerProvider(resource=resource)
    meter_provider = _build_meter_provider(settings, resource, metric_reader)
    logger_provider = _build_logger_provider(settings, resource, log_exporter)
    # Bridge structlog -> OTel logs. A no-op provider makes this a cheap no-op,
    # so the console log stream is unchanged when nothing is configured.
    attach_otel_logging(logger_provider)
    if capture is not None:
        provider.add_span_processor(SimpleSpanProcessor(capture))

    def build(exporting: bool) -> Telemetry:
        return Telemetry(
            provider=provider,
            exporting_to_signoz=exporting,
            meter_provider=meter_provider,
            logger_provider=logger_provider,
        )

    if exporter is not None:
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        return build(False)

    if settings.telemetry_enabled and settings.signoz_configured:
        # Imported lazily so the package works without the OTLP exporter deps
        # installed in a minimal/test environment.
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter,
        )

        otlp = OTLPSpanExporter(
            endpoint=_traces_endpoint(settings.signoz_otlp_endpoint),
            headers=settings.otlp_headers,
        )
        provider.add_span_processor(BatchSpanProcessor(otlp))
        _log.info(
            "telemetry_configured",
            exporter="otlp_http",
            endpoint=settings.signoz_otlp_endpoint,
            deployment="self_hosted" if settings.signoz_self_hosted else "cloud",
            service=settings.otel_service_name,
        )
        return build(True)

    provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
    _log.info(
        "telemetry_console_only",
        reason="disabled_or_unconfigured",
        telemetry_enabled=settings.telemetry_enabled,
        signoz_configured=settings.signoz_configured,
    )
    return build(False)
