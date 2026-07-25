"""Tests for telemetry export config: Cloud vs self-hosted, and the logs bridge.

The self-hosted path is the one worth pinning: a keyless config must
still export (to the local collector) rather than silently falling back to the
console, and it must send *no* auth header. The logs bridge must carry
structlog's structured fields onto the OTel log record.
"""

from __future__ import annotations

import logging

import pytest
from opentelemetry.sdk._logs import LoggerProvider as SdkLoggerProvider
from opentelemetry.sdk._logs.export import InMemoryLogExporter

from factorylens.config import Settings
from factorylens.logging import configure_logging, get_logger
from factorylens.telemetry import (
    _logs_endpoint,
    _traces_endpoint,
    setup_telemetry,
)


# --- config: what counts as "we have a SigNoz to export to" ------------------


def test_cloud_key_is_configured_and_sends_auth_header():
    s = Settings(signoz_ingestion_key="k", signoz_self_hosted=False)
    assert s.signoz_configured is True
    assert s.otlp_headers == {"signoz-ingestion-key": "k"}


def test_self_hosted_is_configured_with_no_key_and_no_header():
    s = Settings(
        signoz_ingestion_key="",
        signoz_self_hosted=True,
        signoz_otlp_endpoint="http://localhost:4318",
    )
    assert s.signoz_configured is True
    assert s.otlp_headers == {}  # self-hosted needs no auth header


def test_neither_key_nor_self_hosted_is_unconfigured():
    s = Settings(signoz_ingestion_key="", signoz_self_hosted=False)
    assert s.signoz_configured is False
    assert s.otlp_headers == {}


def test_endpoint_helpers_target_the_local_collector():
    assert _traces_endpoint("http://localhost:4318") == "http://localhost:4318/v1/traces"
    assert _logs_endpoint("http://localhost:4318/") == "http://localhost:4318/v1/logs"


# --- setup_telemetry export decision -----------------------------------------


def test_self_hosted_config_exports_rather_than_console():
    """A keyless self-hosted config must actually export, not fall to console."""
    s = Settings(
        telemetry_enabled=True,
        signoz_ingestion_key="",
        signoz_self_hosted=True,
        signoz_otlp_endpoint="http://localhost:4318",
    )
    tel = setup_telemetry(s)
    try:
        assert tel.exporting_to_signoz is True
    finally:
        tel.shutdown()


def test_unconfigured_run_stays_console_only():
    s = Settings(telemetry_enabled=True, signoz_ingestion_key="", signoz_self_hosted=False)
    tel = setup_telemetry(s)
    try:
        assert tel.exporting_to_signoz is False
    finally:
        tel.shutdown()


def test_disabled_telemetry_never_exports_even_self_hosted():
    s = Settings(telemetry_enabled=False, signoz_self_hosted=True)
    tel = setup_telemetry(s)
    try:
        assert tel.exporting_to_signoz is False
    finally:
        tel.shutdown()


# --- logs: structlog -> OTel bridge ------------------------------------------


@pytest.fixture
def log_capture():
    configure_logging(level=logging.INFO)
    exporter = InMemoryLogExporter()
    tel = setup_telemetry(Settings(telemetry_enabled=False), log_exporter=exporter)
    yield tel, exporter
    tel.shutdown()


def _record_named(exporter, body):
    for r in exporter.get_finished_logs():
        if r.log_record.body == body:
            return r.log_record
    return None


def test_structlog_event_reaches_otel_with_its_fields(log_capture):
    tel, exporter = log_capture
    get_logger("pipeline").info(
        "clean", line_id="line_3", rows_dropped=41, null_ratio=0.09
    )
    tel.force_flush()

    rec = _record_named(exporter, "clean")
    assert rec is not None, "the 'clean' event never reached the OTel log exporter"
    attrs = dict(rec.attributes or {})
    assert attrs["line_id"] == "line_3"
    assert attrs["rows_dropped"] == 41
    assert attrs["null_ratio"] == 0.09
    assert rec.severity_text == "INFO"


def test_logger_provider_is_real_when_configured(log_capture):
    tel, _ = log_capture
    assert isinstance(tel.logger_provider, SdkLoggerProvider)


def test_no_logger_provider_when_unconfigured():
    from opentelemetry._logs import NoOpLoggerProvider

    tel = setup_telemetry(Settings(telemetry_enabled=False))
    try:
        assert isinstance(tel.logger_provider, NoOpLoggerProvider)
    finally:
        tel.shutdown()


def test_warning_level_maps_through(log_capture):
    tel, exporter = log_capture
    get_logger("llm").warning("llm_provider_failed", provider="euri")
    tel.force_flush()
    rec = _record_named(exporter, "llm_provider_failed")
    assert rec is not None
    assert rec.severity_text == "WARN"
    assert dict(rec.attributes or {})["provider"] == "euri"
