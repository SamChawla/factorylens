"""Tests for the LLM adapter's provider fallback (piece 4).

Both providers are mocked — these tests never hit a real API. The fallback path
is the whole point of this module: it only ever runs when the primary is already
failing, which is exactly when nobody is watching. So it gets tested properly.
"""

from __future__ import annotations

import pytest

from factorylens import llm
from factorylens.config import Settings
from factorylens.exceptions import LLMProviderError


def _settings(**overrides) -> Settings:
    base = dict(
        euri_api_key="euri-test-key",
        euri_base_url="https://euri.test/v1",
        euri_model="euri-model",
        groq_api_key="groq-test-key",
        groq_base_url="https://groq.test/v1",
        groq_model="groq-model",
    )
    base.update(overrides)
    return Settings(**base)


class _Resp:
    """Minimal stand-in for a requests.Response."""

    def __init__(self, status_code: int, payload: dict | None = None, text: str = ""):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = text or str(self._payload)

    def json(self):
        return self._payload


def _ok(content: str) -> _Resp:
    return _Resp(200, {"choices": [{"message": {"content": content}}]})


@pytest.fixture
def calls(monkeypatch):
    """Record every provider call so tests can assert who was asked, in order."""
    recorded: list[dict] = []

    def fake_post(url, headers=None, json=None, timeout=None):
        recorded.append({"url": url, "model": json["model"], "headers": headers or {}})
        return recorded[-1].setdefault("response", _ok("default"))

    monkeypatch.setattr(llm.requests, "post", fake_post)
    return recorded


def _respond(monkeypatch, *responses):
    """Queue one response per successive provider call."""
    seq = list(responses)
    seen: list[str] = []

    def fake_post(url, headers=None, json=None, timeout=None):
        seen.append(json["model"])
        result = seq.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(llm.requests, "post", fake_post)
    return seen


def test_primary_answers_and_fallback_is_never_called(monkeypatch):
    seen = _respond(monkeypatch, _ok("from euri"))
    answer = llm.ask("why is line_3 OEE low?", settings=_settings())
    assert answer == "from euri"
    assert seen == ["euri-model"]  # Groq never touched


def test_falls_back_when_primary_errors(monkeypatch):
    seen = _respond(monkeypatch, _Resp(500, text="boom"), _ok("from groq"))
    answer = llm.ask("q", settings=_settings())
    assert answer == "from groq"
    assert seen == ["euri-model", "groq-model"]


def test_falls_back_on_rate_limit(monkeypatch):
    seen = _respond(monkeypatch, _Resp(429, text="rate limited"), _ok("from groq"))
    assert llm.ask("q", settings=_settings()) == "from groq"
    assert seen == ["euri-model", "groq-model"]


def test_falls_back_on_network_exception(monkeypatch):
    seen = _respond(monkeypatch, ConnectionError("dns failure"), _ok("from groq"))
    assert llm.ask("q", settings=_settings()) == "from groq"
    assert seen == ["euri-model", "groq-model"]


def test_falls_back_on_malformed_response(monkeypatch):
    seen = _respond(monkeypatch, _Resp(200, {"unexpected": "shape"}), _ok("from groq"))
    assert llm.ask("q", settings=_settings()) == "from groq"
    assert seen == ["euri-model", "groq-model"]


def test_raises_when_both_providers_fail(monkeypatch):
    _respond(monkeypatch, _Resp(500, text="euri down"), _Resp(503, text="groq down"))
    with pytest.raises(LLMProviderError) as exc:
        llm.ask("q", settings=_settings())
    message = str(exc.value)
    # The error names both providers, so the failure is diagnosable.
    assert "euri" in message.lower() and "groq" in message.lower()


def test_provider_without_a_key_is_skipped(monkeypatch):
    seen = _respond(monkeypatch, _ok("from groq"))
    answer = llm.ask("q", settings=_settings(euri_api_key=""))
    assert answer == "from groq"
    assert seen == ["groq-model"]  # unkeyed primary never called


def test_raises_when_no_provider_is_configured(monkeypatch):
    _respond(monkeypatch)
    with pytest.raises(LLMProviderError, match="no LLM provider"):
        llm.ask("q", settings=_settings(euri_api_key="", groq_api_key=""))


def test_sends_bearer_auth_and_the_question(monkeypatch):
    captured = {}

    def fake_post(url, headers=None, json=None, timeout=None):
        captured.update(url=url, headers=headers, json=json)
        return _ok("ok")

    monkeypatch.setattr(llm.requests, "post", fake_post)
    llm.ask("why is line_3 OEE low?", system="you are terse", settings=_settings())

    assert captured["url"] == "https://euri.test/v1/chat/completions"
    assert captured["headers"]["Authorization"] == "Bearer euri-test-key"
    roles = [m["role"] for m in captured["json"]["messages"]]
    contents = " ".join(m["content"] for m in captured["json"]["messages"])
    assert roles == ["system", "user"]
    assert "why is line_3 OEE low?" in contents


# --- GenAI semantic-convention instrumentation (piece 7) ---------------------


def _ok_with_usage(content: str, *, model="groq-model", prompt=120, completion=45,
                   finish="stop") -> _Resp:
    """An OpenAI-compatible response carrying the usual accounting block."""
    return _Resp(200, {
        "model": model,
        "choices": [{"message": {"content": content}, "finish_reason": finish}],
        "usage": {
            "prompt_tokens": prompt,
            "completion_tokens": completion,
            "total_tokens": prompt + completion,
        },
    })


def _span_of(exporter, name="llm_ask"):
    spans = [s for s in exporter.get_finished_spans() if s.name == name]
    assert spans, f"no {name} span was exported"
    return spans[-1]


@pytest.fixture
def traced():
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )

    from factorylens.telemetry import setup_telemetry

    exporter = InMemorySpanExporter()
    reader = InMemoryMetricReader()
    tel = setup_telemetry(
        Settings(telemetry_enabled=False), exporter=exporter, metric_reader=reader
    )
    yield tel, exporter, reader
    tel.shutdown()


def test_call_captures_token_usage(monkeypatch):
    monkeypatch.setattr(llm.requests, "post",
                        lambda *a, **k: _ok_with_usage("hi", prompt=120, completion=45))
    result = llm._call(llm._providers(_settings())[0], [], 30.0)
    assert result.content == "hi"
    assert result.input_tokens == 120
    assert result.output_tokens == 45
    assert result.finish_reason == "stop"


def test_missing_usage_block_still_answers(monkeypatch):
    """A provider that omits `usage` must not turn a good answer into a failure."""
    monkeypatch.setattr(llm.requests, "post", lambda *a, **k: _ok("plain answer"))
    result = llm._call(llm._providers(_settings())[0], [], 30.0)
    assert result.content == "plain answer"
    assert result.input_tokens is None and result.output_tokens is None


def test_malformed_usage_block_is_ignored_not_fatal(monkeypatch):
    bad = _Resp(200, {
        "choices": [{"message": {"content": "answer"}}],
        "usage": {"prompt_tokens": "lots", "completion_tokens": None},
    })
    monkeypatch.setattr(llm.requests, "post", lambda *a, **k: bad)
    result = llm._call(llm._providers(_settings())[0], [], 30.0)
    assert result.content == "answer"
    assert result.input_tokens is None and result.output_tokens is None


def test_span_carries_genai_semantic_conventions(monkeypatch, traced):
    tel, exporter, _ = traced
    monkeypatch.setattr(llm.requests, "post",
                        lambda *a, **k: _ok_with_usage("a", model="euri-actual"))
    llm.ask("q", settings=_settings(), tracer=tel.tracer())

    attrs = _span_of(exporter).attributes
    assert attrs["gen_ai.operation.name"] == "chat"
    assert attrs["gen_ai.system"] == "euri"
    assert attrs["gen_ai.request.model"] == "euri-model"
    assert attrs["gen_ai.response.model"] == "euri-actual"
    assert attrs["gen_ai.usage.input_tokens"] == 120
    assert attrs["gen_ai.usage.output_tokens"] == 45
    assert tuple(attrs["gen_ai.response.finish_reasons"]) == ("stop",)


def test_span_keeps_the_fallback_attributes_alongside_the_conventions(monkeypatch, traced):
    """The conventions are added, not substituted: fallback facts have no equivalent."""
    tel, exporter, _ = traced
    _respond(monkeypatch, _Resp(500, text="boom"), _ok_with_usage("from groq"))
    llm.ask("q", settings=_settings(), tracer=tel.tracer())

    attrs = _span_of(exporter).attributes
    assert attrs["provider"] == "groq"
    assert attrs["fallback_used"] is True
    assert attrs["attempts"] == 2
    assert attrs["gen_ai.system"] == "groq"


def test_span_omits_token_attributes_when_the_provider_sends_none(monkeypatch, traced):
    tel, exporter, _ = traced
    monkeypatch.setattr(llm.requests, "post", lambda *a, **k: _ok("no usage"))
    llm.ask("q", settings=_settings(), tracer=tel.tracer())

    attrs = _span_of(exporter).attributes
    assert "gen_ai.usage.input_tokens" not in attrs
    assert attrs["gen_ai.request.model"] == "euri-model"


def _points(reader, metric_name):
    data = reader.get_metrics_data()
    return [
        point
        for rm in (data.resource_metrics if data else [])
        for sm in rm.scope_metrics
        for metric in sm.metrics
        if metric.name == metric_name
        for point in metric.data.data_points
    ]


def test_token_usage_metric_splits_input_from_output(monkeypatch, traced):
    tel, _, reader = traced
    monkeypatch.setattr(llm.requests, "post",
                        lambda *a, **k: _ok_with_usage("a", prompt=120, completion=45))
    llm.ask("q", settings=_settings(), tracer=tel.tracer(), meter=tel.meter())

    by_type = {
        p.attributes["gen_ai.token.type"]: p.sum
        for p in _points(reader, llm.TOKEN_USAGE_METRIC)
    }
    assert by_type == {"input": 120, "output": 45}


def test_operation_duration_metric_is_recorded(monkeypatch, traced):
    tel, _, reader = traced
    monkeypatch.setattr(llm.requests, "post", lambda *a, **k: _ok_with_usage("a"))
    llm.ask("q", settings=_settings(), tracer=tel.tracer(), meter=tel.meter())

    points = _points(reader, llm.OPERATION_DURATION_METRIC)
    assert len(points) == 1
    assert points[0].count == 1
    assert points[0].attributes["gen_ai.system"] == "euri"


def test_metrics_are_optional(monkeypatch):
    """No meter wired up must not break the call — the adapter stays standalone."""
    monkeypatch.setattr(llm.requests, "post", lambda *a, **k: _ok_with_usage("a"))
    assert llm.ask("q", settings=_settings()) == "a"
