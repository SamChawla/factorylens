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
