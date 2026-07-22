"""The LLM adapter: one interface, Euri primary with Groq fallback.

Per the coding standards, provider branching lives here and nowhere else — the
CLI calls ``ask(question)`` and never learns which provider answered. Both
providers speak the OpenAI chat-completions shape, so one client covers both and
swapping in a third is a list entry, not a refactor.

The call is itself instrumented: this is an *agent observability* project, so
the agent's own LLM calls emit a span recording which provider served the
answer and whether the fallback was used. When the primary is degraded, that
span is how you find out.
"""

from __future__ import annotations

from dataclasses import dataclass

import requests
from opentelemetry.trace import Tracer

from factorylens.config import Settings, get_settings
from factorylens.exceptions import LLMProviderError
from factorylens.logging import get_logger

_log = get_logger("llm")

DEFAULT_SYSTEM_PROMPT = (
    "You are a manufacturing pipeline observability assistant. Answer from the "
    "telemetry you are given, in plain language a plant engineer would use. If "
    "the data does not support an answer, say so rather than guessing."
)


@dataclass(frozen=True)
class ProviderConfig:
    name: str
    base_url: str
    api_key: str
    model: str

    @property
    def configured(self) -> bool:
        return bool(self.api_key and self.base_url and self.model)

    @property
    def endpoint(self) -> str:
        return self.base_url.rstrip("/") + "/chat/completions"


def _providers(settings: Settings) -> list[ProviderConfig]:
    """Providers in fallback order: primary first."""
    return [
        ProviderConfig("euri", settings.euri_base_url, settings.euri_api_key, settings.euri_model),
        ProviderConfig("groq", settings.groq_base_url, settings.groq_api_key, settings.groq_model),
    ]


def _call(provider: ProviderConfig, messages: list[dict], timeout: float) -> str:
    """Call one provider. Raises LLMProviderError on any failure mode."""
    try:
        response = requests.post(
            provider.endpoint,
            headers={
                "Authorization": f"Bearer {provider.api_key}",
                "Content-Type": "application/json",
            },
            json={"model": provider.model, "messages": messages},
            timeout=timeout,
        )
    except Exception as e:  # network, DNS, TLS, timeout
        raise LLMProviderError(f"{provider.name} request failed: {e}") from e

    if response.status_code != 200:
        raise LLMProviderError(
            f"{provider.name} returned HTTP {response.status_code}: {response.text[:200]}"
        )

    try:
        content = response.json()["choices"][0]["message"]["content"]
    except Exception as e:
        raise LLMProviderError(
            f"{provider.name} returned an unexpected response shape: {e}"
        ) from e

    if not content or not content.strip():
        raise LLMProviderError(f"{provider.name} returned an empty answer")
    return content.strip()


def ask(
    question: str,
    *,
    system: str | None = None,
    settings: Settings | None = None,
    tracer: Tracer | None = None,
    timeout: float = 60.0,
) -> str:
    """Ask the question, trying each configured provider in order.

    Returns the first successful answer. Raises ``LLMProviderError`` naming every
    provider that failed if none succeed — a fallback that fails silently is
    worse than no fallback.
    """
    settings = settings or get_settings()
    messages = [
        {"role": "system", "content": system or DEFAULT_SYSTEM_PROMPT},
        {"role": "user", "content": question},
    ]

    span_cm = (
        tracer.start_as_current_span("llm_ask") if tracer is not None else _NullSpan()
    )
    with span_cm as span:
        failures: list[str] = []
        for attempt, provider in enumerate(_providers(settings)):
            if not provider.configured:
                _log.info("llm_provider_skipped", provider=provider.name, reason="not_configured")
                continue
            try:
                answer = _call(provider, messages, timeout)
            except LLMProviderError as e:
                failures.append(str(e))
                _log.warning("llm_provider_failed", provider=provider.name, error=str(e))
                continue

            fields = {
                "provider": provider.name,
                "model": provider.model,
                "fallback_used": bool(failures),
                "attempts": attempt + 1,
            }
            if span is not None:
                for key, value in fields.items():
                    span.set_attribute(key, value)
            _log.info("llm_answered", **fields)
            return answer

        if not failures:
            raise LLMProviderError(
                "no LLM provider is configured — set EURI_API_KEY or GROQ_API_KEY in .env"
            )
        raise LLMProviderError("all LLM providers failed: " + " | ".join(failures))


class _NullSpan:
    """Context manager used when no tracer is supplied."""

    def __enter__(self):
        return None

    def __exit__(self, *exc_info):
        return False
