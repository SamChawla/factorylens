"""The LLM adapter: one interface, Euri primary with Groq fallback.

Provider branching lives here and nowhere else — the
CLI calls ``ask(question)`` and never learns which provider answered. Both
providers speak the OpenAI chat-completions shape, so one client covers both and
swapping in a third is a list entry, not a refactor.

The call is itself instrumented: this is an *agent observability* project, so
the agent's own LLM calls emit a span recording which provider served the
answer and whether the fallback was used. When the primary is degraded, that
span is how you find out.

Instrumentation follows the OpenTelemetry **GenAI semantic conventions**
(``gen_ai.*``) rather than a vendor instrumentation library. Both
providers speak the same OpenAI-compatible wire format over plain ``requests``,
so a Groq-specific instrumentor would see neither of them; emitting the
conventions directly covers both uniformly and costs no new dependency.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

import requests
from opentelemetry.metrics import Meter
from opentelemetry.trace import Tracer

from factorylens.config import Settings, get_settings
from factorylens.exceptions import LLMProviderError
from factorylens.logging import get_logger

_log = get_logger("llm")

# GenAI semantic-convention metric names. Instruments are created lazily per
# call because a Meter is optional here — the adapter stays usable (and
# testable) with no telemetry wired up at all.
TOKEN_USAGE_METRIC = "gen_ai.client.token.usage"
OPERATION_DURATION_METRIC = "gen_ai.client.operation.duration"
OPERATION_NAME = "chat"

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


@dataclass(frozen=True)
class LLMResult:
    """One provider's answer plus the accounting the response carried with it.

    The token counts are the whole reason this type exists. Every
    OpenAI-compatible response ships a ``usage`` object, and discarding it means
    the question "what did that answer cost?" is unanswerable after the fact.
    Every field but ``content`` is optional: a provider that omits ``usage``
    still yields a perfectly good answer.
    """

    content: str
    model: str | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    finish_reason: str | None = None


def _int_or_none(value: object) -> int | None:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _call(provider: ProviderConfig, messages: list[dict], timeout: float) -> LLMResult:
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
        payload = response.json()
        choice = payload["choices"][0]
        content = choice["message"]["content"]
    except Exception as e:
        raise LLMProviderError(
            f"{provider.name} returned an unexpected response shape: {e}"
        ) from e

    if not content or not content.strip():
        raise LLMProviderError(f"{provider.name} returned an empty answer")

    # Accounting is best-effort: a missing or odd-shaped ``usage`` must never
    # turn a good answer into a failure.
    usage = payload.get("usage") or {}
    return LLMResult(
        content=content.strip(),
        model=payload.get("model") or provider.model,
        input_tokens=_int_or_none(usage.get("prompt_tokens")),
        output_tokens=_int_or_none(usage.get("completion_tokens")),
        finish_reason=choice.get("finish_reason"),
    )


def _record_metrics(
    meter: Meter | None,
    provider: ProviderConfig,
    result: LLMResult,
    duration_s: float,
) -> None:
    """Emit the GenAI semantic-convention metrics for one successful call."""
    if meter is None:
        return
    labels = {
        "gen_ai.system": provider.name,
        "gen_ai.operation.name": OPERATION_NAME,
        "gen_ai.request.model": provider.model,
    }
    meter.create_histogram(
        OPERATION_DURATION_METRIC, unit="s",
        description="Duration of a GenAI chat call, end to end.",
    ).record(duration_s, labels)

    tokens = meter.create_histogram(
        TOKEN_USAGE_METRIC, unit="{token}",
        description="Tokens consumed by a GenAI chat call.",
    )
    for token_type, count in (
        ("input", result.input_tokens),
        ("output", result.output_tokens),
    ):
        if count is not None:
            tokens.record(count, {**labels, "gen_ai.token.type": token_type})


def _set_span_attributes(
    span, provider: ProviderConfig, result: LLMResult, fallback_used: bool, attempts: int
) -> None:
    """Tag the span with GenAI conventions plus this project's own fallback facts."""
    if span is None:
        return

    # GenAI semantic conventions — what a generic LLM dashboard expects to find.
    attrs: dict[str, object] = {
        "gen_ai.operation.name": OPERATION_NAME,
        "gen_ai.system": provider.name,
        "gen_ai.request.model": provider.model,
    }
    if result.model:
        attrs["gen_ai.response.model"] = result.model
    if result.input_tokens is not None:
        attrs["gen_ai.usage.input_tokens"] = result.input_tokens
    if result.output_tokens is not None:
        attrs["gen_ai.usage.output_tokens"] = result.output_tokens
    if result.finish_reason:
        attrs["gen_ai.response.finish_reasons"] = [result.finish_reason]

    # Kept alongside the conventions, not replaced by them: "did the fallback
    # fire, and on which attempt" is specific to this adapter and has no
    # equivalent in the spec.
    attrs.update(
        provider=provider.name,
        model=provider.model,
        fallback_used=fallback_used,
        attempts=attempts,
    )
    for key, value in attrs.items():
        span.set_attribute(key, value)


def ask(
    question: str,
    *,
    system: str | None = None,
    settings: Settings | None = None,
    tracer: Tracer | None = None,
    meter: Meter | None = None,
    timeout: float = 60.0,
) -> str:
    """Ask the question, trying each configured provider in order.

    Returns the first successful answer. Raises ``LLMProviderError`` naming every
    provider that failed if none succeed — a fallback that fails silently is
    worse than no fallback.

    The span keeps its stable ``llm_ask`` name rather than the convention's
    ``{operation} {model}``: a name that varies per model would fragment the
    trace list and every saved filter with it, for information already carried
    by ``gen_ai.request.model``.
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
            started = time.perf_counter()
            try:
                result = _call(provider, messages, timeout)
            except LLMProviderError as e:
                failures.append(str(e))
                _log.warning("llm_provider_failed", provider=provider.name, error=str(e))
                continue
            duration_s = time.perf_counter() - started

            _set_span_attributes(span, provider, result, bool(failures), attempt + 1)
            _record_metrics(meter, provider, result, duration_s)
            _log.info(
                "llm_answered",
                provider=provider.name,
                model=provider.model,
                fallback_used=bool(failures),
                attempts=attempt + 1,
                input_tokens=result.input_tokens,
                output_tokens=result.output_tokens,
                duration_s=round(duration_s, 3),
            )
            return result.content

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
