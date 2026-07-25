"""Typed configuration, loaded from environment (with .env support).

Env vars override everything, same precedence every time. Never hardcode the
SigNoz endpoint or provider keys — they live here and in .env.example only.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- SigNoz / OTLP (telemetry ingest) ---
    # Works against both SigNoz Cloud and a self-hosted instance — only the
    # endpoint and auth differ, and both come from env.
    #   Cloud       : https://ingest.<region>.signoz.cloud:443  + ingestion key
    #   Self-hosted : http://localhost:4318 (Foundry/Docker Compose), no key
    signoz_otlp_endpoint: str = "https://ingest.us.signoz.cloud:443"
    signoz_ingestion_key: str = ""
    # Self-hosted SigNoz needs no ingestion key: set this true and point the
    # endpoint at the local collector. Without it, a keyless config would fall
    # back to console-only and the local export would silently never happen.
    signoz_self_hosted: bool = False
    otel_service_name: str = "factorylens"
    telemetry_enabled: bool = True

    # --- LLM: Euri (primary) ---
    # Defaults are the values verified working live against the provider, not
    # plausible-looking ones: the base URL is /api/v1/euri (plain /v1 is a 404) and the model ID
    # carries the "openai/" prefix Euri requires. A wrong default here fails
    # quietly — the primary 404s and every answer comes from the fallback.
    euri_api_key: str = ""
    euri_base_url: str = "https://api.euron.one/api/v1/euri"
    euri_model: str = "openai/gpt-4.1-mini"

    # --- LLM: Groq (fallback) ---
    # Groq (groq.com), not Grok (x.ai) — different companies. Both are
    # OpenAI-compatible, so swapping providers is an env-var change, not code.
    groq_api_key: str = ""
    groq_base_url: str = "https://api.groq.com/openai/v1"
    groq_model: str = "llama-3.3-70b-versatile"

    @property
    def signoz_configured(self) -> bool:
        """Do we have a SigNoz to export to — Cloud (key) or self-hosted?"""
        return bool(self.signoz_ingestion_key) or self.signoz_self_hosted

    @property
    def otlp_headers(self) -> dict[str, str]:
        """Auth headers for OTLP export. Empty for self-hosted (no key needed)."""
        if self.signoz_ingestion_key:
            return {"signoz-ingestion-key": self.signoz_ingestion_key}
        return {}


@lru_cache
def get_settings() -> Settings:
    """Cached singleton — read config once per process."""
    return Settings()
