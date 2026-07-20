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

    # --- SigNoz Cloud / OTLP ---
    signoz_otlp_endpoint: str = "https://ingest.us.signoz.cloud:443"
    signoz_ingestion_key: str = ""
    otel_service_name: str = "factorylens"
    telemetry_enabled: bool = True

    # --- LLM: Euri (primary) ---
    euri_api_key: str = ""
    euri_base_url: str = "https://api.euron.one/v1"
    euri_model: str = "gpt-4o-mini"

    # --- LLM: Grok (fallback) ---
    grok_api_key: str = ""
    grok_base_url: str = "https://api.x.ai/v1"
    grok_model: str = "grok-2-latest"

    @property
    def signoz_configured(self) -> bool:
        return bool(self.signoz_ingestion_key)


@lru_cache
def get_settings() -> Settings:
    """Cached singleton — read config once per process."""
    return Settings()
