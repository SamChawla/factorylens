"""Shared Playwright helpers for driving the local SigNoz UI.

Targets the self-hosted instance by default (localhost:8080), because that's the
reproducible one — no external dependency, works offline, and the demo/E2E run
the same way on any machine that stood the stack up with `foundryctl cast`.

Config comes from env (never hardcoded secrets):
  SIGNOZ_APP_URL   default http://localhost:8080
  SIGNOZ_UI_EMAIL  default admin@factorylens.local
  SIGNOZ_UI_PASSWORD   required (the local admin password you set at first-run)
  SIGNOZ_API_KEY   used only to resolve the dashboard UUID by title
"""

from __future__ import annotations

import os
from dataclasses import dataclass

import requests
from playwright.sync_api import Page

DEFAULT_BASE = "http://localhost:8080"
DEFAULT_EMAIL = "admin@factorylens.local"
DASHBOARD_TITLE_MATCH = "FactoryLens"


@dataclass(frozen=True)
class SigNozConfig:
    base_url: str
    email: str
    password: str
    api_key: str

    @classmethod
    def from_env(cls) -> "SigNozConfig":
        password = os.environ.get("SIGNOZ_UI_PASSWORD", "")
        if not password:
            raise RuntimeError(
                "SIGNOZ_UI_PASSWORD is not set — export the local SigNoz admin "
                "password before running the demo/E2E."
            )
        return cls(
            base_url=os.environ.get("SIGNOZ_APP_URL", DEFAULT_BASE).rstrip("/"),
            email=os.environ.get("SIGNOZ_UI_EMAIL", DEFAULT_EMAIL),
            password=password,
            api_key=os.environ.get("SIGNOZ_API_KEY", ""),
        )


def login(page: Page, config: SigNozConfig) -> None:
    """Complete SigNoz's two-step login (email -> Next -> password -> submit)."""
    page.goto(f"{config.base_url}/login", wait_until="networkidle", timeout=30000)
    page.fill("input[type=email]", config.email)
    page.click("button:has-text('Next')")
    page.wait_for_selector("input[type=password]", timeout=15000)
    page.fill("input[type=password]", config.password)
    page.click("button:has-text('Sign in with Password')")
    # Land anywhere inside the app shell (home/dashboard), not still on /login.
    page.wait_for_url(lambda url: "/login" not in url, timeout=20000)


def dashboard_uuid(config: SigNozConfig, title_match: str = DASHBOARD_TITLE_MATCH) -> str:
    """Resolve the FactoryLens dashboard's UUID via the management API.

    Looked up by title rather than hardcoded, so the demo works against whatever
    instance imported the dashboard (Cloud or self-hosted, any UUID).
    """
    if not config.api_key:
        raise RuntimeError("SIGNOZ_API_KEY is required to resolve the dashboard UUID.")
    resp = requests.get(
        f"{config.base_url}/api/v1/dashboards",
        headers={"SIGNOZ-API-KEY": config.api_key},
        timeout=20,
    )
    resp.raise_for_status()
    data = resp.json().get("data", [])
    for entry in data:
        body = entry.get("data") if isinstance(entry.get("data"), dict) else entry
        title = (body or {}).get("title", "")
        if title_match.lower() in title.lower():
            return entry.get("uuid") or entry.get("id") or body.get("uuid")
    raise RuntimeError(f"No dashboard whose title contains {title_match!r} was found.")


def open_dashboard(page: Page, config: SigNozConfig, relative_time: str = "30m") -> None:
    """Navigate to the FactoryLens dashboard and wait for panels to settle."""
    uuid = dashboard_uuid(config)
    page.goto(
        f"{config.base_url}/dashboard/{uuid}?relativeTime={relative_time}",
        wait_until="networkidle",
        timeout=45000,
    )
    # Panels render on <canvas> (uPlot). Wait for the first one, then let the
    # rest of the grid finish drawing.
    page.wait_for_selector("canvas", timeout=45000)
    page.wait_for_timeout(3500)
