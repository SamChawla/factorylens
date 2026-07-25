"""End-to-end UI tests: the FactoryLens dashboard and alerts render in SigNoz.

These drive a real browser against a running SigNoz instance (self-hosted by
default), so they prove the *whole* chain — pipeline -> OTel export -> SigNoz
storage -> dashboard query -> rendered panel — not just the Python side.

They **skip cleanly** when SigNoz is unreachable or credentials aren't set, so
`uv run pytest` stays green on a machine with no SigNoz. To run them:

    export SIGNOZ_UI_PASSWORD=...   # local admin password
    export SIGNOZ_API_KEY=...       # to resolve the dashboard UUID
    uv run pytest tests/test_e2e_dashboard.py

Requires `playwright install chromium` once.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "demo"))

requests = pytest.importorskip("requests")
pytest.importorskip("playwright")

from signoz_session import SigNozConfig, dashboard_uuid, login, open_dashboard  # noqa: E402


def _reachable(base: str) -> bool:
    try:
        return requests.get(f"{base}/api/v1/version", timeout=4).status_code == 200
    except Exception:
        return False


@pytest.fixture(scope="module")
def config() -> SigNozConfig:
    if not os.environ.get("SIGNOZ_UI_PASSWORD"):
        pytest.skip("SIGNOZ_UI_PASSWORD not set — skipping SigNoz UI E2E.")
    try:
        cfg = SigNozConfig.from_env()
    except RuntimeError as e:
        pytest.skip(str(e))
    if not _reachable(cfg.base_url):
        pytest.skip(f"SigNoz not reachable at {cfg.base_url} — skipping UI E2E.")
    return cfg


@pytest.fixture
def signoz_page(page, config):
    """A logged-in page on the FactoryLens dashboard."""
    login(page, config)
    open_dashboard(page, config, relative_time="30m")
    return page


def test_login_reaches_the_app(page, config):
    login(page, config)
    assert "/login" not in page.url


def test_dashboard_title_renders(signoz_page):
    assert "FactoryLens" in signoz_page.content()


def test_core_panels_render_a_chart(signoz_page):
    """A graph panel must draw a <canvas> — proof the queries returned data.

    SigNoz lazily renders only the panels currently in view, so we assert at
    least one canvas here and check specific panels in the parametrized test.
    """
    signoz_page.wait_for_selector("canvas", timeout=15000)
    assert signoz_page.locator("canvas").count() >= 1


@pytest.mark.parametrize(
    "title",
    ["OEE per line", "Data-quality trend", "Pipeline stage duration"],
)
def test_named_panel_present_and_not_empty(signoz_page, title):
    """Each named panel exists and does not show SigNoz's 'No Data' placeholder."""
    grid_items = signoz_page.locator(".react-grid-item")
    match = None
    for i in range(grid_items.count()):
        item = grid_items.nth(i)
        try:
            text = item.inner_text(timeout=3000)
        except Exception:
            continue
        if title.lower() in text.lower():
            match = item
            break
    assert match is not None, f"panel {title!r} not found on the dashboard"
    match.scroll_into_view_if_needed()
    signoz_page.wait_for_timeout(1500)
    assert "No Data" not in match.inner_text(), f"panel {title!r} shows No Data"


def test_alerts_page_lists_both_rules(page, config):
    login(page, config)
    page.goto(f"{config.base_url}/alerts", wait_until="networkidle", timeout=30000)
    # The rules table hydrates client-side; wait for the rows, don't snapshot early.
    page.get_by_text("line OEE below 0.65").first.wait_for(timeout=20000)
    page.get_by_text("production line went silent").first.wait_for(timeout=20000)


def test_dashboard_uuid_resolves_by_title(config):
    uuid = dashboard_uuid(config)
    assert uuid and len(uuid) > 10
