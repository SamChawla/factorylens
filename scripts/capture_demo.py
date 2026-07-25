"""Capture a complete FactoryLens demo against the local SigNoz UI.

Runs end to end:
  1. Seed fresh telemetry — `run` (OEE trend), `stream` (alerts + metrics), and a
     few `ask`s (LLM panels) — all against the local self-hosted collector.
  2. Drive the SigNoz UI with Playwright: log in, open the FactoryLens dashboard,
     screenshot it and each panel, then open the Alerts page.
  3. Record the whole browser session to video.

Outputs land in `scripts/output/` (screenshots + a .webm video) — the visual assets
for the README, blog, and demo video.

Usage (from repo root, local SigNoz up and data-capable):
    export SIGNOZ_UI_PASSWORD=...        # the local admin password
    export SIGNOZ_API_KEY=...            # local API key (to resolve dashboard UUID)
    uv run python scripts/capture_demo.py [--no-seed] [--headed]
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

# The Playwright/SigNoz helper lives with the tests, which are its primary
# consumer; this script is the secondary one.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from signoz_session import SigNozConfig, login, open_dashboard  # noqa: E402

OUTPUT = Path(__file__).resolve().parent / "output"
LOCAL_ENV = {
    "SIGNOZ_OTLP_ENDPOINT": os.environ.get("SIGNOZ_OTLP_ENDPOINT", "http://localhost:4318"),
    "SIGNOZ_INGESTION_KEY": "",
    "SIGNOZ_SELF_HOSTED": "true",
}


def _cli(*args: str) -> None:
    """Run a factorylens CLI command against the local collector."""
    env = {**os.environ, **LOCAL_ENV}
    print(f"  $ factorylens {' '.join(args)}")
    subprocess.run(
        [sys.executable, "-m", "factorylens.cli", *args],
        env=env, check=False, capture_output=True, text=True,
    )


def seed_data() -> None:
    """Generate a continuous, recent curve plus alerts and LLM spans."""
    print("Seeding telemetry into local SigNoz...")
    _cli("run", "--runs", "8", "--interval", "3")           # OEE trend
    _cli("stream", "--duration", "35", "--time-scale", "15000")  # alerts + metrics
    for q in (
        "why is line_3's OEE low?",
        "which line drops the most rows?",
        "is line_3 getting worse?",
    ):
        _cli("ask", q)                                       # LLM panels
    print("Seed complete.")


# A blank panel screenshot is a few KB; a drawn chart is >8 KB. Used to detect
# the lazy-render race where uPlot hasn't drawn yet, and retry.
_MIN_PANEL_BYTES = 8000


def _shoot_panel(page, item, path: Path, retries: int = 3) -> None:
    """Screenshot one panel, retrying until its <canvas> has actually drawn.

    SigNoz renders a panel's canvas only when it scrolls into view, and uPlot
    draws asynchronously — so an immediate shot can catch a blank card. Scroll
    it to center, wait for the canvas, and reshoot if the file is suspiciously
    small.
    """
    for attempt in range(retries):
        item.scroll_into_view_if_needed(timeout=8000)
        try:
            canvas = item.locator("canvas").first
            canvas.wait_for(state="visible", timeout=8000)
            box = canvas.bounding_box()
            if not box or box["height"] < 40:  # a stale/zero-size canvas
                raise RuntimeError("canvas not drawn yet")
        except Exception:
            pass
        page.wait_for_timeout(2000 + attempt * 1500)
        item.screenshot(path=str(path))
        if path.stat().st_size >= _MIN_PANEL_BYTES:
            return
        # Blank — nudge a re-render by scrolling away and back.
        page.mouse.wheel(0, -400)
        page.wait_for_timeout(600)


def capture(headed: bool) -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    config = SigNozConfig.from_env()
    print(f"Driving SigNoz UI at {config.base_url} ...")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not headed)
        context = browser.new_context(
            viewport={"width": 1600, "height": 1000},
            record_video_dir=str(OUTPUT),
            record_video_size={"width": 1600, "height": 1000},
        )
        page = context.new_page()

        login(page, config)
        print("  logged in")

        open_dashboard(page, config, relative_time="30m")
        print("  dashboard open")
        page.screenshot(path=str(OUTPUT / "01-dashboard-full.png"), full_page=True)

        # Each panel is a .react-grid-item; match one to each title by its text
        # and screenshot that whole card.
        panels = [
            ("OEE per line", "02-oee.png"),
            ("Data-quality trend", "03-data-quality.png"),
            ("Pipeline stage duration", "04-stage-duration.png"),
            ("LLM token usage", "05-llm-tokens.png"),
            ("Streaming alerts by kind", "06-stream-alerts.png"),
        ]
        grid_items = page.locator(".react-grid-item")
        count = grid_items.count()
        for title, fname in panels:
            shot = False
            for i in range(count):
                item = grid_items.nth(i)
                try:
                    text = item.inner_text(timeout=3000)
                except Exception:
                    continue
                if title.lower() in text.lower():
                    _shoot_panel(page, item, OUTPUT / fname)
                    print(f"  shot {fname}")
                    shot = True
                    break
            if not shot:
                print(f"  ! panel not found: {title}")

        # The alerts page — proof of the two rules.
        page.goto(f"{config.base_url}/alerts", wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(3000)
        page.screenshot(path=str(OUTPUT / "07-alerts.png"), full_page=True)
        print("  shot alerts page")

        context.close()  # flushes the video
        browser.close()

    videos = list(OUTPUT.glob("*.webm"))
    print(f"\nDone. {len(list(OUTPUT.glob('*.png')))} screenshots + "
          f"{len(videos)} video(s) in {OUTPUT}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-seed", action="store_true", help="Skip data seeding.")
    ap.add_argument("--headed", action="store_true", help="Show the browser.")
    args = ap.parse_args()
    if not args.no_seed:
        seed_data()
    capture(headed=args.headed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
