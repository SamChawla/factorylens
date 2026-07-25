# FactoryLens demo & UI automation

Playwright drives the **local self-hosted SigNoz** UI to do two things:

1. **`capture_demo.py`** — seed fresh telemetry, then walk the dashboard and
   alerts, saving screenshots + a recorded video to `demo/output/`. These are the
   visual assets for the README, blog, and demo video.
2. **`tests/test_e2e_dashboard.py`** — assert the whole chain works end to end:
   login → dashboard renders → panels have data (no "No Data") → both alert rules
   are listed. Proof the pipeline's spans actually make it onto a rendered panel,
   not just into ClickHouse.

Both target `localhost:8080` by default — the reproducible instance stood up with
`foundryctl cast` (see [../docs/self-hosted.md](../docs/self-hosted.md)).

## Prerequisites

```bash
uv sync                          # installs playwright (dev dep)
uv run playwright install chromium
```

Self-hosted SigNoz must be running with the FactoryLens dashboard + alerts
imported, and an admin account created.

## Config (env — no secrets in code)

| var | default | purpose |
|-----|---------|---------|
| `SIGNOZ_APP_URL` | `http://localhost:8080` | the SigNoz instance |
| `SIGNOZ_UI_EMAIL` | `admin@factorylens.local` | login email |
| `SIGNOZ_UI_PASSWORD` | — (required) | login password |
| `SIGNOZ_API_KEY` | — (required) | resolves the dashboard UUID by title |

```bash
export SIGNOZ_UI_PASSWORD='<your local admin password>'
export SIGNOZ_API_KEY='<local API key>'
```

## Run the demo capture

```bash
uv run python demo/capture_demo.py            # seed data + capture
uv run python demo/capture_demo.py --no-seed  # capture only (data already present)
uv run python demo/capture_demo.py --headed   # watch the browser drive itself
```

Outputs in `demo/output/`:
- `01-dashboard-full.png` — the whole dashboard
- `02`–`06` — OEE, data-quality, stage-duration, LLM tokens, streaming alerts
- `07-alerts.png` — the two alert rules (OEE one shows **Firing**)
- `page-*.webm` — video of the full walkthrough

`demo/output/` is gitignored — it's regenerable. Copy any keeper into `docs/` if
you want it committed for the README.

## Run the E2E tests

```bash
uv run pytest tests/test_e2e_dashboard.py -q
```

They **skip** automatically when SigNoz is unreachable or `SIGNOZ_UI_PASSWORD`
isn't set, so `uv run pytest` on a bare machine stays green (132 passed, 8
skipped). With SigNoz up and configured, all 8 run against the real UI.

## Why local, not Cloud

The self-hosted instance is the reproducible one: no external dependency, works
offline, and the same script runs identically on any machine that ran
`foundryctl cast`. Cloud login uses a flow that's awkward to automate; local uses
a simple email/password the demo can drive.
