# Getting started with FactoryLens

Zero to a working demo. Every command here was run on a clean checkout; the
numbers shown are what you should actually see.

Everything works **without a SigNoz account** — spans print to the console
instead. SigNoz turns the dashboards on; it is not required to run anything.

---

## 1. Prerequisites

| Need | Why | Check |
|------|-----|-------|
| **Python 3.12+** | `pyproject.toml` requires it | `python --version` |
| **uv** | env + lockfile | `uv --version` |
| **Docker** + Compose | runs self-hosted SigNoz | `docker --version` |
| SigNoz Cloud account | only if you prefer Cloud over self-hosted | — |
| An LLM key | the `ask` command (optional) | — |

No uv? `pip install uv`, or see [astral.sh/uv](https://docs.astral.sh/uv/).

## 2. Install

```bash
git clone <your-repo-url>
cd Signoz_SOC
uv sync          # reproducible env from the committed uv.lock
uv run pytest    # 140 tests (8 UI tests skip without SigNoz running)
```

If the tests pass, the pipeline, generator, OEE math, streaming runtime and LLM
fallback all work. Nothing below can fail for install reasons.

## 3. Configure

```bash
cp .env.example .env
```

First, stand up SigNoz. The reference deployment is **self-hosted**, via
[Foundry](https://github.com/SigNoz/foundry) on Docker Compose — reproducible
from [`casting.yaml`](../casting.yaml) in the repo root:

```bash
curl -fsSL https://signoz.io/foundry.sh | bash   # install foundryctl
foundryctl cast -f casting.yaml                  # UI :8080, OTLP :4318
```

First run pulls several GB. When it's up, open <http://localhost:8080> and create
the admin account (first-run setup). Then import the dashboard and alerts from
this repo before you start checking panels in the UI.

Then edit `.env`:

```bash
# --- Self-hosted: the local collector needs NO ingestion key ---
SIGNOZ_OTLP_ENDPOINT=http://localhost:4318
SIGNOZ_INGESTION_KEY=
SIGNOZ_SELF_HOSTED=true

# --- To use the `ask` command (optional) ---
# Either one is enough; with both, Euri is primary and Groq is the fallback.
EURI_API_KEY=<your euri key>
GROQ_API_KEY=<your groq key>
```

Leave the rest at their defaults — the base URLs and model IDs are the values
verified live against each provider.

> **Prefer SigNoz Cloud?** Same code, different endpoint. Use
> `SIGNOZ_OTLP_ENDPOINT=https://ingest.<region>.signoz.cloud:443` with your
> ingestion key (Settings → Ingestion) and `SIGNOZ_SELF_HOSTED=false`. The region
> must match your account — `ingest.us…` with an `in2` account accepts the
> request and silently discards it.

## 4. Verify telemetry before trusting anything

```bash
uv run factorylens check
```

Expected:

```
Sent factorylens_healthcheck span to http://localhost:4318
flush acknowledged: True
```

Now open <http://localhost:8080> → **Traces**, filter `service = factorylens`, and confirm the
span landed. Do this *first* — if it fails, nothing downstream will show data
and you will waste time debugging the wrong layer.

No key set? You get a yellow "SigNoz not configured" panel and spans print to the
console. That is a valid way to run the whole project.

## 5. The four commands

### `run` — batch mode (deterministic)

Generates the full dataset up front, runs the pipeline, prints OEE per line.

```bash
uv run factorylens run                      # one run
uv run factorylens run --runs 12 --interval 30   # seed a degrading trend (~6 min)
```

```
| line_1 |  0.92 | 0.90 | 0.97 | 0.80 |
| line_2 |  0.87 | 0.90 | 0.97 | 0.76 |
| line_3 |  0.77 | 0.82 | 0.93 | 0.58 |
```

Seeded and scripted — the same numbers every time. This is the safe path for a
live demo.

### `stream` — real-time mode

A simulated PLC feed emits batches over time. Readings are buffered into
event-time windows; each closed window runs through the *same* pipeline.
Condition triggers fire immediately, without waiting for a window.

```bash
uv run factorylens stream --duration 30
uv run factorylens stream -d 60 -w 4 -s 20000    # 4h windows, 20000x compression
```

| flag | default | meaning |
|------|---------|---------|
| `--duration` / `-d` | 30 | wall-clock seconds to run |
| `--window` / `-w` | 8 | window width in *simulated* hours |
| `--time-scale` / `-s` | 6000 | simulated seconds per real second |
| `--cadence` / `-c` | 1.0 | simulated minutes between a line's batches |
| `--temp-max` | 85.0 | temperature that trips an alert |

You should see all three alert kinds, all on `line_3`:

```
ALERT line_3  malformed_reading: malformed reading <blank batch_id> rejected at ingest
ALERT line_3  threshold_breach:  temperature 86.6 > 85.0 (+12 suppressed)
ALERT line_3  line_silent:       no reading for 1.0s (> 1.0s threshold)
window 1 closed  rows=1050  OEE line_1 0.80  line_2 0.76  line_3 0.58
```

and a summary with `peak lag  line_1=0s  line_2=90s  line_3=1800s` — line_3's
gateway falling behind is the signal batch mode structurally cannot show.

### `ask` — the Q&A agent

```bash
uv run factorylens ask "why is line_3's OEE low?"
uv run factorylens ask "is line_3 getting worse?" --runs 6
uv run factorylens ask "..." --show-context      # see exactly what the model got
```

Answers strictly from span data. Ask it something the telemetry can't support
and it says so instead of guessing.

### `check` — telemetry auth test

Covered in step 4.

## 6. Import the dashboard

**Dashboards → New Dashboard → Import JSON** →
[`dashboards/factorylens-dashboard.json`](../dashboards/factorylens-dashboard.json)

Seven panels:

1. Pipeline stage duration
2. Data-quality trend (dropped rows, null ratio) per line
3. OEE per line
4. LLM token usage — input vs output, per provider
5. LLM call latency — p95, per provider
6. LLM calls and fallback rate
7. Streaming alerts by kind, per line

Import **after** at least one `run`, so SigNoz has learned the attribute types.
Panels 4–6 need one `ask`; panel 7 needs one `stream`.

Two metrics are exported but not in the JSON — add them in the UI if you want
them:

- `factorylens.ingest.lag_ms` — p95, grouped by `line_id`
- `factorylens.sensor.temperature` — latest, grouped by `line_id`

## 7. Optional: monitor Claude Code itself

Claude Code exports its own OTel telemetry (token usage, cost, sessions). It
uses the **same ingestion key**, and lands under a separate service name so it
cannot pollute the FactoryLens panels.

```bash
export CLAUDE_CODE_ENABLE_TELEMETRY=1
export OTEL_METRICS_EXPORTER=otlp
export OTEL_LOGS_EXPORTER=otlp
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf     # NOT grpc — SigNoz Cloud is HTTP/443
export OTEL_EXPORTER_OTLP_ENDPOINT=https://ingest.<region>.signoz.cloud:443
export OTEL_EXPORTER_OTLP_HEADERS="signoz-ingestion-key=<your key>"
export OTEL_METRIC_EXPORT_INTERVAL=10000
```

Then import SigNoz's Claude Code dashboard template.

> **Before recording any video with this on:** these metrics carry `user.email`,
> `user.id` and `organization.id` as standard attributes. Your email will be on
> the dashboard.

## 8. Troubleshooting

**`check` says "SigNoz not configured"**
`SIGNOZ_INGESTION_KEY` is empty. `.env` must be in the directory you run from.

**`check` succeeds but SigNoz shows nothing**
Wrong region in the endpoint. `ingest.us...` with an `in2` account accepts the
request and discards it. Match the region shown in Settings → Ingestion.

**Dashboard panels are empty**
Check in order: (1) time range — try *Last 30 minutes*; (2) the panel's signal
has been produced — panels 4–6 need an `ask`, panel 7 needs a `stream`;
(3) imported before the first run, so attribute types weren't known — re-import.

**`ask` fails with "all LLM providers failed"**
The message names each provider and its failure. `404 Route not found` on Euri
means a wrong `EURI_BASE_URL`; Groq rejects `openai/`-prefixed model IDs.

**`ask` always says the fallback was used**
Your Euri config is wrong and it is silently 404ing. Check `fallback_used` on the
`llm_ask` span.

**Streaming shows no `line_silent` alert**
The silence is shorter than the 1s watchdog floor at very high `--time-scale`.
Use the default `-s 6000`, or a longer `--duration`.

**Windows console shows `?` for some characters**
Cosmetic, PowerShell code page. `chcp 65001` fixes it.

## 9. A 5-minute tour

```bash
uv run factorylens check                          # 1. prove telemetry works
uv run factorylens run                            # 2. batch mode, OEE per line
uv run factorylens stream --duration 30           # 3. live feed + alerts
uv run factorylens ask "why is line_3's OEE low?" # 4. ask the telemetry why
```

Then open the dashboard: OEE falling on line_3, dropped rows rising underneath
it, alerts firing in real time, and what the agent's own answer cost in tokens.
