# FactoryLens

**Ask your factory's telemetry why a number is wrong — and get an answer that
cites the data.**

Manufacturing pipeline health & data-quality observability, instrumented with
OpenTelemetry and shipped to **SigNoz Cloud**. Built for the *Agents of SigNoz*
hackathon (WeMakeDevs × SigNoz, Jul 20–26 2026) — **Track 01: AI & Agent
Observability**.

---

## The problem

A factory runs several production lines. Every line's data flows through an ETL
pipeline — ingest → clean → transform → aggregate — before anyone can trust a
number like **OEE** (Overall Equipment Effectiveness), the headline metric plant
managers run the floor on.

When a line's OEE drops, the dashboard tells you *that* it dropped. It rarely
tells you **why**. And the most expensive failure in manufacturing analytics
isn't a machine breaking — it's a number that's quietly wrong:

- sensors that stopped reporting, leaving gaps nobody noticed
- malformed rows silently discarded upstream
- a batch that froze and kept serving the same stale reading for hours

Each of these drags OEE down. None of them are visible in the OEE number itself.
So the engineer ends up asking: *is my line actually broken, or is my data?*

## The solution

FactoryLens instruments **every pipeline stage as an OpenTelemetry span** that
carries the data-quality facts alongside the business metric:

```
pipeline_run (line_id)
├── ingest      rows_in, rows_out, rows_dropped, null_ratio, stale_batch
├── clean       ← malformed rows dropped here, quality measured here
├── transform
└── aggregate   + availability, performance, quality, oee
```

Those spans go to SigNoz, where three panels show pipeline duration, data-quality
trend, and OEE per line. Then a **Q&A agent reads the same spans** and answers in
plain language:

> **"Why is line_3's OEE lower than the other lines?"**
>
> *"Line_3's OEE is lower primarily due to data quality issues: it dropped 240
> rows out of 2000 (rows_dropped=240), indicating a higher rate of malformed
> data compared to line_1 and line_2 which dropped 0 rows. Additionally, line_3
> has a stale_batch=True flag, meaning the batch timestamp stopped advancing…
> These data problems correlate with its lower availability (0.76 vs. 0.92 and
> 0.87), performance (0.82 vs. 0.90), quality (0.93 vs. 0.97), and overall OEE
> (0.58 vs. 0.80 and 0.76) compared to the other lines."*
>
> — verbatim output of `factorylens ask`, not a mock-up.

Every number in that answer came off a span. Ask it something the telemetry
can't support and it says so instead of guessing.

## The impact

The gap between "OEE is down on line 3" and "line 3's sensor feed has been
dropping 12% of rows since Tuesday" is usually hours of an engineer's time spent
joining dashboards to raw tables. FactoryLens closes that gap to one question —
and, because the agent's own LLM calls are instrumented too, you can see in
SigNoz how long the agent took and which provider answered.

---

## Quickstart

```bash
uv sync                      # reproducible env from the committed lockfile
cp .env.example .env         # then fill in your keys (.env is gitignored)
uv run pytest                # 59 tests
```

Minimum config to run against SigNoz Cloud:

```bash
SIGNOZ_OTLP_ENDPOINT=https://ingest.<region>.signoz.cloud:443
SIGNOZ_INGESTION_KEY=<from SigNoz -> Settings -> Ingestion Settings>
EURI_API_KEY=<primary LLM>
GROQ_API_KEY=<fallback LLM>
```

No SigNoz account? Everything still runs — spans print to the console instead.

### Commands

```bash
# 1. Verify telemetry auth before trusting anything else
uv run factorylens check

# 2. Run the pipeline once; export spans; print OEE per line
uv run factorylens run

# 3. Seed a degrading trend: 12 runs, 30s apart, line_3 decaying
uv run factorylens run --runs 12 --interval 30

# 4. Ask the telemetry a question
uv run factorylens ask "why is line_3's OEE low?"
uv run factorylens ask "is line_3 getting worse?" --runs 6
uv run factorylens ask "..." --show-context     # see exactly what the model got
```

### Dashboard

Import [`dashboards/factorylens-dashboard.json`](dashboards/factorylens-dashboard.json)
via **Dashboards → New Dashboard → Import JSON**. Three panels, named in plain
English: pipeline stage duration, data-quality trend per line, OEE per line.
Import *after* a run, so SigNoz has learned the attribute types. Details and a
manual-build fallback: [`dashboards/README.md`](dashboards/README.md).

---

## The demo story

The synthetic generator is **seeded and scripted**, not random — the same
scenario always produces the same data, so the demo tells the same story every
time. Three lines, 2000 batches each:

| line | role | injected faults | OEE |
|------|------|-----------------|-----|
| **line_1** | healthy reference | none | 0.80 |
| **line_2** | data gaps | 15% missing sensor readings | 0.76 |
| **line_3** | the problem line | malformed rows + stale batch, running hot and slow | 0.73 → 0.42 |

Across a 12-run seeding, line_1 holds **flat at 0.80** and line_2 at **0.76**,
while line_3 decays **0.73 → 0.42** as `rows_dropped` climbs **80 → 500**,
`null_ratio` rises **0.00 → 0.27**, and `stale_batch` flips true at the halfway
point. The dashboard shows data quality falling and OEE falling with it; the
agent explains the link.

*(A single `factorylens run` uses the fixed demo scenario: line_3 at OEE 0.58
with 240 rows dropped. The 12-run seeding is what produces the trend above.)*

---

## Design

Decisions and their rationale live in [`.cursor/adr.md`](.cursor/adr.md); the
build plan in [`.cursor/implementation-plan.md`](.cursor/implementation-plan.md).
Highlights:

- **One span per stage, fixed attribute set** — span names stay generic and
  stable; the *data on the span* tells the story. Dashboards and the agent both
  break if attribute names drift, so they never do.
- **One source of truth for row validity** (`schema.py`) — the generator injects
  exactly the corruptions the clean stage drops, so the two halves cannot drift.
- **Staleness is cadence-relative** — a batch is stale when it falls out of the
  line's own rhythm, not when it crosses an absolute age. This holds whether a
  line reports 24 batches or 24,000.
- **One LLM adapter** — `ask(question) -> str`, Euri primary with Groq fallback.
  The CLI never learns which provider answered. Both are OpenAI-compatible, so a
  third is a list entry, not a refactor.
- **The agent reads the spans it exported** — the capture exporter runs
  *alongside* the OTLP one, so what the agent reasons over is exactly what
  reached SigNoz, not a parallel code path that could drift.

### Tested

59 tests, no network calls. Both LLM providers are mocked; the fallback path is
covered because it only ever runs when the primary is already failing — which is
precisely when nobody is watching.

| file | covers |
|------|--------|
| `tests/test_generator.py` | fault injection at exact counts, determinism, degradation |
| `tests/test_oee.py` | OEE math incl. degenerate batches (no run time, no output) |
| `tests/test_pipeline.py` | each stage on clean + deliberately-broken input, span contract |
| `tests/test_llm.py` | provider fallback: 5xx, rate limit, network, malformed, empty |
| `tests/test_agent.py` | span attributes survive intact into the model's context |

## Honest limitations

- The agent analyses a pipeline run it triggers itself, reading spans captured
  in-process. Querying historical telemetry from SigNoz's API is designed for
  (the `TelemetrySource` protocol) but not shipped — it needs a management API
  key that could not be provisioned during the build.
- Dashboard creation is a JSON import rather than an API call, for the same reason.
- Manufacturing data is synthetic. The faults are modelled on real failure modes
  (dead sensors, malformed rows, frozen batches), but no real plant data was used.

## Stack

Python 3.12 · pandas · OpenTelemetry SDK (OTLP/HTTP) · SigNoz Cloud ·
Typer + Rich · structlog · pydantic-settings · pytest · uv
