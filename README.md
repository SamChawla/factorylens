# FactoryLens

**Ask your factory's telemetry why a number is wrong — and get an answer that
cites the data.**

Manufacturing pipeline health & data-quality observability, instrumented with
OpenTelemetry and shipped to **self-hosted SigNoz** (Foundry / Docker Compose;
SigNoz Cloud works identically). Built for the *Agents of SigNoz* hackathon
(WeMakeDevs × SigNoz, Jul 20–26 2026) — **Track 01: AI & Agent Observability**.

📄 **Overview page:** [`docs/index.html`](docs/index.html) — a one-page visual
walkthrough of the whole system (open locally, or enable GitHub Pages to serve it).

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

Those spans go to SigNoz, where seven panels show pipeline duration, data-quality
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

Full step-by-step setup, expected output and troubleshooting:
**[docs/getting-started.md](docs/getting-started.md)**.

```bash
uv sync                      # reproducible env from the committed lockfile
cp .env.example .env         # then fill in your keys (.env is gitignored)
uv run pytest                # 155 tests (UI tests skip without SigNoz)
```

### Stand up SigNoz (self-hosted — the reference deployment)

FactoryLens targets **self-hosted SigNoz**, stood up with
[Foundry](https://github.com/SigNoz/foundry) on Docker Compose. The spec is
[`casting.yaml`](casting.yaml) at the repo root, with `casting.yaml.lock` pinning
the resolved image versions — so the deployment reproduces from this repo alone:

```bash
curl -fsSL https://signoz.io/foundry.sh | bash   # install foundryctl
foundryctl cast -f casting.yaml                  # stands up the whole stack
```

UI on `http://localhost:8080`, OTLP ingest on `:4318`. Then point FactoryLens at
it — the local collector needs **no ingestion key**:

```bash
SIGNOZ_OTLP_ENDPOINT=http://localhost:4318
SIGNOZ_INGESTION_KEY=
SIGNOZ_SELF_HOSTED=true
EURI_API_KEY=<primary LLM>     # optional, for `ask`
GROQ_API_KEY=<fallback LLM>    # optional, for `ask`
```

Full walkthrough — first-run setup, importing the dashboard and alerts, teardown:
**[docs/self-hosted.md](docs/self-hosted.md)**.

<details>
<summary><b>Running against SigNoz Cloud instead</b></summary>

The application code is identical; only the endpoint and auth differ, and both
come from env. Swap the three variables above for:

```bash
SIGNOZ_OTLP_ENDPOINT=https://ingest.<region>.signoz.cloud:443
SIGNOZ_INGESTION_KEY=<from SigNoz -> Settings -> Ingestion Settings>
SIGNOZ_SELF_HOSTED=false
```

Both targets were used during development and both are covered by tests — the
export layer reads its endpoint and auth from env precisely so neither is
special-cased in code.

</details>

No SigNoz at all? Everything still runs — spans print to the console instead.

**Five OTel signals, not three.** Traces, metrics, **logs** (the structlog stream
bridged to OTel), a 7-panel **dashboard**, and two **alert rules**
([`alerts/`](alerts/)) — OEE-below-floor and line-went-silent, both created live.

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

# 5. Consume a live sensor feed instead of a finished dataset
uv run factorylens stream --duration 30
uv run factorylens stream --duration 60 --window 4 --time-scale 20000
```

### Where the data comes from — and how it reaches a real plant

The *demo* runs on a simulated feed, because a demo needs to be deterministic
and can't dial into a factory. But the path to a real plant isn't a promise —
**the production adapters are implemented and tested against the real
protocol.** Three interchangeable implementations of one `SensorSource`:

| source | reads | status |
|--------|-------|--------|
| `MockPlcFeed` | a seeded simulation | what the demo runs on |
| `OpcUaSource` | a live **OPC UA** server (subscription, not polling) | tested end to end against a real server |
| `MqttSource` | **MQTT** on the Sparkplug B topic tree | tested over the real client callback path |

```bash
uv sync --extra industrial     # asyncua + paho-mqtt; core install stays light
```

Two details make these real rather than decorative.

**The timestamps are the protocol's own.** Every OPC UA `DataValue` carries a
`SourceTimestamp` (when the *device* sampled it) and a `ServerTimestamp` (when
the server processed it). `Reading.event_time` is the SourceTimestamp and
`ingest_time` is stamped on arrival — so `factorylens.ingest.lag_ms` measures a
genuine gap between the machine's clock and ours. The event-time/ingest-time
split wasn't invented for this project; it's what OPC UA already gives you.

**Tags are not rows.** A PLC exposes individual tags — a counter, a state word,
a thermocouple — each changing at its own rate, while `planned_min`,
`ideal_cycle_s` and `batch_id` come from the MES at a completely different one.
`TagAssembler` performs that join: it holds the latest value of every mapped tag
per line and cuts a batch when the batch identifier rolls over. An incomplete
batch is discarded with a warning rather than defaulted into a plausible-looking
row — a wrong OEE is worse than a missing one.

Sparkplug's **NDEATH** (the broker publishing an edge node's Last Will when it
drops off) maps directly onto the `line_silent` alert — the industrial
standard's own answer to "this line stopped reporting."

What is *not* claimed: none of this has been run against physical PLC hardware,
and `MqttSource` decodes JSON payloads rather than Sparkplug B's protobuf
encoding (swap `decode_payload` to add it). What *is* proven, by
[`tests/test_industrial.py`](tests/test_industrial.py), is that a live OPC UA
server drives `OpcUaSource` → `StreamRunner` → the unchanged pipeline and emits
the same `ingest`/`clean`/`transform`/`aggregate` spans everything else reads.

What streaming adds that a finished dataset structurally cannot:

| signal | what it catches |
|--------|-----------------|
| `ingest.lag_ms` | a line producing fine while its gateway falls further behind |
| `line_silent` alert | a line that **stopped** reporting — triggered by absence, on a wall-clock timer |
| `threshold_breach` alert | a hot reading, the moment it arrives, without waiting for the window |
| `malformed_reading` alert | a bad row rejected at the door rather than discovered in `clean` |

Staleness is modelled as the line going quiet, not as a frozen timestamp — so
the gap it leaves is real, and `schema.has_stale_batch` detects it with no
change to that function at all.

`run` is still the deterministic, offline path, and it stays: it is what the
tests pin and the fallback if a live demo goes wrong.

### The agent's own LLM calls are instrumented too

`llm_ask` spans carry the OpenTelemetry **GenAI semantic conventions** —
`gen_ai.system`, `gen_ai.request.model`, `gen_ai.usage.input_tokens`,
`gen_ai.usage.output_tokens` — plus `gen_ai.client.token.usage` and
`gen_ai.client.operation.duration` metrics. So "what did that answer cost, and
did the fallback fire?" is a dashboard query, not a guess.

These are emitted directly rather than via a vendor instrumentation library:
both providers are reached over the same OpenAI-compatible HTTP shape, so a
Groq-specific instrumentor (which patches the Groq SDK) would see neither.

### Dashboard

Import [`dashboards/factorylens-dashboard.json`](dashboards/factorylens-dashboard.json)
via **Dashboards → New Dashboard → Import JSON**. Seven panels: pipeline stage
duration, data-quality trend per line, OEE per line, LLM token usage and p95
latency and fallback rate per provider, and streaming alerts by kind.
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

The load-bearing decisions and why they were made:

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

155 tests, no external network calls. Both LLM providers are mocked —
the fallback path is covered because it only ever runs when the primary is
already failing, which is precisely when nobody is watching.

| file | covers |
|------|--------|
| `tests/test_generator.py` | fault injection at exact counts, determinism, degradation |
| `tests/test_oee.py` | OEE math incl. degenerate batches (no run time, no output) |
| `tests/test_pipeline.py` | each stage on clean + deliberately-broken input, span contract |
| `tests/test_llm.py` | provider fallback (5xx, rate limit, network, malformed, empty) + GenAI attributes |
| `tests/test_agent.py` | span attributes survive intact into the model's context |
| `tests/test_sources.py` | the sensor feed: determinism, lag, silence, fault rates |
| `tests/test_stream.py` | windowing, watermarks, lateness, the silence watchdog, alerts |
| `tests/test_telemetry.py` | Cloud vs self-hosted export gating, keyless auth, the logs bridge |
| `tests/test_e2e_dashboard.py` | real browser: panels render with data, alert rules listed |
| `tests/test_industrial.py` | a **live OPC UA server** driving the real adapter and the pipeline |

Two groups skip unless their dependency is present, so a bare `uv run pytest`
stays green: the 8 UI tests need a reachable SigNoz plus `SIGNOZ_UI_PASSWORD`,
and the 15 industrial tests need `uv sync --extra industrial`. The OPC UA ones
stand up a real asyncua server in-process — no PLC required, and no mock in the
loop.

## Honest limitations

- The agent analyses a pipeline run it triggers itself, reading spans captured
  in-process. Querying *historical* telemetry from SigNoz's API is designed for
  (the `TelemetrySource` protocol) but not shipped — so it cannot answer about
  runs it did not trigger.
- The demo's manufacturing data is synthetic. The faults are modelled on real
  failure modes (dead sensors, malformed rows, frozen batches), but no real plant
  data was used. The OPC UA and MQTT adapters are real and tested against a live
  OPC UA server — not against physical PLC hardware.
- `MqttSource` decodes JSON payloads on the Sparkplug topic tree; Sparkplug B's
  protobuf encoding would need the generated module (one function to swap).

## Stack

Python 3.12 · pandas · OpenTelemetry SDK (traces, metrics, logs over OTLP/HTTP) ·
SigNoz (self-hosted via Foundry / Docker Compose; Cloud also supported) ·
OPC UA (asyncua) + MQTT (paho) · Typer + Rich ·
structlog · pydantic-settings · pytest · Playwright · uv

## AI assistance disclosure

This project was built with AI assistance, using **Claude Code** (Anthropic) as a
pair-programming tool throughout: scaffolding modules, drafting tests and
documentation, and working through debugging sessions.

Every architectural decision, the scope, and the problem framing are the author's
own. All AI-generated code was reviewed, and the behaviour it claims is verified
by the test suite and by live runs against a real SigNoz instance — the numbers
quoted in this README (OEE 0.80 / 0.76 / 0.58, 240 dropped rows, token counts)
come from actual runs, not from the model's description of them.
