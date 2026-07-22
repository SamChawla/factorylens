# FactoryLens

Manufacturing pipeline health & data-quality observability, instrumented for
**SigNoz Cloud**. Built for the *Agents of SigNoz* hackathon (WeMakeDevs × SigNoz,
Jul 20–26 2026) — **Track 01: AI & Agent Observability**.

## The problem

A factory runs several production lines. Each line's data flows through an ETL
pipeline (ingest → clean → transform → aggregate) before anyone can trust a
number like **OEE** (Overall Equipment Effectiveness). When a line's OEE looks
wrong, the real question is *why*: missing sensor readings? malformed rows
dropped upstream? a batch that went stale and never updated?

FactoryLens instruments every pipeline stage as an OpenTelemetry span carrying
the data-quality facts (`rows_in`, `rows_dropped`, `null_ratio`, `stale_batch`,
…), ships them to SigNoz Cloud, and puts a Q&A CLI in front so you can *ask* the
telemetry what went wrong instead of hunting through dashboards.

## Status

Under active build. Pieces land in scope order:

1. ✅ Synthetic multi-line data generator with reproducible fault injection
2. ✅ ETL pipeline (one OTel span per stage) + OEE aggregation
3. ⬜ Export to SigNoz Cloud + hand-built dashboard
4. ⬜ Q&A CLI (Euri primary, Groq fallback)
5. ⬜ Demo video

## Quickstart (dev)

```bash
uv sync
uv run pytest
```

Copy `.env.example` to `.env` and fill in SigNoz + LLM credentials before
running against real telemetry. `.env` is gitignored — never commit it.
