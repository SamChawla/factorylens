"""The Q&A agent: read the pipeline's spans, answer questions about them.

The agent answers strictly from telemetry — the same span attributes that were
exported to SigNoz — so an answer is always traceable back to a number on a
span rather than the model's prior beliefs about factories.

Data source is behind ``TelemetrySource`` on purpose. Today the only
implementation reads spans captured in-process during a run (always available,
no external dependency). A SigNoz-query implementation can be added later
without touching the prompt-building or CLI layers.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, Sequence

from opentelemetry.metrics import Meter
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import Tracer

from factorylens import llm
from factorylens.config import Settings, get_settings
from factorylens.generator import DEMO_SCENARIO, degrading_scenario, generate
from factorylens.logging import get_logger
from factorylens.pipeline import run_pipeline
from factorylens.telemetry import Telemetry

_log = get_logger("agent")

STAGES = ("ingest", "clean", "transform", "aggregate")

SYSTEM_PROMPT = (
    "You are a manufacturing pipeline observability assistant. You are given "
    "OpenTelemetry span data from an ETL pipeline that computes OEE (Overall "
    "Equipment Effectiveness = Availability x Performance x Quality) per "
    "production line.\n\n"
    "Answer using ONLY the telemetry provided. Cite the specific numbers that "
    "support your answer (e.g. rows_dropped, null_ratio, stale_batch, the OEE "
    "factors). If the data does not answer the question, say so plainly instead "
    "of speculating. Be concise and concrete, as a plant engineer would be.\n\n"
    "Field meanings: rows_dropped = malformed rows removed by the clean stage; "
    "null_ratio = fraction of missing sensor readings that survived cleaning; "
    "stale_batch = a batch whose timestamp stopped advancing."
)


@dataclass(frozen=True)
class LineFacts:
    """Everything the spans said about one line during one pipeline run."""

    line_id: str
    rows_in: int
    rows_out: int
    rows_dropped: int
    null_ratio: float
    stale_batch: bool
    availability: float
    performance: float
    quality: float
    oee: float
    stage_ms: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class Snapshot:
    """Facts from one or more consecutive pipeline runs, oldest first."""

    runs: list[list[LineFacts]]

    @property
    def latest(self) -> list[LineFacts]:
        return self.runs[-1] if self.runs else []


class TelemetrySource(Protocol):
    """Where the agent gets its spans. Swappable (in-process today, SigNoz later)."""

    def snapshot(self, runs: int = 1) -> Snapshot: ...


def _ms(span: ReadableSpan) -> float:
    return (span.end_time - span.start_time) / 1_000_000


def facts_from_spans(spans: Sequence[ReadableSpan]) -> list[LineFacts]:
    """Fold one run's spans into per-line facts.

    The clean stage supplies data quality, the aggregate stage supplies the OEE
    factors, and every stage contributes its duration.
    """
    by_line: dict[str, dict] = {}
    for span in spans:
        if span.name not in STAGES:
            continue
        attrs = span.attributes or {}
        line_id = attrs.get("line_id")
        if line_id is None:
            continue
        entry = by_line.setdefault(str(line_id), {"stage_ms": {}})
        entry["stage_ms"][span.name] = round(_ms(span), 2)

        if span.name == "clean":
            entry.update(
                rows_in=int(attrs.get("rows_in", 0)),
                rows_out=int(attrs.get("rows_out", 0)),
                rows_dropped=int(attrs.get("rows_dropped", 0)),
                null_ratio=float(attrs.get("null_ratio", 0.0)),
                stale_batch=bool(attrs.get("stale_batch", False)),
            )
        elif span.name == "aggregate":
            entry.update(
                availability=float(attrs.get("availability", 0.0)),
                performance=float(attrs.get("performance", 0.0)),
                quality=float(attrs.get("quality", 0.0)),
                oee=float(attrs.get("oee", 0.0)),
            )

    facts = []
    for line_id in sorted(by_line):
        e = by_line[line_id]
        facts.append(
            LineFacts(
                line_id=line_id,
                rows_in=e.get("rows_in", 0),
                rows_out=e.get("rows_out", 0),
                rows_dropped=e.get("rows_dropped", 0),
                null_ratio=e.get("null_ratio", 0.0),
                stale_batch=e.get("stale_batch", False),
                availability=e.get("availability", 0.0),
                performance=e.get("performance", 0.0),
                quality=e.get("quality", 0.0),
                oee=e.get("oee", 0.0),
                stage_ms=e.get("stage_ms", {}),
            )
        )
    return facts


@dataclass
class LocalPipelineSource:
    """Runs the pipeline and reads the spans it emitted, in-process.

    The capture exporter sits alongside the real one, so these are the exact
    spans that were shipped to SigNoz.
    """

    telemetry: Telemetry
    capture: InMemorySpanExporter

    def snapshot(self, runs: int = 1) -> Snapshot:
        collected: list[list[LineFacts]] = []
        for i in range(runs):
            self.capture.clear()
            scenario = DEMO_SCENARIO if runs == 1 else degrading_scenario(i, runs)
            run_pipeline(generate(scenario), self.telemetry)
            self.telemetry.force_flush()
            collected.append(facts_from_spans(self.capture.get_finished_spans()))
        return Snapshot(runs=collected)


def format_context(snapshot: Snapshot) -> str:
    """Render a snapshot as compact text for the prompt."""
    if not snapshot.runs:
        return "No pipeline telemetry available."

    multi = len(snapshot.runs) > 1
    lines: list[str] = []
    lines.append(
        f"Pipeline telemetry: {len(snapshot.runs)} run(s)"
        + (", oldest first — use this to judge trends." if multi else ".")
    )
    for idx, run in enumerate(snapshot.runs, start=1):
        lines.append(f"\nRun {idx}:" if multi else "\nRun:")
        for f in run:
            stages = " ".join(f"{k}={v}ms" for k, v in sorted(f.stage_ms.items()))
            lines.append(
                f"  {f.line_id}: rows_in={f.rows_in} rows_out={f.rows_out} "
                f"rows_dropped={f.rows_dropped} null_ratio={f.null_ratio:.3f} "
                f"stale_batch={f.stale_batch} | availability={f.availability:.2f} "
                f"performance={f.performance:.2f} quality={f.quality:.2f} "
                f"OEE={f.oee:.2f} | stage durations: {stages}"
            )
    return "\n".join(lines)


def answer(
    question: str,
    snapshot: Snapshot,
    *,
    settings: Settings | None = None,
    tracer: Tracer | None = None,
    meter: Meter | None = None,
) -> str:
    """Answer a question about the pipeline from its telemetry."""
    settings = settings or get_settings()
    context = format_context(snapshot)
    prompt = f"{context}\n\nQuestion: {question}"
    _log.info("agent_question", question=question, runs=len(snapshot.runs))
    return llm.ask(
        prompt, system=SYSTEM_PROMPT, settings=settings, tracer=tracer, meter=meter
    )
