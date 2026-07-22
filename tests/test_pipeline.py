"""Tests for the ETL pipeline stages and orchestration (piece 2).

Covers the clean stage on both clean and deliberately-broken input, the full
run over the demo scenario, and the span contract the Q&A CLI depends on: every
stage span must carry the required data-quality attribute set.
"""

from __future__ import annotations

import pandas as pd
import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

from factorylens import schema
from factorylens.generator import DEMO_SCENARIO, FaultSpec, LineSpec, Scenario, generate
from factorylens.pipeline import (
    REQUIRED_STAGE_ATTRS,
    clean_stage,
    run_pipeline,
)
from factorylens.telemetry import setup_telemetry


@pytest.fixture
def telemetry():
    exporter = InMemorySpanExporter()
    tel = setup_telemetry(exporter=exporter)
    tel.exporter = exporter  # convenience handle for assertions
    yield tel
    tel.shutdown()


def _one_line(faults: FaultSpec, n: int = 40) -> pd.DataFrame:
    scenario = Scenario(
        lines=[LineSpec(line_id="line_1", n_batches=n, faults=faults)],
        seed=7,
        start="2026-07-20T00:00:00",
    )
    return generate(scenario)


def test_clean_stage_keeps_clean_input(telemetry):
    df = _one_line(FaultSpec())
    cleaned = clean_stage(df, "line_1", telemetry.tracer())
    assert len(cleaned) == len(df)


def test_clean_stage_drops_exactly_the_malformed_rows(telemetry):
    df = _one_line(FaultSpec(malformed_ratio=0.1), n=40)
    expected_dropped = round(0.1 * 40)
    cleaned = clean_stage(df, "line_1", telemetry.tracer())
    assert len(cleaned) == len(df) - expected_dropped
    # No malformed rows survive.
    assert schema.malformed_mask(cleaned).sum() == 0


def test_clean_stage_reports_null_ratio_and_stale_on_span(telemetry):
    df = _one_line(FaultSpec(missing_reading_ratio=0.25, stale_batch=True), n=40)
    clean_stage(df, "line_1", telemetry.tracer())
    span = next(s for s in telemetry.exporter.get_finished_spans() if s.name == "clean")
    # 10 missing readings survive cleaning (missing != malformed), over 40 rows.
    assert span.attributes["null_ratio"] == pytest.approx(0.25)
    assert span.attributes["stale_batch"] is True


def test_run_pipeline_returns_a_result_per_line(telemetry):
    raw = generate(DEMO_SCENARIO)
    results = run_pipeline(raw, telemetry)
    assert set(results) == set(schema.LINE_IDS)


def test_problem_line_has_lower_oee_than_healthy_line(telemetry):
    # line_3 is scripted to run hot and slow with more downtime -> lower OEE.
    raw = generate(DEMO_SCENARIO)
    results = run_pipeline(raw, telemetry)
    assert results["line_3"].oee < results["line_1"].oee
    assert 0.0 <= results["line_3"].oee <= 1.0


def test_every_stage_span_carries_required_attributes(telemetry):
    raw = generate(DEMO_SCENARIO)
    run_pipeline(raw, telemetry)
    stage_spans = [
        s
        for s in telemetry.exporter.get_finished_spans()
        if s.name in {"ingest", "clean", "transform", "aggregate"}
    ]
    # 4 stages x 3 lines.
    assert len(stage_spans) == 12
    for span in stage_spans:
        for attr in REQUIRED_STAGE_ATTRS:
            assert attr in span.attributes, f"{span.name} missing {attr}"


def test_clean_span_records_dropped_rows_for_the_malformed_line(telemetry):
    raw = generate(DEMO_SCENARIO)
    run_pipeline(raw, telemetry)
    clean_spans = {
        s.attributes["line_id"]: s
        for s in telemetry.exporter.get_finished_spans()
        if s.name == "clean"
    }
    line_3 = next(l for l in DEMO_SCENARIO.lines if l.line_id == "line_3")
    assert clean_spans["line_3"].attributes["rows_dropped"] == round(
        line_3.faults.malformed_ratio * line_3.n_batches
    )
    assert clean_spans["line_1"].attributes["rows_dropped"] == 0
