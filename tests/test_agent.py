"""Tests for the Q&A agent (piece 4).

The agent must answer from telemetry, so what matters here is that span
attributes survive the trip into the prompt intact. If a number never reaches
the context, the model can only guess — and a confident guess is the worst
failure mode this project could ship.
"""

from __future__ import annotations

import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

from factorylens import agent
from factorylens.generator import DEMO_SCENARIO
from factorylens.telemetry import setup_telemetry


@pytest.fixture
def source():
    capture = InMemorySpanExporter()
    tel = setup_telemetry(exporter=InMemorySpanExporter(), capture=capture)
    yield agent.LocalPipelineSource(telemetry=tel, capture=capture)
    tel.shutdown()


def test_snapshot_has_one_entry_per_line(source):
    snap = source.snapshot(runs=1)
    assert len(snap.runs) == 1
    assert [f.line_id for f in snap.latest] == ["line_1", "line_2", "line_3"]


def test_snapshot_carries_the_data_quality_facts(source):
    snap = source.snapshot(runs=1)
    facts = {f.line_id: f for f in snap.latest}
    # Expectations derive from the scenario, so resizing the demo data doesn't
    # invalidate the test — only a real behaviour change does.
    spec = {l.line_id: l for l in DEMO_SCENARIO.lines}

    line_3 = spec["line_3"]
    assert facts["line_3"].rows_dropped == round(
        line_3.faults.malformed_ratio * line_3.n_batches
    )
    assert facts["line_3"].stale_batch is True

    line_2 = spec["line_2"]  # no malformed rows, so every batch survives cleaning
    assert facts["line_2"].null_ratio == pytest.approx(
        line_2.faults.missing_reading_ratio, abs=1e-3
    )
    assert facts["line_1"].rows_dropped == 0
    assert facts["line_1"].stale_batch is False


def test_snapshot_carries_oee_factors(source):
    facts = {f.line_id: f for f in source.snapshot(runs=1).latest}
    line_3 = facts["line_3"]
    assert 0.0 < line_3.oee < 1.0
    assert line_3.oee < facts["line_1"].oee
    for factor in (line_3.availability, line_3.performance, line_3.quality):
        assert 0.0 < factor <= 1.0


def test_snapshot_records_every_stage_duration(source):
    facts = source.snapshot(runs=1).latest
    for f in facts:
        assert set(f.stage_ms) == set(agent.STAGES)


def test_multi_run_snapshot_captures_the_degradation(source):
    snap = source.snapshot(runs=6)
    assert len(snap.runs) == 6
    first = {f.line_id: f for f in snap.runs[0]}["line_3"]
    last = {f.line_id: f for f in snap.runs[-1]}["line_3"]
    assert last.oee < first.oee
    assert last.rows_dropped > first.rows_dropped


def test_format_context_includes_the_numbers_the_answer_needs(source):
    context = agent.format_context(source.snapshot(runs=1))
    for token in ("line_1", "line_2", "line_3", "rows_dropped", "null_ratio",
                  "stale_batch", "OEE", "availability"):
        assert token in context


def test_format_context_handles_an_empty_snapshot():
    assert "No pipeline telemetry" in agent.format_context(agent.Snapshot(runs=[]))


def test_answer_sends_the_telemetry_to_the_llm(monkeypatch, source):
    captured = {}

    def fake_ask(prompt, *, system=None, settings=None, tracer=None, timeout=60.0):
        captured.update(prompt=prompt, system=system)
        return "because line_3 dropped rows"

    monkeypatch.setattr(agent.llm, "ask", fake_ask)
    result = agent.answer("why is line_3 OEE low?", source.snapshot(runs=1))

    assert result == "because line_3 dropped rows"
    # The question and the telemetry both reach the model.
    assert "why is line_3 OEE low?" in captured["prompt"]
    assert "rows_dropped" in captured["prompt"]
    assert "line_3" in captured["prompt"]
    # The system prompt pins it to the data.
    assert "ONLY the telemetry" in captured["system"]


def test_facts_from_spans_ignores_unrelated_spans():
    # A span with no line_id (or an unrelated name) must not create a bogus line.
    assert agent.facts_from_spans([]) == []
