"""Tests for the streaming runtime (piece 6).

Three layers, tested separately then together:

  - ``WindowBuffer``    — tumbling event-time windows, watermarks, lateness
  - ``SilenceWatchdog`` — processing-time detection of a line that went quiet
  - ``StreamRunner``    — the loop, and the span/metric contract it emits

The integration tests use an unpaced feed with an injected clock, so a full
simulated shift runs in milliseconds without the watchdog concluding that every
line is healthy simply because no wall-clock time passed.
"""

from __future__ import annotations

import itertools
from datetime import datetime, timedelta

import pytest
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

from factorylens import schema, sources, stream
from factorylens.config import Settings
from factorylens.generator import FaultSpec, LineSpec
from factorylens.telemetry import setup_telemetry

ORIGIN = datetime(2026, 7, 20, 0, 0, 0)


def _reading(minutes: float, line_id: str = "line_1", temperature: float = 68.0,
             total: object = 1000, good: object = 970) -> sources.Reading:
    at = ORIGIN + timedelta(minutes=minutes)
    return sources.Reading(
        event_time=at,
        ingest_time=at,
        line_id=line_id,
        batch_id=f"{line_id}-b{int(minutes):03d}",
        planned_min=60.0,
        downtime_min=5.0,
        ideal_cycle_s=2.0,
        total_count=total,
        good_count=good,
        temperature=temperature,
    )


# --- WindowBuffer -------------------------------------------------------------


def _buffer(width_min: float = 60.0, lateness_min: float = 0.0) -> stream.WindowBuffer:
    return stream.WindowBuffer(ORIGIN, width_min, lateness_min)


def test_window_stays_open_until_watermark_passes_its_end():
    buf = _buffer()
    assert buf.add(_reading(0)) == []
    assert buf.add(_reading(30)) == []
    assert buf.add(_reading(59)) == []


def test_window_closes_when_watermark_clears_the_boundary():
    buf = _buffer()
    buf.add(_reading(0))
    closed = buf.add(_reading(60))  # watermark 60 >= window 0's end
    assert [w.index for w in closed] == [0]
    assert len(closed[0].readings) == 1


def test_allowed_lateness_holds_a_window_open_for_laggy_lines():
    """A line 20 minutes behind still lands in its own window."""
    buf = _buffer(width_min=60.0, lateness_min=30.0)
    buf.add(_reading(0, "line_1"))
    assert buf.add(_reading(70, "line_1")) == []  # watermark 70-30=40 < 60
    late_but_admitted = _reading(55, "line_3")
    assert buf.add(late_but_admitted) == []
    closed = buf.add(_reading(95, "line_1"))  # watermark 65 >= 60
    assert [w.index for w in closed] == [0]
    assert {r.line_id for r in closed[0].readings} == {"line_1", "line_3"}
    assert closed[0].late == 0


def test_reading_after_its_window_closed_is_counted_late():
    buf = _buffer()
    buf.add(_reading(0))
    buf.add(_reading(60))  # closes window 0
    buf.add(_reading(30))  # belongs to the closed window 0
    assert buf.late_total == 1


def test_late_reading_is_kept_not_discarded():
    """Late data is misfiled loudly, never silently dropped."""
    buf = _buffer()
    buf.add(_reading(0))
    buf.add(_reading(60))
    buf.add(_reading(30))
    remaining = buf.drain()
    assert sum(len(w.readings) for w in remaining) == 2  # the 60 and the late 30
    assert remaining[0].late == 1


def test_several_windows_close_at_once_on_a_jump():
    """Lateness holds windows open, so one late-arriving jump can close several."""
    buf = _buffer(width_min=60.0, lateness_min=120.0)
    for minutes in (0, 65, 130):
        assert buf.add(_reading(minutes)) == []  # watermark still behind
    assert [w.index for w in buf.add(_reading(200))] == [0]
    assert [w.index for w in buf.add(_reading(320))] == [1, 2]


def test_reading_before_the_origin_gets_its_own_window():
    """A lagged line can predate the origin; it must not fold into window 0."""
    buf = _buffer()
    buf.add(_reading(0, "line_1"))
    closed = buf.add(_reading(-90, "line_3"))
    seen = {w.index for w in closed} | {w.index for w in buf.drain()}
    assert seen == {-2, 0}


def test_aligned_origin_snaps_to_the_calendar_grid():
    shift = stream.aligned_origin(ORIGIN + timedelta(hours=9, minutes=17), 480.0)
    assert shift == ORIGIN + timedelta(hours=8)
    assert stream.aligned_origin(ORIGIN + timedelta(minutes=5), 480.0) == ORIGIN


def test_drain_returns_open_windows_in_order():
    buf = _buffer(width_min=60.0, lateness_min=1000.0)  # nothing closes early
    for minutes in (0, 65, 130):
        buf.add(_reading(minutes))
    assert [w.index for w in buf.drain()] == [0, 1, 2]


def test_zero_width_window_is_rejected():
    with pytest.raises(ValueError, match="positive"):
        stream.WindowBuffer(ORIGIN, 0.0, 0.0)


# --- SilenceWatchdog ----------------------------------------------------------


def test_watchdog_fires_once_when_a_line_goes_quiet():
    dog = stream.SilenceWatchdog(threshold_s=5.0)
    dog.saw("line_3", now=0.0)
    assert dog.check(now=3.0) == []
    assert [lid for lid, _ in dog.check(now=10.0)] == ["line_3"]
    assert dog.check(now=20.0) == []  # already firing, not re-reported


def test_watchdog_reports_recovery():
    dog = stream.SilenceWatchdog(threshold_s=5.0)
    dog.saw("line_3", now=0.0)
    dog.check(now=10.0)
    assert dog.saw("line_3", now=11.0) == "line_3"
    assert dog.saw("line_3", now=12.0) is None


def test_watchdog_can_fire_again_after_recovery():
    dog = stream.SilenceWatchdog(threshold_s=5.0)
    dog.saw("line_3", now=0.0)
    dog.check(now=10.0)
    dog.saw("line_3", now=11.0)
    assert [lid for lid, _ in dog.check(now=20.0)] == ["line_3"]


def test_watchdog_ignores_lines_it_has_never_seen():
    dog = stream.SilenceWatchdog(threshold_s=1.0)
    assert dog.check(now=1000.0) == []


def test_silence_threshold_has_a_floor():
    """Under heavy compression, a GC pause must not read as a dead line."""
    config = stream.StreamConfig(min_silence_s=1.0)
    assert stream.expected_silence_threshold_s(0.001, config) == 1.0
    assert stream.expected_silence_threshold_s(10.0, config) == 50.0


# --- alert cooldown -----------------------------------------------------------


def test_cooldown_suppresses_a_burst_and_counts_it():
    gate = stream._AlertGate(cooldown_s=2.0)
    assert gate.admit("k", "line_1", now=0.0) == 0
    assert gate.admit("k", "line_1", now=0.5) is None
    assert gate.admit("k", "line_1", now=1.0) is None
    assert gate.admit("k", "line_1", now=3.0) == 2  # reports what it swallowed


def test_cooldown_is_per_line_and_per_kind():
    gate = stream._AlertGate(cooldown_s=2.0)
    assert gate.admit("k", "line_1", now=0.0) == 0
    assert gate.admit("k", "line_2", now=0.0) == 0
    assert gate.admit("other", "line_1", now=0.0) == 0


def test_pending_flushes_a_burst_that_ended_mid_cooldown():
    gate = stream._AlertGate(cooldown_s=10.0)
    gate.admit("k", "line_1", now=0.0)
    gate.admit("k", "line_1", now=1.0)
    gate.admit("k", "line_1", now=2.0)
    assert gate.pending() == [("k", "line_1", 2)]
    assert gate.pending() == []  # drained


# --- StreamRunner integration -------------------------------------------------


@pytest.fixture
def wired():
    """A runner with in-memory span and metric capture."""
    capture = InMemorySpanExporter()
    reader = InMemoryMetricReader()
    telemetry = setup_telemetry(
        Settings(telemetry_enabled=False),
        exporter=InMemorySpanExporter(),
        capture=capture,
        metric_reader=reader,
    )
    yield telemetry, capture, reader
    telemetry.shutdown()


def _stepped_clock(step: float = 0.01):
    """Processing clock that advances a fixed amount per reading."""
    counter = itertools.count()
    return lambda: next(counter) * step


def _span_names(capture: InMemorySpanExporter) -> dict[str, int]:
    names: dict[str, int] = {}
    for span in capture.get_finished_spans():
        names[span.name] = names.get(span.name, 0) + 1
    return names


def _metric_names(reader: InMemoryMetricReader) -> set[str]:
    data = reader.get_metrics_data()
    return {
        metric.name
        for rm in (data.resource_metrics if data else [])
        for sm in rm.scope_metrics
        for metric in sm.metrics
    }


def test_run_emits_window_and_pipeline_spans(wired):
    telemetry, capture, _ = wired
    feed = sources.demo_feed(sources.FeedConfig(pace=False))
    runner = stream.StreamRunner(telemetry, stream.StreamConfig(window_min=480.0))

    # A window holds only the lines that reported into it, so the expected
    # pipeline_run count is the sum of lines per window, not windows x 3.
    expected_runs = 0

    def on_window(window: stream.Window, _results) -> None:
        nonlocal expected_runs
        expected_runs += len(window.line_ids)

    stats = runner.run(feed, max_readings=3000, clock=_stepped_clock(),
                       on_window=on_window)

    names = _span_names(capture)
    assert names["stream_window"] == stats.windows
    for stage in ("ingest", "clean", "transform", "aggregate", "pipeline_run"):
        assert names[stage] == expected_runs


def test_stage_spans_keep_the_contract_the_agent_reads(wired):
    """Windowing must not change the span shape the Q&A agent depends on."""
    from factorylens.pipeline import REQUIRED_STAGE_ATTRS

    telemetry, capture, _ = wired
    feed = sources.demo_feed(sources.FeedConfig(pace=False))
    runner = stream.StreamRunner(telemetry, stream.StreamConfig(window_min=480.0))
    runner.run(feed, max_readings=2000, clock=_stepped_clock())

    stages = [s for s in capture.get_finished_spans() if s.name in ("ingest", "clean")]
    assert stages
    for span in stages:
        for attr in REQUIRED_STAGE_ATTRS:
            assert attr in (span.attributes or {}), f"{span.name} missing {attr}"


def test_run_records_the_per_reading_metrics(wired):
    telemetry, _, reader = wired
    feed = sources.demo_feed(sources.FeedConfig(pace=False))
    runner = stream.StreamRunner(telemetry, stream.StreamConfig())
    runner.run(feed, max_readings=500, clock=_stepped_clock())

    assert {
        "factorylens.readings.received",
        "factorylens.ingest.lag_ms",
        "factorylens.sensor.temperature",
    } <= _metric_names(reader)


def test_lagging_line_reports_the_largest_lag(wired):
    telemetry, _, _ = wired
    feed = sources.demo_feed(sources.FeedConfig(pace=False))
    runner = stream.StreamRunner(telemetry, stream.StreamConfig())
    stats = runner.run(feed, max_readings=900, clock=_stepped_clock())

    assert stats.max_lag_s["line_1"] == 0.0
    assert stats.max_lag_s["line_2"] == pytest.approx(90.0)
    assert stats.max_lag_s["line_3"] > stats.max_lag_s["line_2"]


def test_threshold_breach_fires_on_the_hot_line(wired):
    telemetry, _, _ = wired
    feed = sources.demo_feed(sources.FeedConfig(pace=False))
    runner = stream.StreamRunner(
        telemetry, stream.StreamConfig(temp_max=85.0, alert_cooldown_s=0.0)
    )
    fired: list[stream.Alert] = []
    runner.run(feed, max_readings=1500, clock=_stepped_clock(),
               on_alert=fired.append)

    breaches = [a for a in fired if a.kind == stream.ALERT_THRESHOLD]
    assert breaches
    assert {a.line_id for a in breaches} == {"line_3"}  # only line_3 runs hot
    assert all(a.value > 85.0 for a in breaches)


def test_malformed_reading_alerts_at_ingest(wired):
    telemetry, _, _ = wired
    feed = sources.demo_feed(sources.FeedConfig(pace=False))
    runner = stream.StreamRunner(telemetry, stream.StreamConfig(alert_cooldown_s=0.0))
    fired: list[stream.Alert] = []
    runner.run(feed, max_readings=1500, clock=_stepped_clock(), on_alert=fired.append)

    malformed = [a for a in fired if a.kind == stream.ALERT_MALFORMED]
    assert malformed
    assert {a.line_id for a in malformed} == {"line_3"}


def test_silence_alert_fires_for_the_dropout_line(wired):
    telemetry, _, _ = wired
    feed = sources.demo_feed(sources.FeedConfig(pace=False))
    runner = stream.StreamRunner(telemetry, stream.StreamConfig())
    fired: list[stream.Alert] = []
    runner.run(feed, max_readings=3000, silence_threshold_s=0.5,
               clock=_stepped_clock(), on_alert=fired.append)

    silences = [a for a in fired if a.kind == stream.ALERT_SILENCE]
    assert silences
    assert {a.line_id for a in silences} == {"line_3"}


def _stale_flags(wired, feed_spec: sources.FeedSpec, readings: int) -> list[bool]:
    """Run one line through the stream; report has_stale_batch per window."""
    telemetry, _, _ = wired
    feed = sources.MockPlcFeed([feed_spec], sources.FeedConfig(pace=False))
    runner = stream.StreamRunner(telemetry, stream.StreamConfig(window_min=480.0))

    flags: list[bool] = []

    def on_window(window: stream.Window, _results) -> None:
        frame = sources.readings_to_frame(window.readings)
        flags.append(schema.has_stale_batch(frame))

    runner.run(feed, max_readings=readings, silence_threshold_s=0.5,
               clock=_stepped_clock(), on_window=on_window)
    return flags


def test_silence_inside_a_window_becomes_stale_batch(wired):
    """The whole point of modelling staleness as silence.

    ``schema.has_stale_batch`` is untouched — the gap the dropout leaves in the
    window is a real one, so the existing cadence-relative detector finds it.
    """
    dropout = sources.FeedSpec(
        line=LineSpec(line_id="line_3", n_batches=0),
        silence_after_min=100.0,
        silence_for_min=200.0,  # 100..300, comfortably inside window 0 (0..480)
    )
    assert any(_stale_flags(wired, dropout, readings=600))


def test_silence_at_a_window_edge_is_invisible_to_the_batch_detector(wired):
    """An honest limitation, pinned rather than tuned around.

    ``has_stale_batch`` looks for an outlier gap *between* readings it can see.
    A dropout that runs to the end of a window leaves no interior gap, so only
    the live watchdog catches it — which is precisely why the watchdog exists
    and is not merely a nicer restatement of the batch check.
    """
    edge_dropout = sources.FeedSpec(
        line=LineSpec(line_id="line_3", n_batches=0),
        silence_after_min=300.0,
        silence_for_min=180.0,  # 300..480 — runs exactly to the window boundary
    )
    assert not any(_stale_flags(wired, edge_dropout, readings=600))


def test_healthy_lines_never_read_as_stale(wired):
    healthy = sources.FeedSpec(line=LineSpec(line_id="line_1", n_batches=0))
    assert not any(_stale_flags(wired, healthy, readings=600))


def test_streamed_oee_ranks_the_lines_the_same_way_as_batch_mode(wired):
    """The two modes must tell one story: line_1 healthy, line_3 worst."""
    telemetry, _, _ = wired
    feed = sources.demo_feed(sources.FeedConfig(pace=False))
    runner = stream.StreamRunner(telemetry, stream.StreamConfig(window_min=480.0))
    stats = runner.run(feed, max_readings=3000, clock=_stepped_clock())

    results = stats.last_results
    assert results["line_1"].oee > results["line_2"].oee > results["line_3"].oee
    assert results["line_3"].oee < 0.7


def test_max_readings_stops_the_run(wired):
    telemetry, _, _ = wired
    feed = sources.demo_feed(sources.FeedConfig(pace=False))
    runner = stream.StreamRunner(telemetry, stream.StreamConfig())
    stats = runner.run(feed, max_readings=250, clock=_stepped_clock())
    assert stats.readings == 250


def test_open_windows_are_drained_at_end_of_run(wired):
    """A run must not throw away readings it already collected."""
    telemetry, _, _ = wired
    feed = sources.demo_feed(sources.FeedConfig(pace=False))
    runner = stream.StreamRunner(telemetry, stream.StreamConfig(window_min=480.0))

    banked = 0

    def on_window(window: stream.Window, _results) -> None:
        nonlocal banked
        banked += len(window.readings)

    # Far fewer readings than a full window: without a drain, none would be
    # processed at all. Every reading must still land in some window.
    stats = runner.run(feed, max_readings=90, clock=_stepped_clock(),
                       on_window=on_window)
    assert stats.windows >= 1
    assert banked == stats.readings == 90
    assert stats.last_results


def test_clean_feed_raises_no_alerts(wired):
    telemetry, _, _ = wired
    healthy = sources.FeedSpec(
        line=LineSpec(line_id="line_1", n_batches=0, faults=FaultSpec(), temp_mean=68.0)
    )
    feed = sources.MockPlcFeed([healthy], sources.FeedConfig(pace=False))
    runner = stream.StreamRunner(telemetry, stream.StreamConfig())
    fired: list[stream.Alert] = []
    runner.run(feed, max_readings=500, clock=_stepped_clock(), on_alert=fired.append)
    assert fired == []
