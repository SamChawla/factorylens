"""Tests for the real-time sensor sources (piece 6).

Covers the ``Reading`` -> pipeline-row contract, the mock feed's determinism and
fault behaviour, transport lag, scripted silence, and the invariant that keeps
the streaming and batch modes honest: a scalar malformed check that agrees with
the vectorised one it was derived from.
"""

from __future__ import annotations

import itertools

import pandas as pd
import pytest

from factorylens import generator, schema, sources
from factorylens.generator import DEMO_SCENARIO, FaultSpec, LineSpec, generate


def _take(feed: sources.MockPlcFeed, n: int) -> list[sources.Reading]:
    return list(itertools.islice(feed.subscribe(), n))


def _feed(*specs: sources.FeedSpec, **cfg) -> sources.MockPlcFeed:
    config = sources.FeedConfig(pace=False, **cfg)
    return sources.MockPlcFeed(list(specs), config)


def _clean_line(line_id: str = "line_1", **kw) -> sources.FeedSpec:
    return sources.FeedSpec(line=LineSpec(line_id=line_id, n_batches=0, **kw))


# --- Reading / frame contract -------------------------------------------------


def test_reading_row_matches_pipeline_schema():
    reading = _take(_feed(_clean_line()), 1)[0]
    assert set(reading.as_row()) == set(schema.COLUMNS)


def test_readings_to_frame_is_pipeline_shaped():
    readings = _take(_feed(_clean_line()), 5)
    frame = sources.readings_to_frame(readings)
    assert list(frame.columns) == schema.COLUMNS
    assert len(frame) == 5
    # The frame must survive the same validity rules the batch path applies.
    assert not schema.malformed_mask(frame).any()


def test_missing_reading_becomes_nan_not_none():
    """A null temperature has to read as NaN so ``null_ratio`` counts it."""
    spec = sources.FeedSpec(
        line=LineSpec(
            line_id="line_1", n_batches=0,
            faults=FaultSpec(missing_reading_ratio=1.0),
        )
    )
    frame = sources.readings_to_frame(_take(_feed(spec), 10))
    assert frame[schema.TEMPERATURE].isna().all()
    assert schema.null_ratio(frame) == 1.0


# --- determinism --------------------------------------------------------------


def test_same_seed_replays_identically():
    a = _take(_feed(_clean_line(), seed=99), 30)
    b = _take(_feed(_clean_line(), seed=99), 30)
    assert [r.as_row() for r in a] == [r.as_row() for r in b]


def test_different_seed_diverges():
    a = _take(_feed(_clean_line(), seed=1), 30)
    b = _take(_feed(_clean_line(), seed=2), 30)
    assert [r.as_row() for r in a] != [r.as_row() for r in b]


def test_streamed_baseline_matches_generated_baseline():
    """A streamed clean batch is the same animal as a generated one.

    Both call ``generator.baseline_row`` with the same spec, seed and draw
    order, so the physics cannot drift between the two modes.
    """
    spec = LineSpec(line_id="line_1", n_batches=3)
    streamed = _take(
        _feed(sources.FeedSpec(line=spec), seed=5, cadence_min=60.0), 3
    )
    import numpy as np

    rng = np.random.default_rng(5)
    start = pd.Timestamp(sources.FeedConfig().start)
    expected = [
        generator.baseline_row(spec, i, start + pd.Timedelta(minutes=60 * i), rng)
        for i in range(3)
    ]
    assert [r.as_row() for r in streamed] == expected


# --- faults -------------------------------------------------------------------


def test_malformed_ratio_one_corrupts_every_reading():
    spec = sources.FeedSpec(
        line=LineSpec(
            line_id="line_3", n_batches=0, faults=FaultSpec(malformed_ratio=1.0)
        )
    )
    frame = sources.readings_to_frame(_take(_feed(spec), 25))
    assert schema.malformed_mask(frame).all()


def test_clean_line_produces_no_malformed_rows():
    frame = sources.readings_to_frame(_take(_feed(_clean_line()), 50))
    assert not schema.malformed_mask(frame).any()


def test_fault_rate_is_approximately_the_configured_ratio():
    """Streaming faults are Bernoulli, not exact-count — so assert on the rate."""
    spec = sources.FeedSpec(
        line=LineSpec(
            line_id="line_3", n_batches=0, faults=FaultSpec(malformed_ratio=0.2)
        )
    )
    frame = sources.readings_to_frame(_take(_feed(spec, seed=3), 2000))
    rate = schema.malformed_mask(frame).mean()
    assert 0.16 < rate < 0.24


# --- transport: lag and silence ----------------------------------------------


def test_lag_shows_up_as_event_time_behind_ingest_time():
    spec = sources.FeedSpec(line=LineSpec(line_id="line_2", n_batches=0), lag_s=90.0)
    for reading in _take(_feed(spec), 5):
        assert reading.lag.total_seconds() == pytest.approx(90.0)


def test_lag_growth_accumulates_per_batch():
    spec = sources.FeedSpec(
        line=LineSpec(line_id="line_3", n_batches=0), lag_s=10.0, lag_growth_s=4.0
    )
    lags = [r.lag.total_seconds() for r in _take(_feed(spec), 4)]
    assert lags == [10.0, 14.0, 18.0, 22.0]


def test_zero_lag_line_has_no_gap():
    reading = _take(_feed(_clean_line()), 1)[0]
    assert reading.lag.total_seconds() == 0.0


def test_silent_line_stops_emitting_then_returns():
    spec = sources.FeedSpec(
        line=LineSpec(line_id="line_3", n_batches=0),
        silence_after_min=10.0,
        silence_for_min=5.0,
    )
    readings = _take(_feed(spec, cadence_min=1.0), 20)
    batches = {r.batch_id for r in readings}
    # Ticks 10..14 inclusive are silent; the line resumes at tick 15.
    assert "line_3-b009" in batches
    assert not any(f"line_3-b{i:03d}" in batches for i in range(10, 15))
    assert "line_3-b015" in batches


def test_permanent_silence_never_returns():
    spec = sources.FeedSpec(
        line=LineSpec(line_id="line_3", n_batches=0), silence_after_min=5.0
    )
    readings = list(itertools.islice(_feed(spec, cadence_min=1.0).subscribe(), 5))
    assert len(readings) == 5  # generator would spin forever; islice caps it
    assert all(int(r.batch_id.split("b")[-1]) < 5 for r in readings)


def test_silence_does_not_disturb_other_lines():
    """A dropped line must not shift the random stream the healthy lines see."""
    healthy = _clean_line("line_1")
    dropout = sources.FeedSpec(
        line=LineSpec(line_id="line_2", n_batches=0), silence_after_min=3.0
    )
    with_dropout = [
        r for r in _take(_feed(healthy, dropout, seed=11), 12) if r.line_id == "line_1"
    ]
    alone = [r for r in _take(_feed(healthy, seed=11), 12) if r.line_id == "line_1"]
    assert [r.as_row() for r in with_dropout] == [
        r.as_row() for r in alone[: len(with_dropout)]
    ]


# --- the scalar/vectorised malformed invariant -------------------------------


def test_scalar_and_vectorised_malformed_checks_agree():
    """``is_malformed_row`` and ``malformed_mask`` are one rule, two shapes.

    The streaming path judges rows one at a time and the batch path vectorises;
    if these ever disagree, a row could alert at ingest and survive cleaning (or
    the reverse). Pinned over the full demo dataset, faults and all.
    """
    frame = generate(DEMO_SCENARIO)
    vectorised = schema.malformed_mask(frame).tolist()
    scalar = [schema.is_malformed_row(row) for row in frame.to_dict("records")]
    assert scalar == vectorised


def test_scalar_check_catches_each_corruption_kind():
    row = {
        schema.TS: "2026-07-20T00:00:00",
        schema.LINE_ID: "line_1",
        schema.BATCH_ID: "line_1-b000",
        schema.PLANNED_MIN: 60.0,
        schema.DOWNTIME_MIN: 5.0,
        schema.IDEAL_CYCLE_S: 2.0,
        schema.TOTAL_COUNT: 1000,
        schema.GOOD_COUNT: 970,
        schema.TEMPERATURE: 68.0,
    }
    assert not schema.is_malformed_row(row)

    for corrupt in generator._CORRUPTIONS:
        broken = dict(row)
        corrupt(broken)
        assert schema.is_malformed_row(broken), corrupt.__name__


def test_missing_temperature_is_not_malformed():
    """Null readings are counted, not dropped — same rule as the batch path."""
    row = {
        schema.TS: "2026-07-20T00:00:00",
        schema.LINE_ID: "line_1",
        schema.BATCH_ID: "line_1-b000",
        schema.PLANNED_MIN: 60.0,
        schema.DOWNTIME_MIN: 5.0,
        schema.IDEAL_CYCLE_S: 2.0,
        schema.TOTAL_COUNT: 1000,
        schema.GOOD_COUNT: 970,
        schema.TEMPERATURE: None,
    }
    assert not schema.is_malformed_row(row)


# --- config -------------------------------------------------------------------


def test_wall_interval_reflects_time_compression():
    config = sources.FeedConfig(cadence_min=1.0, time_scale=6000.0)
    assert config.wall_interval_s == pytest.approx(0.01)


def test_empty_feed_is_rejected():
    with pytest.raises(ValueError, match="at least one line"):
        sources.MockPlcFeed([])


def test_demo_feed_has_the_same_cast_as_the_batch_demo():
    streamed = {f.line.line_id for f in sources.DEMO_FEEDS}
    batched = {line.line_id for line in DEMO_SCENARIO.lines}
    assert streamed == batched == set(schema.LINE_IDS)
