"""Real-time sensor sources: the seam between the factory floor and the pipeline.

Piece 6. Everything downstream of this module already worked on a DataFrame; the
batch generator handed it one built from a finished dataset. A real plant does
not work that way — readings arrive one at a time, over a wire, late, out of
order, and sometimes not at all.

This module models that arrival process. A ``SensorSource`` yields ``Reading``
objects as they turn up; :mod:`factorylens.stream` reassembles them into windowed
DataFrames and feeds the *unchanged* pipeline. Swapping the mock for a real
OPC UA / MQTT client is a new class implementing one method, not a rewrite.

Why an ``Iterator`` is the protocol: it is the shape both plausible
implementations already have. A simulator is a generator; a real client hands
its ``on_message`` callback to a background thread that pushes onto a
``queue.Queue``, and draining that queue *is* an iterator. Neither has to
pretend to be something it isn't.

The mock deliberately reuses ``generator.baseline_row`` and
``generator.LineSpec``, so a streamed batch is statistically the same animal as a
generated one and the demo tells one story in two modes.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterator, Protocol, Sequence

import numpy as np
import pandas as pd

from factorylens import generator, schema


@dataclass(frozen=True)
class Reading:
    """One batch record as it comes off a line, plus the two clocks that matter.

    ``event_time`` is when the line says the batch happened — the PLC's clock.
    ``ingest_time`` is when we received it. Everything interesting about a
    real-time system lives in the gap between them: a feed that is healthy but
    slow looks identical to a healthy fast one in the *data*, and completely
    different in the *lag*.

    Field names after the timestamps mirror :data:`schema.COLUMNS` exactly, so a
    window of readings becomes a pipeline-shaped DataFrame with no translation
    layer that could drift.
    """

    event_time: datetime
    ingest_time: datetime
    line_id: str
    batch_id: str
    planned_min: float
    downtime_min: float
    ideal_cycle_s: float
    total_count: object  # object, not int: a malformed feed sends "ERR"
    good_count: object
    temperature: float | None

    @property
    def lag(self) -> timedelta:
        """How far behind the feed was when this reading landed."""
        return self.ingest_time - self.event_time

    def as_row(self) -> dict:
        """Render as a raw pipeline row, keyed by :data:`schema.COLUMNS`."""
        return {
            schema.TS: self.event_time.isoformat(),
            schema.LINE_ID: self.line_id,
            schema.BATCH_ID: self.batch_id,
            schema.PLANNED_MIN: self.planned_min,
            schema.DOWNTIME_MIN: self.downtime_min,
            schema.IDEAL_CYCLE_S: self.ideal_cycle_s,
            schema.TOTAL_COUNT: self.total_count,
            schema.GOOD_COUNT: self.good_count,
            schema.TEMPERATURE: np.nan if self.temperature is None else self.temperature,
        }


def readings_to_frame(readings: Sequence[Reading]) -> pd.DataFrame:
    """Turn a window of readings into the raw DataFrame the pipeline expects."""
    return pd.DataFrame([r.as_row() for r in readings], columns=schema.COLUMNS)


class SensorSource(Protocol):
    """Where readings come from. Mock today; OPC UA / MQTT later, same contract."""

    def subscribe(self) -> Iterator[Reading]:
        """Yield readings as they arrive. May block between them."""
        ...

    def close(self) -> None:
        """Stop producing. Safe to call from another thread, and twice."""
        ...


@dataclass(frozen=True)
class FeedSpec:
    """How one line behaves *on the wire*, on top of what it produces.

    ``LineSpec`` says what the line makes (and which faults are baked into the
    data). This says how that reaches us: how late, and whether it stops.

    Transport failures are separate from data failures on purpose — a line can
    be producing perfectly while its gateway silently falls half an hour behind,
    and that is invisible to every metric FactoryLens had before now.
    """

    line: generator.LineSpec
    lag_s: float = 0.0
    """Constant transport delay, in *simulated* seconds."""

    lag_growth_s: float = 0.0
    """Added to the delay per batch — a gateway buffering faster than it drains."""

    lag_max_s: float | None = None
    """Ceiling on the accumulated delay.

    Real backpressure saturates: a gateway's buffer fills and the lag plateaus
    rather than growing without bound. Left uncapped, the delay would eventually
    exceed any allowed-lateness setting and every reading from the line would be
    counted late — which is technically correct and reads as a bug.
    """

    silence_after_min: float | None = None
    """Simulated minutes into the run when this line stops reporting entirely."""

    silence_for_min: float = 0.0
    """How long the silence lasts. 0 with ``silence_after_min`` set = forever."""

    def is_silent(self, elapsed_min: float) -> bool:
        if self.silence_after_min is None or elapsed_min < self.silence_after_min:
            return False
        if self.silence_for_min <= 0:
            return True  # never comes back
        return elapsed_min < self.silence_after_min + self.silence_for_min


@dataclass(frozen=True)
class FeedConfig:
    """The mock's time model.

    Two clocks run at once. Simulated time advances ``time_scale`` seconds for
    every wall-clock second, so an eight-hour shift can be watched in seconds
    without the data pretending sensors report every 30µs. The compression is an
    explicit knob rather than a quiet fiction — a real source ignores it.
    """

    cadence_min: float = 1.0
    """Simulated minutes between one line's consecutive batches."""

    time_scale: float = 6000.0
    """Simulated seconds elapsed per wall-clock second."""

    start: str = "2026-07-20T00:00:00"
    seed: int = 42

    pace: bool = True
    """Sleep to match ``time_scale``. False = emit as fast as possible (tests)."""

    @property
    def wall_interval_s(self) -> float:
        """Wall-clock seconds between a single line's batches."""
        return self.cadence_min * 60.0 / self.time_scale


class MockPlcFeed:
    """A simulated multi-line PLC feed. Stands in for OPC UA / MQTT Sparkplug.

    Emits one batch per line per cadence tick, applying each line's scripted
    faults *as they occur* rather than stamping them onto a finished dataset.

    Faults are rate-based here, not exact-count: a live feed cannot
    know its own denominator in advance, so ``malformed_ratio`` becomes a
    per-reading Bernoulli probability. Still fully seeded, so a given
    ``seed`` replays identically; only the arrival *timing* varies.

    Staleness is modelled as silence — the line stops emitting — rather than as a
    frozen timestamp. That is what actually happens when a batch record hangs,
    and it means :func:`schema.has_stale_batch` detects it at window close with
    no change to that function at all.
    """

    def __init__(self, feeds: Sequence[FeedSpec], config: FeedConfig | None = None) -> None:
        if not feeds:
            raise ValueError("MockPlcFeed needs at least one line")
        self.feeds = list(feeds)
        self.config = config or FeedConfig()
        self._closed = False

    def close(self) -> None:
        self._closed = True

    def _rngs(self) -> list[tuple[np.random.Generator, np.random.Generator]]:
        """One physics RNG and one fault RNG per line.

        Two separations, both load-bearing:

        *Per line* — production lines are physically independent, and a shared
        stream would make one line's behaviour depend on another's. Worse, a line
        going silent would shift every other line's numbers, so the same seed
        would produce different healthy lines depending on whether a *different*
        line broke. Per-line streams make each line reproducible on its own.

        *Physics from faults* — the fault coin-flips would otherwise advance the
        stream that generates downtime and temperature, so a line's output would
        depend on its fault ratios even where no fault fired. Split, a clean
        streamed line is drawn from exactly the sequence
        ``generator.baseline_row`` would produce on its own.
        """
        return [
            (
                np.random.default_rng(self.config.seed + i),
                np.random.default_rng(self.config.seed + 1_000_000 + i),
            )
            for i in range(len(self.feeds))
        ]

    def subscribe(self) -> Iterator[Reading]:
        cfg = self.config
        rngs = self._rngs()
        start = pd.Timestamp(cfg.start)
        cadence = timedelta(minutes=cfg.cadence_min)
        wall_start = time.monotonic()
        tick = 0

        while not self._closed:
            sim_now = start + tick * cadence
            elapsed_min = tick * cfg.cadence_min

            for feed, (physics, faults) in zip(self.feeds, rngs):
                if feed.is_silent(elapsed_min):
                    # A silent line draws nothing, and because its streams are
                    # its own, the other lines are bit-for-bit unaffected.
                    continue
                yield self._emit(feed, tick, sim_now, physics, faults)

            if cfg.pace:
                target = wall_start + (tick + 1) * cfg.wall_interval_s
                delay = target - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
            tick += 1

    def _emit(self, feed: FeedSpec, tick: int, sim_now: pd.Timestamp,
              physics: np.random.Generator, fault_rng: np.random.Generator) -> Reading:
        """Build one reading: baseline physics, then faults, then transport lag."""
        row = generator.baseline_row(feed.line, tick, sim_now, physics)

        # Faults are disjoint per reading, mirroring the batch generator's
        # disjoint index sets: a row is corrupted *or* missing a reading, never
        # both, so the two counts stay independent and readable. Both coins are
        # always flipped, so a line's fault stream doesn't depend on its own
        # outcomes.
        faults = feed.line.faults
        malformed = fault_rng.random() < faults.malformed_ratio
        missing = fault_rng.random() < faults.missing_reading_ratio
        if malformed:
            generator.apply_random_corruption(row, fault_rng)
        elif missing:
            row[schema.TEMPERATURE] = None

        lag_s = feed.lag_s + feed.lag_growth_s * tick
        if feed.lag_max_s is not None:
            lag_s = min(lag_s, feed.lag_max_s)
        event_time = (sim_now - timedelta(seconds=lag_s)).to_pydatetime()

        return Reading(
            event_time=event_time,
            ingest_time=sim_now.to_pydatetime(),
            line_id=row[schema.LINE_ID],
            batch_id=row[schema.BATCH_ID],
            planned_min=row[schema.PLANNED_MIN],
            downtime_min=row[schema.DOWNTIME_MIN],
            ideal_cycle_s=row[schema.IDEAL_CYCLE_S],
            total_count=row[schema.TOTAL_COUNT],
            good_count=row[schema.GOOD_COUNT],
            temperature=row[schema.TEMPERATURE],
        )


# --- The scripted real-time demo --------------------------------------------
# Deliberately the same cast as generator.DEMO_SCENARIO, so `run` and `stream`
# tell one story. What streaming adds is the *transport* failures underneath it:
#   line_1: healthy, on time — the reference.
#   line_2: healthy transport, steady missing readings, mild constant lag.
#   line_3: malformed rows, a gateway falling further behind every batch, and a
#           dead spell partway through -> silence alert, then stale_batch at the
#           window boundary, then the OEE drop the agent gets asked to explain.
DEMO_FEEDS: list[FeedSpec] = [
    FeedSpec(
        line=generator.LineSpec(
            line_id="line_1", n_batches=0, ideal_cycle_s=2.0, temp_mean=68.0
        ),
    ),
    FeedSpec(
        line=generator.LineSpec(
            line_id="line_2",
            n_batches=0,
            faults=generator.FaultSpec(missing_reading_ratio=0.15),
            ideal_cycle_s=2.5,
            downtime_mean=8.0,
            temp_mean=74.0,
        ),
        lag_s=90.0,
    ),
    FeedSpec(
        line=generator.LineSpec(
            line_id="line_3",
            n_batches=0,
            faults=generator.FaultSpec(malformed_ratio=0.12, missing_reading_ratio=0.08),
            ideal_cycle_s=3.0,
            downtime_mean=14.0,
            performance_mean=0.82,
            quality_mean=0.93,
            temp_mean=80.0,
            temp_sd=2.5,
        ),
        lag_s=120.0,
        lag_growth_s=4.0,
        # Plateaus at 30 simulated minutes — a visible ramp on the lag panel,
        # and comfortably inside the default 60-minute allowed lateness, so the
        # backlog never turns into a flood of misfiled rows.
        lag_max_s=1800.0,
        # Silence sized to land *inside* one 8h window (480-960), not across a
        # boundary: an edge-aligned dropout leaves no interior gap, so
        # schema.has_stale_batch cannot see it and only the live watchdog fires.
        # Also long enough to clear the watchdog's 1s wall-clock floor at the
        # default --time-scale and well beyond it.
        silence_after_min=600.0,
        silence_for_min=350.0,
    ),
]
"""``n_batches`` is unused by the feed — a stream has no end — hence 0."""


def demo_feed(config: FeedConfig | None = None) -> MockPlcFeed:
    """The scripted real-time demo feed."""
    return MockPlcFeed(DEMO_FEEDS, config)
