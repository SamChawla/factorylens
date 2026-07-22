"""Synthetic multi-line manufacturing data generator with scripted fault injection.

This is piece 1 of the build. It produces a *raw* production dataset — the kind
of messy CSV an ETL job would actually ingest — with three fault kinds injected
deterministically:

  - missing readings  -> null ``temperature`` on selected rows
  - malformed rows     -> integrity-breaking values the clean stage must drop
  - stale batches      -> a batch record whose timestamp never advanced

Reproducible by construction: same ``Scenario`` (same seed) always yields the
identical DataFrame, so the demo tells the same story every run.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from factorylens import schema


# Batches each line reports per pipeline run. Sized so the pipeline processes a
# realistic volume: stage durations land well clear of OS clock granularity, so
# the SigNoz duration panel shows real work rather than timer noise.
BATCHES_PER_LINE = 2000


@dataclass(frozen=True)
class FaultSpec:
    """Which faults to inject into a line, and how much.

    Ratios are fractions of the line's batches; the injected count is
    ``round(ratio * n_batches)`` — exact and reproducible, not probabilistic.
    """

    missing_reading_ratio: float = 0.0
    malformed_ratio: float = 0.0
    stale_batch: bool = False


@dataclass(frozen=True)
class LineSpec:
    """A production line's baseline behaviour plus its scripted faults."""

    line_id: str
    n_batches: int
    faults: FaultSpec = field(default_factory=FaultSpec)
    planned_min: float = 60.0
    ideal_cycle_s: float = 2.0
    downtime_mean: float = 5.0
    downtime_sd: float = 2.0
    performance_mean: float = 0.90
    performance_sd: float = 0.04
    quality_mean: float = 0.97
    quality_sd: float = 0.015
    temp_mean: float = 68.0
    temp_sd: float = 1.5


@dataclass(frozen=True)
class Scenario:
    """A full run: several lines, a seed, and a time grid."""

    lines: list[LineSpec]
    seed: int = 42
    start: str = "2026-07-20T00:00:00"
    interval_min: int = 60
    stale_age_min: int = 1440  # how far in the past a stale batch's ts is stuck


# Corruption strategies for a malformed row. Each breaks exactly one integrity
# rule enforced by schema.malformed_mask, mirroring how real ingests go wrong.
def _corrupt_negative_total(row: dict) -> None:
    row[schema.TOTAL_COUNT] = -1


def _corrupt_good_gt_total(row: dict) -> None:
    row[schema.GOOD_COUNT] = row[schema.TOTAL_COUNT] + 25


def _corrupt_blank_batch(row: dict) -> None:
    row[schema.BATCH_ID] = ""


def _corrupt_zero_planned(row: dict) -> None:
    row[schema.PLANNED_MIN] = 0.0


def _corrupt_non_numeric(row: dict) -> None:
    row[schema.TOTAL_COUNT] = "ERR"


_CORRUPTIONS = (
    _corrupt_negative_total,
    _corrupt_good_gt_total,
    _corrupt_blank_batch,
    _corrupt_zero_planned,
    _corrupt_non_numeric,
)


def _baseline_rows(spec: LineSpec, start: pd.Timestamp, interval_min: int,
                   rng: np.random.Generator) -> list[dict]:
    """Generate a line's clean baseline batches, before any fault injection."""
    rows: list[dict] = []
    for i in range(spec.n_batches):
        ts = start + pd.Timedelta(minutes=i * interval_min)
        downtime = float(np.clip(
            rng.normal(spec.downtime_mean, spec.downtime_sd), 0.0, spec.planned_min * 0.5
        ))
        run_min = max(spec.planned_min - downtime, 1.0)
        ideal_units = run_min * 60.0 / spec.ideal_cycle_s
        performance = float(np.clip(
            rng.normal(spec.performance_mean, spec.performance_sd), 0.4, 1.0
        ))
        total = int(round(ideal_units * performance))
        quality = float(np.clip(
            rng.normal(spec.quality_mean, spec.quality_sd), 0.5, 1.0
        ))
        good = int(round(total * quality))
        temperature = float(rng.normal(spec.temp_mean, spec.temp_sd))

        rows.append({
            schema.TS: ts.isoformat(),
            schema.LINE_ID: spec.line_id,
            schema.BATCH_ID: f"{spec.line_id}-b{i:03d}",
            schema.PLANNED_MIN: round(spec.planned_min, 2),
            schema.DOWNTIME_MIN: round(downtime, 2),
            schema.IDEAL_CYCLE_S: spec.ideal_cycle_s,
            schema.TOTAL_COUNT: total,
            schema.GOOD_COUNT: good,
            schema.TEMPERATURE: round(temperature, 2),
        })
    return rows


def _inject_faults(rows: list[dict], spec: LineSpec, scenario: Scenario,
                   start: pd.Timestamp, rng: np.random.Generator) -> None:
    """Mutate ``rows`` in place with the line's scripted faults.

    Malformed and missing-reading rows are chosen from *disjoint* index sets so
    the two counts stay exact and independent.
    """
    n = len(rows)
    if n == 0:
        return

    k_malformed = round(spec.faults.malformed_ratio * n)
    k_missing = round(spec.faults.missing_reading_ratio * n)

    all_idx = np.arange(n)
    # A scripted stale batch lives on the last row. Keep that row out of the
    # malformed pool: if cleaning dropped it as malformed, it would also drop
    # the staleness signal, and stale_batch would flicker between runs for no
    # reason the data can explain.
    malformed_pool = all_idx[:-1] if (spec.faults.stale_batch and n > 1) else all_idx
    malformed_idx = rng.choice(
        malformed_pool, size=min(k_malformed, len(malformed_pool)), replace=False
    )
    remaining = np.setdiff1d(all_idx, malformed_idx)
    missing_idx = rng.choice(
        remaining, size=min(k_missing, len(remaining)), replace=False
    )

    for idx in malformed_idx:
        corrupt = _CORRUPTIONS[rng.integers(len(_CORRUPTIONS))]
        corrupt(rows[int(idx)])

    for idx in missing_idx:
        rows[int(idx)][schema.TEMPERATURE] = np.nan

    if spec.faults.stale_batch:
        # The final batch record froze — its timestamp never advanced, and now
        # sits far in the past. The clean stage flags the line as stale.
        stale_ts = start - pd.Timedelta(minutes=scenario.stale_age_min)
        rows[-1][schema.TS] = stale_ts.isoformat()


def generate(scenario: Scenario) -> pd.DataFrame:
    """Build the raw production dataset for a scenario. Deterministic per seed."""
    rng = np.random.default_rng(scenario.seed)
    start = pd.Timestamp(scenario.start)

    frames: list[dict] = []
    for spec in scenario.lines:
        rows = _baseline_rows(spec, start, scenario.interval_min, rng)
        _inject_faults(rows, spec, scenario, start, rng)
        frames.extend(rows)

    df = pd.DataFrame(frames, columns=schema.COLUMNS)
    return df


# --- The scripted demo story --------------------------------------------------
# line_1: healthy reference line.
# line_2: intermittent missing sensor readings (data-quality gap).
# line_3: malformed rows + a stale batch, running hot and slow -> the line whose
#         OEE the Q&A agent gets asked to explain.
DEMO_SCENARIO = Scenario(
    lines=[
        LineSpec(
            line_id="line_1",
            n_batches=BATCHES_PER_LINE,
            ideal_cycle_s=2.0,
            temp_mean=68.0,
        ),
        LineSpec(
            line_id="line_2",
            n_batches=BATCHES_PER_LINE,
            faults=FaultSpec(missing_reading_ratio=0.15),
            ideal_cycle_s=2.5,
            downtime_mean=8.0,
            temp_mean=74.0,
        ),
        LineSpec(
            line_id="line_3",
            n_batches=BATCHES_PER_LINE,
            faults=FaultSpec(malformed_ratio=0.12, stale_batch=True),
            ideal_cycle_s=3.0,
            downtime_mean=14.0,
            performance_mean=0.82,
            quality_mean=0.93,
            temp_mean=80.0,
            temp_sd=2.5,
        ),
    ],
    seed=42,
    start="2026-07-20T00:00:00",
    interval_min=60,
    stale_age_min=1440,
)


def _lerp(a: float, b: float, t: float) -> float:
    return a + (b - a) * t


def degrading_scenario(
    run_index: int, total_runs: int, base: Scenario = DEMO_SCENARIO
) -> Scenario:
    """Scenario for run ``run_index`` of ``total_runs``, with line_3 degrading.

    Used to seed a visible trend across repeated pipeline runs: line_1 stays
    healthy and line_2 keeps its steady data gaps, while line_3's data quality
    and equipment health decay run-over-run — more malformed rows and missing
    readings, more downtime, slower and sloppier output, and a stale batch from
    the halfway point. The result is the demo's causal story: data quality falls,
    then OEE falls, and the spans say exactly why.

    Still deterministic: the seed is derived from the run index, so
    run N of a 12-run seeding is always identical.
    """
    if total_runs < 1:
        raise ValueError("total_runs must be >= 1")
    t = min(max(run_index / (total_runs - 1), 0.0), 1.0) if total_runs > 1 else 0.0

    n_batches = BATCHES_PER_LINE
    # Advance each run's data window so batch timestamps stay continuous.
    start = (
        pd.Timestamp(base.start)
        + pd.Timedelta(minutes=run_index * n_batches * base.interval_min)
    ).isoformat()

    lines = [
        LineSpec(line_id="line_1", n_batches=n_batches, ideal_cycle_s=2.0, temp_mean=68.0),
        LineSpec(
            line_id="line_2",
            n_batches=n_batches,
            faults=FaultSpec(missing_reading_ratio=0.15),
            ideal_cycle_s=2.5,
            downtime_mean=8.0,
            temp_mean=74.0,
        ),
        LineSpec(
            line_id="line_3",
            n_batches=n_batches,
            faults=FaultSpec(
                malformed_ratio=_lerp(0.04, 0.25, t),
                missing_reading_ratio=_lerp(0.0, 0.20, t),
                stale_batch=t >= 0.5,
            ),
            ideal_cycle_s=3.0,
            downtime_mean=_lerp(8.0, 20.0, t),
            performance_mean=_lerp(0.88, 0.72, t),
            quality_mean=_lerp(0.96, 0.88, t),
            temp_mean=_lerp(74.0, 86.0, t),
            temp_sd=2.5,
        ),
    ]
    return Scenario(
        lines=lines,
        seed=base.seed + run_index * 1000,
        start=start,
        interval_min=base.interval_min,
        stale_age_min=base.stale_age_min,
    )
