"""OEE (Overall Equipment Effectiveness) calculation.

    OEE = Availability x Performance x Quality

  - Availability = run time / planned production time
  - Performance  = ideal time for the units made / run time
  - Quality      = good units / total units

Pure and span-free on purpose: the aggregate stage calls this and records the
result on a span, but the math is unit-testable in isolation. Every factor is
clamped to [0, 1] and every division is guarded, so degenerate batches (no
production, no run time) yield 0.0 rather than a crash or a NaN.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from factorylens import schema


@dataclass(frozen=True)
class OEEResult:
    line_id: str
    availability: float
    performance: float
    quality: float
    oee: float
    planned_min: float
    run_min: float
    total_count: int
    good_count: int


def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


def compute_oee(
    df: pd.DataFrame, line_id: str, ideal_cycle_s: float | None = None
) -> OEEResult:
    """Aggregate a single line's cleaned batches into an OEEResult.

    ``ideal_cycle_s`` defaults to the mean of the line's ideal-cycle column.
    Assumes ``df`` holds one line's rows with numeric count/time columns
    (i.e. post-clean, post-transform).
    """
    planned = float(df[schema.PLANNED_MIN].sum())
    downtime = float(df[schema.DOWNTIME_MIN].sum())
    run = planned - downtime
    total = int(df[schema.TOTAL_COUNT].sum())
    good = int(df[schema.GOOD_COUNT].sum())

    if ideal_cycle_s is None:
        ideal_cycle_s = float(df[schema.IDEAL_CYCLE_S].mean()) if len(df) else 0.0

    availability = _clamp01(run / planned) if planned > 0 else 0.0
    ideal_time_min = total * ideal_cycle_s / 60.0
    performance = _clamp01(ideal_time_min / run) if run > 0 else 0.0
    quality = _clamp01(good / total) if total > 0 else 0.0
    oee = availability * performance * quality

    return OEEResult(
        line_id=line_id,
        availability=availability,
        performance=performance,
        quality=quality,
        oee=oee,
        planned_min=planned,
        run_min=max(run, 0.0),
        total_count=total,
        good_count=good,
    )
