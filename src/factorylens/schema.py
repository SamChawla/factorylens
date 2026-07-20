"""Canonical dataset schema and row-validity rules.

Single source of truth shared by the generator (piece 1) and the pipeline's
clean stage (piece 2), so "what makes a row malformed" is defined exactly once
and the two halves can never drift apart.
"""

from __future__ import annotations

import pandas as pd

# --- Columns of the raw production dataset ---
TS = "ts"  # ISO8601 timestamp of the batch record
LINE_ID = "line_id"
BATCH_ID = "batch_id"
PLANNED_MIN = "planned_min"  # planned production time for the batch window
DOWNTIME_MIN = "downtime_min"  # unplanned downtime within the window
IDEAL_CYCLE_S = "ideal_cycle_s"  # ideal seconds per unit
TOTAL_COUNT = "total_count"  # units produced
GOOD_COUNT = "good_count"  # good (non-defective) units
TEMPERATURE = "temperature"  # a sensor reading; nullable (missing-reading fault)

COLUMNS: list[str] = [
    TS,
    LINE_ID,
    BATCH_ID,
    PLANNED_MIN,
    DOWNTIME_MIN,
    IDEAL_CYCLE_S,
    TOTAL_COUNT,
    GOOD_COUNT,
    TEMPERATURE,
]

# Columns whose null-ness the clean stage tracks as null_ratio.
NULLABLE_QUALITY_COLUMNS: list[str] = [TEMPERATURE]

# The three production lines in the demo scenario.
LINE_IDS: tuple[str, str, str] = ("line_1", "line_2", "line_3")

# A batch is "stale" if its timestamp lags the line's newest reading by more
# than this — i.e. a record that stopped updating while the line moved on.
STALE_GAP = pd.Timedelta(hours=24)


def malformed_mask(df: pd.DataFrame) -> pd.Series:
    """Boolean mask: True where a row is structurally invalid and must be dropped.

    A row is malformed if it breaks a hard data-integrity rule that no clean
    downstream calculation could tolerate:
      - counts are negative, or
      - good_count exceeds total_count (impossible), or
      - planned_min is non-positive, or
      - batch_id / line_id is blank, or
      - a required numeric field is non-numeric / non-finite.

    Missing sensor readings (null temperature) are NOT malformed — those are
    counted as null_ratio and the row is kept.
    """
    numeric_cols = [PLANNED_MIN, DOWNTIME_MIN, IDEAL_CYCLE_S, TOTAL_COUNT, GOOD_COUNT]
    coerced = {c: pd.to_numeric(df[c], errors="coerce") for c in numeric_cols}

    non_numeric = pd.Series(False, index=df.index)
    for c in numeric_cols:
        non_numeric |= coerced[c].isna()

    total = coerced[TOTAL_COUNT]
    good = coerced[GOOD_COUNT]
    planned = coerced[PLANNED_MIN]

    negative = (total < 0) | (good < 0)
    good_gt_total = good > total
    bad_planned = planned <= 0

    blank_id = (
        df[BATCH_ID].astype("string").fillna("").str.strip().eq("")
        | df[LINE_ID].astype("string").fillna("").str.strip().eq("")
    )

    mask = non_numeric | negative | good_gt_total | bad_planned | blank_id
    return mask.fillna(True)


def null_ratio(df: pd.DataFrame) -> float:
    """Fraction of null cells across the nullable quality columns.

    This is the missing-reading signal (null ``temperature``), reported per
    stage. Missing readings are kept, not dropped — so this is measured on the
    rows that actually flow downstream.
    """
    if len(df) == 0:
        return 0.0
    cols = NULLABLE_QUALITY_COLUMNS
    total_cells = len(df) * len(cols)
    if total_cells == 0:
        return 0.0
    nulls = int(df[cols].isna().to_numpy().sum())
    return nulls / total_cells


def has_stale_batch(df: pd.DataFrame) -> bool:
    """True if any batch's timestamp lags the line's newest one by > STALE_GAP."""
    if len(df) == 0:
        return False
    ts = pd.to_datetime(df[TS], errors="coerce")
    if ts.notna().sum() == 0:
        return False
    newest = ts.max()
    return bool((ts < (newest - STALE_GAP)).any())
