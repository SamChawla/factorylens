"""Tests for the OEE calculation (piece 2).

OEE = Availability x Performance x Quality. The known-vector test uses numbers
chosen so each factor is a clean value by hand; the edge cases pin the
degenerate paths (no production, no run time) that must not divide by zero.
"""

from __future__ import annotations

import pandas as pd
import pytest

from factorylens import schema
from factorylens.oee import compute_oee


def _line_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=schema.COLUMNS)


def test_compute_oee_known_vector():
    # planned 100, downtime 20 -> run 80  => Availability = 0.80
    # ideal_cycle 1.0s, total 4800 -> ideal time 80 min = run  => Performance = 1.00
    # good 4560 / 4800                                        => Quality = 0.95
    # OEE = 0.80 * 1.00 * 0.95 = 0.76
    df = _line_df([
        {
            schema.TS: "2026-07-20T00:00:00",
            schema.LINE_ID: "line_x",
            schema.BATCH_ID: "line_x-b000",
            schema.PLANNED_MIN: 100.0,
            schema.DOWNTIME_MIN: 20.0,
            schema.IDEAL_CYCLE_S: 1.0,
            schema.TOTAL_COUNT: 4800,
            schema.GOOD_COUNT: 4560,
            schema.TEMPERATURE: 70.0,
        }
    ])
    r = compute_oee(df, "line_x")
    assert r.availability == pytest.approx(0.80)
    assert r.performance == pytest.approx(1.00)
    assert r.quality == pytest.approx(0.95)
    assert r.oee == pytest.approx(0.76)
    assert r.line_id == "line_x"


def test_oee_aggregates_across_batches():
    # Two identical batches aggregate to the same ratios as one.
    row = {
        schema.TS: "2026-07-20T00:00:00",
        schema.LINE_ID: "line_x",
        schema.BATCH_ID: "line_x-b000",
        schema.PLANNED_MIN: 100.0,
        schema.DOWNTIME_MIN: 20.0,
        schema.IDEAL_CYCLE_S: 1.0,
        schema.TOTAL_COUNT: 4800,
        schema.GOOD_COUNT: 4560,
        schema.TEMPERATURE: 70.0,
    }
    df = _line_df([row, {**row, schema.BATCH_ID: "line_x-b001"}])
    r = compute_oee(df, "line_x")
    assert r.oee == pytest.approx(0.76)
    assert r.total_count == 9600
    assert r.good_count == 9120


def test_oee_zero_production_is_safe():
    df = _line_df([{
        schema.TS: "2026-07-20T00:00:00",
        schema.LINE_ID: "line_x",
        schema.BATCH_ID: "line_x-b000",
        schema.PLANNED_MIN: 100.0,
        schema.DOWNTIME_MIN: 20.0,
        schema.IDEAL_CYCLE_S: 1.0,
        schema.TOTAL_COUNT: 0,
        schema.GOOD_COUNT: 0,
        schema.TEMPERATURE: 70.0,
    }])
    r = compute_oee(df, "line_x")
    assert r.quality == 0.0
    assert r.performance == 0.0
    assert r.oee == 0.0
    assert r.availability == pytest.approx(0.80)  # availability still defined


def test_oee_zero_run_time_is_safe():
    # downtime == planned -> no run time; availability and OEE collapse to 0.
    df = _line_df([{
        schema.TS: "2026-07-20T00:00:00",
        schema.LINE_ID: "line_x",
        schema.BATCH_ID: "line_x-b000",
        schema.PLANNED_MIN: 100.0,
        schema.DOWNTIME_MIN: 100.0,
        schema.IDEAL_CYCLE_S: 1.0,
        schema.TOTAL_COUNT: 4800,
        schema.GOOD_COUNT: 4560,
        schema.TEMPERATURE: 70.0,
    }])
    r = compute_oee(df, "line_x")
    assert r.availability == 0.0
    assert r.performance == 0.0
    assert r.oee == 0.0


@pytest.mark.parametrize("factor", ["availability", "performance", "quality", "oee"])
def test_oee_factors_are_bounded(factor):
    # Even with faster-than-ideal output, factors stay within [0, 1].
    df = _line_df([{
        schema.TS: "2026-07-20T00:00:00",
        schema.LINE_ID: "line_x",
        schema.BATCH_ID: "line_x-b000",
        schema.PLANNED_MIN: 100.0,
        schema.DOWNTIME_MIN: 20.0,
        schema.IDEAL_CYCLE_S: 5.0,  # implausibly slow ideal -> performance would exceed 1
        schema.TOTAL_COUNT: 4800,
        schema.GOOD_COUNT: 4560,
        schema.TEMPERATURE: 70.0,
    }])
    r = compute_oee(df, "line_x")
    assert 0.0 <= getattr(r, factor) <= 1.0
