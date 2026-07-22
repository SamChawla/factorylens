"""Tests for the synthetic data generator (piece 1).

The demo's whole value is a *repeatable* story: same scenario in, same faulty
data out, with exactly the faults we scripted. These tests hold that line.
"""

from __future__ import annotations

import pandas as pd
import pytest

from factorylens import schema
from factorylens.generator import (
    DEMO_SCENARIO,
    FaultSpec,
    LineSpec,
    Scenario,
    degrading_scenario,
    generate,
)


def _scenario(faults: FaultSpec, n_batches: int = 40) -> Scenario:
    """One-line scenario with a fixed seed, for isolating a single fault kind."""
    return Scenario(
        lines=[LineSpec(line_id="line_1", n_batches=n_batches, faults=faults)],
        seed=7,
        start="2026-07-20T00:00:00",
        interval_min=60,
        stale_age_min=1440,
    )


def test_generate_is_reproducible():
    scenario = _scenario(FaultSpec(missing_reading_ratio=0.2, malformed_ratio=0.1))
    a = generate(scenario)
    b = generate(scenario)
    pd.testing.assert_frame_equal(a, b)


def test_columns_are_exactly_the_schema():
    df = generate(_scenario(FaultSpec()))
    assert list(df.columns) == schema.COLUMNS


def test_row_count_matches_requested_batches():
    df = generate(_scenario(FaultSpec(), n_batches=30))
    assert len(df) == 30


def test_clean_line_has_no_faults():
    df = generate(_scenario(FaultSpec()))
    assert df[schema.TEMPERATURE].isna().sum() == 0
    assert schema.malformed_mask(df).sum() == 0
    # good_count never exceeds total_count on clean data
    assert (df[schema.GOOD_COUNT] <= df[schema.TOTAL_COUNT]).all()
    assert (df[schema.TOTAL_COUNT] >= 0).all()


@pytest.mark.parametrize("ratio,n", [(0.0, 40), (0.25, 40), (0.5, 20)])
def test_missing_reading_count_is_exact(ratio: float, n: int):
    df = generate(_scenario(FaultSpec(missing_reading_ratio=ratio), n_batches=n))
    assert df[schema.TEMPERATURE].isna().sum() == round(ratio * n)


@pytest.mark.parametrize("ratio,n", [(0.0, 40), (0.1, 40), (0.25, 20)])
def test_malformed_count_is_exact(ratio: float, n: int):
    df = generate(_scenario(FaultSpec(malformed_ratio=ratio), n_batches=n))
    assert schema.malformed_mask(df).sum() == round(ratio * n)


def test_missing_and_malformed_do_not_overlap():
    # A malformed row must not also be counted as a missing reading, so the two
    # injected counts stay independent and exact.
    df = generate(
        _scenario(FaultSpec(missing_reading_ratio=0.3, malformed_ratio=0.3), n_batches=40)
    )
    malformed = schema.malformed_mask(df)
    missing = df[schema.TEMPERATURE].isna()
    assert (malformed & missing).sum() == 0
    assert malformed.sum() == round(0.3 * 40)
    assert missing.sum() == round(0.3 * 40)


def test_stale_batch_produces_an_old_timestamp():
    start = pd.Timestamp("2026-07-20T00:00:00")
    stale = generate(_scenario(FaultSpec(stale_batch=True)))
    fresh = generate(_scenario(FaultSpec(stale_batch=False)))
    assert pd.to_datetime(stale[schema.TS]).min() < start
    assert pd.to_datetime(fresh[schema.TS]).min() >= start


def test_stale_batch_survives_cleaning_even_with_heavy_malformed_injection():
    # Regression: the stale row must never be chosen as a malformed row, or
    # cleaning drops it and the stale_batch signal vanishes at random.
    df = generate(
        _scenario(FaultSpec(malformed_ratio=0.5, stale_batch=True), n_batches=20)
    )
    surviving = df[~schema.malformed_mask(df)]
    assert schema.has_stale_batch(surviving), "stale signal lost during cleaning"


@pytest.mark.parametrize("run_index", range(8))
def test_degrading_scenario_keeps_stale_visible_after_cleaning(run_index):
    scenario = degrading_scenario(run_index, 8)
    line_3 = next(l for l in scenario.lines if l.line_id == "line_3")
    df = generate(scenario)
    line_3_rows = df[df[schema.LINE_ID] == "line_3"]
    surviving = line_3_rows[~schema.malformed_mask(line_3_rows)]
    # Whatever the generator scripted is what the clean stage should report.
    assert schema.has_stale_batch(surviving) == line_3.faults.stale_batch


def test_degrading_scenario_worsens_line_3_monotonically():
    total = 8
    faults = [
        next(l for l in degrading_scenario(i, total).lines if l.line_id == "line_3").faults
        for i in range(total)
    ]
    malformed = [f.malformed_ratio for f in faults]
    missing = [f.missing_reading_ratio for f in faults]
    assert malformed == sorted(malformed) and malformed[-1] > malformed[0]
    assert missing == sorted(missing) and missing[-1] > missing[0]
    # A batch goes stale from the halfway point onward, and stays stale.
    assert not faults[0].stale_batch
    assert faults[-1].stale_batch


def test_degrading_scenario_leaves_reference_lines_alone():
    first = degrading_scenario(0, 6)
    last = degrading_scenario(5, 6)
    for line_id in ("line_1", "line_2"):
        a = next(l for l in first.lines if l.line_id == line_id)
        b = next(l for l in last.lines if l.line_id == line_id)
        assert a.faults == b.faults  # only line_3 degrades


def test_degrading_scenario_is_deterministic():
    a = generate(degrading_scenario(3, 10))
    b = generate(degrading_scenario(3, 10))
    pd.testing.assert_frame_equal(a, b)


def test_degrading_scenario_single_run_is_the_healthy_baseline():
    only = degrading_scenario(0, 1)
    line_3 = next(l for l in only.lines if l.line_id == "line_3")
    assert not line_3.faults.stale_batch
    assert line_3.faults.malformed_ratio < 0.1


def test_demo_scenario_is_reproducible_and_covers_every_fault():
    a = generate(DEMO_SCENARIO)
    b = generate(DEMO_SCENARIO)
    pd.testing.assert_frame_equal(a, b)

    assert set(a[schema.LINE_ID].unique()) == set(schema.LINE_IDS)
    # Each scripted fault kind shows up somewhere in the demo dataset.
    assert a[schema.TEMPERATURE].isna().sum() > 0  # missing readings
    assert schema.malformed_mask(a).sum() > 0  # malformed rows
    assert pd.to_datetime(a[schema.TS]).min() < pd.Timestamp(DEMO_SCENARIO.start)  # stale
