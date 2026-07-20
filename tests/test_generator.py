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


def test_demo_scenario_is_reproducible_and_covers_every_fault():
    a = generate(DEMO_SCENARIO)
    b = generate(DEMO_SCENARIO)
    pd.testing.assert_frame_equal(a, b)

    assert set(a[schema.LINE_ID].unique()) == set(schema.LINE_IDS)
    # Each scripted fault kind shows up somewhere in the demo dataset.
    assert a[schema.TEMPERATURE].isna().sum() > 0  # missing readings
    assert schema.malformed_mask(a).sum() > 0  # malformed rows
    assert pd.to_datetime(a[schema.TS]).min() < pd.Timestamp(DEMO_SCENARIO.start)  # stale
