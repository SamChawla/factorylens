"""The ETL pipeline: ingest -> clean -> transform -> aggregate (piece 2).

Plain-Python stages, each a ``DataFrame -> DataFrame`` function that
emits exactly one OTel span. The pipeline runs once per production
line, nesting the four stage spans under a ``pipeline_run`` span tagged
``line_id``.

Every stage span carries the same required attribute set — ``line_id``,
``rows_in``, ``rows_out``, ``rows_dropped``, ``null_ratio``, ``stale_batch`` —
and those exact fields are logged too, so "what happened here" has one source of
truth used twice (structlog + span). This is the surface the Q&A CLI queries.
"""

from __future__ import annotations

import pandas as pd
from opentelemetry.trace import Span, Tracer

from factorylens import schema
from factorylens.exceptions import PipelineStageError
from factorylens.logging import get_logger
from factorylens.oee import OEEResult, compute_oee
from factorylens.telemetry import Telemetry

_log = get_logger("pipeline")

REQUIRED_STAGE_ATTRS = (
    "line_id",
    "rows_in",
    "rows_out",
    "rows_dropped",
    "null_ratio",
    "stale_batch",
)


def _record(span: Span, event: str, fields: dict) -> None:
    """Write the same fields onto the span and the log — one source of truth."""
    for key, value in fields.items():
        span.set_attribute(key, value)
    _log.info(event, **fields)


def _stage_attrs(
    df: pd.DataFrame, line_id: str, *, rows_in: int, rows_dropped: int
) -> dict:
    return {
        "line_id": line_id,
        "rows_in": rows_in,
        "rows_out": len(df),
        "rows_dropped": rows_dropped,
        "null_ratio": round(schema.null_ratio(df), 4),
        "stale_batch": schema.has_stale_batch(df),
    }


def ingest_stage(df: pd.DataFrame, line_id: str, tracer: Tracer) -> pd.DataFrame:
    """Load the raw batches for a line. Nothing dropped yet — just measured."""
    with tracer.start_as_current_span("ingest") as span:
        try:
            df = df.reset_index(drop=True)
            _record(span, "ingest", _stage_attrs(df, line_id, rows_in=len(df), rows_dropped=0))
            return df
        except Exception as e:
            raise PipelineStageError(f"ingest stage failed for {line_id}: {e}") from e


def clean_stage(df: pd.DataFrame, line_id: str, tracer: Tracer) -> pd.DataFrame:
    """Drop malformed rows; measure remaining null_ratio and staleness."""
    with tracer.start_as_current_span("clean") as span:
        try:
            rows_in = len(df)
            mask = schema.malformed_mask(df)
            cleaned = df[~mask].reset_index(drop=True)
            attrs = _stage_attrs(
                cleaned, line_id, rows_in=rows_in, rows_dropped=int(mask.sum())
            )
            _record(span, "clean", attrs)
            return cleaned
        except Exception as e:
            raise PipelineStageError(f"clean stage failed for {line_id}: {e}") from e


def transform_stage(df: pd.DataFrame, line_id: str, tracer: Tracer) -> pd.DataFrame:
    """Coerce numeric dtypes and derive run time — the inputs OEE needs."""
    with tracer.start_as_current_span("transform") as span:
        try:
            rows_in = len(df)
            out = df.copy()
            for col in (
                schema.PLANNED_MIN,
                schema.DOWNTIME_MIN,
                schema.IDEAL_CYCLE_S,
                schema.TOTAL_COUNT,
                schema.GOOD_COUNT,
            ):
                out[col] = pd.to_numeric(out[col], errors="coerce")
            out["run_min"] = (out[schema.PLANNED_MIN] - out[schema.DOWNTIME_MIN]).clip(lower=0)
            _record(span, "transform", _stage_attrs(out, line_id, rows_in=rows_in, rows_dropped=0))
            return out
        except Exception as e:
            raise PipelineStageError(f"transform stage failed for {line_id}: {e}") from e


def aggregate_stage(df: pd.DataFrame, line_id: str, tracer: Tracer) -> OEEResult:
    """Collapse a line's batches into one OEE result and record it on the span."""
    with tracer.start_as_current_span("aggregate") as span:
        try:
            result = compute_oee(df, line_id)
            attrs = _stage_attrs(df, line_id, rows_in=len(df), rows_dropped=0)
            attrs["rows_out"] = 1  # aggregation collapses the line to one row
            attrs.update(
                availability=round(result.availability, 4),
                performance=round(result.performance, 4),
                quality=round(result.quality, 4),
                oee=round(result.oee, 4),
            )
            _record(span, "aggregate", attrs)
            return result
        except Exception as e:
            raise PipelineStageError(f"aggregate stage failed for {line_id}: {e}") from e


def run_line(df: pd.DataFrame, line_id: str, tracer: Tracer) -> OEEResult:
    """Run all four stages for one line under a ``pipeline_run`` parent span."""
    with tracer.start_as_current_span("pipeline_run") as parent:
        parent.set_attribute("line_id", line_id)
        ingested = ingest_stage(df, line_id, tracer)
        cleaned = clean_stage(ingested, line_id, tracer)
        transformed = transform_stage(cleaned, line_id, tracer)
        result = aggregate_stage(transformed, line_id, tracer)
        parent.set_attribute("oee", round(result.oee, 4))
        return result


def run_pipeline(raw: pd.DataFrame, telemetry: Telemetry) -> dict[str, OEEResult]:
    """Run the pipeline for every line in the raw dataset.

    Returns a ``line_id -> OEEResult`` map; spans are emitted as a side effect
    via the telemetry provider.
    """
    tracer = telemetry.tracer()
    results: dict[str, OEEResult] = {}
    for line_id, line_df in raw.groupby(schema.LINE_ID, sort=True):
        results[str(line_id)] = run_line(line_df, str(line_id), tracer)
    return results
