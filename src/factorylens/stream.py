"""The streaming runtime: readings in, windowed pipeline runs and alerts out.

Piece 6. This is the layer that lets a live feed drive the *existing* batch
pipeline. It answers the two questions a real-time system has to answer, and
they need different machinery:

  **When do I compute?**  On a cadence. Readings accumulate into tumbling
  event-time windows; a window closes when the watermark passes its end, and the
  closed window runs through :func:`pipeline.run_pipeline` completely unchanged.
  This is the steady-state path and it produces exactly the spans SigNoz and the
  Q&A agent already understand.

  **When do I shout?**  On a condition, immediately, without waiting for the
  window. Three triggers: a temperature threshold breach, a malformed row caught
  at the door, and a line going silent. The last one is the interesting one — it
  fires on the *absence* of data, so it cannot be a message handler; it needs a
  wall-clock timer.

Event time vs processing time is the distinction that makes the rest coherent.
Windows close on event time (so their contents are reproducible regardless of
how fast the wall clock ran); the silence watchdog runs on processing time (so a
dead line is noticed even though, by definition, no event arrives to notice it
with).
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable

from opentelemetry.trace import Tracer

from factorylens import schema
from factorylens.logging import get_logger
from factorylens.oee import OEEResult
from factorylens.pipeline import run_pipeline
from factorylens.sources import Reading, SensorSource, readings_to_frame
from factorylens.telemetry import Telemetry

_log = get_logger("stream")

# Alert kinds. Stable strings — they become span attributes and metric labels.
ALERT_THRESHOLD = "threshold_breach"
ALERT_MALFORMED = "malformed_reading"
ALERT_SILENCE = "line_silent"
ALERT_LATE = "late_reading"


@dataclass(frozen=True)
class StreamConfig:
    """Windowing, triggers, and the pacing of the run."""

    window_min: float = 480.0
    """Tumbling window width in simulated minutes. 480 = an 8-hour shift."""

    allowed_lateness_min: float = 60.0
    """How long a window stays open past its end, waiting for laggy feeds.

    A line whose gateway is 20 simulated minutes behind still lands inside its
    own window; one that is further behind than this is counted late and its
    reading is reported rather than silently folded into the wrong window.
    """

    temp_max: float = 85.0
    """Temperature above which a reading trips a threshold alert."""

    silence_factor: float = schema.STALE_GAP_FACTOR
    """Multiples of the expected arrival interval before a line reads as silent.

    Shares :data:`schema.STALE_GAP_FACTOR` with the batch staleness detector on
    purpose: "stopped reporting" means the same thing in both modes.
    """

    min_silence_s: float = 1.0
    """Floor on the silence threshold, in wall-clock seconds.

    Under heavy time compression the expected interval is milliseconds, and a GC
    pause would otherwise read as a dead production line.
    """

    alert_cooldown_s: float = 2.0
    """Per (line, kind) alert suppression window. Real alerting deduplicates;
    at streaming rates, one span per malformed row would be unreadable and
    expensive. Suppressed occurrences are counted onto the next span."""


@dataclass(frozen=True)
class Alert:
    """A condition trigger that fired. Becomes one span and one metric point."""

    kind: str
    line_id: str
    detail: str
    value: float | None = None
    suppressed: int = 0
    """How many further occurrences were folded into this one by the cooldown."""


@dataclass
class Window:
    """A tumbling event-time window of readings for all lines."""

    index: int
    start: datetime
    end: datetime
    readings: list[Reading] = field(default_factory=list)
    late: int = 0

    @property
    def line_ids(self) -> set[str]:
        return {r.line_id for r in self.readings}


def aligned_origin(first_event: datetime, width_min: float) -> datetime:
    """Floor ``first_event`` to a window boundary on the calendar grid.

    Anchoring windows to whichever packet happened to arrive first would make
    boundaries arbitrary and irreproducible — the same data replayed from a
    different starting point would aggregate differently. Aligning to midnight
    means an 8-hour window is an actual shift (00:00, 08:00, 16:00), which is
    also how a plant reports its numbers.

    One consequence is honest and unavoidable: lines whose transport lags behind
    the first arrival can produce readings *before* this origin, landing in a
    partial leading window. Every streaming system has that ragged edge at
    startup; it is visible here rather than quietly folded into window 0.
    """
    day = first_event.replace(hour=0, minute=0, second=0, microsecond=0)
    elapsed_min = (first_event - day).total_seconds() / 60.0
    return day + timedelta(minutes=math.floor(elapsed_min / width_min) * width_min)


class WindowBuffer:
    """Accumulates readings into tumbling event-time windows.

    Holds several windows open at once, because lines lag by different amounts:
    line_1 may already be reporting into window 4 while line_3's gateway is still
    draining window 3. Closing on the first line to cross a boundary would
    systematically truncate the laggiest — and therefore most interesting — line.

    A window closes when the watermark (highest event time seen, minus the
    allowed lateness) passes its end. Readings for an already-closed window are
    counted late and reported, never silently misfiled.
    """

    def __init__(self, origin: datetime, width_min: float, allowed_lateness_min: float) -> None:
        if width_min <= 0:
            raise ValueError("window width must be positive")
        self._origin = origin
        self._width = timedelta(minutes=width_min)
        self._lateness = timedelta(minutes=allowed_lateness_min)
        self._open: dict[int, Window] = {}
        self._closed_through: int | None = None
        self._max_event_time: datetime | None = None
        self.late_total = 0

    def _index_of(self, event_time: datetime) -> int:
        # floor, not int(): lines lag by different amounts, so a reading can
        # legitimately predate the origin (whichever line happened to arrive
        # first). int() truncates toward zero and would fold window -1 into 0.
        return math.floor((event_time - self._origin) / self._width)

    def add(self, reading: Reading) -> list[Window]:
        """Buffer a reading; return any windows the new watermark closed."""
        idx = self._index_of(reading.event_time)

        if self._closed_through is not None and idx <= self._closed_through:
            # Arrived after its window was already computed. Attribute it to the
            # oldest window still open so the data is not lost, and count it.
            self.late_total += 1
            if self._open:
                target = self._open[min(self._open)]
                target.readings.append(reading)
                target.late += 1
            return []

        window = self._open.get(idx)
        if window is None:
            start = self._origin + idx * self._width
            window = Window(index=idx, start=start, end=start + self._width)
            self._open[idx] = window
        window.readings.append(reading)

        if self._max_event_time is None or reading.event_time > self._max_event_time:
            self._max_event_time = reading.event_time
        return self._close_ready()

    def _close_ready(self) -> list[Window]:
        if self._max_event_time is None:
            return []
        watermark = self._max_event_time - self._lateness
        ready = sorted(i for i, w in self._open.items() if w.end <= watermark)
        closed = []
        for idx in ready:
            closed.append(self._open.pop(idx))
            self._closed_through = idx if self._closed_through is None else max(
                self._closed_through, idx
            )
        return closed

    def drain(self) -> list[Window]:
        """Close every remaining window. Called at end of run."""
        remaining = [self._open[i] for i in sorted(self._open)]
        self._open.clear()
        if remaining:
            last = remaining[-1].index
            self._closed_through = last if self._closed_through is None else max(
                self._closed_through, last
            )
        return remaining


class SilenceWatchdog:
    """Fires when a line stops reporting. Runs on processing time, not event time.

    This is the detector that has no batch-mode equivalent: it is triggered by
    nothing happening, so there is no reading to hang it off. It is also what
    makes ``stale_batch`` a real detection rather than a scripted flag — the
    silence leaves a genuine hole in the next window, which
    :func:`schema.has_stale_batch` then finds on its own.
    """

    def __init__(self, threshold_s: float) -> None:
        self.threshold_s = threshold_s
        self._last_seen: dict[str, float] = {}
        self._firing: set[str] = set()

    def saw(self, line_id: str, now: float) -> str | None:
        """Record a reading. Returns the line id if this ended a silent spell."""
        self._last_seen[line_id] = now
        if line_id in self._firing:
            self._firing.discard(line_id)
            return line_id
        return None

    def check(self, now: float) -> list[tuple[str, float]]:
        """Return (line_id, silent_seconds) for lines newly gone quiet."""
        newly = []
        for line_id, last in self._last_seen.items():
            quiet = now - last
            if quiet > self.threshold_s and line_id not in self._firing:
                self._firing.add(line_id)
                newly.append((line_id, quiet))
        return newly


class _AlertGate:
    """Per (line, kind) cooldown, counting what it suppressed."""

    def __init__(self, cooldown_s: float) -> None:
        self._cooldown = cooldown_s
        self._last: dict[tuple[str, str], float] = {}
        self._suppressed: dict[tuple[str, str], int] = {}

    def admit(self, kind: str, line_id: str, now: float) -> int | None:
        """Return suppressed-count if the alert should fire, else None."""
        key = (kind, line_id)
        last = self._last.get(key)
        if last is not None and now - last < self._cooldown:
            self._suppressed[key] = self._suppressed.get(key, 0) + 1
            return None
        self._last[key] = now
        return self._suppressed.pop(key, 0)

    def pending(self) -> list[tuple[str, str, int]]:
        """Drain (kind, line_id, count) for suppressed alerts never reported.

        A burst that stops before the cooldown expires would otherwise vanish:
        the last N occurrences were counted against an alert that never fired.
        Flushed at end of run so the totals are honest.
        """
        out = [(kind, line, n) for (kind, line), n in self._suppressed.items() if n]
        self._suppressed.clear()
        return out


@dataclass
class StreamStats:
    """What a run did. Returned to the CLI for the summary table."""

    readings: int = 0
    windows: int = 0
    late: int = 0
    alerts: dict[str, int] = field(default_factory=dict)
    last_results: dict[str, OEEResult] = field(default_factory=dict)
    max_lag_s: dict[str, float] = field(default_factory=dict)


class StreamRunner:
    """Drives a :class:`SensorSource` through windowing, triggers, and telemetry.

    Telemetry split: per-reading signals are **metrics**, per-window
    work is **spans**. One span per reading would be both unreadable and, on
    SigNoz Cloud, genuinely expensive at three lines × hundreds of readings a
    second — while telling you nothing a histogram doesn't.
    """

    def __init__(self, telemetry: Telemetry, config: StreamConfig | None = None) -> None:
        self.telemetry = telemetry
        self.config = config or StreamConfig()
        self.stats = StreamStats()

        meter = telemetry.meter()
        self._m_readings = meter.create_counter(
            "factorylens.readings.received",
            unit="1",
            description="Batch readings received from the sensor feed.",
        )
        self._m_lag = meter.create_histogram(
            "factorylens.ingest.lag_ms",
            unit="ms",
            description="Gap between a reading's event time and its arrival.",
        )
        self._m_temp = meter.create_gauge(
            "factorylens.sensor.temperature",
            unit="Cel",
            description="Most recent temperature reading per line.",
        )
        self._m_alerts = meter.create_counter(
            "factorylens.alerts.fired",
            unit="1",
            description="Condition triggers fired, by kind.",
        )
        self._m_window_rows = meter.create_histogram(
            "factorylens.window.rows",
            unit="1",
            description="Readings per closed window.",
        )

    # --- telemetry emission ---------------------------------------------------

    def _emit_alert(self, alert: Alert) -> None:
        tracer = self.telemetry.tracer()
        with tracer.start_as_current_span("alert") as span:
            span.set_attribute("alert.kind", alert.kind)
            span.set_attribute("line_id", alert.line_id)
            span.set_attribute("alert.detail", alert.detail)
            span.set_attribute("alert.suppressed", alert.suppressed)
            if alert.value is not None:
                span.set_attribute("alert.value", alert.value)
        self._m_alerts.add(1, {"line_id": alert.line_id, "alert.kind": alert.kind})
        self.stats.alerts[alert.kind] = self.stats.alerts.get(alert.kind, 0) + 1
        _log.info(
            "alert",
            kind=alert.kind,
            line_id=alert.line_id,
            detail=alert.detail,
            suppressed=alert.suppressed,
        )

    def _record_reading(self, reading: Reading) -> None:
        labels = {"line_id": reading.line_id}
        self._m_readings.add(1, labels)
        lag_ms = reading.lag.total_seconds() * 1000.0
        self._m_lag.record(lag_ms, labels)
        if reading.temperature is not None:
            self._m_temp.set(reading.temperature, labels)
        prev = self.stats.max_lag_s.get(reading.line_id, 0.0)
        self.stats.max_lag_s[reading.line_id] = max(prev, lag_ms / 1000.0)

    def _process_window(self, window: Window, tracer: Tracer) -> dict[str, OEEResult]:
        """Run one closed window through the unchanged batch pipeline.

        The pipeline runs *inside* a ``stream_window`` span, so the four stage
        spans and their ``pipeline_run`` parent keep the exact names, attributes
        and nesting the dashboards and the Q&A agent already read — the window
        simply becomes their ancestor, and gives a judge a trace waterfall that
        shows where a window's time actually went.
        """
        frame = readings_to_frame(window.readings)
        with tracer.start_as_current_span("stream_window") as span:
            span.set_attribute("window.index", window.index)
            span.set_attribute("window.start", window.start.isoformat())
            span.set_attribute("window.end", window.end.isoformat())
            span.set_attribute("window.rows", len(window.readings))
            span.set_attribute("window.late_rows", window.late)
            span.set_attribute("window.lines", sorted(window.line_ids))
            results = run_pipeline(frame, self.telemetry)
        self._m_window_rows.record(len(window.readings), {"window.index": window.index})
        self.stats.windows += 1
        self.stats.last_results = results
        _log.info(
            "window_closed",
            index=window.index,
            rows=len(window.readings),
            late=window.late,
            lines=sorted(window.line_ids),
        )
        return results

    # --- the loop -------------------------------------------------------------

    def run(
        self,
        source: SensorSource,
        *,
        max_readings: int | None = None,
        duration_s: float | None = None,
        silence_threshold_s: float = 1.0,
        on_window: Callable[[Window, dict[str, OEEResult]], None] | None = None,
        on_alert: Callable[[Alert], None] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> StreamStats:
        """Consume ``source`` until a stop condition, emitting spans and metrics.

        Stops on whichever of ``max_readings`` / ``duration_s`` arrives first;
        with neither, runs until the source is exhausted. Windows still open at
        that point are drained and processed, so a run never silently discards
        the readings it already paid to collect.

        ``clock`` is the processing-time source for the silence watchdog and the
        alert cooldown. It is injectable because an unpaced feed
        (``FeedConfig.pace=False``) compresses a whole shift into milliseconds of
        wall time, where every line looks permanently healthy and no cooldown
        ever expires — so tests supply a clock that advances with the data.
        """
        cfg = self.config
        tracer = self.telemetry.tracer()
        gate = _AlertGate(cfg.alert_cooldown_s)
        watchdog = SilenceWatchdog(max(silence_threshold_s, cfg.min_silence_s))
        buffer: WindowBuffer | None = None
        started = clock()

        def fire(kind: str, line_id: str, detail: str, value: float | None = None) -> None:
            suppressed = gate.admit(kind, line_id, clock())
            if suppressed is None:
                return
            alert = Alert(kind=kind, line_id=line_id, detail=detail,
                          value=value, suppressed=suppressed)
            self._emit_alert(alert)
            if on_alert is not None:
                on_alert(alert)

        readings = source.subscribe()
        try:
            for reading in readings:
                now = clock()
                self.stats.readings += 1
                self._record_reading(reading)

                if buffer is None:
                    buffer = WindowBuffer(
                        origin=aligned_origin(reading.event_time, cfg.window_min),
                        width_min=cfg.window_min,
                        allowed_lateness_min=cfg.allowed_lateness_min,
                    )

                recovered = watchdog.saw(reading.line_id, now)
                if recovered is not None:
                    fire(ALERT_SILENCE, recovered, "line resumed reporting")

                if reading.temperature is not None and reading.temperature > cfg.temp_max:
                    fire(
                        ALERT_THRESHOLD, reading.line_id,
                        f"temperature {reading.temperature:.1f} > {cfg.temp_max:.1f}",
                        reading.temperature,
                    )

                if schema.is_malformed_row(reading.as_row()):
                    # One corruption kind blanks the batch id, so name it rather
                    # than printing a hole where an identifier should be.
                    batch = reading.batch_id.strip() or "<blank batch_id>"
                    fire(
                        ALERT_MALFORMED, reading.line_id,
                        f"malformed reading {batch} rejected at ingest",
                    )

                for window in buffer.add(reading):
                    results = self._process_window(window, tracer)
                    if on_window is not None:
                        on_window(window, results)

                for line_id, quiet_s in watchdog.check(now):
                    fire(
                        ALERT_SILENCE, line_id,
                        f"no reading for {quiet_s:.1f}s "
                        f"(> {watchdog.threshold_s:.1f}s threshold)",
                        quiet_s,
                    )

                if max_readings is not None and self.stats.readings >= max_readings:
                    break
                if duration_s is not None and now - started >= duration_s:
                    break
        finally:
            source.close()
            # Generators need an explicit close to run their finally blocks; a
            # queue-draining iterator from a real client may not have one.
            closer = getattr(readings, "close", None)
            if callable(closer):
                closer()

        if buffer is not None:
            self.stats.late = buffer.late_total
            for window in buffer.drain():
                results = self._process_window(window, tracer)
                if on_window is not None:
                    on_window(window, results)

        for kind, line_id, count in gate.pending():
            alert = Alert(
                kind=kind, line_id=line_id,
                detail=f"{count} further occurrence(s) suppressed by cooldown",
                suppressed=count,
            )
            self._emit_alert(alert)
            if on_alert is not None:
                on_alert(alert)

        return self.stats


def expected_silence_threshold_s(wall_interval_s: float, config: StreamConfig) -> float:
    """Wall-clock silence threshold for a feed arriving every ``wall_interval_s``."""
    return max(config.silence_factor * wall_interval_s, config.min_silence_s)
