"""Tests for the production sensor sources (OPC UA and MQTT).

The OPC UA tests run a **real** asyncua server in-process and subscribe to it
with the real client — no mock in the loop. That is the point: it proves the
adapter against the actual protocol, including that SourceTimestamp survives
into the reading's event_time, which is what makes ingest lag a real measurement
rather than an invented one.

Skips cleanly when the optional extra isn't installed:

    uv sync --extra industrial
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from datetime import datetime, timedelta, timezone

import pytest

from factorylens import schema
from factorylens.industrial import MqttSource, OpcUaSource, OpcUaTagMap, TagAssembler
from factorylens.sources import readings_to_frame

asyncua = pytest.importorskip("asyncua", reason="needs the 'industrial' extra")

ENDPOINT = "opc.tcp://127.0.0.1:48401/factorylens/"

# The batch fields a line publishes, in the order a real one would settle them:
# slow context first, then the fast counters, then the batch id that cuts it.
BATCH_FIELDS = {
    schema.PLANNED_MIN: 60.0,
    schema.DOWNTIME_MIN: 5.0,
    schema.IDEAL_CYCLE_S: 2.0,
    schema.TOTAL_COUNT: 1500,
    schema.GOOD_COUNT: 1455,
    schema.TEMPERATURE: 68.5,
}


# --- TagAssembler: the tags-are-not-rows join --------------------------------


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _fill(asm: TagAssembler, line: str, batch: str, **overrides):
    """Push a full set of tag values, then the batch id that closes the batch."""
    out = None
    for name, value in {**BATCH_FIELDS, **overrides}.items():
        out = asm.update(line, name, value, _now()) or out
    return asm.update(line, schema.BATCH_ID, batch, _now()) or out


def test_assembler_emits_nothing_until_a_batch_closes():
    asm = TagAssembler()
    for name, value in BATCH_FIELDS.items():
        assert asm.update("line_1", name, value, _now()) is None
    # First batch id only opens a batch; there is no previous one to emit.
    assert asm.update("line_1", schema.BATCH_ID, "b001", _now()) is None


def test_assembler_emits_previous_batch_when_the_id_rolls_over():
    asm = TagAssembler()
    _fill(asm, "line_1", "b001")
    reading = asm.update("line_1", schema.BATCH_ID, "b002", _now())
    assert reading is not None
    assert reading.batch_id == "b001"          # the *completed* batch
    assert reading.total_count == 1500
    assert reading.good_count == 1455
    assert reading.line_id == "line_1"


def test_assembled_reading_is_pipeline_shaped():
    asm = TagAssembler()
    _fill(asm, "line_1", "b001")
    reading = asm.update("line_1", schema.BATCH_ID, "b002", _now())
    frame = readings_to_frame([reading])
    assert list(frame.columns) == schema.COLUMNS
    assert not schema.malformed_mask(frame).any()


def test_incomplete_batch_is_discarded_not_defaulted():
    """A wrong OEE is worse than a missing one."""
    asm = TagAssembler()
    asm.update("line_1", schema.TOTAL_COUNT, 100, _now())
    asm.update("line_1", schema.BATCH_ID, "b001", _now())
    assert asm.update("line_1", schema.BATCH_ID, "b002", _now()) is None


def test_lines_are_assembled_independently():
    asm = TagAssembler()
    _fill(asm, "line_1", "b001", **{schema.TOTAL_COUNT: 1000})
    _fill(asm, "line_2", "b001", **{schema.TOTAL_COUNT: 2000})
    r1 = asm.update("line_1", schema.BATCH_ID, "b002", _now())
    r2 = asm.update("line_2", schema.BATCH_ID, "b002", _now())
    assert r1.total_count == 1000 and r1.line_id == "line_1"
    assert r2.total_count == 2000 and r2.line_id == "line_2"


def test_device_timestamp_becomes_event_time_so_lag_is_measurable():
    asm = TagAssembler()
    behind = _now() - timedelta(seconds=90)
    for name, value in BATCH_FIELDS.items():
        asm.update("line_3", name, value, behind)
    asm.update("line_3", schema.BATCH_ID, "b001", behind)
    reading = asm.update("line_3", schema.BATCH_ID, "b002", behind)
    assert reading.event_time == behind
    assert reading.lag.total_seconds() >= 89  # ingest_time stamped on arrival


# --- OPC UA: against a real in-process server --------------------------------


class _Server:
    """A real asyncua server exposing one line's tags, on a background loop."""

    def __init__(self):
        self.nodes: dict[str, str] = {}   # node id -> field name
        self._vars = {}
        self._loop = None
        self._ready = threading.Event()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()
        assert self._ready.wait(30), "OPC UA server did not start"

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=15)

    def write(self, field_name: str, value):
        """Write a tag from the test thread, onto the server's loop."""
        var = self._vars[field_name]
        fut = asyncio.run_coroutine_threadsafe(_write(var, value), self._loop)
        fut.result(timeout=10)

    def _run(self):
        asyncio.run(self._serve())

    async def _serve(self):
        from asyncua import Server

        self._loop = asyncio.get_running_loop()
        server = Server()
        await server.init()
        server.set_endpoint(ENDPOINT)
        idx = await server.register_namespace("factorylens")
        obj = await server.nodes.objects.add_object(idx, "line_1")
        for name, value in {**BATCH_FIELDS, schema.BATCH_ID: "b001"}.items():
            var = await obj.add_variable(idx, name, value)
            await var.set_writable()
            self._vars[name] = var
            self.nodes[var.nodeid.to_string()] = name
        async with server:
            self._ready.set()
            while not self._stop.is_set():
                await asyncio.sleep(0.1)


async def _write(var, value):
    await var.write_value(value)


@pytest.fixture(scope="module")
def opcua_server():
    server = _Server()
    server.start()
    yield server
    server.stop()


def _collect(source, count, timeout=45):
    """Pull up to `count` readings off a source, on a background thread."""
    got, done = [], threading.Event()

    def run():
        for reading in source.subscribe():
            got.append(reading)
            if len(got) >= count:
                break
        done.set()

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return got, done, t


def test_opcua_source_reads_a_real_server_end_to_end(opcua_server):
    """Real server, real client, real subscription — no mock anywhere."""
    source = OpcUaSource(
        ENDPOINT,
        [OpcUaTagMap(line_id="line_1", nodes=opcua_server.nodes)],
        publish_interval_ms=100,
    )
    got, done, _ = _collect(source, 1)
    time.sleep(3)  # let the subscription establish

    # Change the counters, then roll the batch id — that cuts the batch.
    opcua_server.write(schema.TOTAL_COUNT, 1600)
    opcua_server.write(schema.GOOD_COUNT, 1550)
    time.sleep(1)
    opcua_server.write(schema.BATCH_ID, "b002")

    assert done.wait(30), "no reading assembled from the live OPC UA server"
    source.close()

    reading = got[0]
    assert reading.line_id == "line_1"
    assert reading.batch_id == "b001"       # the completed batch
    assert reading.total_count == 1600
    assert reading.good_count == 1550


def test_opcua_reading_carries_the_servers_source_timestamp(opcua_server):
    """SourceTimestamp -> event_time is what makes ingest lag real."""
    source = OpcUaSource(
        ENDPOINT,
        [OpcUaTagMap(line_id="line_1", nodes=opcua_server.nodes)],
        publish_interval_ms=100,
    )
    got, done, _ = _collect(source, 1)
    time.sleep(3)
    opcua_server.write(schema.TOTAL_COUNT, 1700)
    time.sleep(1)
    opcua_server.write(schema.BATCH_ID, "b003")
    assert done.wait(30)
    source.close()

    reading = got[0]
    assert reading.event_time.tzinfo is not None      # OPC UA stamps are UTC-aware
    assert reading.ingest_time >= reading.event_time  # lag is never negative
    assert reading.lag.total_seconds() < 120          # a live server is not stale


def test_opcua_source_survives_a_dead_server():
    """An unreachable endpoint must end the iterator, not hang the caller."""
    source = OpcUaSource(
        "opc.tcp://127.0.0.1:48999/nope/",
        [OpcUaTagMap(line_id="line_1", nodes={"ns=2;i=1": schema.TOTAL_COUNT})],
    )
    readings = list(source.subscribe())   # returns once the connection fails
    assert readings == []
    source.close()


# --- MQTT: the real callback path, no broker required ------------------------


def _publish(source: MqttSource, node: str, metrics: dict, msgtype="DDATA"):
    topic = f"spBv1.0/plant1/{msgtype}/{node}/dev1"
    payload = json.dumps({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "line_id": node,
        "metrics": metrics,
    }).encode()
    source.handle_message(topic, payload)


def test_mqtt_parses_the_sparkplug_topic_namespace():
    assert MqttSource.parse_topic("spBv1.0/plant1/DDATA/line_1/dev1") == ("DDATA", "line_1")
    assert MqttSource.parse_topic("spBv1.0/plant1/NDEATH/line_3") == ("NDEATH", "line_3")
    assert MqttSource.parse_topic("random/topic") is None


def test_mqtt_assembles_a_batch_from_published_metrics():
    source = MqttSource("localhost")
    _publish(source, "line_1", BATCH_FIELDS)
    _publish(source, "line_1", {schema.BATCH_ID: "b001"})
    _publish(source, "line_1", {schema.BATCH_ID: "b002"})
    reading = source._queue.get_nowait()
    assert reading.batch_id == "b001"
    assert reading.total_count == 1500


def test_mqtt_node_death_is_the_silence_signal():
    """Sparkplug's NDEATH is the standard's own 'this line went quiet'."""
    source = MqttSource("localhost")
    _publish(source, "line_3", BATCH_FIELDS)
    _publish(source, "line_3", {schema.BATCH_ID: "b001"})
    source.handle_message("spBv1.0/plant1/NDEATH/line_3", b"")
    assert source.dead_nodes == ["line_3"]
    # The partial batch is flushed rather than lost with the connection.
    assert source._queue.get_nowait().batch_id == "b001"


def test_mqtt_ignores_undecodable_payloads_without_dying():
    source = MqttSource("localhost")
    source.handle_message("spBv1.0/plant1/DDATA/line_1/dev1", b"\x00not json")
    assert source._queue.empty()


def test_mqtt_uses_the_payload_timestamp_as_event_time():
    source = MqttSource("localhost")
    behind = (datetime.now(timezone.utc) - timedelta(seconds=45)).isoformat()
    for batch in ("b001", "b002"):
        topic = "spBv1.0/plant1/DDATA/line_2/dev1"
        source.handle_message(topic, json.dumps({
            "timestamp": behind, "line_id": "line_2",
            "metrics": {**BATCH_FIELDS, schema.BATCH_ID: batch},
        }).encode())
    reading = source._queue.get_nowait()
    assert reading.lag.total_seconds() >= 44


# --- the whole loop: real OPC UA -> the unchanged pipeline -------------------


def test_a_real_opcua_feed_drives_the_pipeline_and_emits_spans(opcua_server):
    """The claim, proven end to end.

    A live OPC UA server feeds OpcUaSource, which feeds StreamRunner, which runs
    the *unchanged* pipeline and emits the same spans SigNoz and the Q&A agent
    already read. Nothing in this path is mocked.
    """
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )

    from factorylens import stream
    from factorylens.config import Settings
    from factorylens.telemetry import setup_telemetry

    capture = InMemorySpanExporter()
    telemetry = setup_telemetry(
        Settings(telemetry_enabled=False),
        exporter=InMemorySpanExporter(),
        capture=capture,
    )
    source = OpcUaSource(
        ENDPOINT,
        [OpcUaTagMap(line_id="line_1", nodes=opcua_server.nodes)],
        publish_interval_ms=100,
    )
    runner = stream.StreamRunner(telemetry, stream.StreamConfig(window_min=480.0))

    stats = {}

    def drive():
        stats["result"] = runner.run(source, max_readings=2, silence_threshold_s=999)

    worker = threading.Thread(target=drive, daemon=True)
    worker.start()
    time.sleep(3)  # subscription establishes

    # Three batch rollovers -> two completed batches reach the pipeline.
    for i, batch in enumerate(("b010", "b011", "b012")):
        opcua_server.write(schema.TOTAL_COUNT, 1500 + i * 10)
        opcua_server.write(schema.GOOD_COUNT, 1450 + i * 10)
        time.sleep(0.8)
        opcua_server.write(schema.BATCH_ID, batch)
        time.sleep(0.8)

    worker.join(timeout=40)
    source.close()
    telemetry.shutdown()

    assert stats.get("result") is not None, "StreamRunner never completed"
    assert stats["result"].readings >= 2

    names = {s.name for s in capture.get_finished_spans()}
    for stage in ("ingest", "clean", "transform", "aggregate", "pipeline_run"):
        assert stage in names, f"{stage} span missing — pipeline did not run"
    assert "stream_window" in names
