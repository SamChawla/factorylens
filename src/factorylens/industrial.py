"""Production sensor sources: OPC UA and MQTT.

These are the real implementations of the ``SensorSource`` protocol — the ones a
plant would actually run. ``MockPlcFeed`` in :mod:`factorylens.sources` stands in
for them during the demo; nothing downstream knows the difference, because all
three yield the same ``Reading`` stream.

Requires the optional extra::

    uv sync --extra industrial

Two things about real plants shape this module.

**Tags are not rows.** A PLC exposes individual tags — a counter, a state word, a
thermocouple — each changing at its own rate. A batch *record* is an assembly of
the latest tag values, cut at a trigger (typically when the batch/work-order
identifier rolls over). :class:`TagAssembler` does that cut, so the rest of
FactoryLens keeps receiving whole ``Reading`` objects.

**Everything is a callback.** Both OPC UA subscriptions and MQTT deliver on a
background thread or event loop, not on the caller's. Each source therefore
pushes into a ``queue.Queue`` and exposes the drain as an iterator — which is
exactly why ``SensorSource.subscribe`` returns ``Iterator[Reading]``. The mock is
a generator; these are queue drains; the protocol fits both without either
pretending to be something it isn't.
"""

from __future__ import annotations

import json
import queue
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterator, Mapping

from factorylens import schema
from factorylens.logging import get_logger
from factorylens.sources import Reading

_log = get_logger("industrial")

# Pushed onto the queue to end a subscribe() loop cleanly.
_SENTINEL = object()

# The batch fields a Reading needs beyond the identifiers. Anything missing when
# the trigger fires is reported rather than silently defaulted.
_REQUIRED = (
    schema.PLANNED_MIN,
    schema.DOWNTIME_MIN,
    schema.IDEAL_CYCLE_S,
    schema.TOTAL_COUNT,
    schema.GOOD_COUNT,
)


@dataclass
class TagAssembler:
    """Turns a stream of individual tag changes into whole batch readings.

    Holds the latest value of every mapped tag per line. When the trigger field
    changes — by default ``batch_id``, i.e. the line started a new batch — the
    values accumulated for the *previous* batch are emitted as one ``Reading``.

    This is the join a real deployment needs and the demo mock skips: fast PLC
    tags (counters, temperatures) have to be aggregated and enriched with slower
    MES context (planned time, ideal cycle, the batch identifier) before OEE
    means anything.
    """

    trigger_field: str = schema.BATCH_ID
    _state: dict[str, dict[str, Any]] = field(default_factory=dict)
    _event_time: dict[str, datetime] = field(default_factory=dict)

    def update(
        self, line_id: str, field_name: str, value: Any, source_time: datetime
    ) -> Reading | None:
        """Record one tag change. Returns a Reading when a batch completes.

        ``source_time`` is the device's own timestamp for the value — OPC UA's
        SourceTimestamp, or the timestamp carried in an MQTT payload. It becomes
        the Reading's ``event_time``, so ingest lag stays measurable.
        """
        line = self._state.setdefault(line_id, {})
        completed: Reading | None = None

        if field_name == self.trigger_field:
            previous = line.get(self.trigger_field)
            # A new batch identifier means the previous batch is finished.
            if previous is not None and previous != value:
                completed = self._emit(line_id, previous)

        line[field_name] = value
        # Track the newest device timestamp seen for this line.
        prior = self._event_time.get(line_id)
        if prior is None or source_time > prior:
            self._event_time[line_id] = source_time
        return completed

    def flush(self, line_id: str) -> Reading | None:
        """Emit whatever has accumulated for a line (end of run, or on NDEATH)."""
        batch_id = self._state.get(line_id, {}).get(self.trigger_field)
        return self._emit(line_id, batch_id) if batch_id is not None else None

    def _emit(self, line_id: str, batch_id: Any) -> Reading | None:
        values = self._state.get(line_id, {})
        missing = [f for f in _REQUIRED if f not in values]
        if missing:
            # Incomplete batches are dropped loudly rather than defaulted into a
            # plausible-looking row — a wrong OEE is worse than a missing one.
            _log.warning(
                "incomplete_batch_discarded",
                line_id=line_id, batch_id=batch_id, missing=missing,
            )
            return None

        event_time = self._event_time.get(line_id) or datetime.now(timezone.utc)
        return Reading(
            event_time=event_time,
            ingest_time=datetime.now(timezone.utc),
            line_id=line_id,
            batch_id=str(batch_id),
            planned_min=values[schema.PLANNED_MIN],
            downtime_min=values[schema.DOWNTIME_MIN],
            ideal_cycle_s=values[schema.IDEAL_CYCLE_S],
            total_count=values[schema.TOTAL_COUNT],
            good_count=values[schema.GOOD_COUNT],
            temperature=values.get(schema.TEMPERATURE),
        )


class _QueueSource:
    """Shared plumbing: a background producer feeding a queue the caller drains."""

    def __init__(self) -> None:
        self._queue: queue.Queue = queue.Queue()
        self._closed = threading.Event()
        self._worker: threading.Thread | None = None

    def close(self) -> None:
        """Stop producing. Safe from another thread, and safe to call twice."""
        if self._closed.is_set():
            return
        self._closed.set()
        self._queue.put(_SENTINEL)

    def subscribe(self) -> Iterator[Reading]:
        """Drain the queue as readings arrive. This *is* the iterator protocol."""
        self._worker = threading.Thread(target=self._produce, daemon=True)
        self._worker.start()
        while True:
            try:
                item = self._queue.get(timeout=0.5)
            except queue.Empty:
                if self._closed.is_set():
                    break
                continue
            if item is _SENTINEL:
                break
            yield item

    def _produce(self) -> None:  # pragma: no cover - overridden
        raise NotImplementedError


@dataclass(frozen=True)
class OpcUaTagMap:
    """Which OPC UA node holds which batch field, for one production line.

    ``nodes`` maps a node id (e.g. ``"ns=2;i=8"``) to a schema field name such as
    ``total_count``. Node ids come from the plant's tag dictionary; nothing here
    is guessed at runtime.
    """

    line_id: str
    nodes: Mapping[str, str]


class OpcUaSource(_QueueSource):
    """Reads a live OPC UA server — the dominant industrial protocol.

    Creates a *subscription* rather than polling: the server samples each node at
    ``sampling_interval_ms`` and pushes changes every ``publish_interval_ms``, so
    an idle line costs no traffic.

    The important detail for this project is the timestamp. Every OPC UA
    ``DataValue`` carries a **SourceTimestamp** (when the device sampled the
    value) and a **ServerTimestamp** (when the server processed it). We take
    SourceTimestamp as the reading's ``event_time`` and stamp ``ingest_time`` on
    arrival — so ``factorylens.ingest.lag_ms`` measures a real gap between the
    machine's clock and ours, not an invented one.

    asyncua is async and ``SensorSource`` is sync, so the client runs on an
    asyncio loop in a daemon thread and hands readings over through the queue.
    """

    def __init__(
        self,
        endpoint: str,
        tag_maps: list[OpcUaTagMap],
        *,
        publish_interval_ms: float = 500.0,
        assembler: TagAssembler | None = None,
        security_string: str | None = None,
        username: str | None = None,
        password: str | None = None,
    ) -> None:
        super().__init__()
        self.endpoint = endpoint
        self.tag_maps = tag_maps
        self.publish_interval_ms = publish_interval_ms
        self.assembler = assembler or TagAssembler()
        # Real plants do not accept anonymous, unencrypted OPC UA. `security_string`
        # is asyncua's standard form:
        #   "Basic256Sha256,SignAndEncrypt,client-cert.der,client-key.pem"
        # Left unset here because the demo server is local and open; set it (and
        # username/password if the server uses user auth) for any real endpoint.
        self.security_string = security_string
        self.username = username
        self.password = password
        # node id -> (line_id, field name), built once from the tag dictionary.
        self._route: dict[str, tuple[str, str]] = {
            node_id: (tm.line_id, field_name)
            for tm in tag_maps
            for node_id, field_name in tm.nodes.items()
        }

    def _on_change(self, node_id: str, value: Any, source_time: datetime) -> None:
        """Called from the asyncio loop for every subscribed tag change."""
        route = self._route.get(node_id)
        if route is None:
            return
        line_id, field_name = route
        reading = self.assembler.update(line_id, field_name, value, source_time)
        if reading is not None:
            self._queue.put(reading)

    def _produce(self) -> None:
        import asyncio

        asyncio.run(self._consume())

    async def _consume(self) -> None:
        import asyncio

        from asyncua import Client

        source = self

        class _Handler:
            """asyncua calls this back on every data change."""

            def datachange_notification(self, node, val, data) -> None:
                monitored = getattr(data, "monitored_item", None)
                dv = getattr(monitored, "Value", None) if monitored else None
                stamp = getattr(dv, "SourceTimestamp", None) if dv else None
                if stamp is None:
                    stamp = datetime.now(timezone.utc)
                elif stamp.tzinfo is None:
                    stamp = stamp.replace(tzinfo=timezone.utc)
                source._on_change(node.nodeid.to_string(), val, stamp)

        try:
            client = Client(self.endpoint)
            if self.username:
                client.set_user(self.username)
                if self.password:
                    client.set_password(self.password)
            if self.security_string:
                # Certificate-based encryption, as any production endpoint requires.
                await client.set_security_string(self.security_string)
            async with client:
                sub = await client.create_subscription(
                    self.publish_interval_ms, _Handler()
                )
                nodes = [client.get_node(nid) for nid in self._route]
                await sub.subscribe_data_change(nodes)
                _log.info(
                    "opcua_subscribed",
                    endpoint=self.endpoint, tags=len(nodes),
                    lines=[tm.line_id for tm in self.tag_maps],
                )
                while not self._closed.is_set():
                    await asyncio.sleep(0.1)
                await sub.delete()
        except Exception as e:  # a dead server must not hang the caller
            _log.warning("opcua_disconnected", endpoint=self.endpoint, error=str(e))
        finally:
            self._queue.put(_SENTINEL)


class MqttSource(_QueueSource):
    """Reads batch readings from an MQTT broker, Sparkplug-style.

    Topics follow the Sparkplug B namespace
    ``spBv1.0/{group}/{msgtype}/{edge_node}[/{device}]``, and the message type is
    what carries the meaning:

    - ``DDATA`` / ``NDATA`` — tag values; routed into the assembler.
    - ``NDEATH`` / ``DDEATH`` — the broker publishing an edge node's Last Will
      because it dropped off. That is the industrial standard's own "this line
      went silent", so it flushes any partial batch and is surfaced to the
      caller through ``on_node_death``.

    Payloads are decoded as JSON. Full Sparkplug B encodes with protobuf and
    would need the generated ``sparkplug_b_pb2`` module; plain JSON over the
    Sparkplug topic tree is common in practice and keeps this dependency-light.
    Swap ``decode_payload`` to handle protobuf without touching anything else.
    """

    def __init__(
        self,
        host: str,
        *,
        port: int = 1883,
        topic: str = "spBv1.0/+/+/+/+",
        assembler: TagAssembler | None = None,
        username: str | None = None,
        password: str | None = None,
    ) -> None:
        super().__init__()
        self.host = host
        self.port = port
        self.topic = topic
        self.assembler = assembler or TagAssembler()
        self.username = username
        self.password = password
        self.dead_nodes: list[str] = []

    # --- message handling, exercised directly by tests -----------------------

    @staticmethod
    def parse_topic(topic: str) -> tuple[str, str] | None:
        """Return (msgtype, edge_node) from a Sparkplug topic, or None."""
        parts = topic.split("/")
        if len(parts) < 4 or not parts[0].startswith("spBv"):
            return None
        return parts[2], parts[3]

    @staticmethod
    def decode_payload(payload: bytes) -> dict:
        """JSON payload -> dict. Replace for Sparkplug B protobuf."""
        return json.loads(payload.decode("utf-8"))

    def handle_message(self, topic: str, payload: bytes) -> None:
        """Route one broker message. Called from paho's network thread."""
        parsed = self.parse_topic(topic)
        if parsed is None:
            return
        msgtype, edge_node = parsed

        if msgtype.endswith("DEATH"):
            self.dead_nodes.append(edge_node)
            _log.warning("mqtt_node_death", edge_node=edge_node, topic=topic)
            reading = self.assembler.flush(edge_node)
            if reading is not None:
                self._queue.put(reading)
            return

        try:
            body = self.decode_payload(payload)
        except Exception as e:
            _log.warning("mqtt_payload_undecodable", topic=topic, error=str(e))
            return

        line_id = body.get("line_id", edge_node)
        source_time = _parse_timestamp(body.get("timestamp"))
        for name, value in (body.get("metrics") or {}).items():
            reading = self.assembler.update(line_id, name, value, source_time)
            if reading is not None:
                self._queue.put(reading)

    def _produce(self) -> None:
        import paho.mqtt.client as mqtt

        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        if self.username:
            client.username_pw_set(self.username, self.password or "")

        def on_connect(c, userdata, flags, reason_code, properties=None):
            c.subscribe(self.topic)
            _log.info("mqtt_subscribed", host=self.host, topic=self.topic)

        def on_message(c, userdata, msg):
            self.handle_message(msg.topic, msg.payload)

        client.on_connect = on_connect
        client.on_message = on_message
        try:
            client.connect(self.host, self.port, keepalive=30)
            client.loop_start()
            while not self._closed.is_set():
                threading.Event().wait(0.2)
            client.loop_stop()
            client.disconnect()
        except Exception as e:
            _log.warning("mqtt_disconnected", host=self.host, error=str(e))
        finally:
            self._queue.put(_SENTINEL)


def _parse_timestamp(raw: Any) -> datetime:
    """Best-effort device timestamp: ISO string, epoch millis, or now."""
    if isinstance(raw, str):
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    elif isinstance(raw, (int, float)):
        return datetime.fromtimestamp(raw / 1000.0, tz=timezone.utc)
    return datetime.now(timezone.utc)
