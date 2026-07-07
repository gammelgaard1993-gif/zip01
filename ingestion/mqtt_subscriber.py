from __future__ import annotations

import asyncio
import json
import logging
from time import perf_counter
from typing import Any, cast

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

from config import MQTT_CLIENT_ID   
from ingestion.queue import PriorityEventQueue
from ingestion.validator import ValidationError, validate_raw_event
from core.metrics import increment_counter
from models import Priority, ValidatedEvent

logger = logging.getLogger(__name__)


class MQTTSubscriber:
    def __init__(self, broker_url: str, broker_port: int, topic: str, event_queue: PriorityEventQueue) -> None:
        self.broker_url = broker_url
        self.broker_port = broker_port
        self.topic = topic
        self.event_queue = event_queue
        # VERSION2 callbacks construct cleanly under paho 2.x. manual_ack=True gives QoS-1
        # backpressure without blocking paho's delivery thread: NORMAL pubacks are deferred until
        # the bounded lane accepts (broker throttles on a full inflight window); 
        # HIGH is acked at once so fall_warn is never stalled behind NORMAL.
        self.client = mqtt.Client(
            CallbackAPIVersion.VERSION2,
            client_id=MQTT_CLIENT_ID,
            clean_session=False,
            manual_ack=True,
        )
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.loop: asyncio.AbstractEventLoop | None = None

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: Any,
        reason_code: Any,
        properties: Any = None,
    ) -> None:
        client.subscribe(self.topic, qos=1)

    def _on_message(self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage) -> None:
        increment_counter("events_ingested_total")

        try:
            raw_payload = json.loads(msg.payload.decode("utf-8"))
        except json.JSONDecodeError:
            increment_counter("events_rejected_invalid_json")
            logger.warning(
                json.dumps(
                    {
                        "event": "invalid_json",
                        "topic": msg.topic,
                    }
                )
            )
            # Rejected — ack now so it doesn't hold the inflight window (manual_ack suppresses auto-puback).
            client.ack(msg.mid, msg.qos)
            return

        if not isinstance(raw_payload, dict):
            increment_counter("events_rejected_invalid_json")
            logger.warning(
                json.dumps(
                    {
                        "event": "invalid_json_type",
                        "topic": msg.topic,
                    }
                )
            )
            client.ack(msg.mid, msg.qos)
            return

        payload = cast(dict[str, Any], raw_payload)

        try:
            validated = validate_raw_event(payload)
        except ValidationError as exc:
            reason = getattr(exc, "reason", "validation_error")
            if reason in {"clock_skew_future", "clock_skew_past"}:
                increment_counter("events_rejected_clock_skew")
                increment_counter(f"events_rejected_{reason}")
                logger.warning(
                    json.dumps(
                        {
                            "event": "clock_skew",
                            "device_id": payload.get("device_id"),
                            "type": payload.get("type"),
                            "reason": reason,
                            "offset_seconds": getattr(exc, "offset_seconds", None),
                        }
                    )
                )
            else:
                increment_counter("events_rejected_invalid_schema")
                logger.warning(
                    json.dumps(
                        {
                            "event": "validation_reject",
                            "device_id": payload.get("device_id"),
                            "type": payload.get("type"),
                            "reason": reason,
                        }
                    )
                )
            # Rejected — ack so it doesn't occupy the inflight window.
            client.ack(msg.mid, msg.qos)
            return

        if self.loop is None:
            raise RuntimeError("MQTT subscriber loop is not initialized")

        was_pressured = validated.priority.value == "normal" and self.event_queue.normal_is_full()
        if was_pressured:
            increment_counter("queue_pressure")
            logger.warning(
                json.dumps(
                    {
                        "event": "queue_pressure",
                        "lane": "NORMAL",
                        "depth": self.event_queue.qsize_normal(),
                    }
                )
            )

        logger.info(
            json.dumps(
                {
                    "event": "ingested",
                    "device_id": validated.device_id,
                    "type": validated.type,
                    "late": validated.late,
                }
            )
        )

        # Hand off to the loop without blocking paho's delivery thread; blocking would stall a
        # following HIGH fall_warn behind NORMAL work (priority inversion).
        submit_perf = perf_counter()
        mid, qos = msg.mid, msg.qos
        future = asyncio.run_coroutine_threadsafe(
            self._enqueue(validated, was_pressured, submit_perf), self.loop
        )

        if validated.priority == Priority.HIGH:
            # Unbounded HIGH lane resolves at once; ack immediately so fall_warn never waits.
            client.ack(mid, qos)
            return

        # NORMAL: defer the puback until the bounded lane accepts. The broker throttles once its
        # inflight window fills with un-acked NORMAL — real QoS-1 backpressure, no drops, no block.
        # The callback runs on the loop thread; paho socket writes are mutex-guarded, so it's safe.
        future.add_done_callback(lambda _f: client.ack(mid, qos))

    async def _enqueue(self, event: ValidatedEvent, was_pressured: bool, submit_perf: float) -> None:
        # On the loop: HIGH returns at once; a full NORMAL lane awaits capacity (delays, never drops).
        await self.event_queue.put(event)

        # Record how long a pressured event waited for NORMAL capacity (measured on the loop).
        if was_pressured:
            block_ms = int((perf_counter() - submit_perf) * 1000)
            increment_counter("queue_pressure_block_ms_total", block_ms)
            logger.warning(
                json.dumps(
                    {
                        "event": "queue_pressure_resolved",
                        "lane": "NORMAL",
                        "block_ms": block_ms,
                        "depth": self.event_queue.qsize_normal(),
                    }
                )
            )

    def start(self) -> None:
        self.loop = asyncio.get_running_loop()
        self.client.connect(self.broker_url, self.broker_port)
        self.client.loop_start()

    def stop(self) -> None:
        self.client.loop_stop()
        self.client.disconnect()
