from __future__ import annotations

import sqlite3
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any, cast
from unittest.mock import MagicMock, patch

from ingestion.mqtt_subscriber import MQTTSubscriber
from ingestion.queue import PriorityEventQueue
from ingestion.validator import ValidationError, validate_raw_event
from models import Priority, ValidatedEvent


def _events_db() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:", check_same_thread=False)
    connection.execute(
        "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, device_id TEXT NOT NULL, "
        "room_id TEXT NOT NULL, type TEXT NOT NULL, ts TEXT NOT NULL, payload TEXT NOT NULL, "
        "received_at TEXT NOT NULL, late INTEGER NOT NULL DEFAULT 0)"
    )
    connection.commit()
    return connection


class _FakeMQTTMessage:
    def __init__(
        self,
        payload: bytes,
        topic: str = "teton/devices/dev_1/events",
        mid: int = 1,
        qos: int = 1,
    ) -> None:
        self.payload = payload
        self.topic = topic
        self.mid = mid
        self.qos = qos


class _CompletedFuture:
    def result(self) -> None:
        return None

    def done(self) -> bool:
        return True

    def add_done_callback(self, fn: Any) -> None:
        fn(self)


class _FailedFuture:
    def __init__(self, exc: BaseException) -> None:
        self._exc = exc

    def result(self) -> None:
        raise self._exc

    def done(self) -> bool:
        return True

    def add_done_callback(self, fn: Any) -> None:
        fn(self)


class Phase2ValidatorTests(unittest.TestCase):
    def _raw_event(self, event_type: str = "heartbeat", ts: str | None = None) -> dict[str, object]:
        event: dict[str, object] = {
            "device_id": "dev_1",
            "room_id": "room_1",
            "type": event_type,
            "ts": ts or datetime.now(timezone.utc).isoformat(),
            "seq": 1,
        }
        if event_type == "fall_warn":
            event["confidence"] = 0.9
        return event

    def test_validator_assigns_high_priority_for_fall_warn(self) -> None:
        validated = validate_raw_event(self._raw_event(event_type="fall_warn"))
        self.assertEqual(validated.priority, Priority.HIGH)

    def test_validator_marks_late_within_past_hour(self) -> None:
        ts = (datetime.now(timezone.utc) - timedelta(seconds=45)).isoformat()
        validated = validate_raw_event(self._raw_event(ts=ts))
        self.assertTrue(validated.late)

    def test_validator_rejects_future_clock_skew(self) -> None:
        ts = (datetime.now(timezone.utc) + timedelta(hours=1, seconds=1)).isoformat()
        with self.assertRaises(ValidationError) as ctx:
            validate_raw_event(self._raw_event(ts=ts))
        self.assertEqual(ctx.exception.reason, "clock_skew_future")


class Phase2QueueTests(unittest.IsolatedAsyncioTestCase):
    def _event(self, priority: Priority) -> ValidatedEvent:
        now = datetime.now(timezone.utc)
        return ValidatedEvent(
            device_id="dev_1",
            room_id="room_1",
            type="heartbeat",
            ts=now,
            payload={},
            late=False,
            priority=priority,
            received_at=now,
        )

    async def test_high_lane_drains_before_normal(self) -> None:
        queue = PriorityEventQueue(normal_max_size=10)
        await queue.put(self._event(Priority.NORMAL))
        await queue.put(self._event(Priority.HIGH))

        first = await queue.get()
        second = await queue.get()

        self.assertEqual(first.priority, Priority.HIGH)
        self.assertEqual(second.priority, Priority.NORMAL)


class Phase2MQTTSubscriberTests(unittest.TestCase):
    def _validated_event(self, event_type: str = "heartbeat") -> ValidatedEvent:
        now = datetime.now(timezone.utc)
        return ValidatedEvent(
            device_id="dev_1",
            room_id="room_1",
            type=event_type,
            ts=now,
            payload={},
            late=False,
            priority=Priority.NORMAL,
            received_at=now,
        )

    def test_on_connect_subscribes_with_qos_1(self) -> None:
        queue = PriorityEventQueue(normal_max_size=5)
        subscriber = MQTTSubscriber("localhost", 1883, "teton/devices/+/events", queue, _events_db())
        client = MagicMock()

        cast(Any, subscriber)._on_connect(client, None, {}, 0)

        client.subscribe.assert_called_once_with("teton/devices/+/events", qos=1)

    @patch("ingestion.mqtt_subscriber.increment_counter")
    def test_on_message_rejects_invalid_json(self, increment_counter_mock: MagicMock) -> None:
        queue = PriorityEventQueue(normal_max_size=5)
        subscriber = MQTTSubscriber("localhost", 1883, "teton/devices/+/events", queue, _events_db())
        subscriber.loop = cast(Any, MagicMock())  # not used on invalid input path

        cast(Any, subscriber)._on_message(MagicMock(), None, _FakeMQTTMessage(b"{invalid"))

        increment_counter_mock.assert_any_call("events_ingested_total")
        increment_counter_mock.assert_any_call("events_rejected_invalid_json")

    @patch("ingestion.mqtt_subscriber.increment_counter")
    def test_on_message_rejects_invalid_utf8(self, increment_counter_mock: MagicMock) -> None:
        # A payload that isn't valid UTF-8 must be caught (not left to crash the paho callback)
        # and acked so it doesn't sit stuck in the broker's inflight window forever.
        queue = PriorityEventQueue(normal_max_size=5)
        subscriber = MQTTSubscriber("localhost", 1883, "teton/devices/+/events", queue, _events_db())
        subscriber.loop = cast(Any, MagicMock())
        client = cast(Any, MagicMock())

        cast(Any, subscriber)._on_message(client, None, _FakeMQTTMessage(b"\xff\xfe\x00\x01"))

        increment_counter_mock.assert_any_call("events_ingested_total")
        increment_counter_mock.assert_any_call("events_rejected_invalid_json")
        client.ack.assert_called_once_with(1, 1)

    @patch("ingestion.mqtt_subscriber.asyncio.run_coroutine_threadsafe")
    @patch("ingestion.mqtt_subscriber.validate_raw_event")
    @patch("ingestion.mqtt_subscriber.increment_counter")
    def test_on_message_does_not_ack_when_enqueue_fails(
        self,
        increment_counter_mock: MagicMock,
        validate_raw_event_mock: MagicMock,
        run_threadsafe_mock: MagicMock,
    ) -> None:
        # If persist/enqueue fails after admission, the message must NOT be acked -- acking here
        # would tell the broker "delivered" for an event that was never durably written, losing it
        # for good instead of letting the broker redeliver.
        queue = PriorityEventQueue(normal_max_size=5)
        subscriber = MQTTSubscriber("localhost", 1883, "teton/devices/+/events", queue, _events_db())
        subscriber.loop = cast(Any, MagicMock())
        client = cast(Any, MagicMock())

        validate_raw_event_mock.return_value = self._validated_event(event_type="fall_warn")

        def _run_threadsafe_side_effect(coro: object, loop: object) -> _FailedFuture:
            if hasattr(coro, "close"):
                cast(Any, coro).close()
            return _FailedFuture(RuntimeError("persist failed"))

        run_threadsafe_mock.side_effect = _run_threadsafe_side_effect

        cast(Any, subscriber)._on_message(client, None, _FakeMQTTMessage(b'{"type": "fall_warn"}'))

        client.ack.assert_not_called()
        increment_counter_mock.assert_any_call("events_persist_failed")

    @patch("ingestion.mqtt_subscriber.asyncio.run_coroutine_threadsafe")
    @patch("ingestion.mqtt_subscriber.validate_raw_event")
    @patch("ingestion.mqtt_subscriber.increment_counter")
    def test_on_message_valid_event_enqueues_with_backpressure_signal(
        self,
        increment_counter_mock: MagicMock,
        validate_raw_event_mock: MagicMock,
        run_threadsafe_mock: MagicMock,
    ) -> None:
        queue = PriorityEventQueue(normal_max_size=1)
        subscriber = MQTTSubscriber("localhost", 1883, "teton/devices/+/events", queue, _events_db())
        subscriber.loop = cast(Any, MagicMock())

        validated = self._validated_event(event_type="heartbeat")
        validate_raw_event_mock.return_value = validated

        # Fill normal lane so queue pressure branch is exercised.
        queue.normal_queue.put_nowait(validated)

        def _run_threadsafe_side_effect(coro: object, loop: object) -> _CompletedFuture:
            if hasattr(coro, "close"):
                cast(Any, coro).close()
            return _CompletedFuture()

        run_threadsafe_mock.side_effect = _run_threadsafe_side_effect

        payload: dict[str, Any] = {
            "device_id": "dev_1",
            "room_id": "room_1",
            "type": "heartbeat",
            "ts": datetime.now(timezone.utc).isoformat(),
            "seq": 1,
        }
        client = cast(Any, MagicMock())
        cast(Any, subscriber)._on_message(
            client, None, _FakeMQTTMessage(str(payload).replace("'", '"').encode("utf-8"))
        )

        increment_counter_mock.assert_any_call("events_ingested_total")
        increment_counter_mock.assert_any_call("queue_pressure")
        validate_raw_event_mock.assert_called_once()
        run_threadsafe_mock.assert_called_once()
        # Ack fires only after the deferred future resolves successfully.
        client.ack.assert_called_once_with(1, 1)


if __name__ == "__main__":
    unittest.main()
