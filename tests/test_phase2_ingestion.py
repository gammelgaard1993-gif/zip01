from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from typing import Any, cast
from unittest.mock import MagicMock, patch

from ingestion.mqtt_subscriber import MQTTSubscriber
from ingestion.queue import PriorityEventQueue
from ingestion.validator import ValidationError, validate_raw_event
from models import Priority, ValidatedEvent


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


class Phase2ValidatorTests(unittest.TestCase):
    def _raw_event(self, event_type: str = "heartbeat", ts: str | None = None) -> dict[str, object]:
        return {
            "device_id": "dev_1",
            "room_id": "room_1",
            "type": event_type,
            "ts": ts or datetime.now(timezone.utc).isoformat(),
            "seq": 1,
        }

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
        subscriber = MQTTSubscriber("localhost", 1883, "teton/devices/+/events", queue)
        client = MagicMock()

        cast(Any, subscriber)._on_connect(client, None, {}, 0)

        client.subscribe.assert_called_once_with("teton/devices/+/events", qos=1)

    @patch("ingestion.mqtt_subscriber.increment_counter")
    def test_on_message_rejects_invalid_json(self, increment_counter_mock: MagicMock) -> None:
        queue = PriorityEventQueue(normal_max_size=5)
        subscriber = MQTTSubscriber("localhost", 1883, "teton/devices/+/events", queue)
        subscriber.loop = cast(Any, MagicMock())  # not used on invalid input path

        cast(Any, subscriber)._on_message(MagicMock(), None, _FakeMQTTMessage(b"{invalid"))

        increment_counter_mock.assert_any_call("events_ingested_total")
        increment_counter_mock.assert_any_call("events_rejected_invalid_json")

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
        subscriber = MQTTSubscriber("localhost", 1883, "teton/devices/+/events", queue)
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
        cast(Any, subscriber)._on_message(
            MagicMock(), None, _FakeMQTTMessage(str(payload).replace("'", '"').encode("utf-8"))
        )

        increment_counter_mock.assert_any_call("events_ingested_total")
        increment_counter_mock.assert_any_call("queue_pressure")
        validate_raw_event_mock.assert_called_once()
        run_threadsafe_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
