from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from ingestion.queue import PriorityEventQueue
from ingestion.validator import ValidationError, validate_raw_event
from models import Priority, ValidatedEvent


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


if __name__ == "__main__":
    unittest.main()
