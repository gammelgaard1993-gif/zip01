from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from config import HIGH_QUEUE_MAX_SIZE
from ingestion.queue import PriorityEventQueue
from models import Priority, ValidatedEvent


class _GatedPriorityEventQueue(PriorityEventQueue):
    def __init__(self, normal_max_size: int) -> None:
        super().__init__(normal_max_size)
        self.wait_started = asyncio.Event()
        self.release_wait = asyncio.Event()

    async def _wait_until_available(self) -> None:
        self.wait_started.set()
        await self.release_wait.wait()


class QueueBackpressureTests(unittest.IsolatedAsyncioTestCase):
    def _event(self, index: int) -> ValidatedEvent:
        return ValidatedEvent(
            device_id=f"dev_{index}",
            room_id="room_1",
            type="heartbeat",
            ts=datetime.now(timezone.utc),
            payload={},
            late=False,
            priority=Priority.NORMAL,
            received_at=datetime.now(timezone.utc),
        )

    def _high_event(self, index: int) -> ValidatedEvent:
        return ValidatedEvent(
            device_id=f"dev_{index}",
            room_id="room_1",
            type="fall_warn",
            ts=datetime.now(timezone.utc),
            payload={},
            late=False,
            priority=Priority.HIGH,
            received_at=datetime.now(timezone.utc),
        )

    async def test_put_blocks_when_normal_lane_full(self) -> None:
        queue = PriorityEventQueue(normal_max_size=1)
        await queue.put(self._event(1))

        pending_put = asyncio.create_task(queue.put(self._event(2)))
        await asyncio.sleep(0)
        self.assertFalse(pending_put.done())

        _ = await queue.get()
        await asyncio.wait_for(pending_put, timeout=1.0)
        self.assertEqual(queue.qsize_normal(), 1)

    async def test_idle_get_wakes_on_high_arrival(self) -> None:
        queue = PriorityEventQueue(normal_max_size=10)
        getter = asyncio.create_task(queue.get())
        await asyncio.sleep(0)
        self.assertFalse(getter.done())

        await queue.put(self._high_event(1))
        result = await asyncio.wait_for(getter, timeout=1.0)
        self.assertEqual(result.priority, Priority.HIGH)

    async def test_idle_get_serves_high_before_normal_on_simultaneous_arrival(self) -> None:
        queue = PriorityEventQueue(normal_max_size=10)
        getter = asyncio.create_task(queue.get())
        await asyncio.sleep(0)

        await queue.put(self._event(1))
        await queue.put(self._high_event(2))

        first = await asyncio.wait_for(getter, timeout=1.0)
        self.assertEqual(first.priority, Priority.HIGH)

        second = await asyncio.wait_for(queue.get(), timeout=1.0)
        self.assertEqual(second.priority, Priority.NORMAL)
        self.assertTrue(queue.empty())

    async def test_cancelled_get_does_not_lose_event_when_lane_refills(self) -> None:
        queue = _GatedPriorityEventQueue(normal_max_size=1)
        first_event = self._event(1)
        second_event = self._event(2)

        getter = asyncio.create_task(queue.get())
        await queue.wait_started.wait()
        await queue.put(first_event)
        blocked_put = asyncio.create_task(queue.put(second_event))

        getter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await getter

        first = await queue.get()
        self.assertEqual(first.device_id, first_event.device_id)
        await asyncio.wait_for(blocked_put, timeout=1.0)
        second = await queue.get()
        self.assertEqual(second.device_id, second_event.device_id)
        self.assertTrue(queue.empty())


class HighLaneBoundTests(unittest.IsolatedAsyncioTestCase):
    """The HIGH (fall_warn) lane is bounded by config.HIGH_QUEUE_MAX_SIZE."""

    def _high_event(self, index: int) -> ValidatedEvent:
        return ValidatedEvent(
            device_id=f"dev_{index}",
            room_id="room_1",
            type="fall_warn",
            ts=datetime.now(timezone.utc),
            payload={},
            late=False,
            priority=Priority.HIGH,
            received_at=datetime.now(timezone.utc),
        )

    async def test_high_lane_maxsize_matches_config(self) -> None:
        queue = PriorityEventQueue(normal_max_size=10)
        self.assertEqual(queue.high_queue.maxsize, HIGH_QUEUE_MAX_SIZE)
        self.assertGreater(queue.high_queue.maxsize, 0)

    async def test_full_high_lane_backpressures_put_without_dropping(self) -> None:
        with patch("ingestion.queue.HIGH_QUEUE_MAX_SIZE", 2):
            queue = PriorityEventQueue(normal_max_size=10)
        self.assertEqual(queue.high_queue.maxsize, 2)
        await queue.put(self._high_event(1))
        await queue.put(self._high_event(2))
        self.assertEqual(queue.qsize_high(), 2)

        pending_put = asyncio.create_task(queue.put(self._high_event(3)))
        await asyncio.sleep(0)
        self.assertFalse(pending_put.done())
        self.assertEqual(queue.qsize_high(), 2)

        first = await queue.get()
        self.assertEqual(first.priority, Priority.HIGH)
        await asyncio.wait_for(pending_put, timeout=1.0)
        self.assertEqual(queue.qsize_high(), 2)

        drained = [first]
        while not queue.empty():
            drained.append(await queue.get())
        self.assertEqual(len(drained), 3)
        self.assertTrue(all(event.priority == Priority.HIGH for event in drained))
        self.assertEqual(
            {event.device_id for event in drained},
            {"dev_1", "dev_2", "dev_3"},
        )


if __name__ == "__main__":
    unittest.main()
