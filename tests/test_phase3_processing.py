from __future__ import annotations

import asyncio
import json
import sqlite3
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any, cast
from unittest.mock import patch

from config import WORKER_COUNT, WORKER_NORMAL_QUEUE_MAX_SIZE
from ingestion.queue import PriorityEventQueue
from models import Priority, ValidatedEvent
from processing.alarm_bus import AlarmBus
from processing.handlers.fall_warn import FallWarnHandler
from processing.handlers.heartbeat import HeartbeatHandler
from processing.handlers.presence import PresenceHandler
from processing.handlers.generic import GenericEventHandler
from processing.worker_pool import WorkerPool
from tests.fakes import FakeRedis


class FakeAlarmBus:
    def __init__(self) -> None:
        self.published: list[Any] = []

    async def publish(self, alarm: Any) -> None:
        self.published.append(alarm)


class Phase3ProcessingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.executescript(
            """
            CREATE TABLE events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL,
                room_id TEXT NOT NULL,
                type TEXT NOT NULL,
                ts TEXT NOT NULL,
                payload TEXT NOT NULL,
                received_at TEXT NOT NULL,
                late INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE fall_warnings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL,
                room_id TEXT NOT NULL,
                ts TEXT NOT NULL,
                confidence REAL NOT NULL,
                dedup_key TEXT NOT NULL UNIQUE,
                received_at TEXT NOT NULL,
                published_at TEXT
            );
            """
        )
        self.db.commit()

    def tearDown(self) -> None:
        self.db.close()

    def _event(
        self,
        event_type: str,
        *,
        device_id: str = "dev_1",
        room_id: str = "room_1",
        ts: datetime | None = None,
        payload: dict[str, Any] | None = None,
        priority: Priority | None = None,
    ) -> ValidatedEvent:
        now = ts or datetime.now(timezone.utc)
        return ValidatedEvent(
            device_id=device_id,
            room_id=room_id,
            type=event_type,
            ts=now,
            payload=payload or {},
            late=False,
            priority=priority or (
                Priority.HIGH if event_type == "fall_warn" else Priority.NORMAL
            ),
            received_at=now,
        )

    async def test_worker_pool_uses_configured_worker_count_and_consistent_hashing(self) -> None:
        pool = WorkerPool(PriorityEventQueue(10), AlarmBus(), self.db, cast(Any, FakeRedis()))
        pool_internal = cast(Any, pool)

        self.assertEqual(len(pool.worker_queues), WORKER_COUNT)
        self.assertEqual(pool_internal._worker_index("dev_1"), pool_internal._worker_index("dev_1"))
        self.assertTrue(0 <= pool_internal._worker_index("dev_2") < WORKER_COUNT)

    async def test_worker_lanes_are_bounded_and_high_drains_before_normal(self) -> None:
        pool = WorkerPool(PriorityEventQueue(10), AlarmBus(), self.db, cast(Any, FakeRedis()))
        for worker_queue in pool.worker_queues:
            self.assertIsInstance(worker_queue, PriorityEventQueue)
            self.assertEqual(worker_queue.normal_max_size(), WORKER_NORMAL_QUEUE_MAX_SIZE)

        lane = pool.worker_queues[0]
        await lane.put(self._event("motion"))
        await lane.put(self._event("motion"))
        await lane.put(self._event("fall_warn"))
        first = await lane.get()
        self.assertEqual(first.priority, Priority.HIGH)

    async def test_stop_drains_buffered_events_before_shutdown(self) -> None:
        applied: list[str] = []

        async def record(self: GenericEventHandler, event: ValidatedEvent) -> None:
            applied.append(event.device_id)

        queue = PriorityEventQueue(50)
        pool = WorkerPool(queue, AlarmBus(), self.db, cast(Any, FakeRedis()))
        with patch.object(GenericEventHandler, "handle", new=record):
            await pool.start()
            await queue.put(self._event("motion", device_id="dev_drain"))
            await pool.stop()

        self.assertIn("dev_drain", applied)

    async def test_inflight_watermark_registers_and_clears_after_handling(self) -> None:
        async def record(self: GenericEventHandler, event: ValidatedEvent) -> None:
            return None

        queue = PriorityEventQueue(10)
        pool = WorkerPool(queue, AlarmBus(), self.db, cast(Any, FakeRedis()))
        event = self._event("motion", device_id="dev_wm")
        pool.mark_inflight(event.received_at.isoformat())
        self.assertEqual(pool.oldest_inflight_received_at(), event.received_at.isoformat())

        with patch.object(GenericEventHandler, "handle", new=record):
            await pool.start()
            await queue.put(event)
            await asyncio.sleep(0.4)
            await pool.stop()

        self.assertIsNone(pool.oldest_inflight_received_at())

    async def test_worker_pool_error_isolation_continues_processing(self) -> None:
        queue = PriorityEventQueue(20)
        pool = WorkerPool(queue, AlarmBus(), self.db, cast(Any, FakeRedis()))

        call_count = {"n": 0}

        async def flaky_handle(self: GenericEventHandler, event: ValidatedEvent) -> None:
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("boom")

        with patch.object(GenericEventHandler, "handle", new=flaky_handle):
            await pool.start()
            await queue.put(self._event("motion", payload={"seq": 1}))
            await queue.put(self._event("motion", payload={"seq": 2}))
            await asyncio.sleep(0.6)
            await pool.stop()

        # Error isolation: the first handler raised, but the worker still processed the second
        # event (durability is owned by admission, not the worker, so nothing is persisted here).
        self.assertEqual(call_count["n"], 2)

    async def test_heartbeat_handler_updates_last_and_availability(self) -> None:
        redis_client = FakeRedis()
        handler = HeartbeatHandler(redis_client)  # type: ignore[arg-type]
        now = datetime.now(timezone.utc)

        await handler.handle(self._event("heartbeat", ts=now))
        await handler.handle(self._event("heartbeat", ts=now - timedelta(seconds=5)))

        last = redis_client.get("device:dev_1:last_heartbeat")
        self.assertIsNotNone(last)
        self.assertEqual(last, now.isoformat())
        self.assertGreater(handler.availability("dev_1"), 0.0)

    async def test_presence_handler_keeps_latest_state_by_ts(self) -> None:
        redis_client = FakeRedis()
        handler = PresenceHandler(redis_client)  # type: ignore[arg-type]
        now = datetime.now(timezone.utc)

        await handler.handle(self._event("presence", ts=now, payload={"in_room": True}))
        await handler.handle(self._event("presence", ts=now - timedelta(seconds=10), payload={"in_room": False}))

        state = redis_client.hgetall("room:room_1:presence")
        self.assertEqual(state.get("in_room"), "true")
        self.assertEqual(state.get("ts"), now.isoformat())

    async def test_presence_handler_rejects_non_bool_in_room(self) -> None:
        # Defense in depth for data that bypasses ingestion-time validation (e.g. recovery replay
        # of a pre-existing row): bool("false") is True, so a string must raise, not coerce.
        redis_client = FakeRedis()
        handler = PresenceHandler(redis_client)  # type: ignore[arg-type]

        with self.assertRaises(TypeError):
            await handler.handle(self._event("presence", payload={"in_room": "false"}))

    async def test_fall_warn_handler_dedups_persists_and_publishes(self) -> None:
        redis_client = FakeRedis()
        alarm_bus = FakeAlarmBus()
        handler = FallWarnHandler(redis_client, self.db, alarm_bus)
        event = self._event("fall_warn", payload={"confidence": 0.9}, ts=datetime.now(timezone.utc))

        await handler.handle(event)
        await handler.handle(event)

        row_count = self.db.execute("SELECT COUNT(*) FROM fall_warnings").fetchone()[0]
        self.assertEqual(row_count, 1)
        self.assertEqual(len(alarm_bus.published), 1)

    async def test_fall_warn_recovery_republishes_unpublished_row_once(self) -> None:
        event = self._event("fall_warn", payload={"confidence": 0.95}, ts=datetime.now(timezone.utc))
        seed_alarm_bus = FakeAlarmBus()
        seed_handler = FallWarnHandler(FakeRedis(), self.db, seed_alarm_bus)
        await seed_handler.handle(event)

        # Simulate a durable row that exists but was never marked as published
        # (e.g. crash boundary between publish and marker update).
        self.db.execute("UPDATE fall_warnings SET published_at = NULL")
        self.db.commit()

        redis_client = FakeRedis()
        alarm_bus = FakeAlarmBus()
        handler = FallWarnHandler(redis_client, self.db, alarm_bus, replay=True)

        await handler.handle(event)

        row = self.db.execute("SELECT published_at FROM fall_warnings").fetchone()
        self.assertIsNotNone(row[0])
        self.assertEqual(len(alarm_bus.published), 1)

    async def test_generic_event_types_persist_via_admission(self) -> None:
        # Durability now happens at admission (the /events route persisting before enqueue
        # persist_validated_event before ack), not in the worker. Every event type lands in the
        # durable `events` log via that path.
        from core.event_log import persist_validated_event

        persist_validated_event(self.db, self._event("motion", payload={"a": 1}))
        persist_validated_event(self.db, self._event("sleep_state", payload={"a": 2}))
        persist_validated_event(self.db, self._event("net_status", payload={"a": 3}))

        rows = self.db.execute("SELECT type, payload FROM events ORDER BY id ASC").fetchall()
        self.assertEqual([row[0] for row in rows], ["motion", "sleep_state", "net_status"])
        self.assertEqual(json.loads(rows[0][1])["a"], 1)

    async def test_alarm_bus_fanout_and_room_order(self) -> None:
        bus = AlarmBus()
        q1 = await bus.subscribe("room_1")
        q2 = await bus.subscribe("room_1")
        t0 = datetime.now(timezone.utc)

        newer = self._event("fall_warn", room_id="room_1", ts=t0 + timedelta(seconds=1)).ts
        older = self._event("fall_warn", room_id="room_1", ts=t0).ts

        from models import AlarmEvent

        await bus.publish(AlarmEvent("d1", "room_1", newer, 0.8, datetime.now(timezone.utc)))
        await bus.publish(AlarmEvent("d2", "room_1", older, 0.7, datetime.now(timezone.utc)))

        first_1 = await asyncio.wait_for(q1.get(), timeout=1.0)
        second_1 = await asyncio.wait_for(q1.get(), timeout=1.0)
        first_2 = await asyncio.wait_for(q2.get(), timeout=1.0)
        second_2 = await asyncio.wait_for(q2.get(), timeout=1.0)

        self.assertLessEqual(first_1.ts, second_1.ts)
        self.assertLessEqual(first_2.ts, second_2.ts)


if __name__ == "__main__":
    unittest.main()
