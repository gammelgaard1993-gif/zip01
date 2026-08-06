from __future__ import annotations

import asyncio
import sqlite3
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any, cast
from unittest.mock import patch

from core.event_log import persist_validated_event
from ingestion.queue import PriorityEventQueue
from models import Priority, ValidatedEvent
from processing.alarm_bus import AlarmBus
from tests.fakes import FakeRedis
from processing.handlers.generic import GenericEventHandler
from processing.worker_pool import WorkerPool


class WorkerOrderingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.execute(
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
            )
            """
        )
        self.db.commit()

    def tearDown(self) -> None:
        self.db.close()

    async def test_same_device_out_of_order_arrival_applies_in_ts_order(self) -> None:
        # F-05: within the reorder window the worker sorts a device's buffered events by ts before
        # invoking handlers, so a slightly-late arrival is APPLIED in ts order. Durability itself
        # now happens at admission (arrival order) and is replayed ORDER BY ts on recovery, so the
        # meaningful guarantee here is handler application order, not durable-log order.
        applied_ts: list[datetime] = []

        async def record(self: GenericEventHandler, event: ValidatedEvent) -> None:
            applied_ts.append(event.ts)

        event_queue = PriorityEventQueue(normal_max_size=100)
        pool = WorkerPool(
            event_queue=event_queue,
            alarm_bus=AlarmBus(),
            db_connection=self.db,
            redis_client=cast(Any, FakeRedis()),
        )

        base = datetime.now(timezone.utc)
        newer = ValidatedEvent(
            device_id="dev_1",
            room_id="room_1",
            type="motion",
            ts=base,
            payload={"seq": 2},
            late=False,
            priority=Priority.NORMAL,
            received_at=base,
        )
        older = ValidatedEvent(
            device_id="dev_1",
            room_id="room_1",
            type="motion",
            ts=base - timedelta(seconds=2),
            payload={"seq": 1},
            late=False,
            priority=Priority.NORMAL,
            received_at=base,
        )

        with patch.object(GenericEventHandler, "handle", new=record):
            await pool.start()
            await event_queue.put(newer)
            await event_queue.put(older)
            await asyncio.sleep(0.6)
            await pool.stop()

        self.assertEqual(len(applied_ts), 2)
        self.assertEqual(applied_ts, sorted(applied_ts))

    def _heartbeat(self, device_id: str, ts: datetime) -> ValidatedEvent:
        return ValidatedEvent(
            device_id=device_id,
            room_id="room_1",
            type="heartbeat",
            ts=ts,
            payload={},
            late=False,
            priority=Priority.NORMAL,
            received_at=datetime.now(timezone.utc),
        )

    async def test_late_event_straddling_flush_keeps_ts_aware_state_correct(self) -> None:
        # F-05 boundary (finding #4): per-device ts ordering within the reorder window is exact,
        # but a late event that arrives AFTER its device buffer already flushed is applied later
        # than chronologically-newer events. Guaranteeing strict order for unbounded lateness
        # would need unbounded buffering, so the durable append-log may be out of ts order here.
        # The ts-aware handlers still keep the *aggregation* correct and nothing is lost: an older
        # heartbeat applied after a flush must NOT overwrite the newer last_heartbeat.
        redis = FakeRedis()
        event_queue = PriorityEventQueue(normal_max_size=100)
        pool = WorkerPool(
            event_queue=event_queue,
            alarm_bus=AlarmBus(),
            db_connection=self.db,
            redis_client=cast(Any, redis),
        )
        await pool.start()

        base = datetime.now(timezone.utc).replace(microsecond=0)
        first = self._heartbeat("dev_1", base)
        persist_validated_event(self.db, first)  # durable at admission
        await event_queue.put(first)
        await asyncio.sleep(0.3)  # let the first reorder buffer flush complete

        # A late heartbeat (30s older) arrives only after the buffer already flushed.
        late = self._heartbeat("dev_1", base - timedelta(seconds=30))
        persist_validated_event(self.db, late)  # durable at admission
        await event_queue.put(late)
        await asyncio.sleep(0.3)
        await pool.stop()

        # Aggregation is correct: last_heartbeat keeps the newest ts despite out-of-order apply.
        self.assertEqual(redis.strings["device:dev_1:last_heartbeat"], base.isoformat())
        # No state lost: both events are durably persisted even though append order is not strict.
        row_count = self.db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        self.assertEqual(row_count, 2)


if __name__ == "__main__":
    unittest.main()
