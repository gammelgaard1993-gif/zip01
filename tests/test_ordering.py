from __future__ import annotations

import asyncio
import sqlite3
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any, cast

from ingestion.queue import PriorityEventQueue
from models import Priority, ValidatedEvent
from processing.alarm_bus import AlarmBus
from processing.worker_pool import WorkerPool


class _Pipeline:
    def __init__(self, redis: "FakeRedis") -> None:
        self.redis = redis
        self.ops: list[tuple[str, tuple[Any, ...]]] = []

    def set(self, name: str, value: str) -> "_Pipeline":
        self.ops.append(("set", (name, value)))
        return self

    def zadd(self, name: str, mapping: dict[str, float]) -> "_Pipeline":
        self.ops.append(("zadd", (name, mapping)))
        return self

    def zremrangebyscore(self, name: str, min: float, max: float) -> "_Pipeline":
        self.ops.append(("zremrangebyscore", (name, min, max)))
        return self

    def execute(self) -> object:
        for op, args in self.ops:
            getattr(self.redis, op)(*args)
        self.ops.clear()
        return True


class FakeRedis:
    """Minimal heartbeat-capable fake (the motion-only test never touches Redis)."""

    def __init__(self) -> None:
        self.strings: dict[str, str] = {}
        self.zsets: dict[str, dict[str, float]] = {}

    def pipeline(self) -> _Pipeline:
        return _Pipeline(self)

    def get(self, name: str) -> str | None:
        return self.strings.get(name)

    def set(self, name: str, value: str) -> object:
        self.strings[name] = value
        return True

    def zadd(self, name: str, mapping: dict[str, float]) -> object:
        self.zsets.setdefault(name, {}).update(mapping)
        return True

    def zremrangebyscore(self, name: str, min: float, max: float) -> object:
        zset = self.zsets.get(name, {})
        for member in [m for m, score in zset.items() if float(min) <= score <= float(max)]:
            zset.pop(member, None)
        return True

    def zcount(self, name: str, min: float, max: float) -> int:
        return sum(1 for score in self.zsets.get(name, {}).values() if float(min) <= score <= float(max))


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

    async def test_same_device_out_of_order_arrival_persists_in_ts_order(self) -> None:
        event_queue = PriorityEventQueue(normal_max_size=100)
        pool = WorkerPool(
            event_queue=event_queue,
            alarm_bus=AlarmBus(),
            db_connection=self.db,
            redis_client=cast(Any, FakeRedis()),
        )
        await pool.start()

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

        await event_queue.put(newer)
        await event_queue.put(older)

        await asyncio.sleep(0.6)

        rows = self.db.execute("SELECT ts FROM events ORDER BY id ASC").fetchall()
        await pool.stop()

        self.assertEqual(len(rows), 2)
        self.assertLess(datetime.fromisoformat(rows[0][0]), datetime.fromisoformat(rows[1][0]))

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
        await event_queue.put(self._heartbeat("dev_1", base))
        await asyncio.sleep(0.3)  # let the first reorder buffer flush complete

        # A late heartbeat (30s older) arrives only after the buffer already flushed.
        await event_queue.put(self._heartbeat("dev_1", base - timedelta(seconds=30)))
        await asyncio.sleep(0.3)
        await pool.stop()

        # Aggregation is correct: last_heartbeat keeps the newest ts despite out-of-order apply.
        self.assertEqual(redis.strings["device:dev_1:last_heartbeat"], base.isoformat())
        # No state lost: both events are durably persisted even though append order is not strict.
        row_count = self.db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        self.assertEqual(row_count, 2)


if __name__ == "__main__":
    unittest.main()
