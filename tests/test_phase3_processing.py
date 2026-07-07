from __future__ import annotations

import asyncio
import json
import sqlite3
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any, cast
from unittest.mock import patch

from config import WORKER_COUNT
from ingestion.queue import PriorityEventQueue
from models import Priority, ValidatedEvent
from processing.alarm_bus import AlarmBus
from processing.handlers.fall_warn import FallWarnHandler
from processing.handlers.heartbeat import HeartbeatHandler
from processing.handlers.presence import PresenceHandler
from processing.handlers.generic import GenericEventHandler
from processing.worker_pool import WorkerPool


class _Pipeline:
    def __init__(self, redis: "FakeRedis") -> None:
        self.redis = redis
        self.ops: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def set(self, name: str, value: str) -> object:
        self.ops.append(("set", (name, value), {}))
        return True

    def zadd(self, name: str, mapping: dict[str, float]) -> object:
        self.ops.append(("zadd", (name, mapping), {}))
        return True

    def zremrangebyscore(self, name: str, min: float, max: float) -> object:
        self.ops.append(("zremrangebyscore", (name, min, max), {}))
        return True

    def hset(self, name: str, mapping: dict[str, str]) -> object:
        self.ops.append(("hset", (name, mapping), {}))
        return True

    def execute(self) -> object:
        for op, args, kwargs in self.ops:
            getattr(self.redis, op)(*args, **kwargs)
        self.ops.clear()
        return True


class FakeRedis:
    def __init__(self) -> None:
        self.strings: dict[str, str] = {}
        self.hashes: dict[str, dict[str, str]] = {}
        self.zsets: dict[str, dict[str, float]] = {}

    def pipeline(self) -> _Pipeline:
        return _Pipeline(self)

    def get(self, name: str) -> str | None:
        return self.strings.get(name)

    def set(self, name: str, value: str, *args: object, **kwargs: object) -> object:
        nx = bool(kwargs.get("nx", False))
        if nx and name in self.strings:
            return False
        self.strings[name] = value
        return True

    def hgetall(self, name: str) -> dict[str, str]:
        return dict(self.hashes.get(name, {}))

    def hset(self, name: str, mapping: dict[str, str]) -> object:
        self.hashes[name] = dict(mapping)
        return True

    def zadd(self, name: str, mapping: dict[str, float]) -> object:
        zset = self.zsets.setdefault(name, {})
        zset.update(mapping)
        return True

    def zremrangebyscore(self, name: str, min: float, max: float) -> object:
        if name not in self.zsets:
            return 0
        members_to_remove = [member for member, score in self.zsets[name].items() if float(min) <= score <= float(max)]
        for member in members_to_remove:
            self.zsets[name].pop(member, None)
        return len(members_to_remove)

    def zrevrangebyscore(
        self,
        name: str,
        max: float | str,
        min: float | str,
        start: int | None = None,
        num: int | None = None,
        withscores: bool = False,
    ) -> list[Any]:
        max_score = float("inf") if max == "+inf" else float(max)
        min_score = float("-inf") if min == "-inf" else float(min)
        ordered = sorted(self.zsets.get(name, {}).items(), key=lambda item: item[1], reverse=True)
        filtered = [(member, score) for member, score in ordered if min_score <= score <= max_score]
        if start is not None and num is not None:
            filtered = filtered[start : start + num]
        if withscores:
            return filtered
        return [member for member, _ in filtered]

    def zcount(self, name: str, min: float, max: float) -> int:
        return sum(1 for score in self.zsets.get(name, {}).values() if float(min) <= score <= float(max))


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
                received_at TEXT NOT NULL
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
            priority=priority or (Priority.HIGH if event_type == "fall_warn" else Priority.NORMAL),
            received_at=now,
        )

    async def test_worker_pool_uses_configured_worker_count_and_consistent_hashing(self) -> None:
        pool = WorkerPool(PriorityEventQueue(10), AlarmBus(), self.db, cast(Any, FakeRedis()))
        pool_internal = cast(Any, pool)

        self.assertEqual(len(pool.worker_queues), WORKER_COUNT)
        self.assertEqual(pool_internal._worker_index("dev_1"), pool_internal._worker_index("dev_1"))
        self.assertTrue(0 <= pool_internal._worker_index("dev_2") < WORKER_COUNT)

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

        rows = self.db.execute("SELECT COUNT(*) FROM events WHERE type='motion'").fetchone()[0]
        self.assertEqual(rows, 2)

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

    async def test_generic_event_types_persist_via_worker_flow(self) -> None:
        queue = PriorityEventQueue(20)
        pool = WorkerPool(queue, AlarmBus(), self.db, cast(Any, FakeRedis()))
        await pool.start()

        await queue.put(self._event("motion", payload={"a": 1}))
        await queue.put(self._event("sleep_state", payload={"a": 2}))
        await queue.put(self._event("net_status", payload={"a": 3}))
        await asyncio.sleep(0.6)
        await pool.stop()

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
