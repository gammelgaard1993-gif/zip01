from __future__ import annotations

import asyncio
import json
import sqlite3
import unittest
from datetime import datetime, timedelta, timezone
from math import ceil
from typing import Any, cast

from api.routes.occupancy import room_occupancy
from core.recovery import RecoveryManager
from ingestion.queue import PriorityEventQueue
from models import Priority, ValidatedEvent
from processing.alarm_bus import AlarmBus
from processing.handlers.presence import PresenceHandler
from processing.worker_pool import WorkerPool


class _Pipeline:
    def __init__(self, redis: "FakeRedis") -> None:
        self.redis = redis
        self.ops: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def set(self, name: str, value: str) -> object:
        self.ops.append(("set", (name, value), {}))
        return self

    def zadd(self, name: str, mapping: dict[str, float]) -> object:
        self.ops.append(("zadd", (name, mapping), {}))
        return self

    def zremrangebyscore(self, name: str, min: float, max: float) -> object:
        self.ops.append(("zremrangebyscore", (name, min, max), {}))
        return self

    def hset(self, name: str, mapping: dict[str, str]) -> object:
        self.ops.append(("hset", (name, mapping), {}))
        return self

    def execute(self) -> object:
        for op, args, kwargs in self.ops:
            getattr(self.redis, op)(*args, **kwargs)
        self.ops.clear()
        return True


class FakeRedis:
    """In-process Redis double covering both handler writes (pipeline) and route reads."""

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
        to_remove = [member for member, score in self.zsets[name].items() if float(min) <= score <= float(max)]
        for member in to_remove:
            self.zsets[name].pop(member, None)
        return len(to_remove)

    def zrangebyscore(
        self,
        name: str,
        min: float | str,
        max: float | str,
        start: int | None = None,
        num: int | None = None,
        withscores: bool = False,
    ) -> list[str]:
        min_score = float("-inf") if min == "-inf" else float(min)
        max_score = float("inf") if max == "+inf" else float(max)
        ordered = sorted(self.zsets.get(name, {}).items(), key=lambda item: item[1])
        values = [member for member, score in ordered if min_score <= score <= max_score]
        if start is not None and num is not None:
            return values[start : start + num]
        return values

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


def _p95(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    rank_index = max(0, ceil(0.95 * len(ordered)) - 1)
    return ordered[rank_index]


def _events_schema() -> str:
    return """
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


class BurstLatencyTests(unittest.IsolatedAsyncioTestCase):
    """Gap #1 — drive a burst of fall warnings through the full hot path and assert p95 <= 1s.

    This is an in-process surrogate for the §12 Phase 7 `make burst` harness (which does not
    exist in the repo): it exercises queue -> worker pool -> FallWarnHandler -> AlarmBus ->
    subscriber and measures ingestion(received_at)-to-delivery latency, the same definition the
    SSE route reports. It validates the cumulative reorder budget (device + alarm buffers) stays
    under the 1s p95 target.
    """

    def setUp(self) -> None:
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.executescript(_events_schema())
        self.db.commit()

    def tearDown(self) -> None:
        self.db.close()

    async def test_burst_fall_warn_p95_latency_under_one_second(self) -> None:
        event_count = 300
        room_id = "room_burst"
        queue = PriorityEventQueue(event_count * 2)
        alarm_bus = AlarmBus()
        pool = WorkerPool(queue, alarm_bus, self.db, cast(Any, FakeRedis()))

        subscriber = await alarm_bus.subscribe(room_id)
        await pool.start()

        try:
            base_ts = datetime.now(timezone.utc)
            for index in range(event_count):
                now = datetime.now(timezone.utc)
                event = ValidatedEvent(
                    device_id=f"dev_{index:04d}",  # unique device avoids dedup collapse
                    room_id=room_id,
                    type="fall_warn",
                    ts=base_ts + timedelta(milliseconds=index),
                    payload={"confidence": 0.9},
                    late=False,
                    priority=Priority.HIGH,
                    received_at=now,
                )
                await queue.put(event)

            latencies_ms: list[float] = []
            for _ in range(event_count):
                alarm = await asyncio.wait_for(subscriber.get(), timeout=10.0)
                delivered_at = datetime.now(timezone.utc)
                latencies_ms.append((delivered_at - alarm.received_at).total_seconds() * 1000.0)
        finally:
            await pool.stop()
            await alarm_bus.unsubscribe(room_id, subscriber)

        # No silent drops: every published fall warning was delivered.
        self.assertEqual(len(latencies_ms), event_count)
        self.assertLessEqual(_p95(latencies_ms), 1000.0)


class RecoveryEquivalenceTests(unittest.IsolatedAsyncioTestCase):
    """Gap #2 (N-03) — recovered hot state must equal freshly-ingested hot state."""

    def setUp(self) -> None:
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.executescript(_events_schema())
        self.db.commit()

    def tearDown(self) -> None:
        self.db.close()

    async def test_recovery_replay_matches_live_ingestion_state(self) -> None:
        now = datetime.now(timezone.utc)
        # Deliberately out-of-ts-order arrival across two devices and two rooms.
        specs: list[tuple[str, str, str, datetime, dict[str, Any]]] = [
            ("heartbeat", "dev_a", "room_1", now - timedelta(seconds=20), {}),
            ("presence", "dev_a", "room_1", now - timedelta(seconds=15), {"in_room": True}),
            ("heartbeat", "dev_a", "room_1", now - timedelta(seconds=40), {}),
            ("presence", "dev_b", "room_2", now - timedelta(seconds=5), {"in_room": False}),
            ("presence", "dev_b", "room_2", now - timedelta(seconds=25), {"in_room": True}),
            ("heartbeat", "dev_b", "room_2", now - timedelta(seconds=10), {}),
        ]
        events = [
            ValidatedEvent(
                device_id=device_id,
                room_id=room_id,
                type=event_type,
                ts=ts,
                payload=payload,
                late=False,
                priority=Priority.NORMAL,
                received_at=now,
            )
            for event_type, device_id, room_id, ts, payload in specs
        ]

        # Path A: normal ingestion through the worker pool (also persists events to SQLite).
        live_redis = FakeRedis()
        queue = PriorityEventQueue(64)
        pool = WorkerPool(queue, AlarmBus(), self.db, cast(Any, live_redis))
        await pool.start()
        try:
            for event in events:
                await queue.put(event)
            await asyncio.sleep(0.4)  # allow per-device reorder buffers to flush
        finally:
            await pool.stop()

        # Path B: recovery replay of the same persisted events into a cold Redis.
        recovered_redis = FakeRedis()
        manager = RecoveryManager(self.db, cast(Any, recovered_redis), cast(Any, _NullAlarmBus()))
        await cast(Any, manager)._replay_events(since_ts=None)

        self.assertEqual(live_redis.strings, recovered_redis.strings)
        self.assertEqual(live_redis.hashes, recovered_redis.hashes)
        self.assertEqual(live_redis.zsets, recovered_redis.zsets)


class OfflineReplayOccupancyTests(unittest.IsolatedAsyncioTestCase):
    """Gap #3 (F-02, §9) — late presence events from an offline device backfill occupancy."""

    async def test_offline_presence_replay_backfills_occupancy_window(self) -> None:
        redis_client = FakeRedis()
        handler = PresenceHandler(cast(Any, redis_client))
        now = datetime.now(timezone.utc)

        # Device was offline ~20 min and now replays buffered transitions: it was in the room
        # from 50 to 30 minutes ago (a 20-minute occupied interval inside the 1h window).
        enter = ValidatedEvent(
            device_id="dev_offline",
            room_id="room_off",
            type="presence",
            ts=now - timedelta(minutes=50),
            payload={"in_room": True},
            late=True,
            priority=Priority.NORMAL,
            received_at=now,
        )
        exit_event = ValidatedEvent(
            device_id="dev_offline",
            room_id="room_off",
            type="presence",
            ts=now - timedelta(minutes=30),
            payload={"in_room": False},
            late=True,
            priority=Priority.NORMAL,
            received_at=now,
        )

        # Replay out of chronological order to prove sorted-set-by-score backfill is correct.
        await handler.handle(exit_event)
        await handler.handle(enter)

        response = await room_occupancy("room_off", window="1h", redis_client=cast(Any, redis_client))

        # 20 occupied minutes within a 60-minute window => ~0.333.
        self.assertAlmostEqual(response.occupied_pct, 20.0 / 60.0, delta=0.02)
        # Latest transition by ts is the exit, so the room currently reads unoccupied.
        self.assertFalse(response.in_room)


class _NullAlarmBus:
    async def publish(self, alarm: object) -> None:
        return None


if __name__ == "__main__":
    unittest.main()
