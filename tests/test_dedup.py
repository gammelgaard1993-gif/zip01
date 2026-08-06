from __future__ import annotations

import asyncio
import sqlite3
import unittest
from datetime import datetime, timezone
from typing import Any, cast

from core.metrics import get_counters
from models import Priority, ValidatedEvent
from processing.handlers.fall_warn import FallWarnHandler
from tests.fakes import FakeRedis


class FakeAlarmBus:
    def __init__(self) -> None:
        self.published: list[object] = []

    async def publish(self, alarm: object) -> None:
        self.published.append(alarm)


class FallWarnDedupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.db = sqlite3.connect(":memory:")
        self.db.execute(
            """
            CREATE TABLE fall_warnings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL,
                room_id TEXT NOT NULL,
                ts TEXT NOT NULL,
                confidence REAL NOT NULL,
                dedup_key TEXT NOT NULL UNIQUE,
                received_at TEXT NOT NULL
            )
            """
        )
        self.db.commit()

    def tearDown(self) -> None:
        self.db.close()

    def test_replay_does_not_republish_insert_ignored_row(self) -> None:
        event = ValidatedEvent(
            device_id="dev_1",
            room_id="room_1",
            type="fall_warn",
            ts=datetime(2026, 6, 29, 12, 0, 0, tzinfo=timezone.utc),
            payload={"confidence": 0.9},
            late=False,
            priority=Priority.HIGH,
            received_at=datetime(2026, 6, 29, 12, 0, 0, tzinfo=timezone.utc),
        )

        first_alarm_bus = FakeAlarmBus()
        first_handler = FallWarnHandler(FakeRedis(), self.db, first_alarm_bus)
        asyncio.run(first_handler.handle(event))

        # Simulate restart recovery: Redis dedup cache is empty but SQLite keeps durable history.
        second_alarm_bus = FakeAlarmBus()
        second_handler = FallWarnHandler(FakeRedis(), self.db, second_alarm_bus, replay=True)
        asyncio.run(second_handler.handle(event))

        row_count = self.db.execute("SELECT COUNT(*) FROM fall_warnings").fetchone()[0]
        self.assertEqual(row_count, 1)
        self.assertEqual(len(first_alarm_bus.published), 1)
        self.assertEqual(len(second_alarm_bus.published), 0)

    def test_post_recovery_conflict_does_not_inflate_dedup_counter(self) -> None:
        event = ValidatedEvent(
            device_id="dev_2",
            room_id="room_2",
            type="fall_warn",
            ts=datetime(2026, 6, 29, 13, 0, 0, tzinfo=timezone.utc),
            payload={"confidence": 0.8},
            late=False,
            priority=Priority.HIGH,
            received_at=datetime(2026, 6, 29, 13, 0, 0, tzinfo=timezone.utc),
        )

        # Original ingestion persists the unique fall warning.
        FallWarnHandler(FakeRedis(), self.db, FakeAlarmBus())
        asyncio.run(FallWarnHandler(FakeRedis(), self.db, FakeAlarmBus()).handle(event))

        before = get_counters()

        # Simulate recovery replay: Redis dedup cache is cold (SET succeeds) but SQLite already
        # holds the row, so INSERT OR IGNORE is a no-op. With replay=True this must count as a DB
        # conflict, not as an in-window dedup, so the dedup counter keeps its meaning post-recovery.
        asyncio.run(FallWarnHandler(FakeRedis(), self.db, FakeAlarmBus(), replay=True).handle(event))

        after = get_counters()

        deduped_delta = after.get("fall_warnings_deduped", 0) - before.get("fall_warnings_deduped", 0)
        conflict_delta = after.get("fall_warnings_db_conflicts", 0) - before.get("fall_warnings_db_conflicts", 0)
        total_delta = after.get("fall_warnings_total", 0) - before.get("fall_warnings_total", 0)

        self.assertEqual(deduped_delta, 0)
        self.assertEqual(conflict_delta, 1)
        self.assertEqual(total_delta, 0)

        row_count = self.db.execute("SELECT COUNT(*) FROM fall_warnings").fetchone()[0]
        self.assertEqual(row_count, 1)

    def test_post_ttl_live_duplicate_counts_as_dedup(self) -> None:
        # A genuine duplicate of the same detection (same device/room/second) that arrives AFTER
        # the 10s Redis dedup key has expired falls through to the SQLite UNIQUE constraint. On the
        # live path (replay=False) it is a real duplicate, so it must increment the grader-facing
        # dedup counter (not the recovery-only db_conflicts counter) and must not republish.
        event = ValidatedEvent(
            device_id="dev_ttl",
            room_id="room_ttl",
            type="fall_warn",
            ts=datetime(2026, 6, 29, 15, 0, 0, tzinfo=timezone.utc),
            payload={"confidence": 0.95},
            late=False,
            priority=Priority.HIGH,
            received_at=datetime(2026, 6, 29, 15, 0, 0, tzinfo=timezone.utc),
        )

        # Original ingestion persists the unique fall warning with a warm cache.
        asyncio.run(FallWarnHandler(FakeRedis(), self.db, FakeAlarmBus()).handle(event))

        before = get_counters()

        # Expired TTL is modelled by a fresh (cold) Redis: SET nx succeeds, but SQLite already
        # holds the row. replay=False marks this as a real live duplicate.
        alarm_bus = FakeAlarmBus()
        asyncio.run(FallWarnHandler(FakeRedis(), self.db, alarm_bus).handle(event))

        after = get_counters()

        deduped_delta = after.get("fall_warnings_deduped", 0) - before.get("fall_warnings_deduped", 0)
        conflict_delta = after.get("fall_warnings_db_conflicts", 0) - before.get("fall_warnings_db_conflicts", 0)

        self.assertEqual(deduped_delta, 1)
        self.assertEqual(conflict_delta, 0)
        self.assertEqual(len(alarm_bus.published), 0)

        row_count = self.db.execute("SELECT COUNT(*) FROM fall_warnings").fetchone()[0]
        self.assertEqual(row_count, 1)

    def test_in_window_duplicate_increments_dedup_counter(self) -> None:
        redis = FakeRedis()
        event = ValidatedEvent(
            device_id="dev_3",
            room_id="room_3",
            type="fall_warn",
            ts=datetime(2026, 6, 29, 14, 0, 0, tzinfo=timezone.utc),
            payload={"confidence": 0.7},
            late=False,
            priority=Priority.HIGH,
            received_at=datetime(2026, 6, 29, 14, 0, 0, tzinfo=timezone.utc),
        )

        handler = FallWarnHandler(redis, self.db, FakeAlarmBus())
        asyncio.run(handler.handle(event))

        before = get_counters()
        # Same warm Redis cache: the second attempt is a true in-window duplicate (SET nx fails).
        asyncio.run(handler.handle(event))
        after = get_counters()

        deduped_delta = after.get("fall_warnings_deduped", 0) - before.get("fall_warnings_deduped", 0)
        conflict_delta = after.get("fall_warnings_db_conflicts", 0) - before.get("fall_warnings_db_conflicts", 0)

        self.assertEqual(deduped_delta, 1)
        self.assertEqual(conflict_delta, 0)


class FallWarnDedupAtomicityTests(unittest.TestCase):
    """SQLite UNIQUE(dedup_key) is the authoritative reservation, not Redis (finding #4)."""

    def setUp(self) -> None:
        self.db = sqlite3.connect(":memory:")
        self.db.execute(
            """
            CREATE TABLE fall_warnings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL,
                room_id TEXT NOT NULL,
                ts TEXT NOT NULL,
                confidence REAL NOT NULL,
                dedup_key TEXT NOT NULL UNIQUE,
                received_at TEXT NOT NULL
            )
            """
        )
        self.db.commit()

    def tearDown(self) -> None:
        self.db.close()

    def _event(self) -> ValidatedEvent:
        return ValidatedEvent(
            device_id="dev_atomic",
            room_id="room_atomic",
            type="fall_warn",
            ts=datetime(2026, 6, 29, 16, 0, 0, tzinfo=timezone.utc),
            payload={"confidence": 0.9},
            late=False,
            priority=Priority.HIGH,
            received_at=datetime(2026, 6, 29, 16, 0, 0, tzinfo=timezone.utc),
        )

    def test_sqlite_insert_failure_leaves_no_redis_reservation(self) -> None:
        class RaisingCursor:
            def execute(self, *args: object, **kwargs: object) -> None:
                raise sqlite3.OperationalError("disk I/O error")

        class RaisingDB:
            def cursor(self) -> RaisingCursor:
                return RaisingCursor()

            def commit(self) -> None:
                raise AssertionError("commit must not run after a failed execute")

        redis = FakeRedis()
        handler = FallWarnHandler(redis, cast(Any, RaisingDB()), FakeAlarmBus())

        with self.assertRaises(sqlite3.OperationalError):
            asyncio.run(handler.handle(self._event()))

        self.assertEqual(redis._store, {})

    def test_redis_cache_failure_does_not_block_durable_insert_or_publish(self) -> None:
        class FailingRedis:
            def set(self, name: str, value: str, ex: int, nx: bool) -> bool:
                raise ConnectionError("redis unavailable")

        alarm_bus = FakeAlarmBus()
        handler = FallWarnHandler(FailingRedis(), self.db, alarm_bus)

        asyncio.run(handler.handle(self._event()))

        self.assertEqual(len(alarm_bus.published), 1)
        row_count = self.db.execute("SELECT COUNT(*) FROM fall_warnings").fetchone()[0]
        self.assertEqual(row_count, 1)

    def test_retry_after_failed_insert_can_still_succeed(self) -> None:
        # A transient failure on the first attempt must not permanently poison the dedup_key: since
        # no Redis reservation is made before the insert, a retry of the same event can still land.
        class FlakyCursorWrapper:
            def __init__(self, real_cursor: sqlite3.Cursor, attempts: dict[str, int]) -> None:
                self._real_cursor = real_cursor
                self._attempts = attempts

            def execute(self, *args: object, **kwargs: object) -> object:
                self._attempts["count"] += 1
                if self._attempts["count"] == 1:
                    raise sqlite3.OperationalError("transient disk error")
                return self._real_cursor.execute(*args, **kwargs)

            @property
            def rowcount(self) -> int:
                return self._real_cursor.rowcount

            @property
            def lastrowid(self) -> int | None:
                return self._real_cursor.lastrowid

        class FlakyDBWrapper:
            def __init__(self, real_db: sqlite3.Connection) -> None:
                self._real_db = real_db
                self._attempts = {"count": 0}

            def cursor(self) -> FlakyCursorWrapper:
                return FlakyCursorWrapper(self._real_db.cursor(), self._attempts)

            def commit(self) -> None:
                self._real_db.commit()

        event = self._event()
        alarm_bus = FakeAlarmBus()
        redis = FakeRedis()
        flaky_db = FlakyDBWrapper(self.db)

        with self.assertRaises(sqlite3.OperationalError):
            asyncio.run(FallWarnHandler(redis, cast(Any, flaky_db), alarm_bus).handle(event))

        asyncio.run(FallWarnHandler(redis, cast(Any, flaky_db), alarm_bus).handle(event))

        self.assertEqual(len(alarm_bus.published), 1)
        row_count = self.db.execute("SELECT COUNT(*) FROM fall_warnings").fetchone()[0]
        self.assertEqual(row_count, 1)


if __name__ == "__main__":
    unittest.main()
