from __future__ import annotations

import json
import sqlite3
import unittest
from datetime import datetime, timedelta, timezone
from fnmatch import fnmatch
from typing import Any, cast

from core.recovery import RecoveryManager, SnapshotState


class FakeAlarmBus:
    async def publish(self, alarm: object) -> None:
        return None


class FakeRedis:
    def __init__(self) -> None:
        self.strings: dict[str, str] = {}
        self.hashes: dict[str, dict[str, str]] = {}
        self.zsets: dict[str, dict[str, float]] = {}

    def keys(self, pattern: str) -> list[str]:
        keys: list[str] = []
        keys.extend([key for key in self.strings if fnmatch(key, pattern)])
        keys.extend([key for key in self.hashes if fnmatch(key, pattern)])
        keys.extend([key for key in self.zsets if fnmatch(key, pattern)])
        return keys

    def scan_iter(self, match: str, count: int = 100) -> list[str]:
        return self.keys(match)

    def type(self, name: str) -> str:
        if name in self.strings:
            return "string"
        if name in self.hashes:
            return "hash"
        if name in self.zsets:
            return "zset"
        return "none"

    def get(self, name: str) -> str | None:
        return self.strings.get(name)

    def hgetall(self, name: str) -> dict[str, str]:
        return dict(self.hashes.get(name, {}))

    def zrange(self, name: str, start: int, end: int, withscores: bool = False) -> list[tuple[str, float]]:
        entries = sorted(self.zsets.get(name, {}).items(), key=lambda item: item[1])
        return entries

    def set(self, name: str, value: str) -> object:
        self.strings[name] = value
        return True

    def hset(self, name: str, mapping: dict[str, str]) -> object:
        self.hashes[name] = dict(mapping)
        return True

    def zadd(self, name: str, mapping: dict[str, float]) -> object:
        zset = self.zsets.setdefault(name, {})
        zset.update(mapping)
        return True

    def delete(self, *names: str) -> int:
        removed = 0
        for name in names:
            if name in self.strings:
                del self.strings[name]
                removed += 1
            if name in self.hashes:
                del self.hashes[name]
                removed += 1
            if name in self.zsets:
                del self.zsets[name]
                removed += 1
        return removed


class _FakePipeline:
    def __init__(self, redis: "PipelineFakeRedis") -> None:
        self._redis = redis
        self._ops: list[tuple[str, tuple[Any, ...]]] = []

    def set(self, name: str, value: str) -> object:
        self._ops.append(("set", (name, value)))
        return self

    def zadd(self, name: str, mapping: dict[str, float]) -> object:
        self._ops.append(("zadd", (name, mapping)))
        return self

    def zremrangebyscore(self, name: str, min: float, max: float) -> object:
        self._ops.append(("zremrangebyscore", (name, min, max)))
        return self

    def execute(self) -> object:
        for op, args in self._ops:
            if op == "set":
                name, value = args
                self._redis.strings[name] = value
            elif op == "zadd":
                name, mapping = args
                zset = self._redis.zsets.setdefault(name, {})
                zset.update(mapping)
            elif op == "zremrangebyscore":
                name, min_score, max_score = args
                zset = self._redis.zsets.get(name, {})
                for member in [m for m, score in zset.items() if min_score <= score <= max_score]:
                    del zset[member]
        self._ops = []
        return None


class PipelineFakeRedis(FakeRedis):
    def get(self, name: str) -> str | None:
        return self.strings.get(name)

    def pipeline(self) -> _FakePipeline:
        return _FakePipeline(self)


class _ReadPipeline:
    def __init__(self, redis: "CaptureFakeRedis") -> None:
        self._redis = redis
        self._ops: list[tuple[str, tuple[Any, ...]]] = []

    def get(self, name: str) -> "_ReadPipeline":
        self._ops.append(("get", (name,)))
        return self

    def hgetall(self, name: str) -> "_ReadPipeline":
        self._ops.append(("hgetall", (name,)))
        return self

    def zrange(self, name: str, start: int, end: int, withscores: bool = False) -> "_ReadPipeline":
        self._ops.append(("zrange", (name, start, end, withscores)))
        return self

    def execute(self) -> list[Any]:
        results: list[Any] = []
        for op, args in self._ops:
            if op == "get":
                results.append(self._redis.get(args[0]))
            elif op == "hgetall":
                results.append(self._redis.hgetall(args[0]))
            elif op == "zrange":
                results.append(self._redis.zrange(args[0], args[1], args[2], withscores=args[3]))
        self._ops = []
        return results


class CaptureFakeRedis(FakeRedis):
    def get(self, name: str) -> str | None:
        return self.strings.get(name)

    def pipeline(self) -> _ReadPipeline:
        return _ReadPipeline(self)


class RecoveryManagerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.db = sqlite3.connect(":memory:")
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
            CREATE TABLE state_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_ts TEXT NOT NULL,
                state_json TEXT NOT NULL
            );
            """
        )
        self.db.commit()

    def tearDown(self) -> None:
        self.db.close()

    async def test_capture_state_scans_managed_keys_and_pipelines_reads(self) -> None:
        # Finding #5: capture uses SCAN (not KEYS) and a single pipelined batch of value reads.
        # Only the four managed key shapes are captured; unrelated keys are ignored.
        redis = CaptureFakeRedis()
        redis.strings["device:dev_1:last_heartbeat"] = "2026-06-30T00:00:00+00:00"
        redis.hashes["room:room_1:presence"] = {"in_room": "true", "ts": "2026-06-30T00:00:00+00:00"}
        redis.zsets["device:dev_1:heartbeats"] = {"2026-06-30T00:00:00+00:00": 1000.0}
        redis.zsets["room:room_1:occupancy"] = {
            json.dumps({"ts": "2026-06-30T00:00:00+00:00", "in_room": True}): 1000.0
        }
        # Keys outside the managed patterns must not be captured.
        redis.strings["unrelated:key"] = "ignored"

        manager = RecoveryManager(self.db, cast(Any, redis), cast(Any, FakeAlarmBus()))
        state = cast(Any, manager)._capture_state()

        self.assertEqual(state["strings"], {"device:dev_1:last_heartbeat": "2026-06-30T00:00:00+00:00"})
        self.assertEqual(state["hashes"]["room:room_1:presence"]["in_room"], "true")
        self.assertIn("device:dev_1:heartbeats", state["zsets"])
        self.assertIn("room:room_1:occupancy", state["zsets"])
        self.assertEqual(state["zsets"]["device:dev_1:heartbeats"][0]["score"], 1000.0)
        self.assertNotIn("unrelated:key", state["strings"])

    async def test_restore_state_applies_snapshot_and_clears_stale_hot_keys(self) -> None:
        fake_redis = FakeRedis()
        fake_redis.strings["device:stale:last_heartbeat"] = "2026-06-29T00:00:00+00:00"

        snapshot_ts = datetime(2026, 6, 29, 12, 0, 0, tzinfo=timezone.utc).isoformat()
        snapshot_state: SnapshotState = {
            "strings": {"device:dev_1:last_heartbeat": snapshot_ts},
            "hashes": {"room:room_1:presence": {"in_room": "true", "ts": snapshot_ts}},
            "zsets": {
                "room:room_1:occupancy": [
                    {
                        "member": json.dumps({"ts": snapshot_ts, "in_room": True}),
                        "score": datetime.fromisoformat(snapshot_ts).timestamp(),
                    }
                ]
            },
        }
        self.db.execute(
            "INSERT INTO state_snapshots (snapshot_ts, state_json) VALUES (?, ?)",
            (snapshot_ts, json.dumps(snapshot_state)),
        )
        self.db.commit()

        manager = RecoveryManager(self.db, cast(Any, fake_redis), cast(Any, FakeAlarmBus()))
        await manager.restore_state()

        self.assertNotIn("device:stale:last_heartbeat", fake_redis.strings)
        self.assertEqual(fake_redis.strings["device:dev_1:last_heartbeat"], snapshot_ts)
        self.assertEqual(fake_redis.hashes["room:room_1:presence"]["in_room"], "true")
        self.assertIn("room:room_1:occupancy", fake_redis.zsets)

    async def test_replay_events_uses_inclusive_received_at_cutoff(self) -> None:
        fake_redis = FakeRedis()
        manager = RecoveryManager(self.db, cast(Any, fake_redis), cast(Any, FakeAlarmBus()))

        now = datetime.now(timezone.utc)
        snapshot_ts = (now - timedelta(minutes=2)).isoformat()

        # Received BEFORE the snapshot → already captured in it → must be skipped.
        before_received = (now - timedelta(minutes=3)).isoformat()
        self.db.execute(
            "INSERT INTO events (device_id, room_id, type, ts, payload, received_at, late) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("dev_before", "room_1", "motion", before_received, json.dumps({"x": 1}), before_received, 0),
        )
        # Received at EXACTLY the snapshot ts → cutoff is inclusive (>=) → must be replayed.
        self.db.execute(
            "INSERT INTO events (device_id, room_id, type, ts, payload, received_at, late) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("dev_boundary", "room_1", "motion", snapshot_ts, json.dumps({"x": 2}), snapshot_ts, 0),
        )
        # Received AFTER the snapshot → must be replayed.
        after_received = (now - timedelta(minutes=1)).isoformat()
        self.db.execute(
            "INSERT INTO events (device_id, room_id, type, ts, payload, received_at, late) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("dev_after", "room_1", "motion", after_received, json.dumps({"x": 3}), after_received, 0),
        )
        self.db.commit()

        # The cutoff is on received_at (ingestion order), inclusive of the boundary.
        replayed = await cast(Any, manager)._replay_events(since_ts=snapshot_ts)

        self.assertEqual(replayed, 2)

    async def test_replay_includes_late_event_ingested_after_snapshot(self) -> None:
        # F-01/F-02 + §4.4/§11 recovery regression: a late event (old device ts, but ingested
        # AFTER the snapshot) must survive a hard-kill recovery. Because the replay cutoff is on
        # received_at (ingestion order) rather than ts (device clock), the late event is not
        # silently dropped even though its ts predates the snapshot.
        fake_redis = FakeRedis()
        manager = RecoveryManager(self.db, cast(Any, fake_redis), cast(Any, FakeAlarmBus()))

        now = datetime.now(timezone.utc)
        snapshot_ts = (now - timedelta(minutes=5)).isoformat()

        # Late event: device ts is 20 minutes old (well within the 1h late window) but it was
        # received only 1 minute ago — AFTER the snapshot taken 5 minutes ago. It is NOT in the
        # snapshot, so it must be replayed.
        late_ts = (now - timedelta(minutes=20)).isoformat()
        late_received = (now - timedelta(minutes=1)).isoformat()
        self.db.execute(
            "INSERT INTO events (device_id, room_id, type, ts, payload, received_at, late) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("dev_late", "room_1", "motion", late_ts, json.dumps({"x": 1}), late_received, 1),
        )
        # Already-captured event: received before the snapshot, so it must NOT be replayed again.
        captured_ts = (now - timedelta(minutes=30)).isoformat()
        captured_received = (now - timedelta(minutes=10)).isoformat()
        self.db.execute(
            "INSERT INTO events (device_id, room_id, type, ts, payload, received_at, late) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("dev_old", "room_1", "motion", captured_ts, json.dumps({"x": 0}), captured_received, 0),
        )
        self.db.commit()

        replayed = await cast(Any, manager)._replay_events(since_ts=snapshot_ts)

        # Only the late event (received after the snapshot) is replayed; the already-captured
        # event (received before the snapshot) is correctly skipped. Under a ts-based cutoff the
        # late event would be dropped and this would be 0.
        self.assertEqual(replayed, 1)

    async def test_replay_event_at_exact_received_at_boundary_is_not_dropped(self) -> None:
        fake_redis = FakeRedis()
        manager = RecoveryManager(self.db, cast(Any, fake_redis), cast(Any, FakeAlarmBus()))

        now = datetime.now(timezone.utc)
        snapshot_ts = (now - timedelta(minutes=1)).isoformat()
        # A single late event (old ts) received at exactly the snapshot ts must still be replayed.
        late_ts = (now - timedelta(minutes=10)).isoformat()
        self.db.execute(
            "INSERT INTO events (device_id, room_id, type, ts, payload, received_at, late) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("dev_boundary", "room_1", "motion", late_ts, json.dumps({"x": 1}), snapshot_ts, 1),
        )
        self.db.commit()

        replayed = await cast(Any, manager)._replay_events(since_ts=snapshot_ts)

        self.assertEqual(replayed, 1)

    async def test_recovery_replay_applies_events_in_ts_order(self) -> None:
        # F-05: recovery reads events ORDER BY ts ASC, so the final hot state must reflect ts
        # ordering even when events were written to SQLite out of chronological order. The
        # timestamp-aware HeartbeatHandler keeps the latest ts as last_heartbeat.
        fake_redis = PipelineFakeRedis()
        manager = RecoveryManager(self.db, cast(Any, fake_redis), cast(Any, FakeAlarmBus()))

        now = datetime.now(timezone.utc)
        earliest = (now - timedelta(seconds=30)).replace(microsecond=0)
        middle = (now - timedelta(seconds=20)).replace(microsecond=0)
        latest = (now - timedelta(seconds=10)).replace(microsecond=0)
        received_at = now.isoformat()

        # Insert deliberately out of ts order.
        for ts in (middle, earliest, latest):
            self.db.execute(
                "INSERT INTO events (device_id, room_id, type, ts, payload, received_at, late) VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("dev_hb", "room_1", "heartbeat", ts.isoformat(), json.dumps({}), received_at, 0),
            )
        self.db.commit()

        replayed = await cast(Any, manager)._replay_events(since_ts=None)

        self.assertEqual(replayed, 3)
        self.assertEqual(fake_redis.strings["device:dev_hb:last_heartbeat"], latest.isoformat())
        heartbeats = fake_redis.zsets["device:dev_hb:heartbeats"]
        self.assertEqual(
            sorted(heartbeats.keys()),
            sorted(ts.isoformat() for ts in (earliest, middle, latest)),
        )


if __name__ == "__main__":
    unittest.main()
