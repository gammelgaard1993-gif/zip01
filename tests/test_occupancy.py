from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any, cast

from api.routes.occupancy import room_occupancy
from models import Priority, ValidatedEvent
from processing.handlers.presence import PresenceHandler


class FakeRedis:
    def __init__(self) -> None:
        self.zsets: dict[str, list[tuple[str, float]]] = {}
        self.hashes: dict[str, dict[str, str]] = {}

    def zadd(self, name: str, mapping: dict[str, float]) -> None:
        entries = self.zsets.setdefault(name, [])
        for member, score in mapping.items():
            entries.append((member, score))
        entries.sort(key=lambda item: item[1])

    def hset(self, name: str, mapping: dict[str, str]) -> None:
        self.hashes[name] = dict(mapping)

    def zrangebyscore(
        self,
        name: str,
        min: float | str,
        max: float | str,
        start: int | None = None,
        num: int | None = None,
        withscores: bool = False,
    ) -> list[str]:
        min_score = float(min)
        max_score = float(max)
        values = [member for member, score in self.zsets.get(name, []) if min_score <= score <= max_score]
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
    ) -> list[str]:
        max_score = float(max)
        min_score = float(min) if min != "-inf" else float("-inf")
        values = [member for member, score in self.zsets.get(name, []) if min_score <= score <= max_score]
        values.reverse()
        if start is not None and num is not None:
            return values[start : start + num]
        return values

    def hgetall(self, name: str) -> dict[str, str]:
        return dict(self.hashes.get(name, {}))


class OccupancyRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_returns_zero_when_room_has_no_presence(self) -> None:
        redis_client = FakeRedis()

        response = await room_occupancy("room_empty", window="1m", redis_client=cast(Any, redis_client))

        self.assertFalse(response.in_room)
        self.assertEqual(response.occupied_pct, 0.0)
        self.assertEqual(response.window_seconds, 60)

    async def test_computes_occupancy_percentage_from_transitions(self) -> None:
        redis_client = FakeRedis()
        now = datetime.now(timezone.utc)
        key = "room:room_1:occupancy"

        # In 1 minute window, occupancy is true from now-50s to now-20s => 30/60 = 0.5.
        enter_ts = now - timedelta(seconds=50)
        exit_ts = now - timedelta(seconds=20)
        enter_payload = json.dumps({"ts": enter_ts.isoformat(), "in_room": True})
        exit_payload = json.dumps({"ts": exit_ts.isoformat(), "in_room": False})
        redis_client.zadd(key, {enter_payload: enter_ts.timestamp()})
        redis_client.zadd(key, {exit_payload: exit_ts.timestamp()})
        redis_client.hset("room:room_1:presence", {"in_room": "false", "ts": exit_ts.isoformat()})

        response = await room_occupancy("room_1", window="1m", redis_client=cast(Any, redis_client))

        self.assertFalse(response.in_room)
        self.assertAlmostEqual(response.occupied_pct, 0.5, delta=0.08)


class _CombinedPipeline:
    def __init__(self, redis: "CombinedFakeRedis") -> None:
        self.redis = redis
        self.ops: list[tuple[str, tuple[Any, ...]]] = []

    def zadd(self, name: str, mapping: dict[str, float]) -> "_CombinedPipeline":
        self.ops.append(("zadd", (name, mapping)))
        return self

    def hset(self, name: str, mapping: dict[str, str]) -> "_CombinedPipeline":
        self.ops.append(("hset", (name, mapping)))
        return self

    def zremrangebyscore(self, name: str, min: float, max: float) -> "_CombinedPipeline":
        self.ops.append(("zremrangebyscore", (name, min, max)))
        return self

    def execute(self) -> object:
        for op, args in self.ops:
            getattr(self.redis, op)(*args)
        self.ops.clear()
        return True


class CombinedFakeRedis:
    """Supports both the PresenceHandler write path and the occupancy read route."""

    def __init__(self) -> None:
        self.zsets: dict[str, dict[str, float]] = {}
        self.hashes: dict[str, dict[str, str]] = {}

    def pipeline(self) -> _CombinedPipeline:
        return _CombinedPipeline(self)

    def zadd(self, name: str, mapping: dict[str, float]) -> object:
        self.zsets.setdefault(name, {}).update(mapping)
        return True

    def hset(self, name: str, mapping: dict[str, str]) -> object:
        self.hashes[name] = dict(mapping)
        return True

    def hgetall(self, name: str) -> dict[str, str]:
        return dict(self.hashes.get(name, {}))

    def zremrangebyscore(self, name: str, min: float, max: float) -> object:
        zset = self.zsets.get(name, {})
        for member in [m for m, score in zset.items() if float(min) <= score <= float(max)]:
            zset.pop(member, None)
        return True

    def zrangebyscore(
        self,
        name: str,
        min: float | str,
        max: float | str,
        start: int | None = None,
        num: int | None = None,
        withscores: bool = False,
    ) -> list[str]:
        lo = float("-inf") if min == "-inf" else float(min)
        hi = float("inf") if max == "+inf" else float(max)
        ordered = sorted(self.zsets.get(name, {}).items(), key=lambda item: item[1])
        values = [member for member, score in ordered if lo <= score <= hi]
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
        hi = float("inf") if max == "+inf" else float(max)
        lo = float("-inf") if min == "-inf" else float(min)
        ordered = sorted(self.zsets.get(name, {}).items(), key=lambda item: item[1], reverse=True)
        filtered = [(member, score) for member, score in ordered if lo <= score <= hi]
        if start is not None and num is not None:
            filtered = filtered[start : start + num]
        if withscores:
            return filtered
        return [member for member, _ in filtered]


class OccupancyInitialStateRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_one_hour_window_recovers_initial_state_from_anchor(self) -> None:
        # F-02 finding #6: a room occupied continuously since before the 1h window must report
        # ~100% occupancy. The presence handler keeps one transition before the window as an
        # initial-state anchor instead of trimming it, so the occupancy query can recover the
        # starting state rather than defaulting to "not occupied".
        redis = CombinedFakeRedis()
        handler = PresenceHandler(cast(Any, redis))
        now = datetime.now(timezone.utc)

        # Single "entered" transition 90 minutes ago — older than the 1h (3600s) window cutoff.
        enter_ts = now - timedelta(minutes=90)
        await handler.handle(
            ValidatedEvent(
                device_id="dev_1",
                room_id="room_1",
                type="presence",
                ts=enter_ts,
                payload={"in_room": True},
                late=True,
                priority=Priority.NORMAL,
                received_at=now,
            )
        )

        # The anchor transition must survive the trim instead of being deleted as pre-cutoff.
        self.assertEqual(len(redis.zsets["room:room_1:occupancy"]), 1)

        response = await room_occupancy("room_1", window="1h", redis_client=cast(Any, redis))

        self.assertTrue(response.in_room)
        self.assertAlmostEqual(response.occupied_pct, 1.0, delta=0.01)


if __name__ == "__main__":
    unittest.main()
