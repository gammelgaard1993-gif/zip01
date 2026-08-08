from __future__ import annotations

import asyncio
import json
import os
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any, cast
from unittest.mock import patch

from redis import Redis
from redis.exceptions import WatchError

from models import Priority, ValidatedEvent
from processing.handlers.presence import PresenceHandler
from tests.fakes import FakeRedis, _Pipeline


def _presence_event(
    device_id: str,
    room_id: str,
    ts: datetime,
    in_room: bool,
) -> ValidatedEvent:
    return ValidatedEvent(
        device_id=device_id,
        room_id=room_id,
        type="presence",
        ts=ts,
        payload={"in_room": in_room},
        late=False,
        priority=Priority.NORMAL,
        received_at=datetime.now(timezone.utc),
    )


class _ConflictPipeline(_Pipeline):
    def execute(self) -> list[object]:
        redis = cast("_ConflictRedis", self._redis)
        if redis.inject_conflict:
            redis.inject_conflict = False
            redis.hashes[redis.state_key] = {
                "in_room": "true",
                "ts": redis.newer_ts.isoformat(),
                "tie_breaker": "dev_new:1",
            }
            transition = json.dumps(
                {
                    "ts": redis.newer_ts.isoformat(),
                    "in_room": True,
                    "tie_breaker": "dev_new:1",
                }
            )
            redis.zsets.setdefault(redis.transitions_key, {})[transition] = redis.newer_ts.timestamp()
            self.reset()
            raise WatchError("simulated concurrent update")
        return super().execute()


class _ConflictRedis(FakeRedis):
    def __init__(self, room_id: str, newer_ts: datetime) -> None:
        super().__init__()
        self.inject_conflict = True
        self.newer_ts = newer_ts
        self.state_key = f"room:{room_id}:presence"
        self.transitions_key = f"room:{room_id}:occupancy"

    def pipeline(self) -> _Pipeline:
        return _ConflictPipeline(self)


class PresenceAtomicUpdateTests(unittest.TestCase):
    @patch("processing.handlers.presence.increment_counter")
    def test_watch_conflict_rechecks_state_before_older_update(
        self, increment_counter: Any
    ) -> None:
        room_id = "room_atomic"
        newer_ts = datetime.now(timezone.utc)
        older_ts = newer_ts - timedelta(seconds=10)
        redis_client = _ConflictRedis(room_id, newer_ts)
        handler = PresenceHandler(cast(Any, redis_client))

        handler._apply(_presence_event("dev_old", room_id, older_ts, False))

        state = redis_client.hgetall(redis_client.state_key)
        self.assertEqual(state["ts"], newer_ts.isoformat())
        self.assertEqual(state["in_room"], "true")
        self.assertEqual(len(redis_client.zsets[redis_client.transitions_key]), 2)
        increment_counter.assert_called_once_with("presence_watch_conflicts")

    def test_equal_timestamps_converge_independent_of_arrival_order(self) -> None:
        room_id = "room_equal"
        timestamp = datetime.now(timezone.utc)
        lower = _presence_event("dev_a", room_id, timestamp, True)
        higher = _presence_event("dev_z", room_id, timestamp, False)

        first_redis = FakeRedis()
        first_handler = PresenceHandler(cast(Any, first_redis))
        first_handler._apply(lower)
        first_handler._apply(higher)

        second_redis = FakeRedis()
        second_handler = PresenceHandler(cast(Any, second_redis))
        second_handler._apply(higher)
        second_handler._apply(lower)

        state_key = f"room:{room_id}:presence"
        transitions_key = f"room:{room_id}:occupancy"
        self.assertEqual(first_redis.hgetall(state_key), second_redis.hgetall(state_key))
        self.assertEqual(first_redis.hgetall(state_key)["in_room"], "false")
        self.assertEqual(first_redis.zsets[transitions_key], second_redis.zsets[transitions_key])
        self.assertEqual(len(first_redis.zsets[transitions_key]), 1)


@unittest.skipUnless(os.getenv("TEST_REDIS_URL"), "TEST_REDIS_URL is not configured")
class PresenceRealRedisConcurrencyTests(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_same_room_updates_keep_newest_state(self) -> None:
        redis_url = os.environ["TEST_REDIS_URL"]
        first_client = Redis.from_url(redis_url, decode_responses=True)
        second_client = Redis.from_url(redis_url, decode_responses=True)
        executor = ThreadPoolExecutor(max_workers=2)
        room_id = f"presence-test-{uuid.uuid4().hex}"
        state_key = f"room:{room_id}:presence"
        transitions_key = f"room:{room_id}:occupancy"
        first_client.delete(state_key, transitions_key)
        try:
            newer_ts = datetime.now(timezone.utc)
            older_ts = newer_ts - timedelta(seconds=10)
            older_handler = PresenceHandler(first_client, executor=executor)
            newer_handler = PresenceHandler(second_client, executor=executor)

            for _ in range(25):
                first_client.delete(state_key, transitions_key)
                await asyncio.gather(
                    older_handler.handle(
                        _presence_event("dev_old", room_id, older_ts, False)
                    ),
                    newer_handler.handle(
                        _presence_event("dev_new", room_id, newer_ts, True)
                    ),
                )
                state = first_client.hgetall(state_key)
                self.assertEqual(state["ts"], newer_ts.isoformat())
                self.assertEqual(state["in_room"], "true")
                self.assertEqual(first_client.zcard(transitions_key), 2)
        finally:
            first_client.delete(state_key, transitions_key)
            executor.shutdown(wait=True)
            first_client.close()
            second_client.close()


if __name__ == "__main__":
    unittest.main()