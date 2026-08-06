from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any, cast

from api.routes.occupancy import room_occupancy
from models import Priority, ValidatedEvent
from processing.handlers.presence import PresenceHandler
from tests.fakes import FakeRedis
class OccupancyRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_returns_zero_when_room_has_no_presence(self) -> None:
        redis_client = FakeRedis()

        response = await room_occupancy("room_empty", window="1m", redis_client=cast(Any, redis_client), redis_executor=None)

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

        response = await room_occupancy("room_1", window="1m", redis_client=cast(Any, redis_client), redis_executor=None)

        self.assertFalse(response.in_room)
        self.assertAlmostEqual(response.occupied_pct, 0.5, delta=0.08)


class OccupancyInitialStateRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_one_hour_window_recovers_initial_state_from_anchor(self) -> None:
        # F-02 finding #6: a room occupied continuously since before the 1h window must report
        # ~100% occupancy. The presence handler keeps one transition before the window as an
        # initial-state anchor instead of trimming it, so the occupancy query can recover the
        # starting state rather than defaulting to "not occupied".
        redis = FakeRedis()
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

        response = await room_occupancy("room_1", window="1h", redis_client=cast(Any, redis), redis_executor=None)

        self.assertTrue(response.in_room)
        self.assertAlmostEqual(response.occupied_pct, 1.0, delta=0.01)


if __name__ == "__main__":
    unittest.main()
