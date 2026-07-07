from __future__ import annotations

import asyncio
import json
import sqlite3
import unittest
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, cast

from api.routes.alarms import alarms_stream, get_alarms
from models import AlarmEvent
from processing.alarm_bus import AlarmBus


async def _next_chunk(iterator: AsyncIterator[str]) -> str:
    return await anext(iterator)


class AlarmRoutesTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_get_alarms_since_is_inclusive_and_sorted(self) -> None:
        base = datetime(2026, 6, 29, 12, 0, 0, tzinfo=timezone.utc)
        rows = [
            ("dev_1", "room_1", (base - timedelta(seconds=2)).isoformat(), 0.7, "k1", base.isoformat()),
            ("dev_2", "room_1", base.isoformat(), 0.8, "k2", base.isoformat()),
            ("dev_3", "room_1", (base + timedelta(seconds=2)).isoformat(), 0.9, "k3", base.isoformat()),
        ]
        self.db.executemany(
            "INSERT INTO fall_warnings (device_id, room_id, ts, confidence, dedup_key, received_at) VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )
        self.db.commit()

        response = await get_alarms(since=base.timestamp(), room_id="room_1", db_connection=self.db)

        self.assertEqual([item.device_id for item in response.alarms], ["dev_2", "dev_3"])
        self.assertEqual(response.alarms[0].ts, base.isoformat())

    async def test_alarm_stream_replays_since_then_streams_live(self) -> None:
        alarm_bus = AlarmBus()
        room_id = "room_9"
        replay_ts = datetime(2026, 6, 29, 13, 0, 0, tzinfo=timezone.utc)
        self.db.execute(
            "INSERT INTO fall_warnings (device_id, room_id, ts, confidence, dedup_key, received_at) VALUES (?, ?, ?, ?, ?, ?)",
            ("dev_replay", room_id, replay_ts.isoformat(), 0.85, "k-replay", replay_ts.isoformat()),
        )
        self.db.commit()

        response = await alarms_stream(
            room_id=room_id,
            since=replay_ts.isoformat(),
            db_connection=self.db,
            alarm_bus=alarm_bus,
        )

        iterator = cast(AsyncIterator[str], response.body_iterator)

        first_chunk = await asyncio.wait_for(_next_chunk(iterator), timeout=1.0)
        self.assertIn('"device_id": "dev_replay"', first_chunk)

        live_alarm = AlarmEvent(
            device_id="dev_live",
            room_id=room_id,
            ts=replay_ts + timedelta(seconds=5),
            confidence=0.91,
            received_at=datetime.now(timezone.utc),
        )

        pending_next = asyncio.create_task(_next_chunk(iterator))
        await alarm_bus.publish(live_alarm)
        second_chunk = await asyncio.wait_for(pending_next, timeout=1.0)

        self.assertIn('"device_id": "dev_live"', second_chunk)
        live_payload = json.loads(second_chunk.removeprefix("data: ").strip())
        self.assertEqual(live_payload["room_id"], room_id)


if __name__ == "__main__":
    unittest.main()
