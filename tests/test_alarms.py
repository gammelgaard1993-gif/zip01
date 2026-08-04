from __future__ import annotations

import asyncio
import json
import sqlite3
import unittest
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, cast
from unittest.mock import patch

from fastapi import HTTPException

from api.routes.alarms import alarms_stream, get_alarms
from models import AlarmEvent
from processing.alarm_bus import AlarmBus


async def _next_chunk(iterator: AsyncIterator[str]) -> str:
    return await anext(iterator)


async def _json_response(response: object) -> dict[str, object]:
    iterator = cast(AsyncIterator[str], cast(object, response).body_iterator)
    chunks = [chunk async for chunk in iterator]
    return cast(dict[str, object], json.loads("".join(chunks)))


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
        payload = await _json_response(response)
        alarms = cast(list[dict[str, object]], payload["alarms"])

        self.assertEqual([item["device_id"] for item in alarms], ["dev_2", "dev_3"])
        self.assertEqual(alarms[0]["ts"], base.isoformat())

    async def test_invalid_since_returns_400(self) -> None:
        # NaN/inf/negative/out-of-range (or a value that overflows datetime.fromtimestamp) must be
        # rejected with 400. Before the fix these flowed straight into datetime.fromtimestamp,
        # raising an uncaught OverflowError/ValueError (or, for a negative, silently returning a
        # pre-epoch history) instead of a clean 400.
        for bad_since in (float("inf"), float("nan"), -1.0, 1e300):
            with self.subTest(since=bad_since):
                with self.assertRaises(HTTPException) as ctx:
                    await get_alarms(since=bad_since, room_id="room_1", db_connection=self.db)
                self.assertEqual(ctx.exception.status_code, 400)
                self.assertEqual(ctx.exception.detail, "invalid since")

    async def test_valid_since_returns_history(self) -> None:
        # Regression guard: the default (0.0) returns full history and a recent epoch filters
        # inclusively, both without raising.
        base = datetime(2026, 6, 29, 12, 0, 0, tzinfo=timezone.utc)
        rows = [
            ("dev_1", "room_1", (base - timedelta(seconds=2)).isoformat(), 0.7, "v1", base.isoformat()),
            ("dev_2", "room_1", base.isoformat(), 0.8, "v2", base.isoformat()),
            ("dev_3", "room_1", (base + timedelta(seconds=2)).isoformat(), 0.9, "v3", base.isoformat()),
        ]
        self.db.executemany(
            "INSERT INTO fall_warnings (device_id, room_id, ts, confidence, dedup_key, received_at) VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )
        self.db.commit()

        full_response = await get_alarms(since=0.0, room_id="room_1", db_connection=self.db)
        full = await _json_response(full_response)
        full_alarms = cast(list[dict[str, object]], full["alarms"])
        self.assertEqual([item["device_id"] for item in full_alarms], ["dev_1", "dev_2", "dev_3"])
        self.assertEqual(full["since"], 0.0)

        recent_response = await get_alarms(since=base.timestamp(), room_id="room_1", db_connection=self.db)
        recent = await _json_response(recent_response)
        recent_alarms = cast(list[dict[str, object]], recent["alarms"])
        self.assertEqual([item["device_id"] for item in recent_alarms], ["dev_2", "dev_3"])
        self.assertEqual(recent["since"], base.timestamp())

    async def test_get_alarms_batches_complete_history_with_identical_timestamps(self) -> None:
        base = datetime(2026, 6, 29, 12, 30, 0, tzinfo=timezone.utc)
        rows = [
            (f"dev_{index}", "room_1", base.isoformat(), 0.8, f"page-{index}", base.isoformat())
            for index in range(5)
        ]
        self.db.executemany(
            "INSERT INTO fall_warnings (device_id, room_id, ts, confidence, dedup_key, received_at) VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )
        self.db.commit()

        with patch("api.routes.alarms.config.ALARM_REPLAY_BATCH_SIZE", 2):
            response = await get_alarms(since=0.0, room_id="room_1", db_connection=self.db)
            payload = await _json_response(response)

        alarms = cast(list[dict[str, object]], payload["alarms"])
        self.assertEqual([item["device_id"] for item in alarms], [f"dev_{index}" for index in range(5)])

    async def test_get_alarms_freezes_high_water_without_hiding_new_rows(self) -> None:
        base = datetime(2026, 6, 29, 12, 45, 0, tzinfo=timezone.utc)
        self.db.execute(
            "INSERT INTO fall_warnings (device_id, room_id, ts, confidence, dedup_key, received_at) VALUES (?, ?, ?, ?, ?, ?)",
            ("dev_existing", "room_1", base.isoformat(), 0.8, "snapshot-existing", base.isoformat()),
        )
        self.db.commit()

        response = await get_alarms(since=0.0, room_id="room_1", db_connection=self.db)
        iterator = cast(AsyncIterator[str], response.body_iterator)
        self.assertEqual(await _next_chunk(iterator), '{"alarms":[')
        first_alarm = await _next_chunk(iterator)
        self.assertIn('"device_id":"dev_existing"', first_alarm)

        self.db.execute(
            "INSERT INTO fall_warnings (device_id, room_id, ts, confidence, dedup_key, received_at) VALUES (?, ?, ?, ?, ?, ?)",
            ("dev_new", "room_1", (base + timedelta(seconds=1)).isoformat(), 0.9, "snapshot-new", base.isoformat()),
        )
        self.db.commit()
        remaining = "".join([chunk async for chunk in iterator])
        self.assertNotIn("dev_new", remaining)

        next_response = await get_alarms(since=0.0, room_id="room_1", db_connection=self.db)
        next_payload = await _json_response(next_response)
        next_alarms = cast(list[dict[str, object]], next_payload["alarms"])
        self.assertEqual([item["device_id"] for item in next_alarms], ["dev_existing", "dev_new"])

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

    async def test_alarm_stream_subscribes_before_replay_without_duplicates(self) -> None:
        room_id = "room_race"
        replay_ts = datetime(2026, 6, 29, 14, 0, 0, tzinfo=timezone.utc)

        class RaceAlarmBus(AlarmBus):
            async def subscribe(self, subscribed_room_id: str) -> asyncio.Queue[AlarmEvent]:
                queue = await super().subscribe(subscribed_room_id)
                cursor = self_outer.db.execute(
                    "INSERT INTO fall_warnings (device_id, room_id, ts, confidence, dedup_key, received_at) VALUES (?, ?, ?, ?, ?, ?)",
                    ("dev_race", room_id, replay_ts.isoformat(), 0.8, "race-insert", replay_ts.isoformat()),
                )
                self_outer.db.commit()
                await self.publish(
                    AlarmEvent(
                        device_id="dev_race",
                        room_id=room_id,
                        ts=replay_ts,
                        confidence=0.8,
                        received_at=replay_ts,
                        fall_warning_id=cursor.lastrowid,
                    )
                )
                return queue

        self_outer = self
        alarm_bus = RaceAlarmBus()

        response = await alarms_stream(
            room_id=room_id,
            since=replay_ts.isoformat(),
            db_connection=self.db,
            alarm_bus=alarm_bus,
        )
        iterator = cast(AsyncIterator[str], response.body_iterator)

        first = await asyncio.wait_for(_next_chunk(iterator), timeout=1.0)
        self.assertIn('"device_id": "dev_race"', first)

        inserted_ts = replay_ts + timedelta(seconds=2)
        cursor = self.db.execute(
            "INSERT INTO fall_warnings (device_id, room_id, ts, confidence, dedup_key, received_at) VALUES (?, ?, ?, ?, ?, ?)",
            ("dev_after_high_water", room_id, inserted_ts.isoformat(), 0.9, "race-new", inserted_ts.isoformat()),
        )
        self.db.commit()
        await alarm_bus.publish(
            AlarmEvent(
                device_id="dev_after_high_water",
                room_id=room_id,
                ts=inserted_ts,
                confidence=0.9,
                received_at=inserted_ts,
                fall_warning_id=cursor.lastrowid,
            )
        )

        second = await asyncio.wait_for(_next_chunk(iterator), timeout=1.0)
        self.assertIn('"device_id": "dev_after_high_water"', second)

    async def test_alarm_stream_replay_batches_identical_timestamps(self) -> None:
        alarm_bus = AlarmBus()
        room_id = "room_batches"
        replay_ts = datetime(2026, 6, 29, 15, 0, 0, tzinfo=timezone.utc)
        rows = [
            (f"dev_{index}", room_id, replay_ts.isoformat(), 0.8, f"batch-{index}", replay_ts.isoformat())
            for index in range(3)
        ]
        self.db.executemany(
            "INSERT INTO fall_warnings (device_id, room_id, ts, confidence, dedup_key, received_at) VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )
        self.db.commit()

        with patch("api.routes.alarms.config.ALARM_REPLAY_BATCH_SIZE", 2):
            response = await alarms_stream(
                room_id=room_id,
                since=replay_ts.isoformat(),
                db_connection=self.db,
                alarm_bus=alarm_bus,
            )
            iterator = cast(AsyncIterator[str], response.body_iterator)
            chunks = [
                await asyncio.wait_for(_next_chunk(iterator), timeout=1.0)
                for _ in rows
            ]
            await iterator.aclose()

        self.assertEqual(
            [json.loads(chunk.removeprefix("data: ").strip())["device_id"] for chunk in chunks],
            ["dev_0", "dev_1", "dev_2"],
        )


if __name__ == "__main__":
    unittest.main()
