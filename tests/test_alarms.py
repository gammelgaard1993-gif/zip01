from __future__ import annotations

import asyncio
import json
import sqlite3
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, cast
from unittest.mock import patch

from fastapi import HTTPException
from fastapi.responses import StreamingResponse

from api.routes.alarms import alarms_stream, get_alarms
from core.metrics import get_alarm_path_stage_latency_ms_p95, get_counters
from models import AlarmEvent
from processing.alarm_bus import AlarmBus


async def _next_chunk(iterator: AsyncIterator[object]) -> str:
    chunk = await anext(iterator)
    if isinstance(chunk, bytes):
        return chunk.decode("utf-8")
    return str(chunk)


async def _json_response(response: StreamingResponse) -> dict[str, object]:
    iterator = cast(AsyncIterator[str], cast(Any, response).body_iterator)
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
                received_at TEXT NOT NULL,
                published_at TEXT
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
        iterator = cast(AsyncIterator[str], cast(Any, response).body_iterator)
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

        iterator = cast(AsyncIterator[str], cast(Any, response).body_iterator)

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

    async def test_alarm_bus_replays_current_dispatch_batch_to_late_subscribers(self) -> None:
        class PausingAlarmBus(AlarmBus):
            def __init__(self) -> None:
                super().__init__()
                self._release_delivery = asyncio.Event()
                self._delivery_started = asyncio.Event()

            async def wait_for_delivery_started(self) -> None:
                await self._delivery_started.wait()

            def release_delivery(self) -> None:
                self._release_delivery.set()

            def seed_room_buffer(self, room_id: str, alarms: list[AlarmEvent]) -> None:
                self._room_buffers[room_id] = list(alarms)

            async def _dispatch_room(self, room_id: str) -> None:
                current_task = asyncio.current_task()
                try:
                    async with self._lock:
                        room_buffer = self._room_buffers.get(room_id, [])
                        if not room_buffer:
                            return
                        alarms_to_publish = list(room_buffer)
                        self._room_buffers[room_id] = []
                        self._inflight_batches[room_id] = alarms_to_publish
                        self._delivery_started.set()

                    await self._release_delivery.wait()

                    async with self._lock:
                        subscriber_queues = list(self._subscribers.get(room_id, []))

                    for alarm in alarms_to_publish:
                        for queue in list(subscriber_queues):
                            queue.put_nowait(alarm)
                finally:
                    async with self._lock:
                        self._inflight_batches.pop(room_id, None)
                        mapped_task = self._dispatch_tasks.get(room_id)
                        if mapped_task is current_task:
                            self._dispatch_tasks.pop(room_id, None)

        alarm_bus: PausingAlarmBus = PausingAlarmBus()
        room_id = "room_late_subscriber"
        alarm = AlarmEvent(
            device_id="dev_late",
            room_id=room_id,
            ts=datetime(2026, 6, 29, 14, 30, 0, tzinfo=timezone.utc),
            confidence=0.95,
            received_at=datetime.now(timezone.utc),
        )

        await alarm_bus.publish(alarm)
        await asyncio.wait_for(alarm_bus.wait_for_delivery_started(), timeout=1.0)

        queue = await alarm_bus.subscribe(room_id)
        alarm_bus.release_delivery()

        received = await asyncio.wait_for(queue.get(), timeout=1.0)
        self.assertEqual(received.device_id, alarm.device_id)

    async def test_replay_to_full_subscriber_queue_is_buffered_until_drained(self) -> None:
        class SeedableAlarmBus(AlarmBus):
            def seed_room_buffer(self, room_id: str, alarms: list[AlarmEvent]) -> None:
                self._room_buffers[room_id] = list(alarms)

        with patch("processing.alarm_bus.SSE_SUBSCRIBER_QUEUE_MAX_SIZE", 1):
            alarm_bus: SeedableAlarmBus = SeedableAlarmBus()
            room_id = "room_buffered_replay"
            first_alarm = AlarmEvent(
                device_id="dev_buffered_1",
                room_id=room_id,
                ts=datetime(2026, 6, 29, 19, 0, 0, tzinfo=timezone.utc),
                confidence=0.9,
                received_at=datetime.now(timezone.utc),
            )
            second_alarm = AlarmEvent(
                device_id="dev_buffered_2",
                room_id=room_id,
                ts=datetime(2026, 6, 29, 19, 0, 1, tzinfo=timezone.utc),
                confidence=0.95,
                received_at=datetime.now(timezone.utc),
            )
            alarm_bus.seed_room_buffer(room_id, [first_alarm, second_alarm])

            queue = cast(Any, await alarm_bus.subscribe(room_id))

            self.assertEqual(queue.qsize(), 1)
            self.assertGreaterEqual(queue.pending_count(), 2)

            first = await asyncio.wait_for(queue.get(), timeout=1.0)
            self.assertEqual(first.device_id, first_alarm.device_id)

            second = await asyncio.wait_for(queue.get(), timeout=1.0)
            self.assertEqual(second.device_id, second_alarm.device_id)

    async def test_alarm_stream_records_sse_delivery_latency(self) -> None:
        alarm_bus = AlarmBus()
        room_id = "room_latency_trace"

        response = await alarms_stream(
            room_id=room_id,
            since=None,
            db_connection=self.db,
            alarm_bus=alarm_bus,
        )
        iterator = cast(AsyncIterator[str], cast(Any, response).body_iterator)

        first_chunk_task = asyncio.create_task(_next_chunk(iterator))
        await asyncio.sleep(0.05)

        await alarm_bus.publish(
            AlarmEvent(
                device_id="dev_latency_trace",
                room_id=room_id,
                ts=datetime(2026, 6, 29, 18, 0, 0, tzinfo=timezone.utc),
                confidence=0.95,
                received_at=datetime.now(timezone.utc),
            )
        )

        chunk = await asyncio.wait_for(first_chunk_task, timeout=0.5)
        self.assertIn('"device_id": "dev_latency_trace"', chunk)
        self.assertGreaterEqual(get_alarm_path_stage_latency_ms_p95("sse_delivery"), 0)

    async def test_alarm_stream_invalid_since_returns_400(self) -> None:
        alarm_bus = AlarmBus()
        with self.assertRaises(HTTPException) as ctx:
            await alarms_stream(
                room_id="room_1",
                since="not-a-timestamp",
                db_connection=self.db,
                alarm_bus=alarm_bus,
            )
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(ctx.exception.detail, "invalid since")

    async def test_alarm_stream_subscribes_before_replay_without_duplicates(self) -> None:
        room_id = "room_race"
        replay_ts = datetime(2026, 6, 29, 14, 0, 0, tzinfo=timezone.utc)

        class RaceAlarmBus(AlarmBus):
            async def subscribe(self, room_id: str) -> asyncio.Queue[AlarmEvent]:
                queue = await super().subscribe(room_id)
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
        iterator = cast(AsyncIterator[str], cast(Any, response).body_iterator)

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
            iterator = cast(AsyncIterator[str], cast(Any, response).body_iterator)
            chunks = [
                await asyncio.wait_for(_next_chunk(iterator), timeout=1.0)
                for _ in rows
            ]
            await cast(Any, iterator).aclose()

        self.assertEqual(
            [json.loads(chunk.removeprefix("data: ").strip())["device_id"] for chunk in chunks],
            ["dev_0", "dev_1", "dev_2"],
        )

    async def test_saturated_subscriber_is_evicted_and_dispatch_does_not_block(self) -> None:
        # A subscriber that never drains its queue must be evicted once it saturates, rather than
        # having AlarmBus._dispatch_room block forever on `queue.put(...)` (the pre-fix behavior),
        # which would also stall fan-out to every other subscriber in the room.
        before = get_counters().get("sse_subscribers_evicted", 0)
        with patch("processing.alarm_bus.SSE_SUBSCRIBER_QUEUE_MAX_SIZE", 2):
            alarm_bus = AlarmBus()
            room_id = "room_evict"
            stalled_queue = await alarm_bus.subscribe(room_id)  # intentionally never read from

            base_ts = datetime(2026, 6, 29, 16, 0, 0, tzinfo=timezone.utc)

            async def _publish_burst() -> None:
                for index in range(5):
                    await alarm_bus.publish(
                        AlarmEvent(
                            device_id=f"dev_{index}",
                            room_id=room_id,
                            ts=base_ts + timedelta(seconds=index),
                            confidence=0.9,
                            received_at=base_ts,
                        )
                    )
                await asyncio.sleep(0.2)  # let the reorder-buffer dispatch task run to completion

            await asyncio.wait_for(_publish_burst(), timeout=2.0)

        after = get_counters().get("sse_subscribers_evicted", 0)
        self.assertGreaterEqual(after - before, 1)
        # Evicted (capped at the bound) rather than grown to hold all 5 published alarms.
        self.assertLessEqual(stalled_queue.qsize(), 2)

    async def test_publish_delivers_to_active_subscribers_without_waiting_for_dispatch_cycle(self) -> None:
        alarm_bus = AlarmBus()
        room_id = "room_immediate_publish"
        queue = await alarm_bus.subscribe(room_id)

        alarm = AlarmEvent(
            device_id="dev_immediate_publish",
            room_id=room_id,
            ts=datetime(2026, 6, 29, 18, 0, 0, tzinfo=timezone.utc),
            confidence=0.95,
            received_at=datetime.now(timezone.utc),
        )

        await alarm_bus.publish(alarm)
        received = await asyncio.wait_for(queue.get(), timeout=0.05)

        self.assertEqual(received.device_id, alarm.device_id)

    async def test_alarm_stream_delivers_without_waiting_for_reorder_window(self) -> None:
        with patch("processing.alarm_bus.ALARM_REORDER_BUFFER_MS", 500):
            alarm_bus = AlarmBus()
            room_id = "room_immediate"

            response = await alarms_stream(
                room_id=room_id, since=None, db_connection=self.db, alarm_bus=alarm_bus
            )
            iterator = cast(AsyncIterator[str], cast(Any, response).body_iterator)

            first_chunk_task = asyncio.create_task(_next_chunk(iterator))
            await asyncio.sleep(0.05)

            await alarm_bus.publish(
                AlarmEvent(
                    device_id="dev_immediate",
                    room_id=room_id,
                    ts=datetime(2026, 6, 29, 18, 0, 0, tzinfo=timezone.utc),
                    confidence=0.95,
                    received_at=datetime.now(timezone.utc),
                )
            )

            chunk = await asyncio.wait_for(first_chunk_task, timeout=0.2)
            self.assertIn('"device_id": "dev_immediate"', chunk)

    async def test_alarm_stream_delivers_bursts_without_waiting_for_reorder_window(self) -> None:
        with patch("processing.alarm_bus.ALARM_REORDER_BUFFER_MS", 300):
            alarm_bus = AlarmBus()
            room_id = "room_burst"

            response = await alarms_stream(
                room_id=room_id, since=None, db_connection=self.db, alarm_bus=alarm_bus
            )
            iterator = cast(AsyncIterator[str], cast(Any, response).body_iterator)

            first_chunk_task = asyncio.create_task(_next_chunk(iterator))
            await asyncio.sleep(0.05)

            first_alarm = AlarmEvent(
                device_id="dev_burst_first",
                room_id=room_id,
                ts=datetime(2026, 6, 29, 18, 0, 0, tzinfo=timezone.utc),
                confidence=0.92,
                received_at=datetime.now(timezone.utc),
            )
            second_alarm = AlarmEvent(
                device_id="dev_burst_second",
                room_id=room_id,
                ts=datetime(2026, 6, 29, 18, 0, 1, tzinfo=timezone.utc),
                confidence=0.94,
                received_at=datetime.now(timezone.utc),
            )

            await alarm_bus.publish(first_alarm)
            await alarm_bus.publish(second_alarm)

            chunk = await asyncio.wait_for(first_chunk_task, timeout=0.5)
            self.assertIn('"device_id": "dev_burst_first"', chunk)

    async def test_alarm_stream_closes_after_subscriber_is_evicted(self) -> None:
        # The SSE route's consume loop must eventually close the connection for an evicted,
        # fully-drained subscriber instead of looping on a queue that will never receive anything
        # new again (a zombie stream the client would have no way to detect as dead).
        with patch("processing.alarm_bus.SSE_SUBSCRIBER_QUEUE_MAX_SIZE", 1):
            alarm_bus = AlarmBus()
            room_id = "room_evict_stream"

            response = await alarms_stream(
                room_id=room_id, since=None, db_connection=self.db, alarm_bus=alarm_bus
            )
            iterator = cast(AsyncIterator[str], cast(Any, response).body_iterator)

            # The generator subscribes lazily on first iteration, so it must be pumped once
            # (subscribing it) before publishing -- otherwise the burst below would have no
            # subscriber to evict at all.
            first_chunk_task = asyncio.create_task(_next_chunk(iterator))
            await asyncio.sleep(0.05)

            base_ts = datetime(2026, 6, 29, 17, 0, 0, tzinfo=timezone.utc)
            for index in range(4):
                await alarm_bus.publish(
                    AlarmEvent(
                        device_id=f"dev_{index}",
                        room_id=room_id,
                        ts=base_ts + timedelta(seconds=index),
                        confidence=0.9,
                        received_at=base_ts,
                    )
                )
            await asyncio.sleep(0.2)  # let dispatch saturate and evict the sole subscriber

            chunks = [await asyncio.wait_for(first_chunk_task, timeout=2.0)]
            chunks.extend([chunk async for chunk in iterator])

        # The generator yields whatever made it into the queue before eviction, then breaks
        # instead of hanging forever once it observes disconnected+drained.
        self.assertGreaterEqual(len(chunks), 1)


if __name__ == "__main__":
    unittest.main()
