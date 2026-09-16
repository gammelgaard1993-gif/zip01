from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
import time
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

    async def test_subscriber_joining_immediately_after_publish_is_not_double_delivered(self) -> None:
        # Targets the specific pre-claim window a specialist review flagged: subscribe() racing
        # publish() before the scheduled dispatch task has had a chance to run at all (so the
        # batch is still sitting in _room_buffers, not yet in _inflight_batches). subscribe() no
        # longer replays from _room_buffers (see AlarmBus.subscribe's docstring/comment) -- the
        # correctness argument is that a subscriber that wins the race and joins _subscribers
        # before the dispatch task claims the batch is naturally included in that task's own
        # (still-to-come) atomic snapshot, so it must get exactly one delivery, not zero and not
        # two.
        alarm_bus = AlarmBus()
        room_id = "room_pre_claim_race"
        alarm = AlarmEvent(
            device_id="dev_pre_claim",
            room_id=room_id,
            ts=datetime(2026, 6, 29, 15, 30, 0, tzinfo=timezone.utc),
            confidence=0.88,
            received_at=datetime.now(timezone.utc),
        )

        await alarm_bus.publish(alarm)  # schedules but does not run the dispatch task yet
        queue = await alarm_bus.subscribe(room_id)  # wins the race, joins before the task runs

        received = await asyncio.wait_for(queue.get(), timeout=1.0)
        self.assertEqual(received.device_id, alarm.device_id)
        await asyncio.sleep(0.05)  # let the dispatch task actually run and finish
        self.assertTrue(queue.empty(), "alarm must be delivered exactly once, not duplicated by the dispatch task")

    async def test_alarm_bus_replays_current_dispatch_batch_to_late_subscribers(self) -> None:
        # Uses the real _dispatch_room (not a reimplementation of it) and only pauses right
        # before _broadcast's actual delivery, so a subscriber can join in the exact window this
        # is meant to cover: after the batch is marked in-flight (and, for a real reorder-delayed
        # batch, during the reorder-delay sleep) but before delivery happens. Asserting the queue
        # is empty after the one expected item guards against the double-delivery regression a
        # specialist review found in an earlier version of this dispatch path (subscribe()'s
        # in-flight replay racing a late-taken subscriber snapshot in delivery).
        class PausingBroadcastAlarmBus(AlarmBus):
            def __init__(self) -> None:
                super().__init__()
                self._broadcast_started = asyncio.Event()
                self._release_broadcast = asyncio.Event()

            async def _broadcast(self, room_id: str, alarms: list[AlarmEvent], subscriber_queues: Any) -> None:
                self._broadcast_started.set()
                await self._release_broadcast.wait()
                await super()._broadcast(room_id, alarms, subscriber_queues)

        alarm_bus = PausingBroadcastAlarmBus()
        room_id = "room_late_subscriber"
        alarm = AlarmEvent(
            device_id="dev_late",
            room_id=room_id,
            ts=datetime(2026, 6, 29, 14, 30, 0, tzinfo=timezone.utc),
            confidence=0.95,
            received_at=datetime.now(timezone.utc),
        )

        await alarm_bus.publish(alarm)
        await asyncio.wait_for(alarm_bus._broadcast_started.wait(), timeout=1.0)

        queue = await alarm_bus.subscribe(room_id)
        alarm_bus._release_broadcast.set()

        received = await asyncio.wait_for(queue.get(), timeout=1.0)
        self.assertEqual(received.device_id, alarm.device_id)
        self.assertTrue(queue.empty(), "late subscriber must receive the in-flight batch exactly once")

    async def test_late_subscriber_during_real_reorder_delay_is_not_double_delivered(self) -> None:
        # Companion to the test above, using a real (non-zero) reorder-delay sleep with a
        # multi-alarm batch instead of an artificial _broadcast pause -- this is the exact shape
        # a specialist review used to prove that only moving the subscriber snapshot's *position*
        # in _dispatch_room (independent of how it's threaded into _broadcast/_deliver_local)
        # actually closes the race for delayed batches; a single-alarm/no-delay test alone cannot
        # tell the two apart.
        alarm_bus = AlarmBus()
        alarm_bus._reorder_buffer_seconds = 0.15
        room_id = "room_real_reorder_delay"
        base_ts = datetime(2026, 6, 29, 15, 0, 0, tzinfo=timezone.utc)
        first_alarm = AlarmEvent(
            device_id="dev_reorder_1",
            room_id=room_id,
            ts=base_ts,
            confidence=0.9,
            received_at=datetime.now(timezone.utc),
        )
        second_alarm = AlarmEvent(
            device_id="dev_reorder_2",
            room_id=room_id,
            ts=base_ts + timedelta(seconds=1),
            confidence=0.92,
            received_at=datetime.now(timezone.utc),
        )

        await alarm_bus.publish(first_alarm)
        await alarm_bus.publish(second_alarm)
        await asyncio.sleep(0.05)  # join well inside the real 150ms reorder-delay window
        queue = await alarm_bus.subscribe(room_id)

        received_first = await asyncio.wait_for(queue.get(), timeout=1.0)
        received_second = await asyncio.wait_for(queue.get(), timeout=1.0)
        self.assertEqual({received_first.device_id, received_second.device_id}, {"dev_reorder_1", "dev_reorder_2"})
        await asyncio.sleep(0.2)  # let dispatch fully finish before checking for stray duplicates
        self.assertTrue(queue.empty(), "batch must be delivered exactly once, not duplicated by live delivery")

    async def test_replay_to_full_subscriber_queue_is_buffered_until_drained(self) -> None:
        # Uses the real dispatch/publish flow (not manually seeded _room_buffers, which -- after
        # the fix removing subscribe()'s _room_buffers replay -- would never be delivered, since
        # normally a non-empty room_buffer always has a pending dispatch task backing it). Pauses
        # inside an overridden _broadcast so the subscriber joins while both alarms are held in
        # _inflight_batches (after the dispatch task's atomic snapshot, which was empty, was
        # already taken), so it must receive both alarms via subscribe()'s in-flight replay --
        # exercising the same queue-full/backlog-buffering path the original test covered.
        class PausingBroadcastAlarmBus(AlarmBus):
            def __init__(self) -> None:
                super().__init__()
                self._broadcast_started = asyncio.Event()
                self._release_broadcast = asyncio.Event()

            async def _broadcast(self, room_id: str, alarms: list[AlarmEvent], subscriber_queues: Any) -> None:
                self._broadcast_started.set()
                await self._release_broadcast.wait()
                await super()._broadcast(room_id, alarms, subscriber_queues)

        with patch("processing.alarm_bus.SSE_SUBSCRIBER_QUEUE_MAX_SIZE", 1):
            alarm_bus = PausingBroadcastAlarmBus()
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
            # Both publish() calls run to completion before the dispatch task they schedule gets
            # a chance to run (no real suspension point in between), so the dispatch task sees
            # both alarms as a single (delayed, since len > 1) batch, matching the original
            # test's two-alarm-batch intent.
            await alarm_bus.publish(first_alarm)
            await alarm_bus.publish(second_alarm)
            await asyncio.wait_for(alarm_bus._broadcast_started.wait(), timeout=1.0)

            queue = cast(Any, await alarm_bus.subscribe(room_id))
            alarm_bus._release_broadcast.set()

            self.assertEqual(queue.qsize(), 1)
            self.assertGreaterEqual(queue.pending_count(), 2)

            first = await asyncio.wait_for(queue.get(), timeout=1.0)
            self.assertEqual(first.device_id, first_alarm.device_id)

            second = await asyncio.wait_for(queue.get(), timeout=1.0)
            self.assertEqual(second.device_id, second_alarm.device_id)
            self.assertTrue(queue.empty(), "alarms must be delivered exactly once, not duplicated by live delivery")

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


class _FakeRedisPubSubBroker:
    """Minimal in-memory PUBLISH/PSUBSCRIBE fanout shared by multiple fake redis client
    handles, standing in for a real Redis server so the AlarmBus cross-instance bridge can be
    tested without Docker/a live Redis connection (see core/redis_client.py's USE_FAKE_REDIS note
    for why the app itself falls back to fakeredis, not this: this stub only needs to support the
    narrow publish/psubscribe/get_message surface AlarmBus's bridge actually calls)."""

    def __init__(self) -> None:
        self._subscriber_queues: list[Any] = []
        self._lock = threading.Lock()

    def client(self) -> "_FakeRedisPubSubClient":
        return _FakeRedisPubSubClient(self)


class _FakeRedisPubSubClient:
    def __init__(self, broker: _FakeRedisPubSubBroker) -> None:
        self._broker = broker

    def publish(self, channel: str, payload: str) -> None:
        with self._broker._lock:
            queues = list(self._broker._subscriber_queues)
        for q in queues:
            q.put({"type": "pmessage", "channel": channel, "data": payload})

    def pubsub(self, ignore_subscribe_messages: bool = True) -> "_FakePubSubHandle":
        return _FakePubSubHandle(self._broker)


class _FakePubSubHandle:
    def __init__(self, broker: _FakeRedisPubSubBroker) -> None:
        self._broker = broker
        self._queue: Any = None

    def psubscribe(self, pattern: str) -> None:
        import queue as queue_module

        self._queue = queue_module.Queue()
        with self._broker._lock:
            self._broker._subscriber_queues.append(self._queue)

    def get_message(self, timeout: float = 1.0) -> dict[str, Any] | None:
        import queue as queue_module

        try:
            return self._queue.get(timeout=timeout)
        except queue_module.Empty:
            return None

    def close(self) -> None:
        with self._broker._lock:
            if self._queue in self._broker._subscriber_queues:
                self._broker._subscriber_queues.remove(self._queue)


class AlarmBusCrossInstanceBridgeTests(unittest.IsolatedAsyncioTestCase):
    """Verifies the redis pub/sub bridge (processing/alarm_bus.py) fans an alarm published by
    one AlarmBus "instance" out to a subscriber on a different AlarmBus "instance" sharing the
    same (fake) redis backend -- the scenario that is broken without the bridge (see the
    multi-instance architecture review: SSE delivery was previously per-process only)."""

    async def asyncSetUp(self) -> None:
        self.broker = _FakeRedisPubSubBroker()
        self.bus_a = AlarmBus(redis_client=self.broker.client())
        self.bus_b = AlarmBus(redis_client=self.broker.client())
        await self.bus_a.start()
        await self.bus_b.start()

    async def asyncTearDown(self) -> None:
        await self.bus_a.stop()
        await self.bus_b.stop()

    async def test_alarm_published_on_one_instance_is_delivered_to_subscriber_on_another(self) -> None:
        room_id = "room_cross_instance"
        subscriber_queue = await self.bus_b.subscribe(room_id)

        alarm = AlarmEvent(
            device_id="dev_cross_instance",
            room_id=room_id,
            ts=datetime(2026, 6, 29, 18, 0, 0, tzinfo=timezone.utc),
            confidence=0.93,
            received_at=datetime.now(timezone.utc),
        )
        await self.bus_a.publish(alarm)

        received = await asyncio.wait_for(subscriber_queue.get(), timeout=2.0)
        self.assertEqual(received.device_id, alarm.device_id)
        self.assertEqual(received.room_id, room_id)

    async def test_publishing_instance_also_receives_its_own_alarm_exactly_once(self) -> None:
        # The publishing instance delivers its own batch directly/synchronously (see
        # AlarmBus._broadcast) rather than round-tripping through its own bridge listener; the
        # bridge PUBLISH is tagged with an instance id and the listener explicitly ignores
        # self-originated messages so the local subscriber above is never double-delivered.
        room_id = "room_self_delivery"
        subscriber_queue = await self.bus_a.subscribe(room_id)

        alarm = AlarmEvent(
            device_id="dev_self_delivery",
            room_id=room_id,
            ts=datetime(2026, 6, 29, 18, 0, 0, tzinfo=timezone.utc),
            confidence=0.91,
            received_at=datetime.now(timezone.utc),
        )
        await self.bus_a.publish(alarm)

        received = await asyncio.wait_for(subscriber_queue.get(), timeout=2.0)
        self.assertEqual(received.device_id, alarm.device_id)
        self.assertTrue(subscriber_queue.empty(), "alarm must be delivered exactly once, not twice")

    async def test_late_subscriber_on_remote_instance_is_not_double_delivered_under_slow_publish(
        self,
    ) -> None:
        # Regression test for a race a specialist review found in an earlier version of this
        # bridge: _broadcast() used to only await the PUBLISH network call and rely on the
        # listener thread to perform the actual local delivery later, asynchronously. A
        # subscriber joining bus_b during that gap could observe both the in-flight-batch replay
        # AND the deferred bridge delivery -- i.e. the same alarm twice. The fix makes local
        # delivery on the publishing instance fully synchronous (independent of redis latency)
        # and has remote instances' listeners ignore self-originated messages, so this scenario
        # (artificially slow PUBLISH + a subscriber joining bus_b mid-flight) must still result
        # in exactly one delivery to that subscriber.
        original_publish = _FakeRedisPubSubClient.publish

        def slow_publish(self_client: "_FakeRedisPubSubClient", channel: str, payload: str) -> None:
            time.sleep(0.15)
            original_publish(self_client, channel, payload)

        room_id = "room_slow_publish_race"
        alarm = AlarmEvent(
            device_id="dev_slow_publish",
            room_id=room_id,
            ts=datetime(2026, 6, 29, 18, 0, 0, tzinfo=timezone.utc),
            confidence=0.87,
            received_at=datetime.now(timezone.utc),
        )

        with patch.object(_FakeRedisPubSubClient, "publish", slow_publish):
            publish_task = asyncio.create_task(self.bus_a.publish(alarm))
            await asyncio.sleep(0.03)  # join bus_b well inside the artificial 150ms publish delay
            subscriber_queue = await self.bus_b.subscribe(room_id)
            await publish_task

            received = await asyncio.wait_for(subscriber_queue.get(), timeout=2.0)
            self.assertEqual(received.device_id, alarm.device_id)
            await asyncio.sleep(0.05)
            self.assertTrue(subscriber_queue.empty(), "alarm must be delivered exactly once, not twice")


if __name__ == "__main__":
    unittest.main()
