from __future__ import annotations

import asyncio
import json
import threading
import time
import unittest
import warnings
from datetime import datetime, timezone
from typing import Any, cast
from unittest.mock import MagicMock

from ingestion.mqtt_subscriber import MQTTSubscriber
from ingestion.queue import PriorityEventQueue
from models import Priority, ValidatedEvent


class QueueBackpressureTests(unittest.IsolatedAsyncioTestCase):
    def _event(self, index: int) -> ValidatedEvent:
        return ValidatedEvent(
            device_id=f"dev_{index}",
            room_id="room_1",
            type="heartbeat",
            ts=datetime.now(timezone.utc),
            payload={},
            late=False,
            priority=Priority.NORMAL,
            received_at=datetime.now(timezone.utc),
        )

    def _high_event(self, index: int) -> ValidatedEvent:
        return ValidatedEvent(
            device_id=f"dev_{index}",
            room_id="room_1",
            type="fall_warn",
            ts=datetime.now(timezone.utc),
            payload={},
            late=False,
            priority=Priority.HIGH,
            received_at=datetime.now(timezone.utc),
        )

    async def test_put_blocks_when_normal_lane_full(self) -> None:
        queue = PriorityEventQueue(normal_max_size=1)
        await queue.put(self._event(1))

        pending_put = asyncio.create_task(queue.put(self._event(2)))
        await asyncio.sleep(0)
        self.assertFalse(pending_put.done())

        _ = await queue.get()
        await asyncio.wait_for(pending_put, timeout=1.0)
        self.assertEqual(queue.qsize_normal(), 1)

    async def test_idle_get_wakes_on_high_arrival(self) -> None:
        # An idle consumer parked in get() (both lanes empty) must wake when a HIGH event
        # arrives, even though no NORMAL event ever comes.
        queue = PriorityEventQueue(normal_max_size=10)
        getter = asyncio.create_task(queue.get())
        await asyncio.sleep(0)
        self.assertFalse(getter.done())

        await queue.put(self._high_event(1))
        result = await asyncio.wait_for(getter, timeout=1.0)
        self.assertEqual(result.priority, Priority.HIGH)

    async def test_idle_get_serves_high_before_normal_on_simultaneous_arrival(self) -> None:
        # When both lanes receive an item while the consumer is parked, HIGH is served first
        # and the NORMAL event is retained (not dropped).
        queue = PriorityEventQueue(normal_max_size=10)
        getter = asyncio.create_task(queue.get())
        await asyncio.sleep(0)

        await queue.put(self._event(1))
        await queue.put(self._high_event(2))

        first = await asyncio.wait_for(getter, timeout=1.0)
        self.assertEqual(first.priority, Priority.HIGH)

        second = await asyncio.wait_for(queue.get(), timeout=1.0)
        self.assertEqual(second.priority, Priority.NORMAL)
        self.assertTrue(queue.empty())


class _FakeMQTTMessage:
    def __init__(
        self,
        payload: bytes,
        topic: str = "teton/devices/dev_1/events",
        mid: int = 1,
        qos: int = 1,
    ) -> None:
        self.payload = payload
        self.topic = topic
        self.mid = mid
        self.qos = qos


class SubscriberPriorityInversionTests(unittest.TestCase):
    """F-07 / §9: a saturated NORMAL lane must never stall the HIGH fall_warn lane.

    paho delivers messages serially on a single network thread and the subscriber uses
    ``manual_ack=True``: NORMAL puback is deferred until the event is accepted into the bounded
    lane, so a saturated NORMAL lane leaves that message un-acked (broker backpressure). A HIGH
    fall_warn delivered while NORMAL is un-acked/backpressured must still reach the unbounded HIGH
    lane and be acked immediately — never blocked behind NORMAL. These tests drive ``_on_message``
    from the test thread (the paho thread) while the asyncio loop runs in a background thread.
    """

    @staticmethod
    def _raw(event_type: str, device_id: str) -> bytes:
        return json.dumps(
            {
                "device_id": device_id,
                "room_id": "room_1",
                "type": event_type,
                "ts": datetime.now(timezone.utc).isoformat(),
                "seq": 1,
            }
        ).encode("utf-8")

    def test_high_delivered_and_acked_while_normal_backpressured(self) -> None:
        loop = asyncio.new_event_loop()
        ready = threading.Event()

        def _run_loop() -> None:
            asyncio.set_event_loop(loop)
            ready.set()
            loop.run_forever()

        loop_thread = threading.Thread(target=_run_loop, daemon=True)
        loop_thread.start()
        self.assertTrue(ready.wait(timeout=2.0))

        normal_mid, high_mid = 101, 202
        try:
            queue = PriorityEventQueue(normal_max_size=1)
            # Saturate the NORMAL lane so the next NORMAL event cannot be accepted (its ack stays
            # deferred — the backpressured state).
            saturate = asyncio.run_coroutine_threadsafe(queue.put(self._normal_event()), loop)
            saturate.result(timeout=1.0)
            self.assertTrue(queue.normal_is_full())

            subscriber = MQTTSubscriber("localhost", 1883, "teton/devices/+/events", queue)
            subscriber.loop = loop
            client = cast(Any, MagicMock())

            # A NORMAL event whose lane is full must NOT block the paho thread; its ack is deferred
            # until the lane accepts it, so nothing is acked yet.
            start = time.perf_counter()
            cast(Any, subscriber)._on_message(
                client, None, _FakeMQTTMessage(self._raw("motion", "dev_norm"), mid=normal_mid)
            )
            normal_elapsed = time.perf_counter() - start
            self.assertLess(normal_elapsed, 0.5, "NORMAL ingest blocked the MQTT delivery thread")
            time.sleep(0.05)
            client.ack.assert_not_called()

            # The very next message is a HIGH fall_warn. Even though the NORMAL lane is saturated
            # and the prior NORMAL message is still un-acked, the fall_warn must reach the unbounded
            # HIGH lane and be acked immediately.
            cast(Any, subscriber)._on_message(
                client, None, _FakeMQTTMessage(self._raw("fall_warn", "dev_fall"), mid=high_mid)
            )

            deadline = time.perf_counter() + 1.0
            while queue.qsize_high() == 0 and time.perf_counter() < deadline:
                time.sleep(0.01)

            self.assertEqual(queue.qsize_high(), 1, "HIGH fall_warn was starved by a saturated NORMAL lane")

            acked_mids = [call.args[0] for call in client.ack.call_args_list]
            self.assertIn(high_mid, acked_mids, "HIGH fall_warn was not acked immediately")
            self.assertNotIn(
                normal_mid,
                acked_mids,
                "backpressured NORMAL message was acked before its lane accepted it",
            )
        finally:
            # The NORMAL ingest is intentionally still parked on the saturated lane. Cancel any
            # pending tasks cleanly before tearing the loop down to avoid teardown noise.
            async def _shutdown() -> None:
                pending = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)

            try:
                asyncio.run_coroutine_threadsafe(_shutdown(), loop).result(timeout=1.0)
            except Exception:
                pass
            loop.call_soon_threadsafe(loop.stop)
            loop_thread.join(timeout=2.0)
            loop.close()

    @staticmethod
    def _normal_event() -> ValidatedEvent:
        now = datetime.now(timezone.utc)
        return ValidatedEvent(
            device_id="dev_seed",
            room_id="room_1",
            type="heartbeat",
            ts=now,
            payload={},
            late=False,
            priority=Priority.NORMAL,
            received_at=now,
        )


class SubscriberConstructionTests(unittest.TestCase):
    """P0: the subscriber must construct on a clean paho-mqtt 2.x install.

    paho 2.x deprecates the implicit v1 callback API — constructing without an explicit
    CallbackAPIVersion emits a DeprecationWarning that becomes a hard failure under
    ``-W error`` and is slated for removal in a future release. Building the real paho client
    with warnings promoted to errors proves the version mismatch is gone; every other test in
    this file passes a MagicMock as the paho *client* argument and so never exercised this path.
    """

    def test_real_client_constructs_without_deprecation(self) -> None:
        queue = PriorityEventQueue(normal_max_size=1)
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            subscriber = MQTTSubscriber("localhost", 1883, "teton/devices/+/events", queue)
        self.assertIsNotNone(subscriber.client)


class NormalManualAckBackpressureTests(unittest.TestCase):
    """P0 / F-07: NORMAL backpressure must bound in-flight memory without dropping or blocking.

    With ``manual_ack=True`` the subscriber defers the QoS-1 puback for a NORMAL event until the
    event is accepted into the bounded NORMAL lane. A real broker then stops delivering once its
    inflight window fills with un-acked NORMAL messages (bounded memory, real backpressure) — all
    without blocking paho's single delivery thread. This test proves the ack is withheld while the
    lane is full and fires (with no event dropped) once the lane drains.
    """

    @staticmethod
    def _raw(event_type: str, device_id: str) -> bytes:
        return json.dumps(
            {
                "device_id": device_id,
                "room_id": "room_1",
                "type": event_type,
                "ts": datetime.now(timezone.utc).isoformat(),
                "seq": 1,
            }
        ).encode("utf-8")

    @staticmethod
    def _normal_event() -> ValidatedEvent:
        now = datetime.now(timezone.utc)
        return ValidatedEvent(
            device_id="dev_seed",
            room_id="room_1",
            type="heartbeat",
            ts=now,
            payload={},
            late=False,
            priority=Priority.NORMAL,
            received_at=now,
        )

    def test_normal_ack_deferred_until_lane_accepts_then_no_drop(self) -> None:
        loop = asyncio.new_event_loop()
        ready = threading.Event()

        def _run_loop() -> None:
            asyncio.set_event_loop(loop)
            ready.set()
            loop.run_forever()

        loop_thread = threading.Thread(target=_run_loop, daemon=True)
        loop_thread.start()
        self.assertTrue(ready.wait(timeout=2.0))

        normal_mid = 55
        try:
            # Saturate the lane so the incoming NORMAL event cannot be accepted yet.
            queue = PriorityEventQueue(normal_max_size=1)
            asyncio.run_coroutine_threadsafe(queue.put(self._normal_event()), loop).result(1.0)
            self.assertTrue(queue.normal_is_full())

            subscriber = MQTTSubscriber("localhost", 1883, "teton/devices/+/events", queue)
            subscriber.loop = loop
            client = cast(Any, MagicMock())

            # Deliver a NORMAL event. The callback returns immediately (no thread block) and the
            # ack is withheld because the lane is full — the message stays un-acked (backpressure).
            start = time.perf_counter()
            cast(Any, subscriber)._on_message(
                client, None, _FakeMQTTMessage(self._raw("motion", "dev_norm"), mid=normal_mid)
            )
            self.assertLess(time.perf_counter() - start, 0.5)
            time.sleep(0.05)
            client.ack.assert_not_called()

            # Drain the seed so the deferred NORMAL event is accepted into the lane. Its ack must
            # now fire, and the event must be present (never dropped).
            drained = asyncio.run_coroutine_threadsafe(queue.get(), loop).result(2.0)
            self.assertIsNotNone(drained)

            deadline = time.perf_counter() + 1.0
            while not client.ack.called and time.perf_counter() < deadline:
                time.sleep(0.01)
            client.ack.assert_called_once_with(normal_mid, 1)
            self.assertEqual(queue.qsize_normal(), 1, "NORMAL event was dropped instead of enqueued")
        finally:
            async def _shutdown() -> None:
                pending = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)

            try:
                asyncio.run_coroutine_threadsafe(_shutdown(), loop).result(timeout=1.0)
            except Exception:
                pass
            loop.call_soon_threadsafe(loop.stop)
            loop_thread.join(timeout=2.0)
            loop.close()


if __name__ == "__main__":
    unittest.main()
