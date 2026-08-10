from __future__ import annotations

import asyncio
import json
import logging
from collections import deque
from datetime import datetime, timezone
from typing import AsyncIterator, Dict

from config import ALARM_REORDER_BUFFER_MS, SSE_SUBSCRIBER_QUEUE_MAX_SIZE
from core.metrics import increment_counter, observe_alarm_feed_latency_ms
from models import AlarmEvent

logger = logging.getLogger(__name__)


class _SubscriberQueue(asyncio.Queue["AlarmEvent"]):
    """A per-subscriber alarm queue with explicit state and bounded replay buffering.

    The main queue is bounded so one stalled/slow SSE client cannot grow memory without limit or
    block fan-out to other subscribers in the same room. When the queue is full, replayed alarms
    are buffered in a small backlog instead of being dropped; once that backlog is also saturated,
    the subscriber is evicted and marked disconnected so the stream can close after draining.
    """

    def __init__(self) -> None:
        super().__init__(maxsize=SSE_SUBSCRIBER_QUEUE_MAX_SIZE)
        self._backlog: deque[AlarmEvent] = deque()
        self.disconnected = asyncio.Event()
        self.state = "active"

    def put_nowait(self, item: AlarmEvent) -> None:
        if not self.full():
            super().put_nowait(item)
            return

        if len(self._backlog) >= SSE_SUBSCRIBER_QUEUE_MAX_SIZE:
            raise asyncio.QueueFull

        self._backlog.append(item)

    def pending_count(self) -> int:
        return self.qsize() + len(self._backlog)

    def drain_backlog(self) -> None:
        while not self.full() and self._backlog:
            super().put_nowait(self._backlog.popleft())

    async def get(self) -> AlarmEvent:
        self.drain_backlog()
        return await super().get()

    def mark_evicted(self) -> None:
        self.state = "evicted"
        self.disconnected.set()


def is_subscriber_disconnected(queue: "asyncio.Queue[AlarmEvent]") -> bool:
    """True once a subscriber queue has been evicted and fully drained."""
    return (
        isinstance(queue, _SubscriberQueue)
        and queue.state == "evicted"
        and queue.pending_count() == 0
    )


class AlarmBus:
    def __init__(self) -> None:
        self._subscribers: Dict[str, list[asyncio.Queue[AlarmEvent]]] = {}
        self._room_buffers: Dict[str, list[AlarmEvent]] = {}
        self._inflight_batches: Dict[str, list[AlarmEvent]] = {}
        self._dispatch_tasks: Dict[str, asyncio.Task[None]] = {}
        self._lock = asyncio.Lock()
        self._reorder_buffer_seconds = ALARM_REORDER_BUFFER_MS / 1000.0

    async def publish(self, alarm: AlarmEvent) -> None:
        async with self._lock:
            room_buffer = self._room_buffers.setdefault(alarm.room_id, [])
            room_buffer.append(alarm)
            room_buffer.sort(key=lambda item: item.ts)

            logger.info(
                json.dumps(
                    {
                        "event": "alarm_bus_publish_received",
                        "room_id": alarm.room_id,
                        "device_id": alarm.device_id,
                        "ts": alarm.ts.isoformat(),
                        "received_at": alarm.received_at.isoformat(),
                        "fall_warning_id": alarm.fall_warning_id,
                        "buffer_size": len(room_buffer),
                    }
                )
            )

            dispatch_task = self._dispatch_tasks.get(alarm.room_id)
            if dispatch_task is None or dispatch_task.done():
                self._dispatch_tasks[alarm.room_id] = asyncio.create_task(self._dispatch_room(alarm.room_id))

    async def subscribe(self, room_id: str) -> asyncio.Queue[AlarmEvent]:
        queue: asyncio.Queue[AlarmEvent] = _SubscriberQueue()
        async with self._lock:
            self._subscribers.setdefault(room_id, []).append(queue)
            # Replay alarms already buffered but not yet dispatched so a subscriber that
            # arrives after publish() but before the room dispatch completes doesn't miss them.
            for alarm in self._inflight_batches.get(room_id, []):
                try:
                    queue.put_nowait(alarm)
                except asyncio.QueueFull:
                    break
            for alarm in self._room_buffers.get(room_id, []):
                try:
                    queue.put_nowait(alarm)
                except asyncio.QueueFull:
                    break
        return queue

    async def unsubscribe(self, room_id: str, queue: asyncio.Queue[AlarmEvent]) -> None:
        async with self._lock:
            if room_id not in self._subscribers:
                return
            subscribers = self._subscribers[room_id]
            if queue in subscribers:
                subscribers.remove(queue)
            if not subscribers:
                self._subscribers.pop(room_id, None)

    async def _evict_saturated_subscriber(self, room_id: str, queue: asyncio.Queue[AlarmEvent]) -> None:
        # The queue is full because the subscriber isn't draining it fast enough. Stop sending it
        # anything further (unsubscribe) rather than blocking fan-out to the room's other
        # subscribers or letting this queue grow past its bound; the client must reconnect with
        # `since` to resume without a gap.
        await self.unsubscribe(room_id, queue)
        if isinstance(queue, _SubscriberQueue):
            queue.mark_evicted()
        increment_counter("sse_subscribers_evicted")

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
                should_delay = len(alarms_to_publish) > 1

            if should_delay and self._reorder_buffer_seconds > 0:
                await asyncio.sleep(self._reorder_buffer_seconds)

            async with self._lock:
                subscriber_queues = list(self._subscribers.get(room_id, []))
                logger.info(
                    json.dumps(
                        {
                            "event": "alarm_bus_dispatching",
                            "room_id": room_id,
                            "alarm_count": len(alarms_to_publish),
                            "subscriber_count": len(subscriber_queues),
                            "reorder_buffer_ms": int(self._reorder_buffer_seconds * 1000),
                        }
                    )
                )

            # Observe feed latency at dispatch time (server ingestion -> alarm surfaced to the
            # feed), independent of whether any SSE client is connected. Sampling only inside the
            # SSE generator meant /metrics reported alarm_feed_latency_ms_p95 = 0 when no one was
            # subscribed, which reads as "passing" while actually being unmeasured.
            dispatch_now = datetime.now(timezone.utc)
            for alarm in alarms_to_publish:
                observe_alarm_feed_latency_ms((dispatch_now - alarm.received_at).total_seconds() * 1000.0)

            for alarm in alarms_to_publish:
                for queue in list(subscriber_queues):
                    try:
                        queue.put_nowait(alarm)
                        logger.info(
                            json.dumps(
                                {
                                    "event": "alarm_bus_delivered",
                                    "room_id": room_id,
                                    "device_id": alarm.device_id,
                                    "ts": alarm.ts.isoformat(),
                                    "received_at": alarm.received_at.isoformat(),
                                    "fall_warning_id": alarm.fall_warning_id,
                                }
                            )
                        )
                    except asyncio.QueueFull:
                        logger.warning(
                            json.dumps(
                                {
                                    "event": "alarm_bus_queue_full",
                                    "room_id": room_id,
                                    "device_id": alarm.device_id,
                                    "ts": alarm.ts.isoformat(),
                                    "received_at": alarm.received_at.isoformat(),
                                }
                            )
                        )
                        await self._evict_saturated_subscriber(room_id, queue)
                        subscriber_queues.remove(queue)
        finally:
            # Pop-then-recreate happens atomically under the lock, so publish()'s own
            # locked "create only if no task or task.done()" check can never observe a
            # gap and schedule a second dispatch task for this room.
            async with self._lock:
                self._inflight_batches.pop(room_id, None)
                mapped_task = self._dispatch_tasks.get(room_id)
                if mapped_task is current_task:
                    self._dispatch_tasks.pop(room_id, None)

                if self._room_buffers.get(room_id):
                    loop = None
                    try:
                        loop = asyncio.get_running_loop()
                    except RuntimeError:
                        loop = None
                    if loop is not None:
                        self._dispatch_tasks[room_id] = loop.create_task(self._dispatch_room(room_id))

    async def stream(self, room_id: str) -> AsyncIterator[AlarmEvent]:
        queue = await self.subscribe(room_id)
        try:
            while True:
                alarm = await queue.get()
                yield alarm
                if is_subscriber_disconnected(queue):
                    break
        finally:
            await self.unsubscribe(room_id, queue)
