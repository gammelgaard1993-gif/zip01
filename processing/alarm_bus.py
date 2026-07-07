from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import AsyncIterator, Dict

from config import ALARM_REORDER_BUFFER_MS
from core.metrics import observe_alarm_feed_latency_ms
from models import AlarmEvent


class AlarmBus:
    def __init__(self) -> None:
        self._subscribers: Dict[str, list[asyncio.Queue[AlarmEvent]]] = {}
        self._room_buffers: Dict[str, list[AlarmEvent]] = {}
        self._dispatch_tasks: Dict[str, asyncio.Task[None]] = {}
        self._lock = asyncio.Lock()
        self._reorder_buffer_seconds = ALARM_REORDER_BUFFER_MS / 1000.0

    async def publish(self, alarm: AlarmEvent) -> None:
        async with self._lock:
            room_buffer = self._room_buffers.setdefault(alarm.room_id, [])
            room_buffer.append(alarm)
            room_buffer.sort(key=lambda item: item.ts)

            dispatch_task = self._dispatch_tasks.get(alarm.room_id)
            if dispatch_task is None or dispatch_task.done():
                self._dispatch_tasks[alarm.room_id] = asyncio.create_task(self._dispatch_room(alarm.room_id))

    async def subscribe(self, room_id: str) -> asyncio.Queue[AlarmEvent]:
        queue: asyncio.Queue[AlarmEvent] = asyncio.Queue()
        async with self._lock:
            self._subscribers.setdefault(room_id, []).append(queue)
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

    async def _dispatch_room(self, room_id: str) -> None:
        current_task = asyncio.current_task()
        try:
            await asyncio.sleep(self._reorder_buffer_seconds)
            async with self._lock:
                room_buffer = self._room_buffers.get(room_id, [])
                if not room_buffer:
                    return
                alarms_to_publish = list(room_buffer)
                self._room_buffers[room_id] = []
                subscriber_queues = list(self._subscribers.get(room_id, []))

            # Observe feed latency at dispatch time (server ingestion -> alarm surfaced to the
            # feed), independent of whether any SSE client is connected. Sampling only inside the
            # SSE generator meant /metrics reported alarm_feed_latency_ms_p95 = 0 when no one was
            # subscribed, which reads as "passing" while actually being unmeasured.
            dispatch_now = datetime.now(timezone.utc)
            for alarm in alarms_to_publish:
                observe_alarm_feed_latency_ms((dispatch_now - alarm.received_at).total_seconds() * 1000.0)

            for alarm in alarms_to_publish:
                for queue in subscriber_queues:
                    await queue.put(alarm)
        finally:
            async with self._lock:
                mapped_task = self._dispatch_tasks.get(room_id)
                if mapped_task is current_task:
                    self._dispatch_tasks.pop(room_id, None)

                if self._room_buffers.get(room_id):
                    self._dispatch_tasks[room_id] = asyncio.create_task(self._dispatch_room(room_id))

    async def stream(self, room_id: str) -> AsyncIterator[AlarmEvent]:
        queue = await self.subscribe(room_id)
        try:
            while True:
                alarm = await queue.get()
                yield alarm
        finally:
            await self.unsubscribe(room_id, queue)
