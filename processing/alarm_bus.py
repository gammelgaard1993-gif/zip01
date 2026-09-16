from __future__ import annotations

import asyncio
import json
import logging
import threading
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict, Optional

from config import ALARM_REORDER_BUFFER_MS, SSE_SUBSCRIBER_QUEUE_MAX_SIZE
from core.metrics import (
    increment_counter,
    observe_alarm_feed_latency_ms,
    observe_alarm_path_stage_latency_ms,
)
from models import AlarmEvent

logger = logging.getLogger(__name__)

# Cross-instance fan-out channel prefix (see AlarmBus's redis pub/sub bridge below). One channel
# per room keeps psubscribe("alarms:*") simple while still letting a listener filter by room from
# the channel name alone if it ever needs to.
_REDIS_ALARM_CHANNEL_PREFIX = "alarms:"


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
    """Per-room reorder-buffered alarm fan-out, with an optional cross-instance bridge.

    Single-instance behavior (redis_client=None, the default -- used by every existing unit test)
    is unchanged: a dispatched batch is delivered directly to this process's local subscriber
    queues, synchronously, exactly as before this bridge existed. When a real (shared)
    redis_client is supplied, a dispatched batch is delivered locally the same synchronous way
    AND published (tagged with this instance's id) to a Redis channel so other instances' bridges
    can relay it to their own local subscribers. The listener explicitly ignores messages tagged
    with its own instance id -- this instance already delivered that batch directly, so
    re-delivering it from the bridge would double-fire it to local subscribers. Keeping local
    delivery synchronous (instead of routing it through the same round trip as remote delivery)
    preserves the original atomicity between "batch dispatched" and "in-flight marker cleared"
    that subscribe()'s replay-of-in-flight-batch logic depends on to avoid missing or
    double-delivering to a subscriber that joins mid-dispatch.
    """

    def __init__(self, redis_client: Optional[Any] = None) -> None:
        self._subscribers: Dict[str, list[asyncio.Queue[AlarmEvent]]] = {}
        self._room_buffers: Dict[str, list[AlarmEvent]] = {}
        self._inflight_batches: Dict[str, list[AlarmEvent]] = {}
        self._dispatch_tasks: Dict[str, asyncio.Task[None]] = {}
        self._lock = asyncio.Lock()
        self._reorder_buffer_seconds = ALARM_REORDER_BUFFER_MS / 1000.0

        self._redis_client = redis_client
        self._instance_id = uuid.uuid4().hex
        self._bridge_loop: asyncio.AbstractEventLoop | None = None
        self._bridge_thread: threading.Thread | None = None
        self._bridge_stop_event = threading.Event()
        self._bridge_ready = threading.Event()

    async def start(self) -> None:
        """Start the cross-instance pub/sub bridge. No-op if no redis_client was supplied.

        Blocks until the background listener's psubscribe has actually been acknowledged, so a
        caller can never start publishing (e.g. worker_pool.start()) before this instance is
        guaranteed to receive other instances' alarms -- PUBLISH silently drops messages sent to
        a channel with zero current subscribers, so an unbounded "not subscribed yet" window
        would risk silently losing a batch published by another instance during this instance's
        own boot.
        """
        if self._redis_client is None or self._bridge_thread is not None:
            return
        self._bridge_loop = asyncio.get_running_loop()
        self._bridge_stop_event.clear()
        self._bridge_ready.clear()
        thread = threading.Thread(target=self._pubsub_listen_loop, name="alarm-bus-pubsub", daemon=True)
        self._bridge_thread = thread
        thread.start()
        ready = await asyncio.get_running_loop().run_in_executor(None, self._bridge_ready.wait, 5.0)
        if not ready:
            logger.warning("alarm bus pubsub bridge did not confirm subscription within timeout")

    async def stop(self, timeout: float = 5.0) -> None:
        """Stop the cross-instance pub/sub bridge. No-op if it was never started."""
        thread = self._bridge_thread
        if thread is None:
            return
        self._bridge_stop_event.set()
        await asyncio.get_running_loop().run_in_executor(None, thread.join, timeout)
        if thread.is_alive():
            logger.warning("alarm bus pubsub bridge did not stop within timeout")
            return
        self._bridge_thread = None

    async def publish(self, alarm: AlarmEvent) -> None:
        async with self._lock:
            room_buffer = self._room_buffers.setdefault(alarm.room_id, [])
            room_buffer.append(alarm)

            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
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
            # Replay alarms already claimed as in-flight (_dispatch_room has taken its atomic
            # subscriber snapshot for this batch already, so this queue -- having just been
            # added above -- is guaranteed NOT to be in that snapshot and must get the batch via
            # this replay instead) so a subscriber arriving mid-dispatch doesn't miss it.
            #
            # Deliberately NOT replaying from _room_buffers here: any alarm sitting in
            # _room_buffers still has a pending (or about-to-be-scheduled) dispatch task for
            # this room (publish() guarantees one exists whenever room_buffer is non-empty), and
            # that task's own future atomic snapshot will include the queue just appended above,
            # since it happens after this lock is released. Replaying those alarms here as well
            # would double-deliver them: once now, once when that pending dispatch runs.
            for alarm in self._inflight_batches.get(room_id, []):
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

    async def _broadcast(
        self,
        room_id: str,
        alarms: list[AlarmEvent],
        subscriber_queues: list[asyncio.Queue[AlarmEvent]],
    ) -> None:
        """Deliver an already-reordered batch locally, and fan it out to other instances.

        Local delivery always happens synchronously, right here -- identical timing to the
        pre-bridge behavior, whether or not a redis_client is configured -- so subscribe()'s
        replay-of-in-flight-batch handoff (see _dispatch_room's finally block) is unaffected by
        cross-instance fan-out latency. When a redis_client is configured, the batch is also
        PUBLISHed (tagged with this instance's id) purely for OTHER instances' listeners to pick
        up; this instance's own listener ignores messages carrying its own instance id (see
        _pubsub_listen_loop) since it has already delivered the batch directly, right here.

        `subscriber_queues` must be the snapshot _dispatch_room took atomically with marking this
        batch in-flight (before the reorder-delay sleep), NOT a fresh live snapshot taken here --
        see _deliver_local's docstring for why that matters.
        """
        await self._deliver_local(room_id, alarms, subscriber_queues)

        if self._redis_client is None:
            return
        payload = json.dumps(
            {
                "origin_instance_id": self._instance_id,
                "room_id": room_id,
                "alarms": [
                    {
                        "device_id": alarm.device_id,
                        "room_id": alarm.room_id,
                        "ts": alarm.ts.isoformat(),
                        "confidence": alarm.confidence,
                        "received_at": alarm.received_at.isoformat(),
                        "fall_warning_id": alarm.fall_warning_id,
                    }
                    for alarm in alarms
                ],
            }
        )
        channel = f"{_REDIS_ALARM_CHANNEL_PREFIX}{room_id}"
        try:
            await asyncio.get_running_loop().run_in_executor(None, self._redis_client.publish, channel, payload)
        except Exception:
            # Local subscribers already got their delivery above; a PUBLISH failure here only
            # costs cross-instance fan-out (other instances' subscribers miss this batch live --
            # recoverable via their own SSE `since=` reconnect + durable SQLite replay), not this
            # instance's own delivery.
            logger.warning(
                json.dumps({"event": "alarm_bus_publish_failed", "room_id": room_id}), exc_info=True
            )

    async def _deliver_local(
        self,
        room_id: str,
        alarms: list[AlarmEvent],
        subscriber_queues: Optional[list[asyncio.Queue[AlarmEvent]]] = None,
    ) -> None:
        """Deliver an already-ordered batch to this process's local subscribers only.

        `subscriber_queues`, when provided, MUST be a snapshot taken atomically (under
        self._lock) with marking the batch in-flight in _dispatch_room, i.e. before
        subscribe() could observe this batch as in-flight and replay it into a newly-joined
        queue. Fetching a *fresh* snapshot here (after the reorder-delay sleep) would double-
        deliver to any queue that subscribed during that sleep: subscribe() already replayed the
        batch into it via _inflight_batches, and a fresh snapshot here would include that same
        queue and deliver the batch to it again. Remote (bridge-relayed) batches have no local
        in-flight/replay state on this instance to race against, so they pass `None` and get a
        live snapshot instead.
        """
        if subscriber_queues is None:
            async with self._lock:
                subscriber_queues = list(self._subscribers.get(room_id, []))

        # Observe feed latency at delivery time (server ingestion -> alarm surfaced to the feed),
        # independent of whether any SSE client is connected. Sampling only inside the SSE
        # generator meant /metrics reported alarm_feed_latency_ms_p95 = 0 when no one was
        # subscribed, which reads as "passing" while actually being unmeasured.
        dispatch_now = datetime.now(timezone.utc)
        for alarm in alarms:
            observe_alarm_feed_latency_ms((dispatch_now - alarm.received_at).total_seconds() * 1000.0)
            observe_alarm_path_stage_latency_ms(
                "alarm_bus_dispatch",
                (dispatch_now - alarm.received_at).total_seconds() * 1000.0,
            )

        for alarm in alarms:
            for queue in list(subscriber_queues):
                try:
                    queue.put_nowait(alarm)
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(
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

    def _pubsub_listen_loop(self) -> None:
        """Background thread: relay redis-published alarm batches to local subscribers.

        Runs in its own thread (redis-py's pubsub client is blocking/sync) and hands each
        received batch back onto the event loop via run_coroutine_threadsafe. Polls with a
        timeout instead of a blocking listen() so _bridge_stop_event is checked periodically
        without needing to close the connection from another thread.
        """
        pubsub = self._redis_client.pubsub(ignore_subscribe_messages=True)
        try:
            pubsub.psubscribe(f"{_REDIS_ALARM_CHANNEL_PREFIX}*")
            self._bridge_ready.set()
            while not self._bridge_stop_event.is_set():
                try:
                    message = pubsub.get_message(timeout=1.0)
                except Exception:
                    logger.warning("alarm bus pubsub get_message failed", exc_info=True)
                    continue
                if message is None or message.get("type") != "pmessage":
                    continue
                try:
                    payload = json.loads(message["data"])
                    if payload.get("origin_instance_id") == self._instance_id:
                        # This instance already delivered the batch directly in _broadcast --
                        # redelivering it here would double-fire it to local subscribers.
                        continue
                    room_id = payload["room_id"]
                    alarms = [
                        AlarmEvent(
                            device_id=item["device_id"],
                            room_id=item["room_id"],
                            ts=datetime.fromisoformat(item["ts"]),
                            confidence=item["confidence"],
                            received_at=datetime.fromisoformat(item["received_at"]),
                            fall_warning_id=item.get("fall_warning_id"),
                        )
                        for item in payload["alarms"]
                    ]
                except (KeyError, ValueError, TypeError):
                    logger.warning("alarm bus pubsub message malformed, dropping", exc_info=True)
                    continue
                loop = self._bridge_loop
                if loop is not None:
                    asyncio.run_coroutine_threadsafe(self._deliver_local(room_id, alarms), loop)
        finally:
            try:
                pubsub.close()
            except Exception:
                pass

    async def _dispatch_room(self, room_id: str) -> None:
        current_task = asyncio.current_task()
        try:
            async with self._lock:
                room_buffer = self._room_buffers.get(room_id, [])
                if not room_buffer:
                    return
                alarms_to_publish = sorted(room_buffer, key=lambda item: item.ts)
                self._room_buffers[room_id] = []
                self._inflight_batches[room_id] = alarms_to_publish
                should_delay = len(alarms_to_publish) > 1
                # Snapshot subscribers atomically with marking the batch in-flight above, NOT
                # after the reorder-delay sleep below. subscribe() replays a batch it finds in
                # _inflight_batches into a newly-joined queue; if this snapshot were instead
                # taken fresh after the sleep, any queue that subscribed mid-sleep would receive
                # the batch twice -- once via that replay, once via this (now stale) snapshot
                # delivered through _broadcast/_deliver_local. Taking it here means a queue that
                # subscribes after this point is, by construction, excluded from this snapshot
                # and only ever gets the batch via subscribe()'s replay.
                subscriber_queues = list(self._subscribers.get(room_id, []))

            if should_delay and self._reorder_buffer_seconds > 0:
                await asyncio.sleep(self._reorder_buffer_seconds)

            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
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

            await self._broadcast(room_id, alarms_to_publish, subscriber_queues)
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
