from __future__ import annotations

import asyncio
import hashlib
import logging
from concurrent.futures import Executor
from sqlite3 import Connection
from typing import TYPE_CHECKING, Dict, List

from config import DEVICE_REORDER_BUFFER_MS, ROUTER_TASK_COUNT, WORKER_COUNT, WORKER_NORMAL_QUEUE_MAX_SIZE
from ingestion.queue import PriorityEventQueue
from models import ValidatedEvent
from processing.handlers.base import EventHandler
from processing.handlers.heartbeat import HeartbeatHandler
from processing.handlers.generic import GenericEventHandler
from processing.handlers.presence import PresenceHandler
from processing.handlers.fall_warn import FallWarnHandler
from processing.alarm_bus import AlarmBus
from redis import Redis

if TYPE_CHECKING:
    from core.db_writer import BatchedSQLiteWriter

logger = logging.getLogger(__name__)


class WorkerPool:
    def __init__(
        self,
        event_queue: PriorityEventQueue,
        alarm_bus: AlarmBus,
        db_connection: Connection,
        redis_client: Redis,
        redis_executor: Executor | None = None,
        sqlite_writer: "BatchedSQLiteWriter | None" = None,
    ) -> None:
        self.event_queue: PriorityEventQueue = event_queue
        self.alarm_bus: AlarmBus = alarm_bus
        self.db_connection: Connection = db_connection
        self.redis_client: Redis = redis_client
        # Optional Phase 6 (#13) offload dependencies: a shared thread pool for synchronous
        # redis-py calls and a dedicated single-writer thread for SQLite writes. Both default to
        # None, in which case handlers run exactly as before (synchronous, on the worker task).
        self.redis_executor = redis_executor
        self.sqlite_writer = sqlite_writer
        # Each worker owns a bounded two-lane priority queue: HIGH (fall_warn) is drained before
        # NORMAL, so downstream routing preserves priority and can never grow an unbounded FIFO.
        self.worker_queues: List[PriorityEventQueue] = [
            PriorityEventQueue(WORKER_NORMAL_QUEUE_MAX_SIZE) for _ in range(WORKER_COUNT)
        ]
        self.workers: List[asyncio.Task[None]] = []
        self.router_tasks: List[asyncio.Task[None]] = []
        self.flush_tasks: set[asyncio.Task[None]] = set()
        self.reorder_buffer_seconds = DEVICE_REORDER_BUFFER_MS / 1000.0
        # In-flight received_at multiset: an event is registered at admission
        # and deregistered once its handler runs. The oldest entry is the snapshot replay cutoff,
        # so an event still queued/buffered during a snapshot capture is never excluded from
        # recovery replay.
        self._inflight_received_at: dict[str, int] = {}

    def mark_inflight(self, received_at_iso: str) -> None:
        self._inflight_received_at[received_at_iso] = self._inflight_received_at.get(received_at_iso, 0) + 1

    def unmark_inflight(self, received_at_iso: str) -> None:
        count = self._inflight_received_at.get(received_at_iso)
        if count is None:
            return
        if count <= 1:
            self._inflight_received_at.pop(received_at_iso, None)
        else:
            self._inflight_received_at[received_at_iso] = count - 1

    def oldest_inflight_received_at(self) -> str | None:
        # UTC isoformat strings sort lexically in chronological order, so min() is the oldest.
        if not self._inflight_received_at:
            return None
        return min(self._inflight_received_at)

    async def start(self) -> None:
        self.workers = [asyncio.create_task(self._worker_loop(index, queue)) for index, queue in enumerate(self.worker_queues)]
        self.router_tasks = [asyncio.create_task(self._router_loop()) for _ in range(ROUTER_TASK_COUNT)]

    async def stop(self, drain: bool = True, drain_timeout: float = 2.0) -> None:
        # Graceful drain: enqueued events are already durable (persisted at admission), so draining
        # here just applies their remaining hot-state handlers now instead of on the next restart.
        # Bounded by drain_timeout so shutdown can never hang; anything still unprocessed at the
        # deadline is simply replayed from the durable log on restart.
        if drain:
            loop = asyncio.get_running_loop()
            deadline = loop.time() + drain_timeout
            # 1) Let the router move everything from the global queue into the worker lanes.
            while loop.time() < deadline:
                if self.event_queue.qsize_high() == 0 and self.event_queue.qsize_normal() == 0:
                    break
                await asyncio.sleep(0.02)
            # 2) Let the workers drain their lanes and finish any buffered per-device flushes.
            while loop.time() < deadline:
                lanes_pending = any(
                    q.qsize_high() or q.qsize_normal() for q in self.worker_queues
                )
                if not lanes_pending and not self.flush_tasks:
                    break
                await asyncio.sleep(0.02)
        for router_task in self.router_tasks:
            router_task.cancel()
        for worker in self.workers:
            worker.cancel()
        for flush_task in list(self.flush_tasks):
            flush_task.cancel()
        await asyncio.gather(*(t for t in self.router_tasks if not t.done()), return_exceptions=True)
        await asyncio.gather(*(t for t in self.workers if not t.done()), return_exceptions=True)
        await asyncio.gather(*(t for t in self.flush_tasks if not t.done()), return_exceptions=True)

    async def _router_loop(self) -> None:
        # One of ROUTER_TASK_COUNT peer consumers of the shared priority queue, so one congested
        # worker queue can't head-of-line-block routing to the rest. Concurrent put()s into the
        # same worker queue need no ordering coordination: per-device processing order is decided
        # by each event's own ts field (re-sorted in the worker pool), not by put() arrival order
        # -- the only exception is a tie-break among equal-ts events, which becomes nondeterministic
        # across router tasks instead of FIFO; this is permitted since ts is the sole ordering key.
        while True:
            event = await self.event_queue.get()
            index = self._worker_index(event.device_id)
            await self.worker_queues[index].put(event)

    def _worker_index(self, device_id: str) -> int:
        # Consistent hash on device_id: every event for a device always lands on the same worker,
        # which is what lets a single worker own that device's reorder buffer and preserve
        # per-device ordering without cross-worker coordination.
        digest = hashlib.sha256(device_id.encode("utf-8")).digest()
        return digest[0] % len(self.worker_queues)

    async def _worker_loop(self, index: int, queue: PriorityEventQueue) -> None:
        device_buffers: Dict[str, List[ValidatedEvent]] = {}
        flush_tasks: Dict[str, asyncio.Task[None]] = {}
        handlers: dict[str, EventHandler] = {
            "heartbeat": HeartbeatHandler(self.redis_client, executor=self.redis_executor),
            "presence": PresenceHandler(self.redis_client, executor=self.redis_executor),
            "fall_warn": FallWarnHandler(
                self.redis_client,
                self.db_connection,
                self.alarm_bus,
                writer=self.sqlite_writer,
                executor=self.redis_executor,
            ),
            "motion": GenericEventHandler(self.db_connection),
            "sleep_state": GenericEventHandler(self.db_connection),
            "net_status": GenericEventHandler(self.db_connection),
        }

        while True:
            event = await queue.get()
            # Buffer per device and keep it ts-ordered so a slightly-late event that arrives within
            # the reorder window is handled in timestamp order rather than arrival order.
            device_events = device_buffers.setdefault(event.device_id, [])
            device_events.append(event)
            device_events.sort(key=lambda item: item.ts)

            # Arm a single in-flight flush per device. A flush already scheduled will pick up this
            # event when it fires, so we only schedule a new one when none is pending.
            flush_task = flush_tasks.get(event.device_id)
            if flush_task is None or flush_task.done():
                task = asyncio.create_task(
                    self._flush_device_buffer(
                        worker_index=index,
                        device_id=event.device_id,
                        device_buffers=device_buffers,
                        handlers=handlers,
                        flush_tasks=flush_tasks,
                    )
                )
                self.flush_tasks.add(task)
                task.add_done_callback(self.flush_tasks.discard)
                flush_tasks[event.device_id] = task

    async def _flush_device_buffer(
        self,
        worker_index: int,
        device_id: str,
        device_buffers: Dict[str, List[ValidatedEvent]],
        handlers: dict[str, EventHandler],
        flush_tasks: Dict[str, asyncio.Task[None]],
    ) -> None:
        try:
            # Hold events for the reorder window before draining so out-of-order arrivals within
            # DEVICE_REORDER_BUFFER_MS get sorted into place first.
            await asyncio.sleep(self.reorder_buffer_seconds)

            device_events = device_buffers.get(device_id)
            if device_events is None:
                return

            while device_events:
                # Re-sort on each iteration: an event may have been appended during the await above.
                device_events.sort(key=lambda item: item.ts)
                next_event = device_events.pop(0)
                # Durability already happened at admission: the /events route persists the event
                # to SQLite before the worker runs. The worker owns only the derived hot state, so
                # a handler failure here is isolated and never risks the durable record.
                handler = handlers.get(next_event.type, handlers["motion"])
                try:
                    await handler.handle(next_event)
                except Exception:
                    # Failure isolation: a single handler error is logged and skipped so it can't
                    # kill the worker or stall this device's buffer. The event is already durable
                    # (persisted at admission), so only recoverable hot state is affected.
                    logger.exception(
                        "worker handler failure",
                        extra={
                            "worker_index": worker_index,
                            "device_id": next_event.device_id,
                            "event_type": next_event.type,
                            "event_ts": next_event.ts.isoformat(),
                        },
                    )
                finally:
                    # Deregister from the in-flight watermark once processed (applied or failed);
                    # the durable record already exists, so this only relaxes the snapshot cutoff.
                    self.unmark_inflight(next_event.received_at.isoformat())

            device_buffers.pop(device_id, None)
        finally:
            flush_tasks.pop(device_id, None)
