from __future__ import annotations

import asyncio
import hashlib
import logging
from concurrent.futures import ThreadPoolExecutor
from sqlite3 import Connection
from typing import Dict, List

from config import DEVICE_REORDER_BUFFER_MS, WORKER_COUNT, USE_EXECUTOR_IO
from core.event_log import persist_validated_event
from ingestion.queue import PriorityEventQueue
from models import ValidatedEvent
from processing.handlers.base import EventHandler
from processing.handlers.heartbeat import HeartbeatHandler
from processing.handlers.generic import GenericEventHandler
from processing.handlers.presence import PresenceHandler
from processing.handlers.fall_warn import FallWarnHandler
from processing.alarm_bus import AlarmBus
from redis import Redis

logger = logging.getLogger(__name__)


class WorkerPool:
    def __init__(
        self,
        event_queue: PriorityEventQueue,
        alarm_bus: AlarmBus,
        db_connection: Connection,
        redis_client: Redis,
    ) -> None:
        self.event_queue: PriorityEventQueue = event_queue
        self.alarm_bus: AlarmBus = alarm_bus
        self.db_connection: Connection = db_connection
        self.redis_client: Redis = redis_client
        self.worker_queues: List[asyncio.Queue[ValidatedEvent]] = [asyncio.Queue() for _ in range(WORKER_COUNT)]
        self.workers: List[asyncio.Task[None]] = []
        self.router_task: asyncio.Task[None] | None = None
        self.flush_tasks: set[asyncio.Task[None]] = set()
        self.reorder_buffer_seconds = DEVICE_REORDER_BUFFER_MS / 1000.0
        # Experimental I/O offload (config.USE_EXECUTOR_IO). A single-worker thread pool serialises
        # the blocking SQLite writes off the event loop. max_workers=1 is deliberate: the sqlite3
        # connection is not safe for concurrent use from multiple threads, so funnelling every
        # write through one thread keeps them ordered while still freeing the loop during the I/O.
        self._io_executor: ThreadPoolExecutor | None = (
            ThreadPoolExecutor(max_workers=1, thread_name_prefix="io-writer") if USE_EXECUTOR_IO else None
        )

    async def start(self) -> None:
        self.workers = [asyncio.create_task(self._worker_loop(index, queue)) for index, queue in enumerate(self.worker_queues)]
        self.router_task = asyncio.create_task(self._router_loop())

    async def stop(self) -> None:
        if self.router_task:
            self.router_task.cancel()
        for worker in self.workers:
            worker.cancel()
        for flush_task in self.flush_tasks:
            flush_task.cancel()
        await asyncio.gather(*(t for t in self.workers if not t.done()), return_exceptions=True)
        await asyncio.gather(*(t for t in self.flush_tasks if not t.done()), return_exceptions=True)
        # Tear down the offload thread (if enabled) so the process can exit cleanly.
        if self._io_executor is not None:
            self._io_executor.shutdown(wait=False)

    async def _router_loop(self) -> None:
        # Single consumer of the priority queue. It only routes (never handles) so the HIGH lane
        # can drain as fast as events arrive; all blocking work happens on the worker tasks.
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

    async def _worker_loop(self, index: int, queue: asyncio.Queue[ValidatedEvent]) -> None:
        device_buffers: Dict[str, List[ValidatedEvent]] = {}
        flush_tasks: Dict[str, asyncio.Task[None]] = {}
        handlers: dict[str, EventHandler] = {
            "heartbeat": HeartbeatHandler(self.redis_client),
            "presence": PresenceHandler(self.redis_client),
            "fall_warn": FallWarnHandler(self.redis_client, self.db_connection, self.alarm_bus),
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
                # Persist-before-handle: the durable SQLite record is the source of truth for
                # recovery/replay, so it must land even if the in-memory handler below fails.
                if self._io_executor is not None:
                    # Offload the blocking INSERT+commit to the writer thread. It is still awaited
                    # before the handler runs, so the persist-before-handle durability rule holds;
                    # the only change is the event loop is free to serve other devices during the
                    # disk write instead of blocking on it.
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(
                        self._io_executor, persist_validated_event, self.db_connection, next_event
                    )
                else:
                    persist_validated_event(self.db_connection, next_event)
                handler = handlers.get(next_event.type, handlers["motion"])
                try:
                    await handler.handle(next_event)
                except Exception:
                    # Failure isolation: a single handler error is logged and skipped so it can't
                    # kill the worker or stall this device's buffer. The event is already durable.
                    logger.exception(
                        "worker handler failure",
                        extra={
                            "worker_index": worker_index,
                            "device_id": next_event.device_id,
                            "event_type": next_event.type,
                            "event_ts": next_event.ts.isoformat(),
                        },
                    )

            device_buffers.pop(device_id, None)
        finally:
            flush_tasks.pop(device_id, None)
