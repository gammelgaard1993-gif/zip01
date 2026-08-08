from __future__ import annotations

import asyncio

from config import HIGH_QUEUE_MAX_SIZE
from models import Priority, ValidatedEvent

class PriorityEventQueue:
    def __init__(self, normal_max_size: int) -> None:
        self.high_queue: asyncio.Queue[ValidatedEvent] = asyncio.Queue(maxsize=HIGH_QUEUE_MAX_SIZE)
        self.normal_queue: asyncio.Queue[ValidatedEvent] = asyncio.Queue(maxsize=normal_max_size)
        self._normal_max_size = normal_max_size
        self._item_available = asyncio.Event()

    def qsize_high(self) -> int:
        return self.high_queue.qsize()

    def qsize_normal(self) -> int:
        return self.normal_queue.qsize()

    def normal_max_size(self) -> int:
        return self._normal_max_size

    def normal_is_full(self) -> bool:
        return self.normal_queue.full()

    async def put(self, event: ValidatedEvent) -> None:
        if event.priority == Priority.HIGH:
            await self.high_queue.put(event)
        else:
            await self.normal_queue.put(event)
        self._item_available.set()

    def put_nowait(self, event: ValidatedEvent) -> None:
        if event.priority == Priority.HIGH:
            self.high_queue.put_nowait(event)
        else:
            self.normal_queue.put_nowait(event)
        self._item_available.set()

    async def _wait_until_available(self) -> None:
        await self._item_available.wait()

    async def get(self) -> ValidatedEvent:
        while True:
            try:
                return self.high_queue.get_nowait()
            except asyncio.QueueEmpty:
                pass

            try:
                return self.normal_queue.get_nowait()
            except asyncio.QueueEmpty:
                pass

            self._item_available.clear()
            if not self.high_queue.empty() or not self.normal_queue.empty():
                continue
            await self._wait_until_available()

    def empty(self) -> bool:
        return self.high_queue.empty() and self.normal_queue.empty()

    def qsize(self) -> int:
        return self.qsize_high() + self.qsize_normal()

    def clear(self) -> None:
        while not self.high_queue.empty():
            self.high_queue.get_nowait()
        while not self.normal_queue.empty():
            self.normal_queue.get_nowait()
